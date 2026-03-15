package store

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/progress"
	"github.com/urvisavla/stellar-events/internal/query"
)

// =============================================================================
// Configuration Types
// =============================================================================

// Unique index type prefixes within CFUnique
const (
	UniqueTypeContract byte = 0x00 // Contract ID
	UniqueTypeTopic0   byte = 0x01 // Topic 0
	UniqueTypeTopic1   byte = 0x02 // Topic 1
	UniqueTypeTopic2   byte = 0x03 // Topic 2
	UniqueTypeTopic3   byte = 0x04 // Topic 3
)

// StoreOptions configures what indexes to update when storing events.
type StoreOptions struct {
	UniqueIndexes bool // Maintain unique value indexes with counts
}

const (
	// SegmentSize is the number of ledgers per index segment.
	// At ~5 seconds per ledger, 10,000 ledgers ≈ 14 hours.
	SegmentSize uint32 = 10_000
)

// =============================================================================
// Segment Functions (shared by bitmap and segment-index indexes)
// =============================================================================

// SegmentID calculates the segment ID for a given ledger sequence.
func SegmentID(ledgerSeq uint32) uint32 {
	return ledgerSeq / SegmentSize
}

// GetSegmentsForRange returns all segment IDs that cover the given ledger range.
func GetSegmentsForRange(startLedger, endLedger uint32) []uint32 {
	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	segments := make([]uint32, 0, endSegment-startSegment+1)
	for s := startSegment; s <= endSegment; s++ {
		segments = append(segments, s)
	}
	return segments
}

// =============================================================================
// Storage Backend Interfaces
// =============================================================================

// BitmapLoader loads roaring bitmaps for query terms from a storage backend.
// Implementations: RocksDBReader (loads from RocksDB CFs), SegmentReader (loads from .hash/.pack files).
type BitmapLoader interface {
	// LoadTermBitmap loads a bitmap for a specific term in a specific segment,
	// trimmed to the given ledger range. Returns nil bitmap if the term doesn't exist.
	// fieldIndex: 0=contracts, 1-4=topic positions 0-3.
	LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [16]byte,
		startLedger, endLedger uint32) (*roaring.Bitmap, BitmapLoadStats, error)
}

// BitmapLoadStats holds I/O stats from loading a single bitmap term.
type BitmapLoadStats struct {
	BytesRead  int64
	ReadTime   time.Duration
	DecodeTime time.Duration
}

// EventFetcher provides event blob retrieval from a storage backend.
// Implementations: RocksDBEventFetcher (RocksDB cfEvents), SegmentDataEventFetcher (eventstore files).
type EventFetcher interface {
	NewEventIterator() EventIterator
}

// EventIterator seeks and reads event blobs by key.
// Callers must copy Key()/Value() data before the next Seek()/Next() call.
type EventIterator interface {
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Close()
}

// LedgerOffsetsLoader loads segment ledger offsetss from a storage backend.
// Implementations: RocksDBIndexStore (from RocksDB), SegmentReader (from mmap'd flat files).
type LedgerOffsetsLoader interface {
	LoadSegmentLedgerOffsets(segmentID uint32) (*SegmentLedgerOffsets, error)
}

// QueryBackend provides a unified query interface over a storage backend.
// Implementations: RocksDBReader (RocksDB), SegmentReader (flat files).
type QueryBackend interface {
	BitmapLoader
	FetchByIDs(perSegment map[uint32]*roaring.Bitmap, limit int, result *QueryResult) ([]*query.Event, error)
	FetchByRange(startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error)
	Close() error
}

// EventKV is a storage-agnostic key-value pair for event data.
type EventKV struct {
	Key   []byte
	Value []byte
}

// uniqueKey builds a key for the CFUnique column family: [type_prefix][value].
func uniqueKey(uniqueType byte, value []byte) []byte {
	key := make([]byte, 1+len(value))
	key[0] = uniqueType
	copy(key[1:], value)
	return key
}

// QueryResult holds detailed results from a bitmap32 event-level query.
// Uses sequential event IDs + FromBuffer for near-zero-cost decode.
type QueryResult struct {
	// Ledger range
	LedgerRange      uint32 // endLedger - startLedger + 1
	MatchingLocalIDs int    // Local IDs matching index query

	// Index stats
	SegmentsTouched int   // Number of segments touched
	SegmentsScanned int   // Number of segments scanned
	IndexBytesRead  int64 // Bytes read from bitmap index

	// Event fetch stats
	EventsScanned  int   // Events scanned from storage
	EventsReturned int   // Events returned after filtering
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	IndexLookupTime    time.Duration // Time querying bitmap index
	IndexReadTime      time.Duration // Time reading bitmap segments from storage (I/O)
	IndexDecodeTime    time.Duration // Time decoding bitmap segments (CPU - near zero with FromBuffer)
	IndexIntersectTime time.Duration // Time spent on bitmap OR/AND operations
	EventFetchTime     time.Duration // Time fetching events
	GroupsDecompressed int           // Number of group blocks decompressed
	DecodeTime         time.Duration // Time decoding events
	FilterTime         time.Duration // Time filtering events
	TotalTime          time.Duration // Total query time
}

// SegmentLedgerOffsets maps dense sequential local IDs to (ledger, eventSeq) pairs
// within a segment. Stored as a fixed-size cumulative array of event counts.
//
// Entry i = total events in ledgers at offsets 0 through i (inclusive cumulative sum).
// The array has exactly SegmentSize entries (10,000), stored as little-endian uint32s.
//
// Key format in CFDefault: [0xFF][segmentID:4] — 5 bytes
// Value: [cumulative_count:4] × SegmentSize — exactly 40,000 bytes (fixed)
type SegmentLedgerOffsets struct {
	SegmentID uint32
	Data      []byte // raw 40,000-byte cumulative array (zero-copy from RocksDB)
}

const (
	// SegmentLedgerOffsetsSize is the size of the cumulative array in bytes.
	SegmentLedgerOffsetsSize = int(SegmentSize) * 4 // 40,000 bytes

	// segmentLedgerOffsetsPrefix is the key prefix used to avoid collision with text keys in CFDefault.
	segmentLedgerOffsetsPrefix = 0xFF
)

// SegmentLedgerOffsetsKey returns the CFDefault key for a segment's ledger offsets.
// Format: [0xFF][segmentID:4] — 5 bytes
func SegmentLedgerOffsetsKey(segmentID uint32) []byte {
	key := make([]byte, 5)
	key[0] = segmentLedgerOffsetsPrefix
	binary.BigEndian.PutUint32(key[1:5], segmentID)
	return key
}

// TotalEvents returns the total number of events in this segment (last cumulative entry).
func (m *SegmentLedgerOffsets) TotalEvents() uint32 {
	if len(m.Data) < SegmentLedgerOffsetsSize {
		return 0
	}
	return m.getCumulative(int(SegmentSize) - 1)
}

// LedgerRangeToIDRange converts a ledger offset range to a dense ID range. O(1).
// startOff and endOff are ledger offsets within the segment (0-based, inclusive).
// Returns (startID, endID) where endID is inclusive. If the range has no events,
// returns (0, 0) with startID > endID being impossible, so caller should check endID >= startID.
func (m *SegmentLedgerOffsets) LedgerRangeToIDRange(startOff, endOff uint16) (uint32, uint32) {
	if len(m.Data) < SegmentLedgerOffsetsSize {
		return 0, 0
	}

	var startID uint32
	if startOff > 0 {
		startID = m.getCumulative(int(startOff) - 1)
	}

	endID := m.getCumulative(int(endOff))
	if endID == 0 {
		return 0, 0
	}
	return startID, endID - 1
}

// DenseIDToLedgerAndSeq converts a dense local ID to an absolute ledger sequence
// and event sequence within that ledger. O(log N) via binary search.
func (m *SegmentLedgerOffsets) DenseIDToLedgerAndSeq(denseID uint32) (ledger uint32, seq uint16) {
	if len(m.Data) < SegmentLedgerOffsetsSize {
		return 0, 0
	}

	segmentStart := m.SegmentID * SegmentSize
	n := int(SegmentSize)

	// Binary search for smallest i where cumulative[i] > denseID
	i := sort.Search(n, func(j int) bool {
		return m.getCumulative(j) > denseID
	})

	if i >= n {
		// denseID is out of range
		return 0, 0
	}

	ledger = segmentStart + uint32(i)
	if i == 0 {
		seq = uint16(denseID)
	} else {
		seq = uint16(denseID - m.getCumulative(i-1))
	}
	return ledger, seq
}

// getCumulative reads the cumulative count at offset i from the raw data.
func (m *SegmentLedgerOffsets) getCumulative(i int) uint32 {
	off := i * 4
	return binary.LittleEndian.Uint32(m.Data[off : off+4])
}

// EncodeSegmentLedgerOffsets builds a cumulative array from per-ledger event counts.
// ledgerEventCounts[i] = number of events at ledger offset i.
// Returns the raw 40,000-byte cumulative array.
func EncodeSegmentLedgerOffsets(segmentID uint32, ledgerEventCounts [SegmentSize]uint32) []byte {
	data := make([]byte, SegmentLedgerOffsetsSize)
	var cumulative uint32
	for i := uint32(0); i < SegmentSize; i++ {
		cumulative += ledgerEventCounts[i]
		off := i * 4
		binary.LittleEndian.PutUint32(data[off:off+4], cumulative)
	}
	return data
}

// MergeSegmentLedgerOffsetsData merges new per-ledger event counts into an existing
// cumulative array. For ledger offsets present in both old and new data, the new
// count REPLACES the old (idempotent on re-ingestion). For offsets only in the
// existing data, old counts are preserved (supports continuation after restart).
// Returns a new 40,000-byte cumulative array.
func MergeSegmentLedgerOffsetsData(existing []byte, newCounts map[uint16]uint32) []byte {
	if len(existing) < SegmentLedgerOffsetsSize {
		// No existing data — build from scratch
		var counts [SegmentSize]uint32
		for off, count := range newCounts {
			counts[off] = count
		}
		return EncodeSegmentLedgerOffsets(0, counts)
	}

	// Decode existing cumulative into per-ledger counts
	var counts [SegmentSize]uint32
	prev := uint32(0)
	for i := uint32(0); i < SegmentSize; i++ {
		off := i * 4
		cum := binary.LittleEndian.Uint32(existing[off : off+4])
		counts[i] = cum - prev
		prev = cum
	}

	// Merge: replace overlapping offsets (idempotent), add new-only offsets
	for off, count := range newCounts {
		if counts[off] > 0 {
			// Re-ingested ledger: replace to avoid double-counting
			counts[off] = count
		} else {
			// New ledger (continuation): add
			counts[off] = count
		}
	}

	// Re-encode
	return EncodeSegmentLedgerOffsets(0, counts)
}

// segmentEventCounter tracks per-ledger event counts during ingestion for a single segment.
type segmentEventCounter struct {
	eventCounts [SegmentSize]uint32 // per-ledger-offset event count
	nextDenseID uint32              // running total = next ID to assign
}

// assignDenseID assigns the next dense local ID for a ledger within this segment.
func (c *segmentEventCounter) assignDenseID(ledgerOffset uint16) uint32 {
	id := c.nextDenseID
	c.nextDenseID++
	c.eventCounts[ledgerOffset]++
	return id
}

// =============================================================================
// Mmap Support
// =============================================================================

// MmapFile wraps a memory-mapped file region for zero-copy reads.
type MmapFile struct {
	data []byte // mmap'd region (nil for empty files)
}

// OpenMmap memory-maps a file for reading. The returned MmapFile must be
// closed when no longer needed. Empty files return a valid MmapFile with nil data.
func OpenMmap(path string) (*MmapFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(fi.Size())
	if size == 0 {
		return &MmapFile{}, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	return &MmapFile{data: data}, nil
}

// Close unmaps the memory-mapped region.
func (m *MmapFile) Close() error {
	if m.data != nil {
		return syscall.Munmap(m.data)
	}
	return nil
}

// Len returns the size of the mapped region.
func (m *MmapFile) Len() int {
	return len(m.data)
}

// Data returns the underlying memory-mapped byte slice.
func (m *MmapFile) Data() []byte {
	return m.data
}

// =============================================================================
// Store
// =============================================================================

// Store manages storing and querying events.
// Coordinates RocksDB storage, in-memory bitmap indexes, and flat file segments.
type Store struct {
	rocksDB *RocksDBBackend // nil when RocksDB disabled

	// Index coordinator (owns in-memory bitmap + delegates to IndexFlusher)
	indexStore *IndexStore

	// Flat file segment directory path (empty = disabled)
	segmentPath string

	// Unified query backend (RocksDB or flat files)
	queryBackend QueryBackend

	// Segment data writer for flat file event storage (nil = disabled)
	segmentDataWriter *SegmentDataWriter

	// Tracks the last segment ID seen during StoreEvents for auto-finalization.
	lastSegmentID  uint32
	hasLastSegment bool

	// Per-segment accumulators for metrics
	segStartTime   time.Time
	segEvents      int
	segLedgers     int
	segEventBytes  int64
	lastLedgerSeq  uint32

	// Collected segment metrics
	segmentMetrics []progress.SegmentStats
}

// Config configures a Store.
type Config struct {
	// RocksDB (optional — nil disables RocksDB)
	DBPath    string
	RocksOpts *RocksDBOptions

	// Flat file segments
	SegmentPath       string // base directory for segment files (empty = disabled)
	WriteSegmentFiles bool   // write index + data files during ingest
	CompressData      bool   // zstd-compress event data in segment files
	BlockSize         int    // group size for compressed event blocks
}

// New creates a Store from the given options.
func New(opts Config) (*Store, error) {
	es := &Store{
		segmentPath: opts.SegmentPath,
	}

	if opts.RocksOpts != nil {
		backend, err := NewRocksDBBackend(opts.DBPath, opts.RocksOpts)
		if err != nil {
			return nil, err
		}
		es.rocksDB = backend
		es.indexStore = NewIndexStore(backend.bitmapStore, backend.bitmapStore, backend.bitmapStore)
	} else {
		flusher := &SegmentIndexFlusher{}
		es.indexStore = NewIndexStore(flusher, nil, nil)
	}

	// Configure index write targets — cold segments live under <segmentPath>/cold/
	coldPath := ""
	if opts.WriteSegmentFiles && opts.SegmentPath != "" {
		coldPath = filepath.Join(opts.SegmentPath, "cold")
	}
	es.indexStore.SetWriteConfig(coldPath, es.rocksDB != nil)

	// Initialize segment data writer if segment files are enabled
	if coldPath != "" {
		es.segmentDataWriter = NewSegmentDataWriter(coldPath, opts.CompressData, opts.BlockSize)
	}

	// Initialize query backend — cold segments live under <segmentPath>/cold/
	if opts.SegmentPath != "" {
		es.queryBackend = NewSegmentReader(filepath.Join(opts.SegmentPath, "cold"))
	} else if es.rocksDB != nil {
		es.queryBackend = NewRocksDBReader(
			es.indexStore.bitmap,
			NewRocksDBEventFetcher(es.rocksDB.db, es.rocksDB.ro, es.rocksDB.cfEvents),
			es.rocksDB.bitmapStore,
		)
	}

	return es, nil
}

// RocksDB returns the underlying RocksDBBackend for direct access to stats methods.
func (es *Store) RocksDB() *RocksDBBackend {
	return es.rocksDB
}

// IndexStore returns the underlying IndexStore for direct bitmap access.
func (es *Store) IndexStore() *IndexStore {
	return es.indexStore
}

// StoreEvents stores events with optional index updates based on options.
// Automatically finalizes completed segments when a segment boundary is crossed.
// Returns the number of bytes written.
func (es *Store) StoreEvents(events []*event.IngestEvent, opts *StoreOptions) (int64, error) {
	var totalBytes int64

	if opts == nil {
		opts = &StoreOptions{}
	}

	var kvs []EventKV
	countUpdates := make(map[string]uint64)
	var maxLedger uint32

	for _, ev := range events {
		if es.indexStore != nil {
			segmentID := SegmentID(ev.LedgerSequence)
			if ev.LedgerSequence > maxLedger {
				maxLedger = ev.LedgerSequence
			}

			// Auto-finalize completed segment on boundary crossing.
			if es.hasLastSegment && segmentID != es.lastSegmentID {
				if err := es.finalizeCompletedSegment(es.lastSegmentID); err != nil {
					return 0, fmt.Errorf("failed to finalize segment %d: %w", es.lastSegmentID, err)
				}
				// Reset per-segment accumulators for the new segment
				es.segStartTime = time.Now()
				es.segEvents = 0
				es.segLedgers = 0
				es.segEventBytes = 0
				es.lastLedgerSeq = 0
			}
			es.lastSegmentID = segmentID
			es.hasLastSegment = true

			// Initialize segment start time on first event
			if es.segStartTime.IsZero() {
				es.segStartTime = time.Now()
			}

			// Track per-segment stats
			es.segEvents++
			es.segEventBytes += int64(len(ev.RawXDR))
			if ev.LedgerSequence != es.lastLedgerSeq {
				es.segLedgers++
				es.lastLedgerSeq = ev.LedgerSequence
			}

			denseLocalID := es.indexStore.AssignDenseLocalID(segmentID, ev.LedgerSequence)

			key := EncodeKey(segmentID, denseLocalID)
			value := event.EncodeBinaryEvent(ev)
			if es.rocksDB != nil {
				kvs = append(kvs, EventKV{Key: key, Value: value})
			}
			totalBytes += int64(len(value))

			if len(ev.ContractID) > 0 {
				es.indexStore.AddContractEvent(ev.ContractID, segmentID, denseLocalID)
			}

			for pos, topicBytes := range ev.Topics {
				es.indexStore.AddTopicEvent(pos, topicBytes, segmentID, denseLocalID)
			}

			if es.segmentDataWriter != nil {
				if !es.segmentDataWriter.IsActive() {
					if err := es.segmentDataWriter.StartChunk(segmentID); err != nil {
						return 0, fmt.Errorf("failed to start segment data chunk %d: %w", segmentID, err)
					}
				}
				v4Data := event.EncodeBinaryEvent(ev)
				if err := es.segmentDataWriter.AppendEvent(denseLocalID, v4Data); err != nil {
					return 0, fmt.Errorf("failed to append event to file store: %w", err)
				}
			}
		}

		if opts.UniqueIndexes {
			if len(ev.ContractID) > 0 {
				uk := string(uniqueKey(UniqueTypeContract, ev.ContractID))
				countUpdates[uk]++
			}

			for i, topicBytes := range ev.Topics {
				if i > 3 {
					break
				}
				var uniqueType byte
				switch i {
				case 0:
					uniqueType = UniqueTypeTopic0
				case 1:
					uniqueType = UniqueTypeTopic1
				case 2:
					uniqueType = UniqueTypeTopic2
				case 3:
					uniqueType = UniqueTypeTopic3
				}
				uk := string(uniqueKey(uniqueType, topicBytes))
				countUpdates[uk]++
			}
		}
	}

	if es.rocksDB != nil && (len(kvs) > 0 || len(countUpdates) > 0) {
		if err := es.rocksDB.WriteEventBatch(kvs, countUpdates); err != nil {
			return 0, err
		}
		if maxLedger > 0 {
			if err := es.rocksDB.SetLastProcessedLedger(maxLedger); err != nil {
				return 0, fmt.Errorf("failed to update last processed ledger: %w", err)
			}
		}
	}

	return totalBytes, nil
}

// finalizeCompletedSegment flushes in-memory bitmap indexes (for both RocksDB
// and flat file paths) and, when segment files are configured, writes flat file
// indexes for the given segment. Logs heap memory freed by the flush.
func (es *Store) finalizeCompletedSegment(segmentID uint32) error {
	// Snapshot segment wall time before finalization work
	segWallMs := float64(time.Since(es.segStartTime).Milliseconds())

	t0 := time.Now()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	fmt.Fprintf(os.Stderr, "  [finalize %d] GC+memstats: %v\n", segmentID, time.Since(t0))

	t1 := time.Now()
	if err := es.indexStore.Flush(); err != nil {
		return fmt.Errorf("flush bitmap indexes: %w", err)
	}
	fmt.Fprintf(os.Stderr, "  [finalize %d] indexStore.Flush: %v\n", segmentID, time.Since(t1))

	indexTerms := es.indexStore.SegmentTermCount(segmentID)
	contractTerms, t0Terms, t1Terms, t2Terms, t3Terms := es.indexStore.SegmentTermCounts(segmentID)

	coldPath := filepath.Join(es.segmentPath, "cold")
	if es.segmentPath != "" {
		t2 := time.Now()
		if err := FinalizeSegment(es.indexStore, coldPath, segmentID, es.segmentDataWriter); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "  [finalize %d] FinalizeSegment: %v\n", segmentID, time.Since(t2))
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	beforeMB := memBefore.HeapAlloc / (1024 * 1024)
	afterMB := memAfter.HeapAlloc / (1024 * 1024)
	freedMB := int64(beforeMB) - int64(afterMB)
	fmt.Fprintf(os.Stderr, "  [finalize %d] total: %v, heap freed %d MB (%d→%d)\n",
		segmentID, time.Since(t0), freedMB, beforeMB, afterMB)

	// Stat cold output files for on-disk sizes
	var coldIndexBytes, coldEventBytes int64
	if es.segmentPath != "" {
		dirName := fmt.Sprintf("%06d", segmentID)
		segDir := filepath.Join(coldPath, dirName)
		for _, name := range []string{"index.hash", "index.pack"} {
			if fi, err := os.Stat(filepath.Join(segDir, name)); err == nil {
				coldIndexBytes += fi.Size()
			}
		}
		if fi, err := os.Stat(filepath.Join(segDir, "events.pack")); err == nil {
			coldEventBytes = fi.Size()
		}
	}

	heapMB := int64(memAfter.HeapInuse / (1024 * 1024))
	wallSec := segWallMs / 1000.0
	var eventsPerSec, avgEventBytes float64
	if wallSec > 0 {
		eventsPerSec = float64(es.segEvents) / wallSec
	}
	if es.segEvents > 0 {
		avgEventBytes = float64(es.segEventBytes) / float64(es.segEvents)
	}

	fmt.Fprintf(os.Stderr, "[segment %06d] %d ledgers, %d events, %s raw, %d terms, cold: %s events, %s index\n",
		segmentID, es.segLedgers, es.segEvents,
		formatBytesStore(es.segEventBytes), indexTerms,
		formatBytesStore(coldEventBytes), formatBytesStore(coldIndexBytes))
	fmt.Fprintf(os.Stderr, "[segment %06d] %.0f events/s, avg %.0f bytes/event\n",
		segmentID, eventsPerSec, avgEventBytes)
	freezeMs := float64(time.Since(t0).Microseconds()) / 1000
	flushMs := float64(time.Since(t1).Microseconds()) / 1000

	fmt.Fprintf(os.Stderr, "[segment %06d] heap: %d MB, wall: %.0fms\n",
		segmentID, heapMB, segWallMs)
	fmt.Fprintf(os.Stderr, "[segment %06d] freeze: %.0fms (flush %.0fms, index %.0fms), heap freed %d MB\n",
		segmentID, freezeMs, flushMs, freezeMs-flushMs, freedMB)

	es.segmentMetrics = append(es.segmentMetrics, progress.SegmentStats{
		SegmentID:      segmentID,
		Ledgers:        es.segLedgers,
		Events:         es.segEvents,
		HotEventBytes:  es.segEventBytes,
		IndexTerms:     indexTerms,
		ContractTerms:  contractTerms,
		Topic0Terms:    t0Terms,
		Topic1Terms:    t1Terms,
		Topic2Terms:    t2Terms,
		Topic3Terms:    t3Terms,
		ColdEventBytes: coldEventBytes,
		ColdIndexBytes: coldIndexBytes,
		AvgEventBytes:  avgEventBytes,
		EventsPerSec:   eventsPerSec,
		HeapInUseMB:    heapMB,
		IngestWallMs:   segWallMs,
		FreezeWallMs:   freezeMs,
		FlushMs:        flushMs,
		MphfMs:         freezeMs - flushMs,
		HeapFreedMB:    freedMB,
	})

	return nil
}

// SegmentMetrics returns the collected per-segment metrics.
func (es *Store) SegmentMetrics() []progress.SegmentStats {
	return es.segmentMetrics
}

// formatBytesStore formats byte counts as human-readable strings.
func formatBytesStore(bytes int64) string {
	switch {
	case bytes >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GB", float64(bytes)/(1024*1024*1024))
	case bytes >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(bytes)/(1024*1024))
	case bytes >= 1024:
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// Finalize flushes in-memory bitmap indexes and finalizes the last segment.
// Must be called after ingestion completes to ensure the final segment is written.
func (es *Store) Finalize() error {
	if !es.hasLastSegment {
		return nil
	}
	return es.finalizeCompletedSegment(es.lastSegmentID)
}

// QueryEvents executes a query via the query backend.
// When filters (contractIDs / topicGroups) are provided, uses bitmap indexes to
// find matching events. Otherwise falls back to a sequential range scan.
func (es *Store) QueryEvents(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*QueryResult, []*query.Event, error) {
	hasFilters := len(contractIDs) > 0
	if !hasFilters {
		for _, tg := range topicGroups {
			if len(tg) > 0 {
				hasFilters = true
				break
			}
		}
	}

	if !hasFilters {
		return es.queryBackend.FetchByRange(startLedger, endLedger, limit)
	}

	totalStart := time.Now()
	result := &QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	perSegment, err := collectBitmaps(es.queryBackend, contractIDs, topicGroups, startLedger, endLedger, result)
	if err != nil {
		return nil, nil, err
	}

	if result.MatchingLocalIDs == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	events, err := es.queryBackend.FetchByIDs(perSegment, limit, result)
	if err != nil {
		return nil, nil, err
	}

	// Post-filter: verify fetched events actually match query terms.
	// MPHF fingerprint checks reduce but don't eliminate false positives.
	filterStart := time.Now()
	events = postFilterEvents(events, contractIDs, topicGroups)
	result.FilterTime = time.Since(filterStart)
	result.EventsReturned = len(events)

	result.TotalTime = time.Since(totalStart)
	return result, events, nil
}

// PurgeQueryCache drops all cached mmaps, indexes, and readers from the query
// backend so that the OS page cache purge is effective for cold-cache benchmarks.
func (es *Store) PurgeQueryCache() {
	if sr, ok := es.queryBackend.(*SegmentReader); ok {
		sr.PurgeCache()
	}
}

// Close closes the event store.
func (es *Store) Close() {
	if es.indexStore != nil {
		es.indexStore.Close()
	}

	if es.queryBackend != nil {
		es.queryBackend.Close()
	}

	if es.rocksDB != nil {
		es.rocksDB.Close()
	}
}

// postFilterEvents verifies that fetched events actually match all query terms.
// This eliminates false positives from the MPHF-based index lookup.
func postFilterEvents(events []*query.Event, contractIDs [][]byte, topicGroups [4][][]byte) []*query.Event {
	if len(events) == 0 {
		return events
	}

	// Pre-compute contract filter set (strkey-encoded strings)
	var contractSet map[string]struct{}
	if len(contractIDs) > 0 {
		contractSet = make(map[string]struct{}, len(contractIDs))
		for _, cid := range contractIDs {
			if encoded, err := strkey.Encode(strkey.VersionByteContract, cid); err == nil {
				contractSet[encoded] = struct{}{}
			}
		}
	}

	// Pre-compute topic filter sets (base64-encoded strings) per position
	var topicSets [4]map[string]struct{}
	for pos, tg := range topicGroups {
		if len(tg) == 0 {
			continue
		}
		topicSets[pos] = make(map[string]struct{}, len(tg))
		for _, topicXDR := range tg {
			topicSets[pos][base64.StdEncoding.EncodeToString(topicXDR)] = struct{}{}
		}
	}

	filtered := make([]*query.Event, 0, len(events))
	for _, ev := range events {
		// Check contract filter
		if contractSet != nil {
			if _, ok := contractSet[ev.ContractID]; !ok {
				continue
			}
		}

		// Check topic filters at each position
		match := true
		for pos := 0; pos < 4; pos++ {
			if topicSets[pos] == nil {
				continue
			}
			if pos >= len(ev.Topics) {
				match = false
				break
			}
			if _, ok := topicSets[pos][ev.Topics[pos]]; !ok {
				match = false
				break
			}
		}
		if !match {
			continue
		}

		filtered = append(filtered, ev)
	}
	return filtered
}

// collectBitmaps loads bitmaps for a query and intersects them per segment.
// Contract IDs are OR'd within group 0, topic values are OR'd within each
// positional group (1-4), and the resulting group bitmaps are AND'd per segment.
// Returns the per-segment intersection bitmaps and populates index stats on result.
func collectBitmaps(
	loader BitmapLoader,
	contractIDs [][]byte,
	topicGroups [4][][]byte,
	startLedger, endLedger uint32,
	result *QueryResult,
) (map[uint32]*roaring.Bitmap, error) {
	segments := GetSegmentsForRange(startLedger, endLedger)
	result.SegmentsTouched = len(segments)

	indexStart := time.Now()

	// Collect per-group, per-segment bitmaps.
	// groupIdx 0 = contracts, 1-4 = topic positions.
	type groupSegBitmaps struct {
		bitmaps map[uint32][]*roaring.Bitmap
	}
	groups := make(map[int]*groupSegBitmaps)

	// Query contract bitmaps (OR within contracts group).
	if len(contractIDs) > 0 {
		groups[0] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, cid := range contractIDs {
			termKey := ContractTermKey(cid)
			for _, segID := range segments {
				bm, stats, err := loader.LoadTermBitmap(segID, 0, termKey, startLedger, endLedger)
				if err != nil {
					continue
				}
				result.IndexBytesRead += stats.BytesRead
				result.IndexReadTime += stats.ReadTime
				result.IndexDecodeTime += stats.DecodeTime
				if bm != nil && !bm.IsEmpty() {
					result.SegmentsScanned++
					groups[0].bitmaps[segID] = append(groups[0].bitmaps[segID], bm)
				}
			}
		}
	}

	// Query topic bitmaps per position (OR within each position group).
	for pos, tg := range topicGroups {
		if len(tg) == 0 {
			continue
		}
		groupIdx := pos + 1
		groups[groupIdx] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, topicXDR := range tg {
			if len(topicXDR) == 0 {
				continue
			}
			termKey := TopicTermKey(topicXDR)
			for _, segID := range segments {
				bm, stats, err := loader.LoadTermBitmap(segID, groupIdx, termKey, startLedger, endLedger)
				if err != nil {
					continue
				}
				result.IndexBytesRead += stats.BytesRead
				result.IndexReadTime += stats.ReadTime
				result.IndexDecodeTime += stats.DecodeTime
				if bm != nil && !bm.IsEmpty() {
					result.SegmentsScanned++
					groups[groupIdx].bitmaps[segID] = append(groups[groupIdx].bitmaps[segID], bm)
				}
			}
		}
	}

	// Collect all segment IDs across all groups.
	allSegIDs := make(map[uint32]bool)
	for _, g := range groups {
		for segID := range g.bitmaps {
			allSegIDs[segID] = true
		}
	}

	// Parallel intersection: parallel OR within groups, then AND across groups.
	intersectStart := time.Now()
	perSegment := make(map[uint32]*roaring.Bitmap)

	type groupBitmaps struct {
		perSeg map[uint32][]*roaring.Bitmap
	}
	groupList := make([]groupBitmaps, 0, len(groups))
	for _, g := range groups {
		groupList = append(groupList, groupBitmaps{perSeg: g.bitmaps})
	}

	type segAndResult struct {
		segID uint32
		bm    *roaring.Bitmap
	}
	segIDList := make([]uint32, 0, len(allSegIDs))
	for segID := range allSegIDs {
		segIDList = append(segIDList, segID)
	}
	andResults := make([]segAndResult, len(segIDList))
	var andWg sync.WaitGroup
	for i, segID := range segIDList {
		andWg.Add(1)
		go func(idx int, sid uint32) {
			defer andWg.Done()
			for _, g := range groupList {
				if len(g.perSeg[sid]) == 0 {
					return
				}
			}
			groupUnions := make([]*roaring.Bitmap, len(groupList))
			var orWg sync.WaitGroup
			for gIdx := range groupList {
				bms := groupList[gIdx].perSeg[sid]
				if len(bms) == 1 {
					groupUnions[gIdx] = bms[0]
				} else {
					orWg.Add(1)
					go func(oi int, bmaps []*roaring.Bitmap) {
						defer orWg.Done()
						groupUnions[oi] = roaring.FastOr(bmaps...)
					}(gIdx, bms)
				}
			}
			orWg.Wait()
			intersected := roaring.FastAnd(groupUnions...)
			if !intersected.IsEmpty() {
				andResults[idx] = segAndResult{segID: sid, bm: intersected}
			}
		}(i, segID)
	}
	andWg.Wait()
	for _, sr := range andResults {
		if sr.bm != nil {
			perSegment[sr.segID] = sr.bm
			result.MatchingLocalIDs += int(sr.bm.GetCardinality())
		}
	}
	result.IndexIntersectTime = time.Since(intersectStart)
	result.IndexLookupTime = time.Since(indexStart)

	return perSegment, nil
}
