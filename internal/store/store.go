package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/urvisavla/stellar-events/internal/query"
)

// =============================================================================
// Configuration Types
// =============================================================================

// BuildIndexOptions controls which indexes to build during rebuild.
type BuildIndexOptions struct {
	UniqueIndexes bool // Build unique value counts (for stats)
}

// indexEntry holds extracted data for index updates (used by collector pattern)
type indexEntry struct {
	Ledger     uint32
	TxIdx      uint16
	OpIdx      uint16
	EventIdx   uint16
	ContractID []byte   // nil if no contract
	Topics     [][]byte // up to 4 topics
}

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

// SegmentRange returns the ledger range covered by a segment.
func SegmentRange(segmentID uint32) (start, end uint32) {
	start = segmentID * SegmentSize
	end = start + SegmentSize - 1
	return
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
	LoadTermBitmap(segmentID uint32, fieldIndex int, termKey [32]byte,
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

// LedgerMapLoader loads segment ledger maps from a storage backend.
// Implementations: RocksDBIndexStore (from RocksDB), SegmentReader (from mmap'd flat files).
type LedgerMapLoader interface {
	LoadSegmentLedgerMap(segmentID uint32) (*SegmentLedgerMap, error)
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
	DecompressTime     time.Duration // Time spent decompressing event blobs (zstd/dict)
	EventDiskReadTime  time.Duration // Time spent on disk I/O for event data
	GroupsDecompressed int           // Number of group blocks decompressed
	DecodeTime         time.Duration // Time decoding events
	FilterTime         time.Duration // Time filtering events
	TotalTime          time.Duration // Total query time
}

// SegmentLedgerMap maps dense sequential local IDs to (ledger, eventSeq) pairs
// within a segment. Stored as a fixed-size cumulative array of event counts.
//
// Entry i = total events in ledgers at offsets 0 through i (inclusive cumulative sum).
// The array has exactly SegmentSize entries (10,000), stored as little-endian uint32s.
//
// Key format in CFDefault: [0xFF][segmentID:4] — 5 bytes
// Value: [cumulative_count:4] × SegmentSize — exactly 40,000 bytes (fixed)
type SegmentLedgerMap struct {
	SegmentID uint32
	Data      []byte // raw 40,000-byte cumulative array (zero-copy from RocksDB)
}

const (
	// SegmentLedgerMapSize is the size of the cumulative array in bytes.
	SegmentLedgerMapSize = int(SegmentSize) * 4 // 40,000 bytes

	// segmentLedgerMapPrefix is the key prefix used to avoid collision with text keys in CFDefault.
	segmentLedgerMapPrefix = 0xFF
)

// SegmentLedgerMapKey returns the CFDefault key for a segment's ledger map.
// Format: [0xFF][segmentID:4] — 5 bytes
func SegmentLedgerMapKey(segmentID uint32) []byte {
	key := make([]byte, 5)
	key[0] = segmentLedgerMapPrefix
	binary.BigEndian.PutUint32(key[1:5], segmentID)
	return key
}

// TotalEvents returns the total number of events in this segment (last cumulative entry).
func (m *SegmentLedgerMap) TotalEvents() uint32 {
	if len(m.Data) < SegmentLedgerMapSize {
		return 0
	}
	return m.getCumulative(int(SegmentSize) - 1)
}

// LedgerRangeToIDRange converts a ledger offset range to a dense ID range. O(1).
// startOff and endOff are ledger offsets within the segment (0-based, inclusive).
// Returns (startID, endID) where endID is inclusive. If the range has no events,
// returns (0, 0) with startID > endID being impossible, so caller should check endID >= startID.
func (m *SegmentLedgerMap) LedgerRangeToIDRange(startOff, endOff uint16) (uint32, uint32) {
	if len(m.Data) < SegmentLedgerMapSize {
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
func (m *SegmentLedgerMap) DenseIDToLedgerAndSeq(denseID uint32) (ledger uint32, seq uint16) {
	if len(m.Data) < SegmentLedgerMapSize {
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
func (m *SegmentLedgerMap) getCumulative(i int) uint32 {
	off := i * 4
	return binary.LittleEndian.Uint32(m.Data[off : off+4])
}

// EncodeSegmentLedgerMap builds a cumulative array from per-ledger event counts.
// ledgerEventCounts[i] = number of events at ledger offset i.
// Returns the raw 40,000-byte cumulative array.
func EncodeSegmentLedgerMap(segmentID uint32, ledgerEventCounts [SegmentSize]uint32) []byte {
	data := make([]byte, SegmentLedgerMapSize)
	var cumulative uint32
	for i := uint32(0); i < SegmentSize; i++ {
		cumulative += ledgerEventCounts[i]
		off := i * 4
		binary.LittleEndian.PutUint32(data[off:off+4], cumulative)
	}
	return data
}

// MergeSegmentLedgerMapData merges new per-ledger event counts into an existing
// cumulative array. For ledger offsets present in both old and new data, the new
// count REPLACES the old (idempotent on re-ingestion). For offsets only in the
// existing data, old counts are preserved (supports continuation after restart).
// Returns a new 40,000-byte cumulative array.
func MergeSegmentLedgerMapData(existing []byte, newCounts map[uint16]uint32) []byte {
	if len(existing) < SegmentLedgerMapSize {
		// No existing data — build from scratch
		var counts [SegmentSize]uint32
		for off, count := range newCounts {
			counts[off] = count
		}
		return EncodeSegmentLedgerMap(0, counts)
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
	return EncodeSegmentLedgerMap(0, counts)
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
