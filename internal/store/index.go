package store

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring"
)

// =============================================================================
// Index Key Format (36 bytes)
// =============================================================================
//
//   [term_key:32][segment_id:4]
//   - term_key: full SHA-256 hash (32 bytes) of the indexed value
//   - segment_id: ledger_seq / SegmentSize (groups ~14 hours of ledgers)
//
// Used by bitmap and segment-index indexes.
// Column families separate contract vs topic data.

const (
	// IndexKeySize is the size of an index key in bytes (32 hash + 4 segment ID).
	IndexKeySize = 36
)

// ContractTermKey computes the full SHA-256 term key for a contract ID.
func ContractTermKey(contractID []byte) [32]byte {
	return sha256.Sum256(contractID)
}

// TopicTermKey computes the full SHA-256 term key for a topic XDR value.
// Position-implicit: the position is NOT embedded in the hash.
// Instead, position is tracked by separate per-field maps/files.
func TopicTermKey(topicXDR []byte) [32]byte {
	return sha256.Sum256(topicXDR)
}

// bitmapChunk represents a serializable bitmap segment.
type bitmapChunk struct {
	Key        []byte
	Data       []byte
	FieldIndex int // 0=contract, 1=topic0, 2=topic1, 3=topic2, 4=topic3
}

// bitmap32Loader provides segment loading capability for bitmap32 queries.
type bitmap32Loader interface {
	// LoadBitmap32SegmentWithTiming loads a segment and returns bytes read, read time, and decode time.
	// fieldIndex: 0=contracts, 1-4=topic positions 0-3.
	LoadBitmap32SegmentWithTiming(fieldIndex int, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error)
}

// makeIndexKey creates a 36-byte index key from a term key and segment ID.
// Format: [term_key:32][segment_id:4]
func makeIndexKey(termKey [32]byte, segmentID uint32) [IndexKeySize]byte {
	var key [IndexKeySize]byte
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], segmentID)
	return key
}

// ledgerOffsetsLoader loads a SegmentLedgerOffsets for a given segment from storage.
type ledgerOffsetsLoader interface {
	LoadSegmentLedgerOffsets(segmentID uint32) (*SegmentLedgerOffsets, error)
}

// eventBitmap32Index manages segmented 32-bit roaring bitmap indexes for event-level granularity.
// Uses dense sequential local IDs within each segment for compact bitmap representation.
// Uses roaring.Bitmap (32-bit) which supports FromBuffer for near-zero-cost decode.
type eventBitmap32Index struct {
	// Hot segment caches — 5 per-field maps
	contracts map[[IndexKeySize]byte]*roaring.Bitmap
	topic0    map[[IndexKeySize]byte]*roaring.Bitmap
	topic1    map[[IndexKeySize]byte]*roaring.Bitmap
	topic2    map[[IndexKeySize]byte]*roaring.Bitmap
	topic3    map[[IndexKeySize]byte]*roaring.Bitmap

	// Current hot segment ID
	currentSegmentID uint32

	// Segment loader for queries
	loader bitmap32Loader

	// Dense ID assignment: per-segment event counters
	segmentCounters map[uint32]*segmentEventCounter

	// Ledger offsets loader for queries (range trimming)
	ledgerOffsetsLoader ledgerOffsetsLoader
}

// newEventBitmap32Index creates a new event-level bitmap32 index.
func newEventBitmap32Index(loader bitmap32Loader, lmLoader ledgerOffsetsLoader) *eventBitmap32Index {
	return &eventBitmap32Index{
		contracts:       make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic0:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic1:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic2:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic3:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		loader:          loader,
		segmentCounters: make(map[uint32]*segmentEventCounter),
		ledgerOffsetsLoader: lmLoader,
	}
}

// topicMapForPos returns the topic map for the given position (0-3).
func (bi *eventBitmap32Index) topicMapForPos(pos int) map[[IndexKeySize]byte]*roaring.Bitmap {
	switch pos {
	case 0:
		return bi.topic0
	case 1:
		return bi.topic1
	case 2:
		return bi.topic2
	case 3:
		return bi.topic3
	default:
		return nil
	}
}

// AssignDenseLocalID assigns the next dense local ID for an event within a segment.
// Returns the dense ID to use in bitmap/posting list indexes.
func (bi *eventBitmap32Index) AssignDenseLocalID(segmentID uint32, ledger uint32) uint32 {
	counter, exists := bi.segmentCounters[segmentID]
	if !exists {
		counter = &segmentEventCounter{}
		bi.segmentCounters[segmentID] = counter
	}
	ledgerOffset := uint16(ledger - segmentID*SegmentSize)
	return counter.assignDenseID(ledgerOffset)
}

// AddContractEvent adds an event to the contract index using a dense local ID.
func (bi *eventBitmap32Index) AddContractEvent(contractID []byte, segmentID uint32, denseLocalID uint32) {
	termKey := ContractTermKey(contractID)
	key := makeIndexKey(termKey, segmentID)

	bitmap, exists := bi.contracts[key]
	if !exists {
		bitmap = roaring.New()
		bi.contracts[key] = bitmap
	}

	bitmap.Add(denseLocalID)

	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// AddTopicEvent adds an event to the topic index (positional) using a dense local ID.
func (bi *eventBitmap32Index) AddTopicEvent(pos int, topicValue []byte, segmentID uint32, denseLocalID uint32) {
	termKey := TopicTermKey(topicValue)
	key := makeIndexKey(termKey, segmentID)

	m := bi.topicMapForPos(pos)
	if m == nil {
		return
	}

	bitmap, exists := m[key]
	if !exists {
		bitmap = roaring.New()
		m[key] = bitmap
	}

	bitmap.Add(denseLocalID)

	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// getSegmentWithStats retrieves a segment and tracks bytes read.
// fieldIndex: 0=contracts, 1-4=topic positions 0-3.
// Returns fromCache=true if the bitmap came from the hot cache (shared, must clone before mutating).
func (bi *eventBitmap32Index) getSegmentWithStats(fieldIndex int, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, bool, error) {
	key := makeIndexKey(termKey, segmentID)

	// Check hot cache first - no disk read
	var segments map[[IndexKeySize]byte]*roaring.Bitmap
	if fieldIndex == 0 {
		segments = bi.contracts
	} else {
		segments = bi.topicMapForPos(fieldIndex - 1)
	}
	if segments != nil {
		if bitmap, exists := segments[key]; exists {
			return bitmap, 0, 0, 0, true, nil
		}
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		bm, bytes, rt, dt, err := bi.loader.LoadBitmap32SegmentWithTiming(fieldIndex, termKey, segmentID)
		return bm, bytes, rt, dt, false, err
	}

	return nil, 0, 0, 0, false, nil
}

// GetAndClearAllSegments gets and clears all hot segments and segment counters.
// Serializes bitmaps using MarshalBinary (standard format, compatible with FromBuffer).
// Also returns the segment event counters for ledger offsets flush.
func (bi *eventBitmap32Index) GetAndClearAllSegments() ([]bitmapChunk, map[uint32]*segmentEventCounter, error) {
	type fieldMap struct {
		m     map[[IndexKeySize]byte]*roaring.Bitmap
		index int
	}
	fields := []fieldMap{
		{bi.contracts, 0},
		{bi.topic0, 1},
		{bi.topic1, 2},
		{bi.topic2, 3},
		{bi.topic3, 4},
	}

	total := 0
	for _, f := range fields {
		total += len(f.m)
	}
	segments := make([]bitmapChunk, 0, total)

	for _, f := range fields {
		for key, bitmap := range f.m {
			bitmap.RunOptimize()
			data, err := bitmap.ToBytes()
			if err != nil {
				return nil, nil, err
			}
			keyCopy := make([]byte, IndexKeySize)
			copy(keyCopy, key[:])
			segments = append(segments, bitmapChunk{
				Key:        keyCopy,
				Data:       data,
				FieldIndex: f.index,
			})
		}
	}

	// Copy segment counters that have new events since the last flush.
	// Skip stale entries (eventCounts all zero) to avoid overwriting
	// previously correct ledger offsets files with empty data.
	// nextDenseID must persist to ensure dense IDs continue after flush.
	counters := make(map[uint32]*segmentEventCounter)
	for segID, counter := range bi.segmentCounters {
		hasEvents := false
		for _, c := range counter.eventCounts {
			if c > 0 {
				hasEvents = true
				break
			}
		}
		if !hasEvents {
			continue
		}
		counters[segID] = &segmentEventCounter{
			eventCounts: counter.eventCounts,
			nextDenseID: counter.nextDenseID,
		}
		// Reset event counts (captured for flush) but keep nextDenseID
		counter.eventCounts = [SegmentSize]uint32{}
	}

	bi.contracts = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic0 = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic1 = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic2 = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic3 = make(map[[IndexKeySize]byte]*roaring.Bitmap)

	return segments, counters, nil
}

// GetHotSegmentStats returns statistics about hot segments.
func (bi *eventBitmap32Index) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	allMaps := []map[[IndexKeySize]byte]*roaring.Bitmap{
		bi.contracts, bi.topic0, bi.topic1, bi.topic2, bi.topic3,
	}
	for _, m := range allMaps {
		count += len(m)
		for _, bitmap := range m {
			totalCards += bitmap.GetCardinality()
			memBytes += bitmap.GetSizeInBytes()
		}
	}
	return
}

// SegmentMemStats holds per-segment memory estimation for in-memory bitmap data.
type SegmentMemStats struct {
	SegmentID       uint32
	BitmapCount     int    // entries across all 5 maps for this segment
	BitmapDataBytes uint64 // sum of bitmap.GetSizeInBytes()
	TotalCards      uint64 // sum of cardinalities
	OverheadBytes   uint64 // map/key/pointer/object overhead (~194 per entry)
	CounterBytes    uint64 // 40,004 if segmentEventCounter exists
	TotalEstimate   uint64 // sum of all
}

// perEntryOverhead is the estimated overhead per map entry:
// 36 (key) + 8 (pointer) + ~50 (map bucket) + ~100 (bitmap object) = 194
const perEntryOverhead = 194

// counterSize is the memory for a segmentEventCounter:
// [SegmentSize]uint32 (4*10000=40000) + nextDenseID uint32 (4) = 40004
const counterSize = 40004

// GetPerSegmentMemStats returns per-segment memory estimates for all hot bitmap data.
func (bi *eventBitmap32Index) GetPerSegmentMemStats() []SegmentMemStats {
	type segAccum struct {
		bitmapCount     int
		bitmapDataBytes uint64
		totalCards      uint64
	}

	accum := make(map[uint32]*segAccum)

	allMaps := []map[[IndexKeySize]byte]*roaring.Bitmap{
		bi.contracts, bi.topic0, bi.topic1, bi.topic2, bi.topic3,
	}
	for _, m := range allMaps {
		for key, bitmap := range m {
			segID := binary.BigEndian.Uint32(key[32:36])
			a, ok := accum[segID]
			if !ok {
				a = &segAccum{}
				accum[segID] = a
			}
			a.bitmapCount++
			a.bitmapDataBytes += bitmap.GetSizeInBytes()
			a.totalCards += bitmap.GetCardinality()
		}
	}

	result := make([]SegmentMemStats, 0, len(accum))
	for segID, a := range accum {
		overhead := uint64(a.bitmapCount) * perEntryOverhead
		var counterBytes uint64
		if _, ok := bi.segmentCounters[segID]; ok {
			counterBytes = counterSize
		}
		total := a.bitmapDataBytes + overhead + counterBytes
		result = append(result, SegmentMemStats{
			SegmentID:       segID,
			BitmapCount:     a.bitmapCount,
			BitmapDataBytes: a.bitmapDataBytes,
			TotalCards:      a.totalCards,
			OverheadBytes:   overhead,
			CounterBytes:    counterBytes,
			TotalEstimate:   total,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SegmentID < result[j].SegmentID
	})

	return result
}

// GetCurrentSegmentID returns the current segment ID being written to.
func (bi *eventBitmap32Index) GetCurrentSegmentID() uint32 {
	return bi.currentSegmentID
}

// =============================================================================
// IndexStore — Coordinates In-Memory Index and Persistent Storage
// =============================================================================

// IndexFlusher persists bitmap segments and ledger offsetss from the in-memory index.
type IndexFlusher interface {
	Flush(segments []bitmapChunk, counters map[uint32]*segmentEventCounter, writeToStore bool) (map[uint32][]byte, error)
	Close() error
}

// SegmentTermData holds a term hash and its serialized bitmap data.
type SegmentTermData struct {
	TermHash   [32]byte
	BitmapData []byte
}

// FlushedSegmentData holds bitmap terms for a single segment, cached after Flush().
type FlushedSegmentData struct {
	Contracts []SegmentTermData
	Topics    [4][]SegmentTermData
}

// IndexStore coordinates the in-memory bitmap index and a pluggable persistence backend.
// It owns the eventBitmap32Index (in-memory) and delegates persistence to an IndexFlusher.
type IndexStore struct {
	bitmap  *eventBitmap32Index
	flusher IndexFlusher

	// Write targets during Flush
	segmentPath    string // base dir for segment files (empty = file writes disabled)
	writeToRocksDB bool   // write bitmaps and ledger offsetss to RocksDB (default: true)

	// Cache of flushed bitmap data, keyed by segment ID.
	// Populated by Flush(), consumed by PopSegmentTerms().
	flushedTerms map[uint32]*FlushedSegmentData
}

// NewIndexStore creates a new IndexStore with a pluggable flusher and loaders.
// flusher — for persistence (RocksDB or segment-file-only).
// loader — for cold bitmap loads during queries (can be nil for write-only).
// lmLoader — for ledger offsets loads during queries (can be nil for write-only).
func NewIndexStore(flusher IndexFlusher, loader bitmap32Loader, lmLoader ledgerOffsetsLoader) *IndexStore {
	s := &IndexStore{flusher: flusher, writeToRocksDB: true}
	s.bitmap = newEventBitmap32Index(loader, lmLoader)
	return s
}

// SetWriteConfig configures where bitmaps and ledger offsetss are written during Flush().
// segmentPath: base directory for segment flat files (empty = file writes disabled).
// writeToRocksDB: if true, bitmaps and ledger offsetss are written to RocksDB.
func (s *IndexStore) SetWriteConfig(segmentPath string, writeToRocksDB bool) {
	s.segmentPath = segmentPath
	s.writeToRocksDB = writeToRocksDB
}

// Flush persists all hot segments and ledger offsetss.
// Gets data from the in-memory index, passes to RocksDBIndexStore for RocksDB persistence,
// then writes flat files if configured.
func (s *IndexStore) Flush() error {
	segments, counters, err := s.bitmap.GetAndClearAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get event bitmap32 segments: %w", err)
	}

	if len(segments) == 0 && len(counters) == 0 {
		return nil
	}

	// Persist via flusher (RocksDB or segment-file-only) and get back ledger offsets data
	ledgerOffsetsData, err := s.flusher.Flush(segments, counters, s.writeToRocksDB)
	if err != nil {
		return err
	}

	// Cache bitmap chunks organized by segment ID for WriteSegmentDir.
	// The pipeline must not do periodic mid-segment flushes when segment files
	// are enabled, so each term appears at most once — no merge needed.
	s.flushedTerms = make(map[uint32]*FlushedSegmentData)
	for _, seg := range segments {
		segmentID := binary.BigEndian.Uint32(seg.Key[32:36])
		fd, ok := s.flushedTerms[segmentID]
		if !ok {
			fd = &FlushedSegmentData{}
			s.flushedTerms[segmentID] = fd
		}
		var termHash [32]byte
		copy(termHash[:], seg.Key[:32])
		td := SegmentTermData{TermHash: termHash, BitmapData: seg.Data}
		if seg.FieldIndex == 0 {
			fd.Contracts = append(fd.Contracts, td)
		} else {
			pos := seg.FieldIndex - 1 // 1->topic0, 2->topic1, 3->topic2, 4->topic3
			fd.Topics[pos] = append(fd.Topics[pos], td)
		}
	}

	// Write ledger offsetss to segment flat files if configured
	if s.segmentPath != "" {
		for segmentID, data := range ledgerOffsetsData {
			dirPath := filepath.Join(s.segmentPath, fmt.Sprintf("%06d", segmentID))
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return fmt.Errorf("failed to create segment dir %s: %w", dirPath, err)
			}
			if err := writeFileAtomic(filepath.Join(dirPath, LedgerOffsetsFileName), data); err != nil {
				return fmt.Errorf("failed to write ledger offsets file for segment %d: %w", segmentID, err)
			}
		}
	}

	return nil
}

// PopSegmentTerms returns cached bitmap terms for a segment and removes them from the cache.
// Returns nil if no cached data exists (caller should fall back to RocksDB scan).
func (s *IndexStore) PopSegmentTerms(segmentID uint32) *FlushedSegmentData {
	if s.flushedTerms == nil {
		return nil
	}
	fd := s.flushedTerms[segmentID]
	delete(s.flushedTerms, segmentID)
	return fd
}

// AddContractEvent adds an event to the contract index using a dense local ID.
func (s *IndexStore) AddContractEvent(contractID []byte, segmentID uint32, denseLocalID uint32) {
	s.bitmap.AddContractEvent(contractID, segmentID, denseLocalID)
}

// AddTopicEvent adds an event to the topic index (positional) using a dense local ID.
func (s *IndexStore) AddTopicEvent(pos int, topic []byte, segmentID uint32, denseLocalID uint32) {
	s.bitmap.AddTopicEvent(pos, topic, segmentID, denseLocalID)
}

// AssignDenseLocalID assigns the next dense local ID for an event within a segment.
func (s *IndexStore) AssignDenseLocalID(segmentID uint32, ledger uint32) uint32 {
	return s.bitmap.AssignDenseLocalID(segmentID, ledger)
}

// GetHotSegmentStats returns statistics about hot segments.
func (s *IndexStore) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	return s.bitmap.GetHotSegmentStats()
}

// GetPerSegmentMemStats returns per-segment memory estimates.
func (s *IndexStore) GetPerSegmentMemStats() []SegmentMemStats {
	return s.bitmap.GetPerSegmentMemStats()
}

// GetCurrentSegmentID returns the current segment ID being written to.
func (s *IndexStore) GetCurrentSegmentID() uint32 {
	return s.bitmap.GetCurrentSegmentID()
}

// Close releases resources.
func (s *IndexStore) Close() error {
	return s.flusher.Close()
}
