package store

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
)

// =============================================================================
// Bitmap Engine Types — Pure In-Memory
// =============================================================================

// bitmapChunk represents a serializable bitmap segment.
type bitmapChunk struct {
	Key        []byte
	Data       []byte
	FieldIndex int // 0=contract, 1=topic0, 2=topic1, 3=topic2, 4=topic3
}

// bitmap32Loader provides segment loading capability for bitmap32 queries.
type bitmap32Loader interface {
	// LoadBitmap32Segment loads a 32-bit roaring bitmap segment from storage.
	LoadBitmap32Segment(isContract bool, termKey [32]byte, pos int, segmentID uint32) (*roaring.Bitmap, error)
	// LoadBitmap32SegmentWithTiming loads a segment and returns bytes read, read time, and decode time.
	LoadBitmap32SegmentWithTiming(isContract bool, termKey [32]byte, pos int, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error)
}

// makeIndexKey creates a 36-byte index key from a term key and segment ID.
// Format: [term_key:32][segment_id:4]
func makeIndexKey(termKey [32]byte, segmentID uint32) [IndexKeySize]byte {
	var key [IndexKeySize]byte
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], segmentID)
	return key
}

// ledgerMapLoader loads a SegmentLedgerMap for a given segment from storage.
type ledgerMapLoader interface {
	LoadSegmentLedgerMap(segmentID uint32) (*SegmentLedgerMap, error)
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

	// Ledger map loader for queries (range trimming)
	ledgerMapLoader ledgerMapLoader
}

// newEventBitmap32Index creates a new event-level bitmap32 index.
func newEventBitmap32Index(loader bitmap32Loader, lmLoader ledgerMapLoader) *eventBitmap32Index {
	return &eventBitmap32Index{
		contracts:       make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic0:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic1:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic2:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topic3:          make(map[[IndexKeySize]byte]*roaring.Bitmap),
		loader:          loader,
		segmentCounters: make(map[uint32]*segmentEventCounter),
		ledgerMapLoader: lmLoader,
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

// InitializeDenseCounter loads the ledger map for a segment from storage and sets
// the nextDenseID to continue from the persisted state. Called on startup.
func (bi *eventBitmap32Index) InitializeDenseCounter(segmentID uint32, nextDenseID uint32) {
	counter, exists := bi.segmentCounters[segmentID]
	if !exists {
		counter = &segmentEventCounter{}
		bi.segmentCounters[segmentID] = counter
	}
	counter.nextDenseID = nextDenseID
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

// QueryIndexWithStats returns per-segment bitmaps of local IDs matching a key within a ledger range.
// Returns map[segmentID]*roaring.Bitmap so caller knows which segment each local ID belongs to.
// Uses SegmentLedgerMap for range trimming with dense sequential IDs.
// For topic lookups, isContract=false and pos specifies the topic position (0-3).
// lmCache is an optional shared cache for ledger maps — if non-nil, loaded maps are stored/reused
// across calls to avoid redundant loads when querying multiple terms over the same segments.
func (bi *eventBitmap32Index) QueryIndexWithStats(isContract bool, termKey [32]byte, pos int, startLedger, endLedger uint32, lmCache map[uint32]*SegmentLedgerMap) (map[uint32]*roaring.Bitmap, int64, int, time.Duration, time.Duration, error) {
	result := make(map[uint32]*roaring.Bitmap)
	var bytesRead int64
	var segmentsRead int
	var totalReadTime, totalDecodeTime time.Duration

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	for segID := startSegment; segID <= endSegment; segID++ {
		bitmap, segBytes, readTime, decodeTime, fromCache, err := bi.getSegmentWithStats(isContract, termKey, pos, segID)
		if err != nil {
			return nil, bytesRead, segmentsRead, totalReadTime, totalDecodeTime, err
		}

		bytesRead += segBytes
		totalReadTime += readTime
		totalDecodeTime += decodeTime
		if segBytes > 0 {
			segmentsRead++
		}

		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}

		// Trim to ledger range using dense ID boundaries from ledger map
		segmentStart := segID * SegmentSize

		// Compute ledger offsets within this segment
		var startOff uint16
		if startLedger > segmentStart {
			startOff = uint16(startLedger - segmentStart)
		}
		endOff := uint16(SegmentSize - 1)
		if endLedger < segmentStart+SegmentSize-1 {
			endOff = uint16(endLedger - segmentStart)
		}

		// Load ledger map for this segment to get dense ID range
		var startLocalID, endLocalID uint32
		needsTrim := startOff > 0 || endOff < uint16(SegmentSize-1)

		if needsTrim && bi.ledgerMapLoader != nil {
			// Check shared cache first
			var lm *SegmentLedgerMap
			if lmCache != nil {
				lm = lmCache[segID]
			}
			if lm == nil {
				lm, err = bi.ledgerMapLoader.LoadSegmentLedgerMap(segID)
				if err != nil {
					return nil, bytesRead, segmentsRead, totalReadTime, totalDecodeTime,
						fmt.Errorf("failed to load ledger map for segment %d: %w", segID, err)
				}
				if lmCache != nil && lm != nil {
					lmCache[segID] = lm
				}
			}
			if lm != nil {
				startLocalID, endLocalID = lm.LedgerRangeToIDRange(startOff, endOff)
			} else {
				// No ledger map available — cannot trim, include all
				endLocalID = bitmap.Maximum()
			}
		} else {
			// Full segment range — no trimming needed
			endLocalID = bitmap.Maximum()
		}

		// For hot-cache bitmaps, clone before mutating (shared reference).
		// For storage-loaded bitmaps, operate in-place (freshly loaded, not shared).
		trimmed := bitmap
		if fromCache {
			trimmed = bitmap.Clone()
		}
		if startLocalID > 0 {
			trimmed.RemoveRange(0, uint64(startLocalID))
		}
		if !trimmed.IsEmpty() {
			if maxVal := trimmed.Maximum(); endLocalID < maxVal {
				trimmed.RemoveRange(uint64(endLocalID)+1, uint64(maxVal)+1)
			}
		}

		if !trimmed.IsEmpty() {
			result[segID] = trimmed
		}
	}

	return result, bytesRead, segmentsRead, totalReadTime, totalDecodeTime, nil
}

// getSegmentWithStats retrieves a segment and tracks bytes read.
// Returns fromCache=true if the bitmap came from the hot cache (shared, must clone before mutating).
func (bi *eventBitmap32Index) getSegmentWithStats(isContract bool, termKey [32]byte, pos int, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, bool, error) {
	key := makeIndexKey(termKey, segmentID)

	// Check hot cache first - no disk read
	var segments map[[IndexKeySize]byte]*roaring.Bitmap
	if isContract {
		segments = bi.contracts
	} else {
		segments = bi.topicMapForPos(pos)
	}
	if segments != nil {
		if bitmap, exists := segments[key]; exists {
			return bitmap, 0, 0, 0, true, nil
		}
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		bm, bytes, rt, dt, err := bi.loader.LoadBitmap32SegmentWithTiming(isContract, termKey, pos, segmentID)
		return bm, bytes, rt, dt, false, err
	}

	return nil, 0, 0, 0, false, nil
}

// GetAndClearAllSegments gets and clears all hot segments and segment counters.
// Serializes bitmaps using MarshalBinary (standard format, compatible with FromBuffer).
// Also returns the segment event counters for ledger map flush.
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

	// Copy segment counters for ledger map flush, then reset only eventCounts
	// (nextDenseID must persist to ensure dense IDs continue after flush)
	counters := make(map[uint32]*segmentEventCounter)
	for segID, counter := range bi.segmentCounters {
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

// ClearAll clears all hot segments and segment counters.
func (bi *eventBitmap32Index) ClearAll() {
	bi.contracts = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic0 = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic1 = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic2 = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topic3 = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.segmentCounters = make(map[uint32]*segmentEventCounter)
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

// GetCurrentSegmentID returns the current segment ID being written to.
func (bi *eventBitmap32Index) GetCurrentSegmentID() uint32 {
	return bi.currentSegmentID
}

// =============================================================================
// Event Sequence Query Result (bitmap-level, index hits only)
// =============================================================================

// EventSeqQueryResult holds the result of a bitmap32 event key query including stats.
type EventSeqQueryResult struct {
	PerSegment    map[uint32]*roaring.Bitmap // segmentID -> local ID bitmap
	TotalCount    uint64
	BytesRead     int64
	Segments      int
	ReadTime      time.Duration
	DecodeTime    time.Duration
	IntersectTime time.Duration // Time spent on bitmap OR/AND operations
}
