package index

import (
	"time"

	"github.com/RoaringBitmap/roaring"
)

// Bitmap32 index prefixes (event-level granularity using 32-bit roaring)
// Uses sequential event IDs within segments for near-zero-cost FromBuffer decode.
const (
	PrefixEventContractBM32 byte = 0x20 // Contract ID -> local ID bitmap (32-bit)
	PrefixEventTopicBM32    byte = 0x21 // Topic (any position) -> local ID bitmap (32-bit)
)

// EventBitmap32SegmentLoader provides segment loading capability for bitmap32 queries.
type EventBitmap32SegmentLoader interface {
	// LoadEventBitmap32Segment loads a 32-bit roaring bitmap segment from storage.
	LoadEventBitmap32Segment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error)
	// LoadEventBitmap32SegmentWithTiming loads a segment and returns bytes read, read time, and decode time.
	LoadEventBitmap32SegmentWithTiming(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error)
}

// EventBitmap32Index manages segmented 32-bit roaring bitmap indexes for event-level granularity.
// Stores local IDs: (ledger_offset << 16) | event_seq within each segment.
// Uses roaring.Bitmap (32-bit) which supports FromBuffer for near-zero-cost decode.
type EventBitmap32Index struct {
	// Hot segment cache - maps key to bitmap of local IDs
	hotSegments map[[keySize]byte]*roaring.Bitmap

	// Current hot segment ID
	currentSegmentID uint32

	// Segment loader for queries
	loader EventBitmap32SegmentLoader
}

// NewEventBitmap32Index creates a new event-level bitmap32 index.
func NewEventBitmap32Index(loader EventBitmap32SegmentLoader) *EventBitmap32Index {
	return &EventBitmap32Index{
		hotSegments: make(map[[keySize]byte]*roaring.Bitmap),
		loader:      loader,
	}
}

// SetLoader sets the segment loader for query operations.
func (bi *EventBitmap32Index) SetLoader(loader EventBitmap32SegmentLoader) {
	bi.loader = loader
}

// AddToIndex adds a local ID to the index for a given key.
func (bi *EventBitmap32Index) AddToIndex(prefix byte, keyValue []byte, ledger uint32, eventSeq uint16) {
	segmentID := SegmentID(ledger)
	segmentStart := segmentID * SegmentSize
	ledgerOffset := uint16(ledger - segmentStart)
	localID := EncodeBitmap32LocalID(ledgerOffset, eventSeq)

	key := makeKey(prefix, keyValue, segmentID)

	bitmap, exists := bi.hotSegments[key]
	if !exists {
		bitmap = roaring.New()
		bi.hotSegments[key] = bitmap
	}

	bitmap.Add(localID)

	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// AddContractEvent adds an event to the contract index.
func (bi *EventBitmap32Index) AddContractEvent(contractID []byte, ledger uint32, eventSeq uint16) {
	bi.AddToIndex(PrefixEventContractBM32, contractID, ledger, eventSeq)
}

// AddTopicEvent adds an event to the topic index (non-positional).
func (bi *EventBitmap32Index) AddTopicEvent(topicValue []byte, ledger uint32, eventSeq uint16) {
	bi.AddToIndex(PrefixEventTopicBM32, topicValue, ledger, eventSeq)
}

// QueryIndexWithStats returns per-segment bitmaps of local IDs matching a key within a ledger range.
// Returns map[segmentID]*roaring.Bitmap so caller knows which segment each local ID belongs to.
func (bi *EventBitmap32Index) QueryIndexWithStats(prefix byte, keyValue []byte, startLedger, endLedger uint32) (map[uint32]*roaring.Bitmap, int64, int, time.Duration, time.Duration, error) {
	result := make(map[uint32]*roaring.Bitmap)
	var bytesRead int64
	var segmentsRead int
	var totalReadTime, totalDecodeTime time.Duration

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	for segID := startSegment; segID <= endSegment; segID++ {
		bitmap, segBytes, readTime, decodeTime, err := bi.getSegmentWithStats(prefix, keyValue, segID)
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

		// Trim to ledger range using local ID boundaries
		segmentStart := segID * SegmentSize

		// Compute local ID boundaries for ledger range
		var startLocalID, endLocalID uint32
		if startLedger > segmentStart {
			startLocalID = EncodeBitmap32LocalID(uint16(startLedger-segmentStart), 0)
		}
		if endLedger < segmentStart+SegmentSize-1 {
			endOffset := uint16(endLedger - segmentStart)
			endLocalID = EncodeBitmap32LocalID(endOffset, 0xFFFF)
		} else {
			endLocalID = EncodeBitmap32LocalID(uint16(SegmentSize-1), 0xFFFF)
		}

		// Clone and trim to range
		trimmed := bitmap.Clone()
		if startLocalID > 0 {
			trimmed.RemoveRange(0, uint64(startLocalID))
		}
		if maxVal := trimmed.Maximum(); endLocalID < maxVal {
			trimmed.RemoveRange(uint64(endLocalID)+1, uint64(maxVal)+1)
		}

		if !trimmed.IsEmpty() {
			result[segID] = trimmed
		}
	}

	return result, bytesRead, segmentsRead, totalReadTime, totalDecodeTime, nil
}

// getSegmentWithStats retrieves a segment and tracks bytes read.
func (bi *EventBitmap32Index) getSegmentWithStats(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	key := makeKey(prefix, keyValue, segmentID)

	// Check hot cache first - no disk read
	if bitmap, exists := bi.hotSegments[key]; exists {
		return bitmap, 0, 0, 0, nil
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadEventBitmap32SegmentWithTiming(prefix, keyValue, segmentID)
	}

	return nil, 0, 0, 0, nil
}

// GetAndClearAllSegments gets and clears all hot segments.
// Serializes bitmaps using MarshalBinary (standard format, compatible with FromBuffer).
func (bi *EventBitmap32Index) GetAndClearAllSegments() ([]EventSegment, error) {
	segments := make([]EventSegment, 0, len(bi.hotSegments))

	for key, bitmap := range bi.hotSegments {
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
		}
		keyCopy := make([]byte, keySize)
		copy(keyCopy, key[:])
		segments = append(segments, EventSegment{
			Key:  keyCopy,
			Data: data,
		})
	}

	bi.hotSegments = make(map[[keySize]byte]*roaring.Bitmap)

	return segments, nil
}

// ClearAll clears all hot segments.
func (bi *EventBitmap32Index) ClearAll() {
	bi.hotSegments = make(map[[keySize]byte]*roaring.Bitmap)
}

// GetHotSegmentStats returns statistics about hot segments.
func (bi *EventBitmap32Index) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	count = len(bi.hotSegments)
	for _, bitmap := range bi.hotSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	return
}

// GetCurrentSegmentID returns the current segment ID being written to.
func (bi *EventBitmap32Index) GetCurrentSegmentID() uint32 {
	return bi.currentSegmentID
}
