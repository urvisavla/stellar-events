package index

import (
	"encoding/binary"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// TOID-level index prefixes (operation-level granularity using Roaring64)
// Matches posting list structure: one index for contracts, one for topics (non-positional)
// Each TOID represents a unique operation; events are fetched by iterating the operation's events.
const (
	PrefixEventContract byte = 0x10 // Contract ID → TOID bitmap
	PrefixEventTopic    byte = 0x11 // Topic (any position) → TOID bitmap
)

// EventSegmentLoader provides segment loading capability for event-level queries.
type EventSegmentLoader interface {
	// LoadEventSegment loads a Roaring64 bitmap segment from storage.
	LoadEventSegment(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, error)
	// LoadEventSegmentWithStats loads a segment and returns bytes read.
	LoadEventSegmentWithStats(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, int64, error)
	// LoadEventSegmentWithTiming loads a segment and returns bytes read, read time, and decode time.
	LoadEventSegmentWithTiming(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, int64, time.Duration, time.Duration, error)
}

// EventBitmapIndex manages segmented Roaring64 bitmap indexes for TOID-level granularity.
// Stores 64-bit TOIDs (operation identifiers), same granularity as posting lists.
type EventBitmapIndex struct {
	// Hot segment cache - maps key to bitmap of TOIDs
	hotSegments map[[keySize]byte]*roaring64.Bitmap

	// Current hot segment ID
	currentSegmentID uint32

	// Segment loader for queries
	loader EventSegmentLoader
}

// NewEventBitmapIndex creates a new event-level bitmap index.
func NewEventBitmapIndex(loader EventSegmentLoader) *EventBitmapIndex {
	return &EventBitmapIndex{
		hotSegments: make(map[[keySize]byte]*roaring64.Bitmap),
		loader:      loader,
	}
}

// SetLoader sets the segment loader for query operations.
func (bi *EventBitmapIndex) SetLoader(loader EventSegmentLoader) {
	bi.loader = loader
}

// AddToIndex adds a TOID to the index.
// The evt parameter is accepted for API compatibility but ignored (bitmap stores at TOID level).
func (bi *EventBitmapIndex) AddToIndex(prefix byte, keyValue []byte, ledger uint32, tx, op, evt uint16) {
	segmentID := SegmentID(ledger)
	toid := EncodeBitmapKey(ledger, tx, op, evt) // evt is ignored, stores TOID

	key := makeKey(prefix, keyValue, segmentID)

	bitmap, exists := bi.hotSegments[key]
	if !exists {
		bitmap = roaring64.New()
		bi.hotSegments[key] = bitmap
	}

	bitmap.Add(toid)

	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// AddContractEvent adds an event to the contract index.
func (bi *EventBitmapIndex) AddContractEvent(contractID []byte, ledger uint32, tx, op, evt uint16) {
	bi.AddToIndex(PrefixEventContract, contractID, ledger, tx, op, evt)
}

// AddTopicEvent adds an event to the topic index (non-positional).
// All topics are indexed regardless of their position, matching posting list semantics.
func (bi *EventBitmapIndex) AddTopicEvent(topicValue []byte, ledger uint32, tx, op, evt uint16) {
	bi.AddToIndex(PrefixEventTopic, topicValue, ledger, tx, op, evt)
}

// QueryIndex returns all event keys matching a key within a ledger range.
func (bi *EventBitmapIndex) QueryIndex(prefix byte, keyValue []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, error) {
	result := roaring64.New()

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	// Filter by ledger range using TOID boundaries (loop-invariant)
	startKey := EncodeTOID(startLedger, 0, 0)
	endKey := EncodeTOID(endLedger, 0xFFFFF, 0xFFF)

	for segID := startSegment; segID <= endSegment; segID++ {
		bitmap, err := bi.getSegment(prefix, keyValue, segID)
		if err != nil {
			return nil, err
		}

		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}

		result.Or(bitmap)
	}

	// Trim to ledger range using native bitmap operations
	if !result.IsEmpty() {
		if startKey > 0 {
			result.RemoveRange(0, startKey)
		}
		if maxVal := result.Maximum(); endKey < maxVal {
			result.RemoveRange(endKey+1, maxVal+1)
		}
	}

	return result, nil
}

// getSegment retrieves a segment from hot cache or storage.
func (bi *EventBitmapIndex) getSegment(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, error) {
	key := makeKey(prefix, keyValue, segmentID)

	// Check hot cache first
	if bitmap, exists := bi.hotSegments[key]; exists {
		return bitmap, nil
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadEventSegment(prefix, keyValue, segmentID)
	}

	return nil, nil
}

// QueryContractEvents queries event keys for a contract ID.
func (bi *EventBitmapIndex) QueryContractEvents(contractID []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, error) {
	return bi.QueryIndex(PrefixEventContract, contractID, startLedger, endLedger)
}

// QueryTopicEvents queries event keys for a topic value (non-positional).
func (bi *EventBitmapIndex) QueryTopicEvents(topicValue []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, error) {
	return bi.QueryIndex(PrefixEventTopic, topicValue, startLedger, endLedger)
}

// QueryIndexWithStats returns event keys and tracks bytes read, read time, and decode time from storage.
func (bi *EventBitmapIndex) QueryIndexWithStats(prefix byte, keyValue []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, int64, int, time.Duration, time.Duration, error) {
	result := roaring64.New()
	var bytesRead int64
	var segmentsRead int
	var totalReadTime, totalDecodeTime time.Duration

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	// Filter by ledger range using TOID boundaries (loop-invariant)
	startKey := EncodeTOID(startLedger, 0, 0)
	endKey := EncodeTOID(endLedger, 0xFFFFF, 0xFFF)

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

		result.Or(bitmap)
	}

	// Trim to ledger range using native bitmap operations
	if !result.IsEmpty() {
		if startKey > 0 {
			result.RemoveRange(0, startKey)
		}
		if maxVal := result.Maximum(); endKey < maxVal {
			result.RemoveRange(endKey+1, maxVal+1)
		}
	}

	return result, bytesRead, segmentsRead, totalReadTime, totalDecodeTime, nil
}

// getSegmentWithStats retrieves a segment and tracks bytes read.
func (bi *EventBitmapIndex) getSegmentWithStats(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, int64, time.Duration, time.Duration, error) {
	key := makeKey(prefix, keyValue, segmentID)

	// Check hot cache first - no disk read
	if bitmap, exists := bi.hotSegments[key]; exists {
		return bitmap, 0, 0, 0, nil
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadEventSegmentWithTiming(prefix, keyValue, segmentID)
	}

	return nil, 0, 0, 0, nil
}

// QueryContractEventsWithStats queries event keys for a contract ID with byte tracking.
func (bi *EventBitmapIndex) QueryContractEventsWithStats(contractID []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, int64, int, time.Duration, time.Duration, error) {
	return bi.QueryIndexWithStats(PrefixEventContract, contractID, startLedger, endLedger)
}

// QueryTopicEventsWithStats queries event keys for a topic with byte tracking.
func (bi *EventBitmapIndex) QueryTopicEventsWithStats(topicValue []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, int64, int, time.Duration, time.Duration, error) {
	return bi.QueryIndexWithStats(PrefixEventTopic, topicValue, startLedger, endLedger)
}

// EventSegment represents a serializable event bitmap segment.
type EventSegment struct {
	Key  []byte
	Data []byte
}

// GetAndClearAllSegments gets and clears all hot segments.
func (bi *EventBitmapIndex) GetAndClearAllSegments() ([]EventSegment, error) {
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

	bi.hotSegments = make(map[[keySize]byte]*roaring64.Bitmap)

	return segments, nil
}

// ClearAll clears all hot segments.
func (bi *EventBitmapIndex) ClearAll() {
	bi.hotSegments = make(map[[keySize]byte]*roaring64.Bitmap)
}

// GetHotSegmentStats returns statistics about hot segments.
func (bi *EventBitmapIndex) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	count = len(bi.hotSegments)
	for _, bitmap := range bi.hotSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	return
}

// GetCurrentSegmentID returns the current segment ID being written to.
func (bi *EventBitmapIndex) GetCurrentSegmentID() uint32 {
	return bi.currentSegmentID
}

// MakeEventKey creates a database key for an event bitmap segment.
// Uses the same format as MakeL1Key for consistency.
func MakeEventKey(prefix byte, keyValue []byte, segmentID uint32) []byte {
	key := make([]byte, keySize)
	key[0] = prefix
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}
	binary.BigEndian.PutUint32(key[33:37], segmentID)
	return key
}
