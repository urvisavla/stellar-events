package index

import (
	"encoding/binary"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// Event-level index prefixes (event-level granularity using Roaring64)
const (
	PrefixEventContract byte = 0x10
	PrefixEventTopic0   byte = 0x11
	PrefixEventTopic1   byte = 0x12
	PrefixEventTopic2   byte = 0x13
	PrefixEventTopic3   byte = 0x14
)

// EventSegmentLoader provides segment loading capability for event-level queries.
type EventSegmentLoader interface {
	// LoadEventSegment loads a Roaring64 bitmap segment from storage.
	LoadEventSegment(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, error)
}

// EventBitmapIndex manages segmented Roaring64 bitmap indexes for event-level granularity.
// Similar to BitmapIndex but stores 64-bit event keys instead of 32-bit ledger numbers.
type EventBitmapIndex struct {
	// Hot segment cache - maps key to bitmap of event keys
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

// AddToIndex adds an event key to the index.
func (bi *EventBitmapIndex) AddToIndex(prefix byte, keyValue []byte, ledger uint32, tx, op, evt uint16) {
	segmentID := SegmentID(ledger)
	eventKey := EncodeBitmapKey(ledger, tx, op, evt)

	key := makeKey(prefix, keyValue, segmentID)

	bitmap, exists := bi.hotSegments[key]
	if !exists {
		bitmap = roaring64.New()
		bi.hotSegments[key] = bitmap
	}

	bitmap.Add(eventKey)

	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// AddContractEvent adds an event to the contract index.
func (bi *EventBitmapIndex) AddContractEvent(contractID []byte, ledger uint32, tx, op, evt uint16) {
	bi.AddToIndex(PrefixEventContract, contractID, ledger, tx, op, evt)
}

// AddTopicEvent adds an event to a topic index.
func (bi *EventBitmapIndex) AddTopicEvent(topicPosition int, topicValue []byte, ledger uint32, tx, op, evt uint16) {
	prefix := EventTopicPrefix(topicPosition)
	if prefix == 0 {
		return
	}
	bi.AddToIndex(prefix, topicValue, ledger, tx, op, evt)
}

// QueryIndex returns all event keys matching a key within a ledger range.
func (bi *EventBitmapIndex) QueryIndex(prefix byte, keyValue []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, error) {
	result := roaring64.New()

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	for segID := startSegment; segID <= endSegment; segID++ {
		bitmap, err := bi.getSegment(prefix, keyValue, segID)
		if err != nil {
			return nil, err
		}

		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}

		// For event-level bitmaps, we need to filter by ledger range
		// Event keys encode ledger in high 32 bits
		startKey := EncodeBitmapKey(startLedger, 0, 0, 0)
		endKey := EncodeBitmapKey(endLedger, 0xFFFF, 0xFF, 0xFF)

		// Filter to range using iterator
		iter := bitmap.Iterator()
		for iter.HasNext() {
			eventKey := iter.Next()
			if eventKey >= startKey && eventKey <= endKey {
				result.Add(eventKey)
			}
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

// QueryTopicEvents queries event keys for a topic value.
func (bi *EventBitmapIndex) QueryTopicEvents(topicPosition int, topicValue []byte, startLedger, endLedger uint32) (*roaring64.Bitmap, error) {
	prefix := EventTopicPrefix(topicPosition)
	if prefix == 0 {
		return nil, nil
	}
	return bi.QueryIndex(prefix, topicValue, startLedger, endLedger)
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

// EventTopicPrefix returns the event-level prefix for a topic position.
func EventTopicPrefix(position int) byte {
	switch position {
	case 0:
		return PrefixEventTopic0
	case 1:
		return PrefixEventTopic1
	case 2:
		return PrefixEventTopic2
	case 3:
		return PrefixEventTopic3
	default:
		return 0
	}
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
