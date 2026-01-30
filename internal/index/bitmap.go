package index

import (
	"encoding/binary"

	"github.com/RoaringBitmap/roaring"
)

// Key size: 1 (prefix) + 32 (keyValue) + 4 (segmentID/ledgerSeq) = 37 bytes
const keySize = 37

// =============================================================================
// Segment Loader Interface
// =============================================================================

// SegmentLoader provides segment loading capability for queries.
// This is implemented by the storage layer (e.g., RocksDBIndexStore).
type SegmentLoader interface {
	// LoadSegment loads a bitmap segment from storage.
	// Returns nil, nil if segment doesn't exist.
	LoadSegment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error)
}

// =============================================================================
// Bitmap Index (Pure In-Memory)
// =============================================================================

// BitmapIndex manages segmented roaring bitmap indexes in memory.
// It provides Add operations for building indexes and Query operations
// that combine in-memory hot segments with persisted segments via SegmentLoader.
//
// This implementation is NOT thread-safe. Callers must ensure single-threaded
// access during ingestion, which is the normal case for the pipeline collector.
type BitmapIndex struct {
	// Hot segment cache - maps key to bitmap of ledgers
	// Key format: <prefix:1><keyValue:32><segmentID:4>
	hotSegments map[[keySize]byte]*roaring.Bitmap

	// Current hot segment ID (the one being actively written)
	currentSegmentID uint32

	// Segment loader for queries (optional, can be nil for write-only mode)
	loader SegmentLoader
}

// NewBitmapIndex creates a new in-memory bitmap index.
// The loader parameter is optional and used for queries to load persisted segments.
func NewBitmapIndex(loader SegmentLoader) *BitmapIndex {
	return &BitmapIndex{
		hotSegments: make(map[[keySize]byte]*roaring.Bitmap),
		loader:      loader,
	}
}

// SetLoader sets the segment loader for query operations.
func (bi *BitmapIndex) SetLoader(loader SegmentLoader) {
	bi.loader = loader
}

// =============================================================================
// Key Generation
// =============================================================================

// MakeL1Key creates a database key for an L1 bitmap segment.
// Format: <prefix:1><keyValue:32><segmentID:4>
// This allocates and is used for storage layer operations.
func MakeL1Key(prefix byte, keyValue []byte, segmentID uint32) []byte {
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

// makeKey builds a key in place without allocation.
// Used internally for map lookups.
func makeKey(prefix byte, keyValue []byte, id uint32) [keySize]byte {
	var key [keySize]byte
	key[0] = prefix
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}
	binary.BigEndian.PutUint32(key[33:37], id)
	return key
}

// =============================================================================
// L1 Index Operations (Ledger-level)
// =============================================================================

// AddToIndex adds a ledger to the L1 bitmap index for a given key.
func (bi *BitmapIndex) AddToIndex(prefix byte, keyValue []byte, ledgerSeq uint32) {
	segmentID := SegmentID(ledgerSeq)
	localLedger := ledgerSeq % SegmentSize

	key := makeKey(prefix, keyValue, segmentID)

	bitmap, exists := bi.hotSegments[key]
	if !exists {
		bitmap = roaring.New()
		bi.hotSegments[key] = bitmap
	}

	bitmap.Add(localLedger)

	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// AddContractIndex adds a ledger to the contract ID L1 index.
func (bi *BitmapIndex) AddContractIndex(contractID []byte, ledgerSeq uint32) {
	bi.AddToIndex(PrefixContractIndex, contractID, ledgerSeq)
}

// AddTopicIndex adds a ledger to a topic L1 index.
func (bi *BitmapIndex) AddTopicIndex(topicPosition int, topicValue []byte, ledgerSeq uint32) {
	var prefix byte
	switch topicPosition {
	case 0:
		prefix = PrefixTopic0Index
	case 1:
		prefix = PrefixTopic1Index
	case 2:
		prefix = PrefixTopic2Index
	case 3:
		prefix = PrefixTopic3Index
	default:
		return // Invalid topic position
	}
	bi.AddToIndex(prefix, topicValue, ledgerSeq)
}

// =============================================================================
// Query Operations
// =============================================================================

// QueryIndex returns all ledgers matching a key within a range.
// Combines hot segments with persisted segments via the loader.
func (bi *BitmapIndex) QueryIndex(prefix byte, keyValue []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	result := roaring.New()

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	for segID := startSegment; segID <= endSegment; segID++ {
		bitmap, err := bi.getL1Segment(prefix, keyValue, segID)
		if err != nil {
			return nil, err
		}

		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}

		segmentBase := segID * SegmentSize

		// Apply range mask for partial segments (first and/or last segment)
		isFirstSegment := segID == startSegment
		isLastSegment := segID == endSegment
		needsRangeMask := isFirstSegment || isLastSegment

		if needsRangeMask {
			// Calculate local range within this segment
			localStart := uint64(0)
			localEnd := uint64(SegmentSize)

			if isFirstSegment && startLedger > segmentBase {
				localStart = uint64(startLedger - segmentBase)
			}
			if isLastSegment {
				segmentEnd := segmentBase + SegmentSize - 1
				if endLedger < segmentEnd {
					localEnd = uint64(endLedger - segmentBase + 1)
				}
			}

			// Create range mask and apply with AND
			mask := roaring.New()
			mask.AddRange(localStart, localEnd)
			bitmap = roaring.And(bitmap, mask)
		}

		if bitmap.IsEmpty() {
			continue
		}

		// Convert local offsets to absolute ledger numbers and merge
		localValues := bitmap.ToArray()
		absoluteValues := make([]uint32, len(localValues))
		for i, local := range localValues {
			absoluteValues[i] = segmentBase + local
		}

		segmentResult := roaring.New()
		segmentResult.AddMany(absoluteValues)
		result.Or(segmentResult)
	}

	return result, nil
}

// getL1Segment retrieves an L1 segment from hot cache or storage.
func (bi *BitmapIndex) getL1Segment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error) {
	key := makeKey(prefix, keyValue, segmentID)

	// Check hot cache first
	if bitmap, exists := bi.hotSegments[key]; exists {
		return bitmap, nil
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadSegment(prefix, keyValue, segmentID)
	}

	return nil, nil
}


// QueryContractIndex queries ledgers for a contract ID.
func (bi *BitmapIndex) QueryContractIndex(contractID []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	return bi.QueryIndex(PrefixContractIndex, contractID, startLedger, endLedger)
}

// QueryTopicIndex queries ledgers for a topic value.
func (bi *BitmapIndex) QueryTopicIndex(topicPosition int, topicValue []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	prefix := TopicL1Prefix(topicPosition)
	if prefix == 0 {
		return nil, nil
	}
	return bi.QueryIndex(prefix, topicValue, startLedger, endLedger)
}

// =============================================================================
// Serialization (for storage implementations)
// =============================================================================

// Segment represents a serializable bitmap segment.
type Segment struct {
	Key  []byte
	Data []byte
}

// GetAndClearAllSegments gets and clears all hot segments.
func (bi *BitmapIndex) GetAndClearAllSegments() ([]Segment, error) {
	segments := make([]Segment, 0, len(bi.hotSegments))

	// Serialize segments
	for key, bitmap := range bi.hotSegments {
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
		}
		// Copy key to slice for storage
		keyCopy := make([]byte, keySize)
		copy(keyCopy, key[:])
		segments = append(segments, Segment{
			Key:  keyCopy,
			Data: data,
		})
	}

	// Clear hot segments
	bi.hotSegments = make(map[[keySize]byte]*roaring.Bitmap)

	return segments, nil
}

// ClearAll clears all hot segments.
func (bi *BitmapIndex) ClearAll() {
	bi.hotSegments = make(map[[keySize]byte]*roaring.Bitmap)
}

// =============================================================================
// Statistics
// =============================================================================

// GetHotSegmentStats returns statistics about hot segments.
func (bi *BitmapIndex) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	count = len(bi.hotSegments)
	for _, bitmap := range bi.hotSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	return
}

// GetCurrentSegmentID returns the current segment ID being written to.
func (bi *BitmapIndex) GetCurrentSegmentID() uint32 {
	return bi.currentSegmentID
}

// =============================================================================
// Helper Functions
// =============================================================================

// TopicL1Prefix returns the L1 prefix for a topic position.
func TopicL1Prefix(position int) byte {
	switch position {
	case 0:
		return PrefixTopic0Index
	case 1:
		return PrefixTopic1Index
	case 2:
		return PrefixTopic2Index
	case 3:
		return PrefixTopic3Index
	default:
		return 0
	}
}
