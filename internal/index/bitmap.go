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
	// LoadL1Segment loads an L1 bitmap segment from storage.
	// Returns nil, nil if segment doesn't exist.
	LoadL1Segment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error)

	// LoadL2Segment loads an L2 bitmap segment from storage.
	// Returns nil, nil if segment doesn't exist.
	LoadL2Segment(prefix byte, keyValue []byte, ledgerSeq uint32) (*roaring.Bitmap, error)
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
	// Level 1: Hot segment cache - maps key to bitmap of ledgers
	// Key format: <prefix:1><keyValue:32><segmentID:4>
	hotSegments map[[keySize]byte]*roaring.Bitmap

	// Level 2: Ledger-centric hot cache for efficient ingestion
	// Only keeps data for the current ledger, flushes when ledger changes
	// Key format: <prefix:1><keyValue:32> (no ledger in key - it's tracked separately)
	currentL2Ledger uint32
	currentL2Map    map[[l2KeySize]byte]*roaring.Bitmap
	// Accumulated L2 segments ready for flush
	pendingL2Segments []Segment

	// Current hot segment ID (the one being actively written)
	currentSegmentID uint32

	// Segment loader for queries (optional, can be nil for write-only mode)
	loader SegmentLoader
}

// l2KeySize is prefix (1) + keyValue (32) = 33 bytes (no ledger in map key)
const l2KeySize = 33

// NewBitmapIndex creates a new in-memory bitmap index.
// The loader parameter is optional and used for queries to load persisted segments.
func NewBitmapIndex(loader SegmentLoader) *BitmapIndex {
	return &BitmapIndex{
		hotSegments:  make(map[[keySize]byte]*roaring.Bitmap),
		currentL2Map: make(map[[l2KeySize]byte]*roaring.Bitmap),
		loader:       loader,
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

// MakeL2Key creates a database key for an L2 bitmap.
// Format: <L2prefix:1><keyValue:32><ledger:4>
// This allocates and is used for storage layer operations.
func MakeL2Key(prefix byte, keyValue []byte, ledgerSeq uint32) []byte {
	key := make([]byte, keySize)
	key[0] = prefix
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}
	binary.BigEndian.PutUint32(key[33:37], ledgerSeq)
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
// L2 Index Operations (Event-level)
// =============================================================================

// AddToL2Index adds an event index to the L2 bitmap for a given key and ledger.
// eventIndex encodes tx:op:event as a single uint32.
// Uses ledger-centric batching: when ledger changes, flushes previous ledger's data.
func (bi *BitmapIndex) AddToL2Index(prefix byte, keyValue []byte, ledgerSeq uint32, eventIndex uint32) {
	// Check if we've moved to a new ledger
	if ledgerSeq != bi.currentL2Ledger && bi.currentL2Ledger != 0 {
		bi.flushCurrentL2Ledger()
	}
	bi.currentL2Ledger = ledgerSeq

	// Use smaller key (no ledger) for current ledger's map
	key := makeL2Key(prefix, keyValue)

	bitmap, exists := bi.currentL2Map[key]
	if !exists {
		bitmap = roaring.New()
		bi.currentL2Map[key] = bitmap
	}

	bitmap.Add(eventIndex)
}

// makeL2Key creates a key for the current ledger's L2 map (no ledger in key).
func makeL2Key(prefix byte, keyValue []byte) [l2KeySize]byte {
	var key [l2KeySize]byte
	key[0] = prefix
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}
	return key
}

// flushCurrentL2Ledger serializes the current ledger's L2 data to pending segments.
func (bi *BitmapIndex) flushCurrentL2Ledger() {
	if len(bi.currentL2Map) == 0 {
		return
	}

	ledger := bi.currentL2Ledger
	for key, bitmap := range bi.currentL2Map {
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			continue // Skip on error
		}

		// Build full key: prefix + keyValue + ledger
		fullKey := make([]byte, keySize)
		copy(fullKey[0:l2KeySize], key[:])
		binary.BigEndian.PutUint32(fullKey[33:37], ledger)

		bi.pendingL2Segments = append(bi.pendingL2Segments, Segment{
			Key:  fullKey,
			Data: data,
		})
	}

	// Clear for next ledger - reuse map to avoid allocation
	for k := range bi.currentL2Map {
		delete(bi.currentL2Map, k)
	}
}

// AddContractL2Index adds an event to the contract ID L2 index.
func (bi *BitmapIndex) AddContractL2Index(contractID []byte, ledgerSeq uint32, txIndex, opIndex, eventIndex uint16) {
	encoded := EncodeEventIndex(txIndex, opIndex, eventIndex)
	bi.AddToL2Index(PrefixContractL2, contractID, ledgerSeq, encoded)
}

// AddTopicL2Index adds an event to a topic L2 index.
func (bi *BitmapIndex) AddTopicL2Index(topicPosition int, topicValue []byte, ledgerSeq uint32, txIndex, opIndex, eventIndex uint16) {
	var prefix byte
	switch topicPosition {
	case 0:
		prefix = PrefixTopic0L2
	case 1:
		prefix = PrefixTopic1L2
	case 2:
		prefix = PrefixTopic2L2
	case 3:
		prefix = PrefixTopic3L2
	default:
		return // Invalid topic position
	}
	encoded := EncodeEventIndex(txIndex, opIndex, eventIndex)
	bi.AddToL2Index(prefix, topicValue, ledgerSeq, encoded)
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
		return bi.loader.LoadL1Segment(prefix, keyValue, segmentID)
	}

	return nil, nil
}

// QueryL2Index returns event indices for a specific ledger.
func (bi *BitmapIndex) QueryL2Index(prefix byte, keyValue []byte, ledgerSeq uint32) (*roaring.Bitmap, error) {
	// Check if this is the current ledger being indexed
	if ledgerSeq == bi.currentL2Ledger {
		l2Key := makeL2Key(prefix, keyValue)
		if bitmap, exists := bi.currentL2Map[l2Key]; exists {
			return bitmap, nil
		}
	}

	// Check pending segments (already serialized but not yet flushed to storage)
	fullKey := MakeL2Key(prefix, keyValue, ledgerSeq)
	for _, seg := range bi.pendingL2Segments {
		if string(seg.Key) == string(fullKey) {
			bitmap := roaring.New()
			if err := bitmap.UnmarshalBinary(seg.Data); err == nil {
				return bitmap, nil
			}
		}
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadL2Segment(prefix, keyValue, ledgerSeq)
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

// QueryIndexHierarchical performs a two-level query: L1 for ledgers, L2 for events.
// If limit > 0, stops collecting event keys once limit is reached (early termination).
func (bi *BitmapIndex) QueryIndexHierarchical(l1Prefix, l2Prefix byte, keyValue []byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error) {
	// Phase 1: Query L1 to get matching ledgers
	ledgerBitmap, err := bi.QueryIndex(l1Prefix, keyValue, startLedger, endLedger)
	if err != nil {
		return nil, 0, err
	}

	matchingLedgers := int(ledgerBitmap.GetCardinality())
	if ledgerBitmap.IsEmpty() {
		return nil, matchingLedgers, nil
	}

	// Phase 2: For each matching ledger, query L2 to get event indices
	var eventKeys []EventKey
	ledgers := ledgerBitmap.ToArray()

	for _, ledgerSeq := range ledgers {
		// Early termination if we have enough events
		if limit > 0 && len(eventKeys) >= limit {
			break
		}

		l2Bitmap, err := bi.QueryL2Index(l2Prefix, keyValue, ledgerSeq)
		if err != nil {
			return nil, matchingLedgers, err
		}

		if l2Bitmap == nil || l2Bitmap.IsEmpty() {
			continue
		}

		// Convert L2 bitmap to event keys
		eventIndices := l2Bitmap.ToArray()
		for _, encodedIdx := range eventIndices {
			if limit > 0 && len(eventKeys) >= limit {
				break
			}
			eventKeys = append(eventKeys, EventKey{
				LedgerSeq:  ledgerSeq,
				EventIndex: encodedIdx,
			})
		}
	}

	return eventKeys, matchingLedgers, nil
}

// QueryContractIndexHierarchical queries using hierarchical approach for contract ID.
func (bi *BitmapIndex) QueryContractIndexHierarchical(contractID []byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error) {
	return bi.QueryIndexHierarchical(PrefixContractIndex, PrefixContractL2, contractID, startLedger, endLedger, limit)
}

// QueryTopicIndexHierarchical queries using hierarchical approach for topics.
func (bi *BitmapIndex) QueryTopicIndexHierarchical(topicPosition int, topicValue []byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error) {
	l1Prefix := TopicL1Prefix(topicPosition)
	l2Prefix := TopicL2Prefix(topicPosition)
	if l1Prefix == 0 || l2Prefix == 0 {
		return nil, 0, nil
	}
	return bi.QueryIndexHierarchical(l1Prefix, l2Prefix, topicValue, startLedger, endLedger, limit)
}

// =============================================================================
// Serialization (for storage implementations)
// =============================================================================

// Segment represents a serializable bitmap segment.
type Segment struct {
	Key  []byte
	Data []byte
}

// FlushResult holds the results of a combined L1+L2 flush operation.
type FlushResult struct {
	L1Segments []Segment
	L2Segments []Segment
}

// GetAndClearAllSegments gets and clears both L1 and L2 segments.
func (bi *BitmapIndex) GetAndClearAllSegments() (*FlushResult, error) {
	// Flush current L2 ledger first
	bi.flushCurrentL2Ledger()

	result := &FlushResult{
		L1Segments: make([]Segment, 0, len(bi.hotSegments)),
		L2Segments: bi.pendingL2Segments, // Take accumulated L2 segments
	}

	// Serialize L1 segments
	for key, bitmap := range bi.hotSegments {
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
		}
		// Copy key to slice for storage
		keyCopy := make([]byte, keySize)
		copy(keyCopy, key[:])
		result.L1Segments = append(result.L1Segments, Segment{
			Key:  keyCopy,
			Data: data,
		})
	}

	// Clear L1 and reset L2
	bi.hotSegments = make(map[[keySize]byte]*roaring.Bitmap)
	bi.pendingL2Segments = nil
	bi.currentL2Ledger = 0

	return result, nil
}

// ClearAll clears all hot segments.
func (bi *BitmapIndex) ClearAll() {
	bi.hotSegments = make(map[[keySize]byte]*roaring.Bitmap)
	bi.currentL2Map = make(map[[l2KeySize]byte]*roaring.Bitmap)
	bi.pendingL2Segments = nil
	bi.currentL2Ledger = 0
}

// =============================================================================
// Statistics
// =============================================================================

// GetHotSegmentStats returns statistics about L1 hot segments.
func (bi *BitmapIndex) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	count = len(bi.hotSegments)
	for _, bitmap := range bi.hotSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	return
}

// GetL2Stats returns statistics about L2 hot segments.
func (bi *BitmapIndex) GetL2Stats() (count int, totalCards uint64) {
	// Count current ledger's map entries
	count = len(bi.currentL2Map)
	for _, bm := range bi.currentL2Map {
		totalCards += bm.GetCardinality()
	}
	// Add pending segments count
	count += len(bi.pendingL2Segments)
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

// TopicL2Prefix returns the L2 prefix for a topic position.
func TopicL2Prefix(position int) byte {
	switch position {
	case 0:
		return PrefixTopic0L2
	case 1:
		return PrefixTopic1L2
	case 2:
		return PrefixTopic2L2
	case 3:
		return PrefixTopic3L2
	default:
		return 0
	}
}
