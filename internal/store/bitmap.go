package store

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Bitmap Engine Types (moved from index/bitmap.go)
// =============================================================================

// eventSegment represents a serializable bitmap segment.
type eventSegment struct {
	Key        []byte
	Data       []byte
	IsContract bool // true = contract CF, false = topic CF
}

// bitmap32Loader provides segment loading capability for bitmap32 queries.
type bitmap32Loader interface {
	// LoadBitmap32Segment loads a 32-bit roaring bitmap segment from storage.
	LoadBitmap32Segment(isContract bool, termKey [16]byte, segmentID uint32) (*roaring.Bitmap, error)
	// LoadBitmap32SegmentWithTiming loads a segment and returns bytes read, read time, and decode time.
	LoadBitmap32SegmentWithTiming(isContract bool, termKey [16]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error)
}

// makeIndexKey creates a 20-byte index key from a truncated term key and bucket ID.
// Format: [term_key:16][bucket_id:4]
func makeIndexKey(termKey [16]byte, bucketID uint32) [IndexKeySize]byte {
	var key [IndexKeySize]byte
	copy(key[0:16], termKey[:])
	binary.BigEndian.PutUint32(key[16:20], bucketID)
	return key
}

// ledgerMapLoader loads a BucketLedgerMap for a given bucket from storage.
type ledgerMapLoader interface {
	LoadBucketLedgerMap(bucketID uint32) (*BucketLedgerMap, error)
}

// eventBitmap32Index manages segmented 32-bit roaring bitmap indexes for event-level granularity.
// Uses dense sequential local IDs within each bucket for compact bitmap representation.
// Uses roaring.Bitmap (32-bit) which supports FromBuffer for near-zero-cost decode.
type eventBitmap32Index struct {
	// Hot segment caches - separate maps for contract and topic
	contractSegments map[[IndexKeySize]byte]*roaring.Bitmap
	topicSegments    map[[IndexKeySize]byte]*roaring.Bitmap

	// Current hot segment ID
	currentBucketID uint32

	// Segment loader for queries
	loader bitmap32Loader

	// Dense ID assignment: per-bucket event counters
	bucketCounters map[uint32]*bucketEventCounter

	// Ledger map loader for queries (range trimming)
	ledgerMapLoader ledgerMapLoader

	// topicTermPositions tracks which topic position (0-3) each term hash belongs to.
	// Populated during AddTopicEvent, used by WriteBucketDir to partition topics
	// into per-position flat files (topic0.idx, topic1.idx, etc.).
	topicTermPositions map[[16]byte]byte
}

// newEventBitmap32Index creates a new event-level bitmap32 index.
func newEventBitmap32Index(loader bitmap32Loader, lmLoader ledgerMapLoader) *eventBitmap32Index {
	return &eventBitmap32Index{
		contractSegments:   make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topicSegments:      make(map[[IndexKeySize]byte]*roaring.Bitmap),
		loader:             loader,
		bucketCounters:     make(map[uint32]*bucketEventCounter),
		ledgerMapLoader:    lmLoader,
		topicTermPositions: make(map[[16]byte]byte),
	}
}

// AssignDenseLocalID assigns the next dense local ID for an event within a bucket.
// Returns the dense ID to use in bitmap/posting list indexes.
func (bi *eventBitmap32Index) AssignDenseLocalID(bucketID uint32, ledger uint32) uint32 {
	counter, exists := bi.bucketCounters[bucketID]
	if !exists {
		counter = &bucketEventCounter{}
		bi.bucketCounters[bucketID] = counter
	}
	ledgerOffset := uint16(ledger - bucketID*BucketSize)
	return counter.assignDenseID(ledgerOffset)
}

// InitializeDenseCounter loads the ledger map for a bucket from storage and sets
// the nextDenseID to continue from the persisted state. Called on startup.
func (bi *eventBitmap32Index) InitializeDenseCounter(bucketID uint32, nextDenseID uint32) {
	counter, exists := bi.bucketCounters[bucketID]
	if !exists {
		counter = &bucketEventCounter{}
		bi.bucketCounters[bucketID] = counter
	}
	counter.nextDenseID = nextDenseID
}

// AddContractEvent adds an event to the contract index using a dense local ID.
func (bi *eventBitmap32Index) AddContractEvent(contractID []byte, bucketID uint32, denseLocalID uint32) {
	termKey := ContractTermKey(contractID)
	key := makeIndexKey(termKey, bucketID)

	bitmap, exists := bi.contractSegments[key]
	if !exists {
		bitmap = roaring.New()
		bi.contractSegments[key] = bitmap
	}

	bitmap.Add(denseLocalID)

	if bucketID > bi.currentBucketID {
		bi.currentBucketID = bucketID
	}
}

// AddTopicEvent adds an event to the topic index (positional) using a dense local ID.
func (bi *eventBitmap32Index) AddTopicEvent(pos int, topicValue []byte, bucketID uint32, denseLocalID uint32) {
	termKey := TopicTermKey(pos, topicValue)
	key := makeIndexKey(termKey, bucketID)

	bitmap, exists := bi.topicSegments[key]
	if !exists {
		bitmap = roaring.New()
		bi.topicSegments[key] = bitmap
	}

	bitmap.Add(denseLocalID)

	// Track term hash → position for flat file partitioning
	bi.topicTermPositions[termKey] = byte(pos)

	if bucketID > bi.currentBucketID {
		bi.currentBucketID = bucketID
	}
}

// QueryIndexWithStats returns per-segment bitmaps of local IDs matching a key within a ledger range.
// Returns map[segmentID]*roaring.Bitmap so caller knows which segment each local ID belongs to.
// Uses BucketLedgerMap for range trimming with dense sequential IDs.
func (bi *eventBitmap32Index) QueryIndexWithStats(isContract bool, termKey [16]byte, startLedger, endLedger uint32) (map[uint32]*roaring.Bitmap, int64, int, time.Duration, time.Duration, error) {
	result := make(map[uint32]*roaring.Bitmap)
	var bytesRead int64
	var segmentsRead int
	var totalReadTime, totalDecodeTime time.Duration

	startSegment := BucketID(startLedger)
	endSegment := BucketID(endLedger)

	for segID := startSegment; segID <= endSegment; segID++ {
		bitmap, segBytes, readTime, decodeTime, err := bi.getSegmentWithStats(isContract, termKey, segID)
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
		segmentStart := segID * BucketSize

		// Compute ledger offsets within this bucket
		var startOff uint16
		if startLedger > segmentStart {
			startOff = uint16(startLedger - segmentStart)
		}
		endOff := uint16(BucketSize - 1)
		if endLedger < segmentStart+BucketSize-1 {
			endOff = uint16(endLedger - segmentStart)
		}

		// Load ledger map for this bucket to get dense ID range
		var startLocalID, endLocalID uint32
		needsTrim := startOff > 0 || endOff < uint16(BucketSize-1)

		if needsTrim && bi.ledgerMapLoader != nil {
			lm, err := bi.ledgerMapLoader.LoadBucketLedgerMap(segID)
			if err != nil {
				return nil, bytesRead, segmentsRead, totalReadTime, totalDecodeTime,
					fmt.Errorf("failed to load ledger map for bucket %d: %w", segID, err)
			}
			if lm != nil {
				startLocalID, endLocalID = lm.LedgerRangeToIDRange(startOff, endOff)
			} else {
				// No ledger map available — cannot trim, include all
				endLocalID = bitmap.Maximum()
			}
		} else {
			// Full bucket range — no trimming needed
			endLocalID = bitmap.Maximum()
		}

		// Clone and trim to range
		trimmed := bitmap.Clone()
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
func (bi *eventBitmap32Index) getSegmentWithStats(isContract bool, termKey [16]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	key := makeIndexKey(termKey, segmentID)

	// Check hot cache first - no disk read
	segments := bi.topicSegments
	if isContract {
		segments = bi.contractSegments
	}
	if bitmap, exists := segments[key]; exists {
		return bitmap, 0, 0, 0, nil
	}

	// Load from storage if loader is available
	if bi.loader != nil {
		return bi.loader.LoadBitmap32SegmentWithTiming(isContract, termKey, segmentID)
	}

	return nil, 0, 0, 0, nil
}

// GetAndClearAllSegments gets and clears all hot segments and bucket counters.
// Serializes bitmaps using MarshalBinary (standard format, compatible with FromBuffer).
// Also returns the bucket event counters for ledger map flush.
func (bi *eventBitmap32Index) GetAndClearAllSegments() ([]eventSegment, map[uint32]*bucketEventCounter, error) {
	segments := make([]eventSegment, 0, len(bi.contractSegments)+len(bi.topicSegments))

	for key, bitmap := range bi.contractSegments {
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, nil, err
		}
		keyCopy := make([]byte, IndexKeySize)
		copy(keyCopy, key[:])
		segments = append(segments, eventSegment{
			Key:        keyCopy,
			Data:       data,
			IsContract: true,
		})
	}

	for key, bitmap := range bi.topicSegments {
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, nil, err
		}
		keyCopy := make([]byte, IndexKeySize)
		copy(keyCopy, key[:])
		segments = append(segments, eventSegment{
			Key:        keyCopy,
			Data:       data,
			IsContract: false,
		})
	}

	// Copy bucket counters for ledger map flush, then reset only eventCounts
	// (nextDenseID must persist to ensure dense IDs continue after flush)
	counters := make(map[uint32]*bucketEventCounter)
	for bucketID, counter := range bi.bucketCounters {
		counters[bucketID] = &bucketEventCounter{
			eventCounts: counter.eventCounts,
			nextDenseID: counter.nextDenseID,
		}
		// Reset event counts (captured for flush) but keep nextDenseID
		counter.eventCounts = [BucketSize]uint32{}
	}

	bi.contractSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topicSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)

	return segments, counters, nil
}

// ClearAll clears all hot segments and bucket counters.
func (bi *eventBitmap32Index) ClearAll() {
	bi.contractSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topicSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.bucketCounters = make(map[uint32]*bucketEventCounter)
	bi.topicTermPositions = make(map[[16]byte]byte)
}

// GetTopicTermPositions returns the topic term position map.
func (bi *eventBitmap32Index) GetTopicTermPositions() map[[16]byte]byte {
	return bi.topicTermPositions
}

// ClearTopicTermPositions clears the topic term position map (after bucket file write).
func (bi *eventBitmap32Index) ClearTopicTermPositions() {
	bi.topicTermPositions = make(map[[16]byte]byte)
}

// GetHotSegmentStats returns statistics about hot segments.
func (bi *eventBitmap32Index) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	count = len(bi.contractSegments) + len(bi.topicSegments)
	for _, bitmap := range bi.contractSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	for _, bitmap := range bi.topicSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	return
}

// GetCurrentBucketID returns the current segment ID being written to.
func (bi *eventBitmap32Index) GetCurrentBucketID() uint32 {
	return bi.currentBucketID
}

// =============================================================================
// Event Sequence Bitmap Store (32-bit Event-level)
// =============================================================================

// BitmapEventSeqStore implements event-level index storage using RocksDB with 32-bit roaring bitmaps.
// Uses FromBuffer for near-zero-cost decode instead of UnmarshalBinary.
type BitmapEventSeqStore struct {
	db          *grocksdb.DB
	cfContracts *grocksdb.ColumnFamilyHandle // Contract ID index CF (bitmap32)
	cfTopics    *grocksdb.ColumnFamilyHandle // Topic index CF (bitmap32)
	cfDefault   *grocksdb.ColumnFamilyHandle // Default CF for ledger maps

	// In-memory event bitmap32 index
	bitmap *eventBitmap32Index

	// RocksDB options
	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewBitmapEventSeqStore creates a new RocksDB-backed event index store using 32-bit bitmaps.
func NewBitmapEventSeqStore(db *grocksdb.DB, cfDefault, cfContracts, cfTopics *grocksdb.ColumnFamilyHandle) (*BitmapEventSeqStore, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)

	store := &BitmapEventSeqStore{
		db:          db,
		cfDefault:   cfDefault,
		cfContracts: cfContracts,
		cfTopics:    cfTopics,
		wo:          wo,
		ro:          grocksdb.NewDefaultReadOptions(),
	}

	store.bitmap = newEventBitmap32Index(store, store)

	return store, nil
}

// LoadBucketLedgerMap implements the ledgerMapLoader interface.
// Reads the ledger map for a bucket from CFDefault.
func (s *BitmapEventSeqStore) LoadBucketLedgerMap(bucketID uint32) (*BucketLedgerMap, error) {
	key := BucketLedgerMapKey(bucketID)
	data, err := s.db.GetCF(s.ro, s.cfDefault, key)
	if err != nil {
		return nil, fmt.Errorf("failed to load ledger map for bucket %d: %w", bucketID, err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, nil
	}

	// Copy data since it becomes invalid after Free
	dataCopy := make([]byte, data.Size())
	copy(dataCopy, data.Data())

	return &BucketLedgerMap{
		BucketID: bucketID,
		Data:     dataCopy,
	}, nil
}

// getCF returns the appropriate column family based on isContract flag.
func (s *BitmapEventSeqStore) getCF(isContract bool) *grocksdb.ColumnFamilyHandle {
	if isContract {
		return s.cfContracts
	}
	return s.cfTopics
}

// Close releases resources.
func (s *BitmapEventSeqStore) Close() error {
	s.wo.Destroy()
	s.ro.Destroy()
	return nil
}

// =============================================================================
// bitmap32Loader Implementation
// =============================================================================

// LoadBitmap32Segment implements bitmap32Loader interface.
func (s *BitmapEventSeqStore) LoadBitmap32Segment(isContract bool, termKey [16]byte, segmentID uint32) (*roaring.Bitmap, error) {
	bitmap, _, _, _, err := s.LoadBitmap32SegmentWithTiming(isContract, termKey, segmentID)
	return bitmap, err
}

// LoadBitmap32SegmentWithTiming loads a segment using FromBuffer for near-zero-cost decode.
func (s *BitmapEventSeqStore) LoadBitmap32SegmentWithTiming(isContract bool, termKey [16]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	dbKey := EncodeIndexKeyWithBucket(termKey, segmentID)
	cf := s.getCF(isContract)

	readStart := time.Now()
	data, err := s.db.GetCF(s.ro, cf, dbKey)
	readTime := time.Since(readStart)
	if err != nil {
		return nil, 0, readTime, 0, fmt.Errorf("failed to get event bitmap32: %w", err)
	}

	if data.Size() == 0 {
		data.Free()
		return nil, 0, readTime, 0, nil
	}

	bytesRead := int64(data.Size())
	decodeStart := time.Now()

	// Must copy - data.Data() is invalid after Free()
	dataCopy := make([]byte, data.Size())
	copy(dataCopy, data.Data())
	data.Free()

	// FromBuffer: near-zero-cost decode into our copy
	bitmap := roaring.New()
	_, err = bitmap.FromBuffer(dataCopy)
	decodeTime := time.Since(decodeStart)
	if err != nil {
		return nil, bytesRead, readTime, decodeTime, fmt.Errorf("failed to decode event bitmap32 via FromBuffer: %w", err)
	}

	return bitmap, bytesRead, readTime, decodeTime, nil
}

// loadBitmap32FromCF loads a bitmap from a column family (for merge during flush).
func (s *BitmapEventSeqStore) loadBitmap32FromCF(cf *grocksdb.ColumnFamilyHandle, key []byte) (*roaring.Bitmap, error) {
	data, err := s.db.GetCF(s.ro, cf, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get event bitmap32: %w", err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, nil
	}

	bitmap := roaring.New()
	_, err = bitmap.FromBuffer(data.Data())
	if err != nil {
		return nil, fmt.Errorf("failed to decode event bitmap32: %w", err)
	}

	// Clone to own the data since data.Data() becomes invalid after Free
	return bitmap.Clone(), nil
}

// =============================================================================
// Write Operations
// =============================================================================

// AddContractEvent adds an event to the contract index using a dense local ID.
func (s *BitmapEventSeqStore) AddContractEvent(contractID []byte, bucketID uint32, denseLocalID uint32) {
	s.bitmap.AddContractEvent(contractID, bucketID, denseLocalID)
}

// AddTopicEvent adds an event to the topic index (positional) using a dense local ID.
func (s *BitmapEventSeqStore) AddTopicEvent(pos int, topic []byte, bucketID uint32, denseLocalID uint32) {
	s.bitmap.AddTopicEvent(pos, topic, bucketID, denseLocalID)
}

// =============================================================================
// Query Operations
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

// QueryEventKeysWithStats returns per-segment bitmaps of matching local IDs.
func (s *BitmapEventSeqStore) QueryEventKeysWithStats(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32) (*EventSeqQueryResult, error) {
	result := &EventSeqQueryResult{
		PerSegment: make(map[uint32]*roaring.Bitmap),
	}

	type segmentBitmaps struct {
		bitmaps []*roaring.Bitmap
	}
	allSegments := make(map[uint32]*segmentBitmaps)

	// Count expected filter terms so we can skip segments missing any term
	expectedTerms := 0

	// Query contract index if specified
	if len(contractID) > 0 {
		expectedTerms++
		termKey := ContractTermKey(contractID)
		perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(true, termKey, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("contract bitmap32 query failed: %w", err)
		}
		result.BytesRead += bytesRead
		result.Segments += segments
		result.ReadTime += readTime
		result.DecodeTime += decodeTime

		for segID, bm := range perSeg {
			if _, ok := allSegments[segID]; !ok {
				allSegments[segID] = &segmentBitmaps{}
			}
			allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
		}
	}

	// Query topic indexes (positional)
	for pos, tg := range topicGroups {
		for _, topic := range tg {
			if len(topic) == 0 {
				continue
			}
			expectedTerms++
			termKey := TopicTermKey(pos, topic)
			perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(false, termKey, startLedger, endLedger)
			if err != nil {
				return nil, fmt.Errorf("topic bitmap32 query failed: %w", err)
			}
			result.BytesRead += bytesRead
			result.Segments += segments
			result.ReadTime += readTime
			result.DecodeTime += decodeTime

			for segID, bm := range perSeg {
				if _, ok := allSegments[segID]; !ok {
					allSegments[segID] = &segmentBitmaps{}
				}
				allSegments[segID].bitmaps = append(allSegments[segID].bitmaps, bm)
			}
		}
	}

	// Intersect per-segment bitmaps
	// Skip segments that don't have bitmaps from ALL filter terms — a missing
	// term means no events in that segment match that term, so AND must be empty.
	intersectStart := time.Now()
	for segID, sb := range allSegments {
		if len(sb.bitmaps) < expectedTerms {
			continue
		}
		intersected := roaring.FastAnd(sb.bitmaps...)
		if !intersected.IsEmpty() {
			result.PerSegment[segID] = intersected
			result.TotalCount += intersected.GetCardinality()
		}
	}
	result.IntersectTime = time.Since(intersectStart)

	return result, nil
}

// QueryEventKeysMultiFilter returns per-segment bitmaps with multi-value OR/AND filtering.
// contractIDs: multiple contract IDs (OR within group)
// topicGroups: per-position topic values (OR within position, AND across positions)
// Semantics: (contract1 OR contract2 OR ...) AND (topic0val1 OR topic0val2 OR ...) AND ...
func (s *BitmapEventSeqStore) QueryEventKeysMultiFilter(
	contractIDs [][]byte,
	topicGroups [4][][]byte,
	startLedger, endLedger uint32,
) (*EventSeqQueryResult, error) {
	result := &EventSeqQueryResult{
		PerSegment: make(map[uint32]*roaring.Bitmap),
	}

	// Collect per-group, per-segment bitmaps
	// groupIdx 0 = contracts, 1-4 = topic positions
	type groupSegBitmaps struct {
		bitmaps map[uint32][]*roaring.Bitmap // segmentID -> bitmaps to OR
	}
	groups := make(map[int]*groupSegBitmaps)

	// Query contract bitmaps (OR within contracts group)
	if len(contractIDs) > 0 {
		groups[0] = &groupSegBitmaps{bitmaps: make(map[uint32][]*roaring.Bitmap)}
		for _, cid := range contractIDs {
			termKey := ContractTermKey(cid)
			perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(true, termKey, startLedger, endLedger)
			if err != nil {
				return nil, fmt.Errorf("contract bitmap32 multi-filter query failed: %w", err)
			}
			result.BytesRead += bytesRead
			result.Segments += segments
			result.ReadTime += readTime
			result.DecodeTime += decodeTime

			for segID, bm := range perSeg {
				groups[0].bitmaps[segID] = append(groups[0].bitmaps[segID], bm)
			}
		}
	}

	// Query topic bitmaps per position (OR within each position group)
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
			termKey := TopicTermKey(pos, topicXDR)
			perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(false, termKey, startLedger, endLedger)
			if err != nil {
				return nil, fmt.Errorf("topic bitmap32 multi-filter query failed: %w", err)
			}
			result.BytesRead += bytesRead
			result.Segments += segments
			result.ReadTime += readTime
			result.DecodeTime += decodeTime

			for segID, bm := range perSeg {
				groups[groupIdx].bitmaps[segID] = append(groups[groupIdx].bitmaps[segID], bm)
			}
		}
	}

	// Collect all segment IDs across all groups
	allSegIDs := make(map[uint32]bool)
	for _, g := range groups {
		for segID := range g.bitmaps {
			allSegIDs[segID] = true
		}
	}

	// For each segment: OR within each group, then AND across groups
	intersectStart := time.Now()
	for segID := range allSegIDs {
		var groupUnions []*roaring.Bitmap

		for _, g := range groups {
			bms := g.bitmaps[segID]
			if len(bms) == 0 {
				// This group has no data for this segment -> AND produces empty
				groupUnions = nil
				break
			}
			if len(bms) == 1 {
				groupUnions = append(groupUnions, bms[0])
			} else {
				groupUnions = append(groupUnions, roaring.FastOr(bms...))
			}
		}

		if len(groupUnions) == 0 {
			continue
		}

		intersected := roaring.FastAnd(groupUnions...)
		if !intersected.IsEmpty() {
			result.PerSegment[segID] = intersected
			result.TotalCount += intersected.GetCardinality()
		}
	}
	result.IntersectTime = time.Since(intersectStart)

	return result, nil
}

// =============================================================================
// Statistics
// =============================================================================

// getEventBitmap32Index returns the underlying event bitmap32 index.
func (s *BitmapEventSeqStore) getEventBitmap32Index() *eventBitmap32Index {
	return s.bitmap
}

// =============================================================================
// Lifecycle
// =============================================================================

// Flush persists all hot segments and ledger maps to RocksDB, merging with existing data.
func (s *BitmapEventSeqStore) Flush() error {
	segments, counters, err := s.bitmap.GetAndClearAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get event bitmap32 segments: %w", err)
	}

	if len(segments) == 0 && len(counters) == 0 {
		return nil
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, seg := range segments {
		cf := s.getCF(seg.IsContract)

		existingBitmap, err := s.loadBitmap32FromCF(cf, seg.Key)
		if err != nil {
			return fmt.Errorf("failed to load existing event bitmap32 for merge: %w", err)
		}

		var finalData []byte
		if existingBitmap != nil {
			newBitmap := roaring.New()
			_, err := newBitmap.FromBuffer(seg.Data)
			if err != nil {
				return fmt.Errorf("failed to decode new event bitmap32: %w", err)
			}
			existingBitmap.Or(newBitmap)
			existingBitmap.RunOptimize()
			finalData, err = existingBitmap.ToBytes()
			if err != nil {
				return fmt.Errorf("failed to serialize merged event bitmap32: %w", err)
			}
		} else {
			finalData = seg.Data
		}

		batch.PutCF(cf, seg.Key, finalData)
	}

	// Write/merge ledger maps for each bucket that had events
	for bucketID, counter := range counters {
		lmKey := BucketLedgerMapKey(bucketID)

		// Read existing ledger map data
		existingData, err := s.db.GetCF(s.ro, s.cfDefault, lmKey)
		if err != nil {
			return fmt.Errorf("failed to read existing ledger map for bucket %d: %w", bucketID, err)
		}

		var existingBytes []byte
		if existingData.Size() > 0 {
			existingBytes = make([]byte, existingData.Size())
			copy(existingBytes, existingData.Data())
		}
		existingData.Free()

		// Build new counts map from the counter
		newCounts := make(map[uint16]uint32)
		for i := uint32(0); i < BucketSize; i++ {
			if counter.eventCounts[i] > 0 {
				newCounts[uint16(i)] = counter.eventCounts[i]
			}
		}

		var finalData []byte
		if len(existingBytes) > 0 {
			finalData = MergeBucketLedgerMapData(existingBytes, newCounts)
		} else {
			finalData = EncodeBucketLedgerMap(bucketID, counter.eventCounts)
		}

		batch.PutCF(s.cfDefault, lmKey, finalData)
	}

	if err := s.db.Write(s.wo, batch); err != nil {
		return fmt.Errorf("failed to write event bitmap32 segments: %w", err)
	}

	return nil
}

// =============================================================================
// Bitmap32 Event-Level Query Result (FromBuffer decode)
// =============================================================================

// Bitmap32EventQueryResult holds detailed results from a bitmap32 event-level query.
// Uses sequential event IDs + FromBuffer for near-zero-cost decode.
type Bitmap32EventQueryResult struct {
	// Ledger range
	LedgerRange      uint32 // endLedger - startLedger + 1
	MatchingLocalIDs int    // Local IDs matching index query

	// Index stats
	BucketsTouched  int   // Number of buckets (segments) touched
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
	DecodeTime         time.Duration // Time decoding events
	FilterTime         time.Duration // Time filtering events
	TotalTime          time.Duration // Total query time
}

// ToUnified converts Bitmap32EventQueryResult to UnifiedQueryResult
func (r *Bitmap32EventQueryResult) ToUnified() *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:       "bitmap32-event",
		LedgerRange:     r.LedgerRange,
		BucketsTouched:  r.BucketsTouched,
		IndexMatches:    r.MatchingLocalIDs,
		MatchUnitName:   "local IDs",
		EventsScanned:   r.EventsScanned,
		EventsReturned:  r.EventsReturned,
		IndexBytesRead:  r.IndexBytesRead,
		EventBytesRead:  r.EventBytesRead,
		IndexLookupTime: r.IndexLookupTime,
		EventFetchTime:  r.EventFetchTime,
		DecodeTime:      r.DecodeTime,
		FilterTime:      r.FilterTime,
		TotalTime:       r.TotalTime,
	}
}
