package store

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/linxGnu/grocksdb"

	"github.com/urvisavla/stellar-events/internal/event"
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
	LoadBitmap32Segment(isContract bool, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, error)
	// LoadBitmap32SegmentWithTiming loads a segment and returns bytes read, read time, and decode time.
	LoadBitmap32SegmentWithTiming(isContract bool, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error)
}

// makeIndexKey creates a 36-byte index key from a SHA-256 term key and bucket ID.
// Format: [term_key:32][bucket_id:4]
func makeIndexKey(termKey [32]byte, bucketID uint32) [IndexKeySize]byte {
	var key [IndexKeySize]byte
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], bucketID)
	return key
}

// eventBitmap32Index manages segmented 32-bit roaring bitmap indexes for event-level granularity.
// Stores local IDs: (ledger_offset << 16) | event_seq within each segment.
// Uses roaring.Bitmap (32-bit) which supports FromBuffer for near-zero-cost decode.
type eventBitmap32Index struct {
	// Hot segment caches - separate maps for contract and topic
	contractSegments map[[IndexKeySize]byte]*roaring.Bitmap
	topicSegments    map[[IndexKeySize]byte]*roaring.Bitmap

	// Current hot segment ID
	currentBucketID uint32

	// Segment loader for queries
	loader bitmap32Loader
}

// newEventBitmap32Index creates a new event-level bitmap32 index.
func newEventBitmap32Index(loader bitmap32Loader) *eventBitmap32Index {
	return &eventBitmap32Index{
		contractSegments: make(map[[IndexKeySize]byte]*roaring.Bitmap),
		topicSegments:    make(map[[IndexKeySize]byte]*roaring.Bitmap),
		loader:           loader,
	}
}

// AddContractEvent adds an event to the contract index.
func (bi *eventBitmap32Index) AddContractEvent(contractID []byte, ledger uint32, eventSeq uint16) {
	termKey := ContractTermKey(contractID)
	segmentID := BucketID(ledger)
	segmentStart := segmentID * BucketSize
	ledgerOffset := uint16(ledger - segmentStart)
	localID := event.EncodeBitmap32LocalID(ledgerOffset, eventSeq)

	key := makeIndexKey(termKey, segmentID)

	bitmap, exists := bi.contractSegments[key]
	if !exists {
		bitmap = roaring.New()
		bi.contractSegments[key] = bitmap
	}

	bitmap.Add(localID)

	if segmentID > bi.currentBucketID {
		bi.currentBucketID = segmentID
	}
}

// AddTopicEvent adds an event to the topic index (non-positional).
func (bi *eventBitmap32Index) AddTopicEvent(topicValue []byte, ledger uint32, eventSeq uint16) {
	termKey := TopicTermKey(topicValue)
	segmentID := BucketID(ledger)
	segmentStart := segmentID * BucketSize
	ledgerOffset := uint16(ledger - segmentStart)
	localID := event.EncodeBitmap32LocalID(ledgerOffset, eventSeq)

	key := makeIndexKey(termKey, segmentID)

	bitmap, exists := bi.topicSegments[key]
	if !exists {
		bitmap = roaring.New()
		bi.topicSegments[key] = bitmap
	}

	bitmap.Add(localID)

	if segmentID > bi.currentBucketID {
		bi.currentBucketID = segmentID
	}
}

// QueryIndexWithStats returns per-segment bitmaps of local IDs matching a key within a ledger range.
// Returns map[segmentID]*roaring.Bitmap so caller knows which segment each local ID belongs to.
func (bi *eventBitmap32Index) QueryIndexWithStats(isContract bool, termKey [32]byte, startLedger, endLedger uint32) (map[uint32]*roaring.Bitmap, int64, int, time.Duration, time.Duration, error) {
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

		// Trim to ledger range using local ID boundaries
		segmentStart := segID * BucketSize

		// Compute local ID boundaries for ledger range
		var startLocalID, endLocalID uint32
		if startLedger > segmentStart {
			startLocalID = event.EncodeBitmap32LocalID(uint16(startLedger-segmentStart), 0)
		}
		if endLedger < segmentStart+BucketSize-1 {
			endOffset := uint16(endLedger - segmentStart)
			endLocalID = event.EncodeBitmap32LocalID(endOffset, 0xFFFF)
		} else {
			endLocalID = event.EncodeBitmap32LocalID(uint16(BucketSize-1), 0xFFFF)
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
func (bi *eventBitmap32Index) getSegmentWithStats(isContract bool, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
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

// GetAndClearAllSegments gets and clears all hot segments.
// Serializes bitmaps using MarshalBinary (standard format, compatible with FromBuffer).
func (bi *eventBitmap32Index) GetAndClearAllSegments() ([]eventSegment, error) {
	segments := make([]eventSegment, 0, len(bi.contractSegments)+len(bi.topicSegments))

	for key, bitmap := range bi.contractSegments {
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			return nil, err
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
			return nil, err
		}
		keyCopy := make([]byte, IndexKeySize)
		copy(keyCopy, key[:])
		segments = append(segments, eventSegment{
			Key:        keyCopy,
			Data:       data,
			IsContract: false,
		})
	}

	bi.contractSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topicSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)

	return segments, nil
}

// ClearAll clears all hot segments.
func (bi *eventBitmap32Index) ClearAll() {
	bi.contractSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)
	bi.topicSegments = make(map[[IndexKeySize]byte]*roaring.Bitmap)
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

	// In-memory event bitmap32 index
	bitmap *eventBitmap32Index

	// RocksDB options
	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewBitmapEventSeqStore creates a new RocksDB-backed event index store using 32-bit bitmaps.
func NewBitmapEventSeqStore(db *grocksdb.DB, cfContracts, cfTopics *grocksdb.ColumnFamilyHandle) (*BitmapEventSeqStore, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)

	store := &BitmapEventSeqStore{
		db:          db,
		cfContracts: cfContracts,
		cfTopics:    cfTopics,
		wo:          wo,
		ro:          grocksdb.NewDefaultReadOptions(),
	}

	store.bitmap = newEventBitmap32Index(store)

	return store, nil
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
func (s *BitmapEventSeqStore) LoadBitmap32Segment(isContract bool, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, error) {
	bitmap, _, _, _, err := s.LoadBitmap32SegmentWithTiming(isContract, termKey, segmentID)
	return bitmap, err
}

// LoadBitmap32SegmentWithTiming loads a segment using FromBuffer for near-zero-cost decode.
func (s *BitmapEventSeqStore) LoadBitmap32SegmentWithTiming(isContract bool, termKey [32]byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
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

// AddContractEvent adds an event to the contract index.
func (s *BitmapEventSeqStore) AddContractEvent(contractID []byte, ledger uint32, eventSeq uint16) {
	s.bitmap.AddContractEvent(contractID, ledger, eventSeq)
}

// AddTopicEvent adds an event to the topic index (non-positional).
func (s *BitmapEventSeqStore) AddTopicEvent(topic []byte, ledger uint32, eventSeq uint16) {
	s.bitmap.AddTopicEvent(topic, ledger, eventSeq)
}

// =============================================================================
// Query Operations
// =============================================================================

// EventSeqQueryResult holds the result of a bitmap32 event key query including stats.
type EventSeqQueryResult struct {
	PerSegment map[uint32]*roaring.Bitmap // segmentID -> local ID bitmap
	TotalCount uint64
	BytesRead  int64
	Segments   int
	ReadTime   time.Duration
	DecodeTime time.Duration
}

// QueryEventKeysWithStats returns per-segment bitmaps of matching local IDs.
func (s *BitmapEventSeqStore) QueryEventKeysWithStats(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*EventSeqQueryResult, error) {
	result := &EventSeqQueryResult{
		PerSegment: make(map[uint32]*roaring.Bitmap),
	}

	type segmentBitmaps struct {
		bitmaps []*roaring.Bitmap
	}
	allSegments := make(map[uint32]*segmentBitmaps)

	// Query contract index if specified
	if len(contractID) > 0 {
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

	// Query topic indexes (non-positional)
	for _, topic := range topics {
		if len(topic) == 0 {
			continue
		}
		termKey := TopicTermKey(topic)
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

	// Intersect per-segment bitmaps
	for segID, sb := range allSegments {
		if len(sb.bitmaps) == 0 {
			continue
		}
		intersected := roaring.FastAnd(sb.bitmaps...)
		if !intersected.IsEmpty() {
			result.PerSegment[segID] = intersected
			result.TotalCount += intersected.GetCardinality()
		}
	}

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

// Flush persists all hot segments to RocksDB, merging with existing data.
func (s *BitmapEventSeqStore) Flush() error {
	segments, err := s.bitmap.GetAndClearAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get event bitmap32 segments: %w", err)
	}

	if len(segments) == 0 {
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
	SegmentsScanned int   // Number of segments scanned
	IndexBytesRead  int64 // Bytes read from bitmap index

	// Event fetch stats
	EventsScanned  int   // Events scanned from storage
	EventsReturned int   // Events returned after filtering
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	IndexLookupTime time.Duration // Time querying bitmap index
	IndexReadTime   time.Duration // Time reading bitmap segments from storage (I/O)
	IndexDecodeTime time.Duration // Time decoding bitmap segments (CPU - near zero with FromBuffer)
	EventFetchTime  time.Duration // Time fetching events
	DecodeTime      time.Duration // Time decoding events
	FilterTime      time.Duration // Time filtering events
	TotalTime       time.Duration // Total query time
}

// ToUnified converts Bitmap32EventQueryResult to UnifiedQueryResult
func (r *Bitmap32EventQueryResult) ToUnified() *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:       "bitmap32-event",
		LedgerRange:     r.LedgerRange,
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
