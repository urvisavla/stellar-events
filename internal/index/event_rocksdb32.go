package index

import (
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/linxGnu/grocksdb"
)

// EventRocksDB32Store implements event-level index storage using RocksDB with 32-bit roaring bitmaps.
// Uses FromBuffer for near-zero-cost decode instead of UnmarshalBinary.
type EventRocksDB32Store struct {
	db          *grocksdb.DB
	cfContracts *grocksdb.ColumnFamilyHandle // Contract ID index CF (bitmap32)
	cfTopics    *grocksdb.ColumnFamilyHandle // Topic index CF (bitmap32)

	// In-memory event bitmap32 index
	bitmap *EventBitmap32Index

	// RocksDB options
	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewEventRocksDB32Store creates a new RocksDB-backed event index store using 32-bit bitmaps.
func NewEventRocksDB32Store(db *grocksdb.DB, cfContracts, cfTopics *grocksdb.ColumnFamilyHandle) (*EventRocksDB32Store, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)

	store := &EventRocksDB32Store{
		db:          db,
		cfContracts: cfContracts,
		cfTopics:    cfTopics,
		wo:          wo,
		ro:          grocksdb.NewDefaultReadOptions(),
	}

	store.bitmap = NewEventBitmap32Index(store)

	return store, nil
}

// getCF returns the appropriate column family for a given prefix.
func (s *EventRocksDB32Store) getCF(prefix byte) *grocksdb.ColumnFamilyHandle {
	if prefix == PrefixEventContractBM32 {
		return s.cfContracts
	}
	return s.cfTopics
}

// Close releases resources.
func (s *EventRocksDB32Store) Close() error {
	s.wo.Destroy()
	s.ro.Destroy()
	return nil
}

// LoadEventBitmap32Segment implements EventBitmap32SegmentLoader interface.
func (s *EventRocksDB32Store) LoadEventBitmap32Segment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error) {
	bitmap, _, _, _, err := s.LoadEventBitmap32SegmentWithTiming(prefix, keyValue, segmentID)
	return bitmap, err
}

// LoadEventBitmap32SegmentWithTiming loads a segment using FromBuffer for near-zero-cost decode.
func (s *EventRocksDB32Store) LoadEventBitmap32SegmentWithTiming(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	dbKey := MakeEventKey(prefix, keyValue, segmentID)
	cf := s.getCF(prefix)

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
func (s *EventRocksDB32Store) loadBitmap32FromCF(cf *grocksdb.ColumnFamilyHandle, key []byte) (*roaring.Bitmap, error) {
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

// AddContractEvent adds an event to the contract index.
func (s *EventRocksDB32Store) AddContractEvent(contractID []byte, ledger uint32, eventSeq uint16) {
	s.bitmap.AddContractEvent(contractID, ledger, eventSeq)
}

// AddTopicEvent adds an event to the topic index (non-positional).
func (s *EventRocksDB32Store) AddTopicEvent(topic []byte, ledger uint32, eventSeq uint16) {
	s.bitmap.AddTopicEvent(topic, ledger, eventSeq)
}

// EventBitmap32QueryResult holds the result of a bitmap32 event key query including stats.
type EventBitmap32QueryResult struct {
	PerSegment map[uint32]*roaring.Bitmap // segmentID -> local ID bitmap
	TotalCount uint64
	BytesRead  int64
	Segments   int
	ReadTime   time.Duration
	DecodeTime time.Duration
}

// QueryEventKeysWithStats returns per-segment bitmaps of matching local IDs.
func (s *EventRocksDB32Store) QueryEventKeysWithStats(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*EventBitmap32QueryResult, error) {
	result := &EventBitmap32QueryResult{
		PerSegment: make(map[uint32]*roaring.Bitmap),
	}

	type segmentBitmaps struct {
		bitmaps []*roaring.Bitmap
	}
	allSegments := make(map[uint32]*segmentBitmaps)

	// Query contract index if specified
	if len(contractID) > 0 {
		perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(PrefixEventContractBM32, contractID, startLedger, endLedger)
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
		perSeg, bytesRead, segments, readTime, decodeTime, err := s.bitmap.QueryIndexWithStats(PrefixEventTopicBM32, topic, startLedger, endLedger)
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

// GetEventBitmap32Index returns the underlying event bitmap32 index.
func (s *EventRocksDB32Store) GetEventBitmap32Index() *EventBitmap32Index {
	return s.bitmap
}

// Flush persists all hot segments to RocksDB, merging with existing data.
func (s *EventRocksDB32Store) Flush() error {
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
		prefix := seg.Key[0]
		cf := s.getCF(prefix)

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
