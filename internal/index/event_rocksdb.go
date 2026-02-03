package index

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/linxGnu/grocksdb"
)

// EventIndexReader provides event-level indexed lookups.
type EventIndexReader interface {
	// QueryEventKeys returns a Roaring64 bitmap of event keys matching the filter criteria.
	QueryEventKeys(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring64.Bitmap, error)
}

// Verify EventRocksDBStore implements EventIndexReader at compile time
var _ EventIndexReader = (*EventRocksDBStore)(nil)

// EventRocksDBStore implements event-level index storage using RocksDB.
// Uses Roaring64 bitmaps to store 64-bit event keys for precise intersection.
type EventRocksDBStore struct {
	db          *grocksdb.DB
	cfContracts *grocksdb.ColumnFamilyHandle // Contract ID index CF
	cfTopics    *grocksdb.ColumnFamilyHandle // Topic index CF

	// In-memory event bitmap index
	bitmap *EventBitmapIndex

	// RocksDB options
	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewEventRocksDBStore creates a new RocksDB-backed event index store.
// Uses separate column families for contracts and topics.
func NewEventRocksDBStore(db *grocksdb.DB, cfContracts, cfTopics *grocksdb.ColumnFamilyHandle) (*EventRocksDBStore, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true)

	store := &EventRocksDBStore{
		db:          db,
		cfContracts: cfContracts,
		cfTopics:    cfTopics,
		wo:          wo,
		ro:          grocksdb.NewDefaultReadOptions(),
	}

	store.bitmap = NewEventBitmapIndex(store)

	return store, nil
}

// getCF returns the appropriate column family for a given prefix.
func (s *EventRocksDBStore) getCF(prefix byte) *grocksdb.ColumnFamilyHandle {
	if prefix == PrefixEventContract {
		return s.cfContracts
	}
	return s.cfTopics
}

// Close releases resources.
func (s *EventRocksDBStore) Close() error {
	s.wo.Destroy()
	s.ro.Destroy()
	return nil
}

// LoadEventSegment implements EventSegmentLoader interface.
func (s *EventRocksDBStore) LoadEventSegment(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, error) {
	bitmap, _, err := s.LoadEventSegmentWithStats(prefix, keyValue, segmentID)
	return bitmap, err
}

// LoadEventSegmentWithStats loads a segment and returns bytes read.
func (s *EventRocksDBStore) LoadEventSegmentWithStats(prefix byte, keyValue []byte, segmentID uint32) (*roaring64.Bitmap, int64, error) {
	dbKey := MakeEventKey(prefix, keyValue, segmentID)
	cf := s.getCF(prefix)
	return s.loadBitmapFromCFWithStats(cf, dbKey)
}

func (s *EventRocksDBStore) loadBitmapFromCF(cf *grocksdb.ColumnFamilyHandle, key []byte) (*roaring64.Bitmap, error) {
	bitmap, _, err := s.loadBitmapFromCFWithStats(cf, key)
	return bitmap, err
}

func (s *EventRocksDBStore) loadBitmapFromCFWithStats(cf *grocksdb.ColumnFamilyHandle, key []byte) (*roaring64.Bitmap, int64, error) {
	data, err := s.db.GetCF(s.ro, cf, key)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get event bitmap: %w", err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, 0, nil
	}

	bytesRead := int64(data.Size())
	bitmap := roaring64.New()
	if err := bitmap.UnmarshalBinary(data.Data()); err != nil {
		return nil, bytesRead, fmt.Errorf("failed to unmarshal event bitmap: %w", err)
	}

	return bitmap, bytesRead, nil
}

// AddContractEvent adds an event to the contract index.
func (s *EventRocksDBStore) AddContractEvent(contractID []byte, ledger uint32, tx, op, evt uint16) error {
	s.bitmap.AddContractEvent(contractID, ledger, tx, op, evt)
	return nil
}

// AddTopicEvent adds an event to the topic index (non-positional).
func (s *EventRocksDBStore) AddTopicEvent(topic []byte, ledger uint32, tx, op, evt uint16) error {
	s.bitmap.AddTopicEvent(topic, ledger, tx, op, evt)
	return nil
}

// EventQueryResult holds the result of an event key query including stats.
type EventQueryResult struct {
	Bitmap     *roaring64.Bitmap
	BytesRead  int64
	Segments   int
}

// QueryEventKeys returns a Roaring64 bitmap of event keys matching the filter criteria.
// Topics are matched non-positionally (same semantics as posting list).
func (s *EventRocksDBStore) QueryEventKeys(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring64.Bitmap, error) {
	result, err := s.QueryEventKeysWithStats(contractID, topics, startLedger, endLedger)
	if err != nil {
		return nil, err
	}
	return result.Bitmap, nil
}

// QueryEventKeysWithStats returns event keys and query statistics.
func (s *EventRocksDBStore) QueryEventKeysWithStats(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*EventQueryResult, error) {
	result := &EventQueryResult{
		Bitmap: roaring64.New(),
	}
	var bitmaps []*roaring64.Bitmap

	// Query contract index if specified
	if len(contractID) > 0 {
		bm, bytesRead, segments, err := s.bitmap.QueryContractEventsWithStats(contractID, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("contract event index query failed: %w", err)
		}
		result.BytesRead += bytesRead
		result.Segments += segments
		if bm != nil {
			bitmaps = append(bitmaps, bm)
		}
	}

	// Query topic indexes (non-positional)
	for _, topic := range topics {
		if len(topic) == 0 {
			continue
		}
		bm, bytesRead, segments, err := s.bitmap.QueryTopicEventsWithStats(topic, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("topic event index query failed: %w", err)
		}
		result.BytesRead += bytesRead
		result.Segments += segments
		if bm != nil {
			bitmaps = append(bitmaps, bm)
		}
	}

	if len(bitmaps) == 0 {
		return result, nil
	}

	// Intersect all bitmaps using FastAnd
	result.Bitmap = roaring64.FastAnd(bitmaps...)
	return result, nil
}

// GetEventBitmapIndex returns the underlying event bitmap index.
func (s *EventRocksDBStore) GetEventBitmapIndex() *EventBitmapIndex {
	return s.bitmap
}

// Flush persists all hot segments to RocksDB, merging with existing data.
func (s *EventRocksDBStore) Flush() error {
	segments, err := s.bitmap.GetAndClearAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get event segments: %w", err)
	}

	if len(segments) == 0 {
		return nil
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, seg := range segments {
		// Extract prefix from key to determine CF
		prefix := seg.Key[0]
		cf := s.getCF(prefix)

		existingBitmap, err := s.loadBitmapFromCF(cf, seg.Key)
		if err != nil {
			return fmt.Errorf("failed to load existing event bitmap for merge: %w", err)
		}

		var finalData []byte
		if existingBitmap != nil {
			newBitmap := roaring64.New()
			if err := newBitmap.UnmarshalBinary(seg.Data); err != nil {
				return fmt.Errorf("failed to unmarshal new event bitmap: %w", err)
			}
			existingBitmap.Or(newBitmap)
			existingBitmap.RunOptimize()
			finalData, err = existingBitmap.ToBytes()
			if err != nil {
				return fmt.Errorf("failed to serialize merged event bitmap: %w", err)
			}
		} else {
			finalData = seg.Data
		}

		batch.PutCF(cf, seg.Key, finalData)
	}

	if err := s.db.Write(s.wo, batch); err != nil {
		return fmt.Errorf("failed to write event segments: %w", err)
	}

	return nil
}

// GetStats returns statistics about the event index.
func (s *EventRocksDBStore) GetStats() *EventBitmapIndexStats {
	count, cards, memBytes := s.bitmap.GetHotSegmentStats()

	stats := &EventBitmapIndexStats{
		HotSegmentCount:    count,
		HotSegmentCards:    cards,
		HotSegmentMemBytes: memBytes,
		CurrentSegmentID:   s.bitmap.GetCurrentSegmentID(),
	}

	// Count stored segments per index type (non-positional)
	stats.ContractIndexCount = s.countIndexEntries(PrefixEventContract)
	stats.TopicIndexCount = s.countIndexEntries(PrefixEventTopic)

	return stats
}

func (s *EventRocksDBStore) countIndexEntries(prefix byte) int64 {
	cf := s.getCF(prefix)
	iter := s.db.NewIteratorCF(s.ro, cf)
	defer iter.Close()

	var count int64
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	return count
}

// SetWriteOptions configures write options.
func (s *EventRocksDBStore) SetWriteOptions(disableWAL bool) {
	s.wo.DisableWAL(disableWAL)
}

// EventBitmapIndexStats holds statistics about the event bitmap index.
type EventBitmapIndexStats struct {
	HotSegmentCount    int
	HotSegmentCards    uint64
	HotSegmentMemBytes uint64
	CurrentSegmentID   uint32
	ContractIndexCount int64
	TopicIndexCount    int64 // All topics (non-positional)
}
