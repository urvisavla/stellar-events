package index

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/linxGnu/grocksdb"
)

// IndexReader provides indexed lookups for events.
// This interface is implemented by RocksDBStore and used by query.Engine.
type IndexReader interface {
	// QueryLedgers returns a bitmap of ledgers matching the filter criteria.
	QueryLedgers(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring.Bitmap, error)

	// QueryEvents returns precise event keys matching the filter criteria.
	QueryEvents(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error)
}

// Verify RocksDBStore implements IndexReader at compile time
var _ IndexReader = (*RocksDBStore)(nil)

// =============================================================================
// RocksDB Index Store
// =============================================================================

// RocksDBStore implements the Store interface using RocksDB for persistence.
// It combines an in-memory BitmapIndex with RocksDB storage.
// Bitmap data is stored directly; RocksDB handles compression via LZ4.
type RocksDBStore struct {
	db *grocksdb.DB
	cf *grocksdb.ColumnFamilyHandle

	// In-memory bitmap index (this store handles its persistence)
	bitmap *BitmapIndex

	// RocksDB options
	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewRocksDBStore creates a new RocksDB-backed index store.
func NewRocksDBStore(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle) (*RocksDBStore, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // WAL disabled for bulk ingestion

	store := &RocksDBStore{
		db: db,
		cf: cf,
		wo: wo,
		ro: grocksdb.NewDefaultReadOptions(),
	}

	// Create bitmap index with this store as the segment loader
	store.bitmap = NewBitmapIndex(store)

	return store, nil
}

// Close releases resources.
func (s *RocksDBStore) Close() error {
	s.wo.Destroy()
	s.ro.Destroy()
	return nil
}

// =============================================================================
// SegmentLoader Implementation
// =============================================================================

// LoadL1Segment loads an L1 bitmap segment from RocksDB.
func (s *RocksDBStore) LoadL1Segment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error) {
	dbKey := MakeL1Key(prefix, keyValue, segmentID)
	return s.loadBitmap(dbKey)
}

// LoadL2Segment loads an L2 bitmap segment from RocksDB.
func (s *RocksDBStore) LoadL2Segment(prefix byte, keyValue []byte, ledgerSeq uint32) (*roaring.Bitmap, error) {
	dbKey := MakeL2Key(prefix, keyValue, ledgerSeq)
	return s.loadBitmap(dbKey)
}

// loadBitmap loads a bitmap from RocksDB.
// RocksDB handles compression transparently via LZ4.
func (s *RocksDBStore) loadBitmap(key []byte) (*roaring.Bitmap, error) {
	data, err := s.db.GetCF(s.ro, s.cf, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get bitmap: %w", err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, nil // No data for this key
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(data.Data()); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	return bitmap, nil
}

// =============================================================================
// Store Interface Implementation - Write Operations
// =============================================================================

// AddContractLedger adds a ledger to the L1 contract index.
func (s *RocksDBStore) AddContractLedger(contractID []byte, ledger uint32) error {
	s.bitmap.AddContractIndex(contractID, ledger)
	return nil
}

// AddTopicLedger adds a ledger to the L1 topic index.
func (s *RocksDBStore) AddTopicLedger(position int, topic []byte, ledger uint32) error {
	s.bitmap.AddTopicIndex(position, topic, ledger)
	return nil
}

// AddContractEvent adds an event to the L2 contract index.
func (s *RocksDBStore) AddContractEvent(contractID []byte, ledger uint32, txIdx, opIdx, eventIdx uint16) error {
	s.bitmap.AddContractL2Index(contractID, ledger, txIdx, opIdx, eventIdx)
	return nil
}

// AddTopicEvent adds an event to the L2 topic index.
func (s *RocksDBStore) AddTopicEvent(position int, topic []byte, ledger uint32, txIdx, opIdx, eventIdx uint16) error {
	s.bitmap.AddTopicL2Index(position, topic, ledger, txIdx, opIdx, eventIdx)
	return nil
}

// =============================================================================
// Store Interface Implementation - Query Operations
// =============================================================================

// QueryLedgers returns a bitmap of ledgers matching the filter criteria.
func (s *RocksDBStore) QueryLedgers(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	var bitmaps []*roaring.Bitmap

	// Query contract index if specified
	if len(contractID) > 0 {
		bm, err := s.bitmap.QueryContractIndex(contractID, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("contract index query failed: %w", err)
		}
		bitmaps = append(bitmaps, bm)
	}

	// Query topic indexes
	for i, topic := range topics {
		if len(topic) == 0 {
			continue
		}
		bm, err := s.bitmap.QueryTopicIndex(i, topic, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("topic%d index query failed: %w", i, err)
		}
		bitmaps = append(bitmaps, bm)
	}

	if len(bitmaps) == 0 {
		return roaring.New(), nil
	}

	// Intersect all bitmaps (AND logic)
	result := bitmaps[0].Clone()
	for i := 1; i < len(bitmaps); i++ {
		result.And(bitmaps[i])
	}

	return result, nil
}

// QueryEvents returns precise event keys matching the filter criteria.
func (s *RocksDBStore) QueryEvents(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) ([]EventKey, int, error) {
	// First, get matching ledgers using L1 index
	ledgerBitmap, err := s.QueryLedgers(contractID, topics, startLedger, endLedger)
	if err != nil {
		return nil, 0, err
	}

	matchingLedgers := int(ledgerBitmap.GetCardinality())
	if ledgerBitmap.IsEmpty() {
		return nil, matchingLedgers, nil
	}

	// Then query L2 indexes for each matching ledger
	var eventKeys []EventKey
	ledgers := ledgerBitmap.ToArray()

	for _, ledgerSeq := range ledgers {
		if limit > 0 && len(eventKeys) >= limit {
			break
		}

		// Get L2 bitmap for this ledger
		// We need to intersect L2 results from all filters
		var l2Bitmaps []*roaring.Bitmap

		if len(contractID) > 0 {
			bm, err := s.bitmap.QueryL2Index(PrefixContractL2, contractID, ledgerSeq)
			if err != nil {
				return nil, matchingLedgers, err
			}
			if bm != nil {
				l2Bitmaps = append(l2Bitmaps, bm)
			}
		}

		for i, topic := range topics {
			if len(topic) == 0 {
				continue
			}
			prefix := TopicL2Prefix(i)
			if prefix == 0 {
				continue
			}
			bm, err := s.bitmap.QueryL2Index(prefix, topic, ledgerSeq)
			if err != nil {
				return nil, matchingLedgers, err
			}
			if bm != nil {
				l2Bitmaps = append(l2Bitmaps, bm)
			}
		}

		if len(l2Bitmaps) == 0 {
			continue
		}

		// Intersect L2 bitmaps
		l2Result := l2Bitmaps[0].Clone()
		for i := 1; i < len(l2Bitmaps); i++ {
			l2Result.And(l2Bitmaps[i])
		}

		if l2Result.IsEmpty() {
			continue
		}

		// Convert to event keys
		eventIndices := l2Result.ToArray()
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

// =============================================================================
// Store Interface Implementation - Statistics
// =============================================================================

// GetStats returns statistics about the index.
func (s *RocksDBStore) GetStats() *BitmapIndexStats {
	count, cards, memBytes := s.bitmap.GetHotSegmentStats()

	stats := &BitmapIndexStats{
		HotSegmentCount:    count,
		HotSegmentCards:    cards,
		HotSegmentMemBytes: memBytes,
		CurrentSegmentID:   s.bitmap.GetCurrentSegmentID(),
	}

	// Count stored segments per index type
	stats.ContractIndexCount = s.countIndexEntries(PrefixContractIndex)
	stats.Topic0IndexCount = s.countIndexEntries(PrefixTopic0Index)
	stats.Topic1IndexCount = s.countIndexEntries(PrefixTopic1Index)
	stats.Topic2IndexCount = s.countIndexEntries(PrefixTopic2Index)
	stats.Topic3IndexCount = s.countIndexEntries(PrefixTopic3Index)

	return stats
}

// countIndexEntries counts the number of stored bitmap segments for an index type.
func (s *RocksDBStore) countIndexEntries(prefix byte) int64 {
	iter := s.db.NewIteratorCF(s.ro, s.cf)
	defer iter.Close()

	prefixBytes := []byte{prefix}
	var count int64

	for iter.Seek(prefixBytes); iter.ValidForPrefix(prefixBytes); iter.Next() {
		count++
	}

	return count
}

// =============================================================================
// Store Interface Implementation - Lifecycle
// =============================================================================

// Flush persists all hot segments to RocksDB.
// Uses a single drain/resume cycle for both L1 and L2 segments for efficiency.
func (s *RocksDBStore) Flush() error {
	result, err := s.bitmap.GetAndClearAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	// Write L1 segments
	if len(result.L1Segments) > 0 {
		batch := grocksdb.NewWriteBatch()
		for _, seg := range result.L1Segments {
			batch.PutCF(s.cf, seg.Key, seg.Data)
		}
		if err := s.db.Write(s.wo, batch); err != nil {
			batch.Destroy()
			return fmt.Errorf("failed to write L1 segments: %w", err)
		}
		batch.Destroy()
	}

	// Write L2 segments in batches
	if len(result.L2Segments) > 0 {
		const maxBatchSize = 10000
		batch := grocksdb.NewWriteBatch()
		batchCount := 0

		for _, seg := range result.L2Segments {
			batch.PutCF(s.cf, seg.Key, seg.Data)
			batchCount++

			if batchCount >= maxBatchSize {
				if err := s.db.Write(s.wo, batch); err != nil {
					batch.Destroy()
					return fmt.Errorf("failed to write L2 segment batch: %w", err)
				}
				batch.Destroy()
				batch = grocksdb.NewWriteBatch()
				batchCount = 0
			}
		}

		if batchCount > 0 {
			if err := s.db.Write(s.wo, batch); err != nil {
				batch.Destroy()
				return fmt.Errorf("failed to write L2 segment batch: %w", err)
			}
		}
		batch.Destroy()
	}

	return nil
}

// =============================================================================
// Additional Methods
// =============================================================================

// GetBitmapIndex returns the underlying bitmap index for direct access.
// This is useful for ingestion where we want to batch index updates.
func (s *RocksDBStore) GetBitmapIndex() *BitmapIndex {
	return s.bitmap
}

// SetWriteOptions configures write options (e.g., enable/disable WAL).
func (s *RocksDBStore) SetWriteOptions(disableWAL bool) {
	s.wo.DisableWAL(disableWAL)
}
