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

// LoadSegment loads a bitmap segment from RocksDB.
func (s *RocksDBStore) LoadSegment(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error) {
	dbKey := MakeL1Key(prefix, keyValue, segmentID)
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

// AddContractLedger adds a ledger to the contract index.
func (s *RocksDBStore) AddContractLedger(contractID []byte, ledger uint32) error {
	s.bitmap.AddContractIndex(contractID, ledger)
	return nil
}

// AddTopicLedger adds a ledger to the topic index.
func (s *RocksDBStore) AddTopicLedger(position int, topic []byte, ledger uint32) error {
	s.bitmap.AddTopicIndex(position, topic, ledger)
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

	// Use FastAnd for efficient intersection (avoids Clone + multiple And calls)
	return roaring.FastAnd(bitmaps...), nil
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

// Flush persists all hot segments to RocksDB, merging with existing data.
func (s *RocksDBStore) Flush() error {
	segments, err := s.bitmap.GetAndClearAllSegments()
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	if len(segments) == 0 {
		return nil
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, seg := range segments {
		// Load existing bitmap from RocksDB (if any)
		existingBitmap, err := s.loadBitmap(seg.Key)
		if err != nil {
			return fmt.Errorf("failed to load existing bitmap for merge: %w", err)
		}

		var finalData []byte
		if existingBitmap != nil {
			// Merge: OR the new bitmap with existing
			newBitmap := roaring.New()
			if err := newBitmap.UnmarshalBinary(seg.Data); err != nil {
				return fmt.Errorf("failed to unmarshal new bitmap: %w", err)
			}
			existingBitmap.Or(newBitmap)
			existingBitmap.RunOptimize()
			finalData, err = existingBitmap.ToBytes()
			if err != nil {
				return fmt.Errorf("failed to serialize merged bitmap: %w", err)
			}
		} else {
			// No existing data, use the new bitmap as-is
			finalData = seg.Data
		}

		batch.PutCF(s.cf, seg.Key, finalData)
	}

	if err := s.db.Write(s.wo, batch); err != nil {
		return fmt.Errorf("failed to write segments: %w", err)
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
