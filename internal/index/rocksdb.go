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
	// QueryLedgersWithStats returns ledgers and query statistics.
	QueryLedgersWithStats(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*LedgerQueryResult, error)
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
	db          *grocksdb.DB
	cfContracts *grocksdb.ColumnFamilyHandle // Contract ID index CF
	cfTopics    *grocksdb.ColumnFamilyHandle // Topic index CF

	// In-memory bitmap index (this store handles its persistence)
	bitmap *BitmapIndex

	// RocksDB options
	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewRocksDBStore creates a new RocksDB-backed index store.
// Uses separate column families for contracts and topics.
func NewRocksDBStore(db *grocksdb.DB, cfContracts, cfTopics *grocksdb.ColumnFamilyHandle) (*RocksDBStore, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // WAL disabled for bulk ingestion

	store := &RocksDBStore{
		db:          db,
		cfContracts: cfContracts,
		cfTopics:    cfTopics,
		wo:          wo,
		ro:          grocksdb.NewDefaultReadOptions(),
	}

	// Create bitmap index with this store as the segment loader
	store.bitmap = NewBitmapIndex(store)

	return store, nil
}

// getCF returns the appropriate column family for a given prefix.
func (s *RocksDBStore) getCF(prefix byte) *grocksdb.ColumnFamilyHandle {
	if prefix == PrefixContractIndex {
		return s.cfContracts
	}
	return s.cfTopics
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
	bitmap, _, err := s.LoadSegmentWithStats(prefix, keyValue, segmentID)
	return bitmap, err
}

// LoadSegmentWithStats loads a segment and returns bytes read.
func (s *RocksDBStore) LoadSegmentWithStats(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, int64, error) {
	dbKey := MakeL1Key(prefix, keyValue, segmentID)
	cf := s.getCF(prefix)
	return s.loadBitmapFromCFWithStats(cf, dbKey)
}

// loadBitmapFromCF loads a bitmap from a specific column family.
// RocksDB handles compression transparently via LZ4.
func (s *RocksDBStore) loadBitmapFromCF(cf *grocksdb.ColumnFamilyHandle, key []byte) (*roaring.Bitmap, error) {
	bitmap, _, err := s.loadBitmapFromCFWithStats(cf, key)
	return bitmap, err
}

func (s *RocksDBStore) loadBitmapFromCFWithStats(cf *grocksdb.ColumnFamilyHandle, key []byte) (*roaring.Bitmap, int64, error) {
	data, err := s.db.GetCF(s.ro, cf, key)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get bitmap: %w", err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, 0, nil // No data for this key
	}

	bytesRead := int64(data.Size())
	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(data.Data()); err != nil {
		return nil, bytesRead, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	return bitmap, bytesRead, nil
}

// =============================================================================
// Store Interface Implementation - Write Operations
// =============================================================================

// AddContractLedger adds a ledger to the contract index.
func (s *RocksDBStore) AddContractLedger(contractID []byte, ledger uint32) error {
	s.bitmap.AddContractIndex(contractID, ledger)
	return nil
}

// AddTopicLedger adds a ledger to the topic index (non-positional).
func (s *RocksDBStore) AddTopicLedger(topic []byte, ledger uint32) error {
	s.bitmap.AddTopicIndex(topic, ledger)
	return nil
}

// =============================================================================
// Store Interface Implementation - Query Operations
// =============================================================================

// LedgerQueryResult holds the result of a ledger query including stats.
type LedgerQueryResult struct {
	Bitmap    *roaring.Bitmap
	BytesRead int64
	Segments  int
}

// QueryLedgers returns a bitmap of ledgers matching the filter criteria.
// Topics are matched non-positionally (same semantics as posting list).
func (s *RocksDBStore) QueryLedgers(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	result, err := s.QueryLedgersWithStats(contractID, topics, startLedger, endLedger)
	if err != nil {
		return nil, err
	}
	return result.Bitmap, nil
}

// QueryLedgersWithStats returns ledgers and query statistics.
func (s *RocksDBStore) QueryLedgersWithStats(contractID []byte, topics [][]byte, startLedger, endLedger uint32) (*LedgerQueryResult, error) {
	result := &LedgerQueryResult{
		Bitmap: roaring.New(),
	}
	var bitmaps []*roaring.Bitmap

	// Query contract index if specified
	if len(contractID) > 0 {
		bm, bytesRead, segments, err := s.bitmap.QueryContractIndexWithStats(contractID, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("contract index query failed: %w", err)
		}
		result.BytesRead += bytesRead
		result.Segments += segments
		bitmaps = append(bitmaps, bm)
	}

	// Query topic index for each topic (non-positional)
	for _, topic := range topics {
		if len(topic) == 0 {
			continue
		}
		bm, bytesRead, segments, err := s.bitmap.QueryTopicIndexWithStats(topic, startLedger, endLedger)
		if err != nil {
			return nil, fmt.Errorf("topic index query failed: %w", err)
		}
		result.BytesRead += bytesRead
		result.Segments += segments
		bitmaps = append(bitmaps, bm)
	}

	if len(bitmaps) == 0 {
		return result, nil
	}

	// Use FastAnd for efficient intersection (avoids Clone + multiple And calls)
	result.Bitmap = roaring.FastAnd(bitmaps...)
	return result, nil
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

	// Count stored segments per index type (non-positional)
	stats.ContractIndexCount = s.countIndexEntries(PrefixContractIndex)
	stats.TopicIndexCount = s.countIndexEntries(PrefixTopicIndex)

	return stats
}

// countIndexEntries counts the number of stored bitmap segments for an index type.
func (s *RocksDBStore) countIndexEntries(prefix byte) int64 {
	cf := s.getCF(prefix)
	iter := s.db.NewIteratorCF(s.ro, cf)
	defer iter.Close()

	var count int64
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
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
		// Extract prefix from key to determine CF
		prefix := seg.Key[0]
		cf := s.getCF(prefix)

		// Load existing bitmap from RocksDB (if any)
		existingBitmap, err := s.loadBitmapFromCF(cf, seg.Key)
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

		batch.PutCF(cf, seg.Key, finalData)
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
