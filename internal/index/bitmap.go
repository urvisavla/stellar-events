package index

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
)

const (
	// SegmentSize is the number of ledgers per bitmap segment
	// 1M ledgers ≈ 2 months of Stellar data
	// Small enough to limit write amplification, large enough to amortize overhead
	SegmentSize uint32 = 1_000_000

	// Index type prefixes
	PrefixContractIndex byte = 0x01
	PrefixTopic0Index   byte = 0x02
	PrefixTopic1Index   byte = 0x03
	PrefixTopic2Index   byte = 0x04
	PrefixTopic3Index   byte = 0x05

	// Compression marker
	compressionMarker byte = 0x01
	noCompression     byte = 0x00
)

// BitmapIndex manages segmented roaring bitmap indexes
type BitmapIndex struct {
	db      *grocksdb.DB
	indexCF *grocksdb.ColumnFamilyHandle

	// Hot segment cache - maps "prefix:keyHash:segmentID" to bitmap
	hotSegments   map[string]*roaring.Bitmap
	hotSegmentsMu sync.RWMutex

	// Current hot segment ID (the one being actively written)
	currentSegmentID uint32

	// Compression
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder

	// Write options
	wo *grocksdb.WriteOptions
	ro *grocksdb.ReadOptions
}

// NewBitmapIndex creates a new bitmap index manager
func NewBitmapIndex(db *grocksdb.DB, indexCF *grocksdb.ColumnFamilyHandle) (*BitmapIndex, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // WAL disabled for bulk ingestion

	return &BitmapIndex{
		db:           db,
		indexCF:      indexCF,
		hotSegments:  make(map[string]*roaring.Bitmap),
		zstdEncoder:  encoder,
		zstdDecoder:  decoder,
		wo:           wo,
		ro:           grocksdb.NewDefaultReadOptions(),
	}, nil
}

// Close releases resources
func (bi *BitmapIndex) Close() {
	bi.zstdEncoder.Close()
	bi.zstdDecoder.Close()
	bi.wo.Destroy()
	bi.ro.Destroy()
}

// SegmentID returns the segment ID for a given ledger sequence
func SegmentID(ledgerSeq uint32) uint32 {
	return ledgerSeq / SegmentSize
}

// SegmentRange returns the ledger range for a segment
func SegmentRange(segmentID uint32) (start, end uint32) {
	start = segmentID * SegmentSize
	end = start + SegmentSize - 1
	return
}

// makeDBKey creates a database key for a bitmap segment
// Format: <prefix:1><keyValue:32><segmentID:4>
func makeDBKey(prefix byte, keyValue []byte, segmentID uint32) []byte {
	key := make([]byte, 1+32+4)
	key[0] = prefix

	// Pad or truncate keyValue to 32 bytes
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}

	// Big-endian segment ID for proper sorting
	binary.BigEndian.PutUint32(key[33:37], segmentID)
	return key
}

// makeCacheKey creates a cache key for the hot segments map
func makeCacheKey(prefix byte, keyValue []byte, segmentID uint32) string {
	dbKey := makeDBKey(prefix, keyValue, segmentID)
	return string(dbKey)
}

// AddToIndex adds a ledger to the bitmap index for a given key
func (bi *BitmapIndex) AddToIndex(prefix byte, keyValue []byte, ledgerSeq uint32) {
	segmentID := SegmentID(ledgerSeq)
	localLedger := ledgerSeq % SegmentSize // Offset within segment

	cacheKey := makeCacheKey(prefix, keyValue, segmentID)

	bi.hotSegmentsMu.Lock()
	defer bi.hotSegmentsMu.Unlock()

	bitmap, exists := bi.hotSegments[cacheKey]
	if !exists {
		bitmap = roaring.New()
		bi.hotSegments[cacheKey] = bitmap
	}

	bitmap.Add(localLedger)

	// Track current segment
	if segmentID > bi.currentSegmentID {
		bi.currentSegmentID = segmentID
	}
}

// AddContractIndex adds a ledger to the contract ID index
func (bi *BitmapIndex) AddContractIndex(contractID []byte, ledgerSeq uint32) {
	bi.AddToIndex(PrefixContractIndex, contractID, ledgerSeq)
}

// AddTopicIndex adds a ledger to a topic index
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

// FlushHotSegments writes all hot segments to the database
func (bi *BitmapIndex) FlushHotSegments() error {
	bi.hotSegmentsMu.Lock()
	defer bi.hotSegmentsMu.Unlock()

	if len(bi.hotSegments) == 0 {
		return nil
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for cacheKey, bitmap := range bi.hotSegments {
		// Optimize bitmap before storage
		bitmap.RunOptimize()

		// Serialize
		data, err := bitmap.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to serialize bitmap: %w", err)
		}

		// Compress
		compressed := bi.zstdEncoder.EncodeAll(data, nil)

		// Add compression marker
		value := make([]byte, 1+len(compressed))
		value[0] = compressionMarker
		copy(value[1:], compressed)

		batch.PutCF(bi.indexCF, []byte(cacheKey), value)
	}

	if err := bi.db.Write(bi.wo, batch); err != nil {
		return fmt.Errorf("failed to write bitmap batch: %w", err)
	}

	// Clear hot segments after successful write
	bi.hotSegments = make(map[string]*roaring.Bitmap)

	return nil
}

// FlushSegment writes a specific segment to disk and removes from hot cache
func (bi *BitmapIndex) FlushSegment(segmentID uint32) error {
	bi.hotSegmentsMu.Lock()
	defer bi.hotSegmentsMu.Unlock()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	var keysToDelete []string

	for cacheKey, bitmap := range bi.hotSegments {
		// Parse segment ID from cache key (last 4 bytes)
		keyBytes := []byte(cacheKey)
		if len(keyBytes) < 37 {
			continue
		}
		sid := binary.BigEndian.Uint32(keyBytes[33:37])
		if sid != segmentID {
			continue
		}

		// Optimize and serialize
		bitmap.RunOptimize()
		data, err := bitmap.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to serialize bitmap: %w", err)
		}

		// Compress
		compressed := bi.zstdEncoder.EncodeAll(data, nil)

		// Add compression marker
		value := make([]byte, 1+len(compressed))
		value[0] = compressionMarker
		copy(value[1:], compressed)

		batch.PutCF(bi.indexCF, keyBytes, value)
		keysToDelete = append(keysToDelete, cacheKey)
	}

	if batch.Count() == 0 {
		return nil
	}

	if err := bi.db.Write(bi.wo, batch); err != nil {
		return fmt.Errorf("failed to write segment %d: %w", segmentID, err)
	}

	// Remove flushed entries from hot cache
	for _, key := range keysToDelete {
		delete(bi.hotSegments, key)
	}

	return nil
}

// loadBitmap loads a bitmap from the database
func (bi *BitmapIndex) loadBitmap(prefix byte, keyValue []byte, segmentID uint32) (*roaring.Bitmap, error) {
	dbKey := makeDBKey(prefix, keyValue, segmentID)

	data, err := bi.db.GetCF(bi.ro, bi.indexCF, dbKey)
	if err != nil {
		return nil, err
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, nil // No data for this segment
	}

	raw := data.Data()

	// Check compression marker
	var bitmapData []byte
	if raw[0] == compressionMarker {
		bitmapData, err = bi.zstdDecoder.DecodeAll(raw[1:], nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress bitmap: %w", err)
		}
	} else {
		bitmapData = raw[1:]
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(bitmapData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bitmap: %w", err)
	}

	return bitmap, nil
}

// QueryIndex returns all ledgers matching a key within a range
func (bi *BitmapIndex) QueryIndex(prefix byte, keyValue []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	result := roaring.New()

	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	for segID := startSegment; segID <= endSegment; segID++ {
		// Check hot cache first
		cacheKey := makeCacheKey(prefix, keyValue, segID)

		bi.hotSegmentsMu.RLock()
		bitmap, inCache := bi.hotSegments[cacheKey]
		if inCache {
			// Clone to avoid holding lock during iteration
			bitmap = bitmap.Clone()
		}
		bi.hotSegmentsMu.RUnlock()

		if !inCache {
			// Load from DB
			var err error
			bitmap, err = bi.loadBitmap(prefix, keyValue, segID)
			if err != nil {
				return nil, err
			}
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
			bitmap = roaring.And(bitmap, mask) // Returns new bitmap, doesn't modify original
		}

		if bitmap.IsEmpty() {
			continue
		}

		// Convert local offsets to absolute ledger numbers and merge
		// Get all values as array, offset them, and add in bulk
		localValues := bitmap.ToArray()
		absoluteValues := make([]uint32, len(localValues))
		for i, local := range localValues {
			absoluteValues[i] = segmentBase + local
		}

		// Bulk add and merge with Or
		segmentResult := roaring.New()
		segmentResult.AddMany(absoluteValues)
		result.Or(segmentResult)
	}

	return result, nil
}

// QueryContractIndex queries ledgers for a contract ID
func (bi *BitmapIndex) QueryContractIndex(contractID []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	return bi.QueryIndex(PrefixContractIndex, contractID, startLedger, endLedger)
}

// QueryTopicIndex queries ledgers for a topic value
func (bi *BitmapIndex) QueryTopicIndex(topicPosition int, topicValue []byte, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
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
		return nil, fmt.Errorf("invalid topic position: %d", topicPosition)
	}
	return bi.QueryIndex(prefix, topicValue, startLedger, endLedger)
}

// GetHotSegmentStats returns statistics about hot segments
func (bi *BitmapIndex) GetHotSegmentStats() (count int, totalCards uint64, memBytes uint64) {
	bi.hotSegmentsMu.RLock()
	defer bi.hotSegmentsMu.RUnlock()

	count = len(bi.hotSegments)
	for _, bitmap := range bi.hotSegments {
		totalCards += bitmap.GetCardinality()
		memBytes += bitmap.GetSizeInBytes()
	}
	return
}

// BitmapIndexStats holds statistics about the bitmap index
type BitmapIndexStats struct {
	HotSegmentCount     int
	HotSegmentCards     uint64
	HotSegmentMemBytes  uint64
	CurrentSegmentID    uint32
	ContractIndexCount  int64
	Topic0IndexCount    int64
	Topic1IndexCount    int64
	Topic2IndexCount    int64
	Topic3IndexCount    int64
}

// GetStats returns statistics about the bitmap index
func (bi *BitmapIndex) GetStats() *BitmapIndexStats {
	count, cards, memBytes := bi.GetHotSegmentStats()

	stats := &BitmapIndexStats{
		HotSegmentCount:    count,
		HotSegmentCards:    cards,
		HotSegmentMemBytes: memBytes,
		CurrentSegmentID:   bi.currentSegmentID,
	}

	// Count stored segments per index type
	stats.ContractIndexCount = bi.countIndexEntries(PrefixContractIndex)
	stats.Topic0IndexCount = bi.countIndexEntries(PrefixTopic0Index)
	stats.Topic1IndexCount = bi.countIndexEntries(PrefixTopic1Index)
	stats.Topic2IndexCount = bi.countIndexEntries(PrefixTopic2Index)
	stats.Topic3IndexCount = bi.countIndexEntries(PrefixTopic3Index)

	return stats
}

// countIndexEntries counts the number of stored bitmap segments for an index type
func (bi *BitmapIndex) countIndexEntries(prefix byte) int64 {
	iter := bi.db.NewIteratorCF(bi.ro, bi.indexCF)
	defer iter.Close()

	prefixBytes := []byte{prefix}
	var count int64

	for iter.Seek(prefixBytes); iter.ValidForPrefix(prefixBytes); iter.Next() {
		count++
	}

	return count
}
