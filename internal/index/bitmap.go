package index

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
)

// Compression markers (internal)
const (
	compressionMarker byte = 0x01
	noCompression     byte = 0x00
)

// BitmapIndex manages segmented roaring bitmap indexes
type BitmapIndex struct {
	db      *grocksdb.DB
	indexCF *grocksdb.ColumnFamilyHandle

	// Level 1: Hot segment cache - maps "prefix:keyHash:segmentID" to bitmap of ledgers
	hotSegments   map[string]*roaring.Bitmap
	hotSegmentsMu sync.RWMutex

	// Level 2: Hot cache - maps "L2prefix:keyHash:ledger" to bitmap of event indices
	hotL2Segments   map[string]*roaring.Bitmap
	hotL2SegmentsMu sync.RWMutex

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
		db:            db,
		indexCF:       indexCF,
		hotSegments:   make(map[string]*roaring.Bitmap),
		hotL2Segments: make(map[string]*roaring.Bitmap),
		zstdEncoder:   encoder,
		zstdDecoder:   decoder,
		wo:            wo,
		ro:            grocksdb.NewDefaultReadOptions(),
	}, nil
}

// Close releases resources
func (bi *BitmapIndex) Close() {
	bi.zstdEncoder.Close()
	bi.zstdDecoder.Close()
	bi.wo.Destroy()
	bi.ro.Destroy()
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

// =============================================================================
// Level 2 Index (Event-level granularity)
// =============================================================================

// makeL2DBKey creates a database key for an L2 bitmap
// Format: <L2prefix:1><keyValue:32><ledger:4>
func makeL2DBKey(prefix byte, keyValue []byte, ledgerSeq uint32) []byte {
	key := make([]byte, 1+32+4)
	key[0] = prefix

	// Pad or truncate keyValue to 32 bytes
	if len(keyValue) >= 32 {
		copy(key[1:33], keyValue[:32])
	} else {
		copy(key[1:1+len(keyValue)], keyValue)
	}

	// Big-endian ledger sequence for proper sorting
	binary.BigEndian.PutUint32(key[33:37], ledgerSeq)
	return key
}

// makeL2CacheKey creates a cache key for the hot L2 segments map
func makeL2CacheKey(prefix byte, keyValue []byte, ledgerSeq uint32) string {
	dbKey := makeL2DBKey(prefix, keyValue, ledgerSeq)
	return string(dbKey)
}

// AddToL2Index adds an event index to the L2 bitmap for a given key and ledger
// eventIndex encodes tx:op:event as a single uint32
func (bi *BitmapIndex) AddToL2Index(prefix byte, keyValue []byte, ledgerSeq uint32, eventIndex uint32) {
	cacheKey := makeL2CacheKey(prefix, keyValue, ledgerSeq)

	bi.hotL2SegmentsMu.Lock()
	defer bi.hotL2SegmentsMu.Unlock()

	bitmap, exists := bi.hotL2Segments[cacheKey]
	if !exists {
		bitmap = roaring.New()
		bi.hotL2Segments[cacheKey] = bitmap
	}

	bitmap.Add(eventIndex)
}

// AddContractL2Index adds an event to the contract ID L2 index
func (bi *BitmapIndex) AddContractL2Index(contractID []byte, ledgerSeq uint32, txIndex, opIndex, eventIndex uint16) {
	encoded := EncodeEventIndex(txIndex, opIndex, eventIndex)
	bi.AddToL2Index(PrefixContractL2, contractID, ledgerSeq, encoded)
}

// AddTopicL2Index adds an event to a topic L2 index
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

// FlushL2Segments writes all hot L2 segments to the database
func (bi *BitmapIndex) FlushL2Segments() error {
	bi.hotL2SegmentsMu.Lock()
	defer bi.hotL2SegmentsMu.Unlock()

	totalSegments := len(bi.hotL2Segments)
	if totalSegments == 0 {
		return nil
	}

	// Log flush start for large flushes
	if totalSegments > 10000 {
		fmt.Printf("Flushing %d L2 bitmap segments...\n", totalSegments)
	}

	start := time.Now()

	// Flush in batches to avoid huge WriteBatch memory usage
	const maxBatchSize = 10000
	batch := grocksdb.NewWriteBatch()
	batchCount := 0
	flushedCount := 0

	for cacheKey, bitmap := range bi.hotL2Segments {
		// Optimize bitmap before storage
		bitmap.RunOptimize()

		// Serialize
		data, err := bitmap.ToBytes()
		if err != nil {
			batch.Destroy()
			return fmt.Errorf("failed to serialize L2 bitmap: %w", err)
		}

		// Compress
		compressed := bi.zstdEncoder.EncodeAll(data, nil)

		// Add compression marker
		value := make([]byte, 1+len(compressed))
		value[0] = compressionMarker
		copy(value[1:], compressed)

		batch.PutCF(bi.indexCF, []byte(cacheKey), value)
		batchCount++

		// Write batch when it reaches max size
		if batchCount >= maxBatchSize {
			if err := bi.db.Write(bi.wo, batch); err != nil {
				batch.Destroy()
				return fmt.Errorf("failed to write L2 bitmap batch: %w", err)
			}
			flushedCount += batchCount
			batch.Destroy()
			batch = grocksdb.NewWriteBatch()
			batchCount = 0
		}
	}

	// Write remaining entries
	if batchCount > 0 {
		if err := bi.db.Write(bi.wo, batch); err != nil {
			batch.Destroy()
			return fmt.Errorf("failed to write L2 bitmap batch: %w", err)
		}
		flushedCount += batchCount
	}
	batch.Destroy()

	// Log flush completion for large flushes
	if totalSegments > 10000 {
		fmt.Printf("Flushed %d L2 segments in %v\n", flushedCount, time.Since(start))
	}

	// Clear hot L2 segments after successful write
	bi.hotL2Segments = make(map[string]*roaring.Bitmap)

	return nil
}

// loadL2Bitmap loads an L2 bitmap from the database
func (bi *BitmapIndex) loadL2Bitmap(prefix byte, keyValue []byte, ledgerSeq uint32) (*roaring.Bitmap, error) {
	dbKey := makeL2DBKey(prefix, keyValue, ledgerSeq)

	value, err := bi.db.GetCF(bi.ro, bi.indexCF, dbKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get L2 bitmap: %w", err)
	}
	defer value.Free()

	if value.Size() == 0 {
		return nil, nil // No data for this key
	}

	data := value.Data()

	// Check compression marker
	if data[0] == compressionMarker {
		decompressed, err := bi.zstdDecoder.DecodeAll(data[1:], nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress L2 bitmap: %w", err)
		}
		data = decompressed
	} else {
		data = data[1:] // Skip marker
	}

	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal L2 bitmap: %w", err)
	}

	return bitmap, nil
}

// QueryL2Index returns event indices for a specific ledger
func (bi *BitmapIndex) QueryL2Index(prefix byte, keyValue []byte, ledgerSeq uint32) (*roaring.Bitmap, error) {
	// Check hot cache first
	cacheKey := makeL2CacheKey(prefix, keyValue, ledgerSeq)

	bi.hotL2SegmentsMu.RLock()
	bitmap, inCache := bi.hotL2Segments[cacheKey]
	if inCache {
		bitmap = bitmap.Clone()
	}
	bi.hotL2SegmentsMu.RUnlock()

	if inCache {
		return bitmap, nil
	}

	// Load from DB
	return bi.loadL2Bitmap(prefix, keyValue, ledgerSeq)
}

// QueryIndexHierarchical performs a two-level query: L1 for ledgers, L2 for events
// If limit > 0, stops collecting event keys once limit is reached (early termination)
func (bi *BitmapIndex) QueryIndexHierarchical(l1Prefix, l2Prefix byte, keyValue []byte, startLedger, endLedger uint32, limit int) (*IndexQueryResult, error) {
	result := &IndexQueryResult{}
	totalStart := time.Now()

	// Phase 1: Query L1 to get matching ledgers
	l1Start := time.Now()
	ledgerBitmap, err := bi.QueryIndex(l1Prefix, keyValue, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("L1 query failed: %w", err)
	}
	result.L1LookupTime = time.Since(l1Start)
	result.MatchingLedgers = int(ledgerBitmap.GetCardinality())

	if ledgerBitmap.IsEmpty() {
		result.TotalTime = time.Since(totalStart)
		return result, nil
	}

	// Phase 2: For each matching ledger, query L2 to get event indices
	l2Start := time.Now()
	ledgers := ledgerBitmap.ToArray()

	for _, ledgerSeq := range ledgers {
		// Early termination if we have enough events
		if limit > 0 && len(result.EventKeys) >= limit {
			break
		}

		l2Bitmap, err := bi.QueryL2Index(l2Prefix, keyValue, ledgerSeq)
		if err != nil {
			return nil, fmt.Errorf("L2 query failed for ledger %d: %w", ledgerSeq, err)
		}

		if l2Bitmap == nil || l2Bitmap.IsEmpty() {
			continue
		}

		// Convert L2 bitmap to event keys
		eventIndices := l2Bitmap.ToArray()
		for _, encodedIdx := range eventIndices {
			// Early termination within ledger
			if limit > 0 && len(result.EventKeys) >= limit {
				break
			}
			result.EventKeys = append(result.EventKeys, EventKey{
				LedgerSeq:  ledgerSeq,
				EventIndex: encodedIdx, // Full 32-bit encoded value: [tx:10][op:10][event:12]
			})
		}
	}

	result.L2LookupTime = time.Since(l2Start)
	result.TotalEvents = len(result.EventKeys)
	result.TotalTime = time.Since(totalStart)

	return result, nil
}

// QueryContractIndexHierarchical queries using hierarchical approach for contract ID
func (bi *BitmapIndex) QueryContractIndexHierarchical(contractID []byte, startLedger, endLedger uint32, limit int) (*IndexQueryResult, error) {
	return bi.QueryIndexHierarchical(PrefixContractIndex, PrefixContractL2, contractID, startLedger, endLedger, limit)
}

// QueryTopicIndexHierarchical queries using hierarchical approach for topics
func (bi *BitmapIndex) QueryTopicIndexHierarchical(topicPosition int, topicValue []byte, startLedger, endLedger uint32, limit int) (*IndexQueryResult, error) {
	var l1Prefix, l2Prefix byte
	switch topicPosition {
	case 0:
		l1Prefix, l2Prefix = PrefixTopic0Index, PrefixTopic0L2
	case 1:
		l1Prefix, l2Prefix = PrefixTopic1Index, PrefixTopic1L2
	case 2:
		l1Prefix, l2Prefix = PrefixTopic2Index, PrefixTopic2L2
	case 3:
		l1Prefix, l2Prefix = PrefixTopic3Index, PrefixTopic3L2
	default:
		return nil, fmt.Errorf("invalid topic position: %d", topicPosition)
	}
	return bi.QueryIndexHierarchical(l1Prefix, l2Prefix, topicValue, startLedger, endLedger, limit)
}

// GetL2Stats returns statistics about L2 hot segments
func (bi *BitmapIndex) GetL2Stats() (count int, totalCards uint64) {
	bi.hotL2SegmentsMu.RLock()
	defer bi.hotL2SegmentsMu.RUnlock()

	count = len(bi.hotL2Segments)
	for _, bm := range bi.hotL2Segments {
		totalCards += bm.GetCardinality()
	}
	return
}

// FlushHotSegments writes all hot segments (L1 and L2) to the database
func (bi *BitmapIndex) FlushHotSegments() error {
	// Flush L1 segments
	if err := bi.flushL1Segments(); err != nil {
		return err
	}

	// Flush L2 segments
	return bi.FlushL2Segments()
}

// flushL1Segments writes L1 hot segments to the database
func (bi *BitmapIndex) flushL1Segments() error {
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
