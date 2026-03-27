package store

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/linxGnu/grocksdb"
)

// Column family names
const (
	CFDefault = "default" // Metadata (last_processed_ledger, etc.)
	CFEvents  = "events"  // Primary event storage (raw XDR or binary)
	CFUnique  = "unique"  // Unique value indexes with counts

	// 32-bit bitmap indexes (event-level, FromBuffer decode)
	CFContractsBM32 = "contracts_bm32" // Contract ID bitmap32 index
	CFTopicsBM32    = "topics_bm32"    // Topic bitmap32 index

	CFIndexDeltas      = "index_deltas"      // Hot segment index deltas
	CFBitmapSnapshots  = "bitmap_snapshots"  // Pre-serialized roaring bitmaps per term
)

// RocksDBOptions contains tuning parameters for RocksDB.
type RocksDBOptions struct {
	WriteBufferSizeMB           int
	MaxWriteBufferNumber        int
	MinWriteBufferNumberToMerge int

	BloomFilterBitsPerKey int

	MaxBackgroundJobs int

	Compression           string
	BottommostCompression string

	DisableWAL            bool
	DisableAutoCompaction bool

	TargetFileSizeMB       int
	MaxBytesForLevelBaseMB int
}

func applyRocksDBOptions(opts *grocksdb.Options, rocksOpts *RocksDBOptions) {
	if rocksOpts == nil {
		opts.SetCompression(grocksdb.LZ4Compression)
		return
	}

	if rocksOpts.WriteBufferSizeMB > 0 {
		opts.SetWriteBufferSize(uint64(rocksOpts.WriteBufferSizeMB) * 1024 * 1024)
	}
	if rocksOpts.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(rocksOpts.MaxWriteBufferNumber)
	}
	if rocksOpts.MinWriteBufferNumberToMerge > 0 {
		opts.SetMinWriteBufferNumberToMerge(rocksOpts.MinWriteBufferNumberToMerge)
	}
	if rocksOpts.MaxBackgroundJobs > 0 {
		opts.SetMaxBackgroundJobs(rocksOpts.MaxBackgroundJobs)
	}

	opts.SetCompression(parseCompression(rocksOpts.Compression))
	if rocksOpts.BottommostCompression != "" {
		opts.SetBottommostCompression(parseCompression(rocksOpts.BottommostCompression))
	}

	if rocksOpts.DisableAutoCompaction {
		opts.SetDisableAutoCompactions(true)
	}
	if rocksOpts.TargetFileSizeMB > 0 {
		opts.SetTargetFileSizeBase(uint64(rocksOpts.TargetFileSizeMB) * 1024 * 1024)
	}
	if rocksOpts.MaxBytesForLevelBaseMB > 0 {
		opts.SetMaxBytesForLevelBase(uint64(rocksOpts.MaxBytesForLevelBaseMB) * 1024 * 1024)
	}
}

func parseCompression(compression string) grocksdb.CompressionType {
	switch strings.ToLower(compression) {
	case "none", "":
		return grocksdb.NoCompression
	case "snappy":
		return grocksdb.SnappyCompression
	case "lz4":
		return grocksdb.LZ4Compression
	case "zstd":
		return grocksdb.ZSTDCompression
	default:
		return grocksdb.LZ4Compression
	}
}

// uint64AddMergeOperator implements a merge operator that adds uint64 values.
type uint64AddMergeOperator struct{}

func (m *uint64AddMergeOperator) Name() string { return "uint64-add" }

func (m *uint64AddMergeOperator) FullMerge(_, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var total uint64
	if len(existingValue) == 8 {
		total = binary.BigEndian.Uint64(existingValue)
	}
	for _, operand := range operands {
		if len(operand) == 8 {
			total += binary.BigEndian.Uint64(operand)
		}
	}
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, total)
	return result, true
}

func (m *uint64AddMergeOperator) PartialMerge(_, leftOperand, rightOperand []byte) ([]byte, bool) {
	var left, right uint64
	if len(leftOperand) == 8 {
		left = binary.BigEndian.Uint64(leftOperand)
	}
	if len(rightOperand) == 8 {
		right = binary.BigEndian.Uint64(rightOperand)
	}
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, left+right)
	return result, true
}

// RocksDBBackend encapsulates all grocksdb-owned resources so that higher-level
// code (Store) can coordinate without importing grocksdb directly.
type RocksDBBackend struct {
	db *grocksdb.DB
	wo *grocksdb.WriteOptions
	ro     *grocksdb.ReadOptions

	cfHandles       []*grocksdb.ColumnFamilyHandle
	cfDefault     *grocksdb.ColumnFamilyHandle
	cfEvents      *grocksdb.ColumnFamilyHandle
	cfUnique      *grocksdb.ColumnFamilyHandle
	cfIndexDeltas     *grocksdb.ColumnFamilyHandle
	cfBitmapSnapshots *grocksdb.ColumnFamilyHandle

	baseOpts *grocksdb.Options
	cfOpts   []*grocksdb.Options
	bbtoList []*grocksdb.BlockBasedTableOptions
	mergeOp  grocksdb.MergeOperator

	bitmapStore *RocksDBIndexStore
}

// NewRocksDBBackend opens a RocksDB database with column families and returns a backend.
func NewRocksDBBackend(dbPath string, rocksOpts *RocksDBOptions) (*RocksDBBackend, error) {
	baseOpts := grocksdb.NewDefaultOptions()
	baseOpts.SetCreateIfMissing(true)
	baseOpts.SetCreateIfMissingColumnFamilies(true)
	applyRocksDBOptions(baseOpts, rocksOpts)

	defaultOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(defaultOpts, rocksOpts)

	eventsOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(eventsOpts, rocksOpts)
	eventsBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	eventsBBTO.SetBlockSize(64 * 1024)
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		eventsBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	eventsOpts.SetBlockBasedTableFactory(eventsBBTO)

	uniqueOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(uniqueOpts, rocksOpts)
	mergeOp := &uint64AddMergeOperator{}
	uniqueOpts.SetMergeOperator(mergeOp)
	uniqueBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	uniqueBBTO.SetBlockSize(4 * 1024)
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		uniqueBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	uniqueOpts.SetBlockBasedTableFactory(uniqueBBTO)

	createBitmapCFOpts := func() (*grocksdb.Options, *grocksdb.BlockBasedTableOptions) {
		opts := grocksdb.NewDefaultOptions()
		applyRocksDBOptions(opts, rocksOpts)
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(16 * 1024)
		if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
			bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
		}
		opts.SetBlockBasedTableFactory(bbto)
		return opts, bbto
	}

	contractsBM32Opts, contractsBM32BBTO := createBitmapCFOpts()
	topicsBM32Opts, topicsBM32BBTO := createBitmapCFOpts()

	// index_deltas CF: small 21-byte values, prefix scans by segmentID
	indexDeltasOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(indexDeltasOpts, rocksOpts)
	indexDeltasBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	indexDeltasBBTO.SetBlockSize(4 * 1024)
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		indexDeltasBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	indexDeltasOpts.SetBlockBasedTableFactory(indexDeltasBBTO)

	// bitmap_snapshots CF: larger values (serialized roaring bitmaps), prefix scans by segmentID
	bitmapSnapshotsOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(bitmapSnapshotsOpts, rocksOpts)
	bitmapSnapshotsBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	bitmapSnapshotsBBTO.SetBlockSize(64 * 1024) // 64 KB blocks for larger bitmap values
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		bitmapSnapshotsBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	bitmapSnapshotsOpts.SetBlockBasedTableFactory(bitmapSnapshotsBBTO)

	cfNames := []string{
		CFDefault, CFEvents, CFUnique,
		CFContractsBM32, CFTopicsBM32,
		CFIndexDeltas, CFBitmapSnapshots,
	}
	cfOpts := []*grocksdb.Options{
		defaultOpts, eventsOpts, uniqueOpts,
		contractsBM32Opts, topicsBM32Opts,
		indexDeltasOpts, bitmapSnapshotsOpts,
	}
	bbtoList := []*grocksdb.BlockBasedTableOptions{
		eventsBBTO, uniqueBBTO,
		contractsBM32BBTO, topicsBM32BBTO,
		indexDeltasBBTO, bitmapSnapshotsBBTO,
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(baseOpts, dbPath, cfNames, cfOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open RocksDB with column families: %w", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	if rocksOpts != nil && rocksOpts.DisableWAL {
		wo.DisableWAL(true)
	}

	bitmapStore, err := NewRocksDBIndexStore(db, cfHandles[0], cfHandles[3], cfHandles[4])
	if err != nil {
		db.Close()
		wo.Destroy()
		return nil, fmt.Errorf("failed to create RocksDB index store: %w", err)
	}

	return &RocksDBBackend{
		db:            db,
		wo:            wo,
		ro:            grocksdb.NewDefaultReadOptions(),
		cfHandles:     cfHandles,
		cfDefault:     cfHandles[0],
		cfEvents:      cfHandles[1],
		cfUnique:      cfHandles[2],
		cfIndexDeltas:     cfHandles[5],
		cfBitmapSnapshots: cfHandles[6],
		baseOpts:      baseOpts,
		cfOpts:        cfOpts,
		bbtoList:      bbtoList,
		mergeOp:       mergeOp,
		bitmapStore:   bitmapStore,
	}, nil
}

// WriteEventBatch writes event key-value pairs and unique count merges to RocksDB.
func (rb *RocksDBBackend) WriteEventBatch(kvs []EventKV, uniqueCounts map[string]uint64) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, kv := range kvs {
		batch.PutCF(rb.cfEvents, kv.Key, kv.Value)
	}

	for keyStr, addCount := range uniqueCounts {
		keyBytes := []byte(keyStr)
		countBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(countBytes, addCount)
		batch.MergeCF(rb.cfUnique, keyBytes, countBytes)
	}

	if batch.Count() > 0 {
		if err := rb.db.Write(rb.wo, batch); err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}
	}
	return nil
}

// SetLastProcessedLedger stores the last processed ledger sequence.
func (rb *RocksDBBackend) SetLastProcessedLedger(sequence uint32) error {
	key := []byte("last_processed_ledger")
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, sequence)
	return rb.db.PutCF(rb.wo, rb.cfDefault, key, value)
}

// GetLastProcessedLedger retrieves the last processed ledger sequence.
func (rb *RocksDBBackend) GetLastProcessedLedger() (uint32, error) {
	key := []byte("last_processed_ledger")
	value, err := rb.db.GetCF(rb.ro, rb.cfDefault, key)
	if err != nil {
		return 0, err
	}
	defer value.Free()

	if value.Size() == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint32(value.Data()), nil
}

// Flush forces all memtables to be flushed to SST files.
func (rb *RocksDBBackend) Flush() error {
	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true)

	for _, cf := range rb.cfHandles {
		if err := rb.db.FlushCF(cf, flushOpts); err != nil {
			return fmt.Errorf("failed to flush column family: %w", err)
		}
	}
	return nil
}


// Close closes the RocksDB database and releases resources.
func (rb *RocksDBBackend) Close() {
	rb.wo.Destroy()
	rb.ro.Destroy()
	rb.db.Close()
}

// EncodeKey creates an 8-byte RocksDB key from segment ID and dense local ID.
// Format: [segmentID:4][denseID:4] (big-endian)
func EncodeKey(segmentID uint32, denseID uint32) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[0:4], segmentID)
	binary.BigEndian.PutUint32(key[4:8], denseID)
	return key
}

// DecodeKey decodes an 8-byte key into segment ID and dense local ID.
func DecodeKey(key []byte) (segmentID uint32, denseID uint32) {
	segmentID = binary.BigEndian.Uint32(key[0:4])
	denseID = binary.BigEndian.Uint32(key[4:8])
	return
}

// FlushHotCFs flushes the index_deltas and events column families to SST files.
func (rb *RocksDBBackend) FlushHotCFs() error {
	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true)

	for _, cf := range []*grocksdb.ColumnFamilyHandle{rb.cfIndexDeltas, rb.cfEvents, rb.cfDefault, rb.cfBitmapSnapshots} {
		if err := rb.db.FlushCF(cf, flushOpts); err != nil {
			return fmt.Errorf("failed to flush hot CF: %w", err)
		}
	}
	return nil
}

// DeleteHotSegment removes all hot segment data for a given segment ID from
// index_deltas CF, events CF, and the ledger offsets key in default CF.
func (rb *RocksDBBackend) DeleteHotSegment(segmentID uint32) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Key range: [segID, 0x00000000] to [segID+1, 0x00000000] (exclusive end)
	startKey := EncodeKey(segmentID, 0)
	endKey := EncodeKey(segmentID+1, 0)

	batch.DeleteRangeCF(rb.cfIndexDeltas, startKey, endKey)
	batch.DeleteRangeCF(rb.cfEvents, startKey, endKey)

	// bitmap_snapshots uses 21-byte keys: [segID:4][fieldIndex:1][termHash:16]
	snapStart := make([]byte, 4)
	binary.BigEndian.PutUint32(snapStart, segmentID)
	snapEnd := make([]byte, 4)
	binary.BigEndian.PutUint32(snapEnd, segmentID+1)
	batch.DeleteRangeCF(rb.cfBitmapSnapshots, snapStart, snapEnd)

	// Delete ledger offsets from default CF
	batch.DeleteCF(rb.cfDefault, SegmentLedgerOffsetsKey(segmentID))

	return rb.db.Write(rb.wo, batch)
}
