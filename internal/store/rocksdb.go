package store

import (
	"bytes"
	"container/heap"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/query"
)

// Column family names
const (
	CFDefault = "default" // Metadata (last_processed_ledger, etc.)
	CFEvents  = "events"  // Primary event storage (raw XDR or binary)
	CFUnique  = "unique"  // Unique value indexes with counts

	// 32-bit bitmap indexes (event-level, FromBuffer decode)
	CFContractsBM32 = "contracts_bm32" // Contract ID bitmap32 index
	CFTopicsBM32    = "topics_bm32"    // Topic bitmap32 index
)

// RocksDBOptions contains tuning parameters for RocksDB.
type RocksDBOptions struct {
	// Write performance
	WriteBufferSizeMB           int
	MaxWriteBufferNumber        int
	MinWriteBufferNumberToMerge int

	// Read performance
	BlockCacheSizeMB          int
	BloomFilterBitsPerKey     int
	CacheIndexAndFilterBlocks bool

	// Background jobs
	MaxBackgroundJobs int

	// Compression
	Compression           string
	BottommostCompression string

	// WAL
	DisableWAL bool

	// Auto compaction
	DisableAutoCompaction bool

	// Compaction tuning
	TargetFileSizeMB       int // Target size for SST files (default: 64, recommend 256-512 for large DBs)
	MaxBytesForLevelBaseMB int // Max bytes for L1 (default: 256, recommend 1024+ for large DBs)
}

// applyRocksDBOptions applies common RocksDB options
func applyRocksDBOptions(opts *grocksdb.Options, rocksOpts *RocksDBOptions) {
	if rocksOpts == nil {
		opts.SetCompression(grocksdb.LZ4Compression)
		return
	}

	// Write performance
	if rocksOpts.WriteBufferSizeMB > 0 {
		opts.SetWriteBufferSize(uint64(rocksOpts.WriteBufferSizeMB) * 1024 * 1024)
	}
	if rocksOpts.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(rocksOpts.MaxWriteBufferNumber)
	}
	if rocksOpts.MinWriteBufferNumberToMerge > 0 {
		opts.SetMinWriteBufferNumberToMerge(rocksOpts.MinWriteBufferNumberToMerge)
	}

	// Background jobs
	if rocksOpts.MaxBackgroundJobs > 0 {
		opts.SetMaxBackgroundJobs(rocksOpts.MaxBackgroundJobs)
	}

	// Compression
	opts.SetCompression(parseCompression(rocksOpts.Compression))
	if rocksOpts.BottommostCompression != "" {
		opts.SetBottommostCompression(parseCompression(rocksOpts.BottommostCompression))
	}

	// Disable auto compaction
	if rocksOpts.DisableAutoCompaction {
		opts.SetDisableAutoCompactions(true)
	}

	// Compaction tuning - larger files = fewer files after compaction
	if rocksOpts.TargetFileSizeMB > 0 {
		opts.SetTargetFileSizeBase(uint64(rocksOpts.TargetFileSizeMB) * 1024 * 1024)
	}
	if rocksOpts.MaxBytesForLevelBaseMB > 0 {
		opts.SetMaxBytesForLevelBase(uint64(rocksOpts.MaxBytesForLevelBaseMB) * 1024 * 1024)
	}
}

// parseCompression converts a compression string to grocksdb compression type
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

// =============================================================================
// BitmapEventSeqStore — RocksDB I/O only for bitmap indexes
// =============================================================================

// BitmapEventSeqStore implements event-level index storage using RocksDB with 32-bit roaring bitmaps.
// Uses FromBuffer for near-zero-cost decode instead of UnmarshalBinary.
// This is a pure I/O layer — it does NOT own the in-memory bitmap index.
type BitmapEventSeqStore struct {
	db          *grocksdb.DB
	cfContracts *grocksdb.ColumnFamilyHandle // Contract ID index CF (bitmap32)
	cfTopics    *grocksdb.ColumnFamilyHandle // Topic index CF (bitmap32)
	cfDefault   *grocksdb.ColumnFamilyHandle // Default CF for ledger maps

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

	return store, nil
}

// LoadSegmentLedgerMap implements the ledgerMapLoader interface.
// Reads the ledger map for a segment from CFDefault.
func (s *BitmapEventSeqStore) LoadSegmentLedgerMap(segmentID uint32) (*SegmentLedgerMap, error) {
	key := SegmentLedgerMapKey(segmentID)
	data, err := s.db.GetCF(s.ro, s.cfDefault, key)
	if err != nil {
		return nil, fmt.Errorf("failed to load ledger map for segment %d: %w", segmentID, err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, nil
	}

	// Copy data since it becomes invalid after Free
	dataCopy := make([]byte, data.Size())
	copy(dataCopy, data.Data())

	return &SegmentLedgerMap{
		SegmentID: segmentID,
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

// LoadBitmap32Segment implements bitmap32Loader interface.
func (s *BitmapEventSeqStore) LoadBitmap32Segment(isContract bool, termKey [32]byte, pos int, segmentID uint32) (*roaring.Bitmap, error) {
	bitmap, _, _, _, err := s.LoadBitmap32SegmentWithTiming(isContract, termKey, pos, segmentID)
	return bitmap, err
}

// LoadBitmap32SegmentWithTiming loads a segment using FromBuffer for near-zero-cost decode.
func (s *BitmapEventSeqStore) LoadBitmap32SegmentWithTiming(isContract bool, termKey [32]byte, pos int, segmentID uint32) (*roaring.Bitmap, int64, time.Duration, time.Duration, error) {
	var dbKey []byte
	if isContract {
		dbKey = EncodeIndexKeyWithSegment(termKey, segmentID)
	} else {
		dbKey = EncodeTopicIndexKey(pos, termKey, segmentID)
	}
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

// Flush persists bitmap segments and ledger maps to RocksDB, merging with existing data.
// Receives data as parameters instead of pulling from internal state.
// Returns the computed ledger map data per segment for optional flat file writes.
func (s *BitmapEventSeqStore) Flush(
	segments []bitmapChunk,
	counters map[uint32]*segmentEventCounter,
	writeToRocksDB bool,
) (map[uint32][]byte, error) {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, seg := range segments {
		isContract := seg.FieldIndex == 0
		cf := s.getCF(isContract)

		// Encode the correct RocksDB key format based on field type.
		// seg.Key contains [termHash:32][segmentID:4] from the hot index.
		// For topics, we need to re-encode as [pos:1][termHash:32][segmentID:4].
		var dbKey []byte
		if isContract {
			dbKey = seg.Key // Already [termHash:32][segmentID:4] = 36 bytes
		} else {
			pos := seg.FieldIndex - 1 // 1=topic0, 2=topic1, 3=topic2, 4=topic3
			var termKey [32]byte
			copy(termKey[:], seg.Key[0:32])
			segmentID := binary.BigEndian.Uint32(seg.Key[32:36])
			dbKey = EncodeTopicIndexKey(pos, termKey, segmentID)
		}

		if writeToRocksDB {
			existingBitmap, err := s.loadBitmap32FromCF(cf, dbKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load existing event bitmap32 for merge: %w", err)
			}

			var finalData []byte
			if existingBitmap != nil {
				newBitmap := roaring.New()
				_, err := newBitmap.FromBuffer(seg.Data)
				if err != nil {
					return nil, fmt.Errorf("failed to decode new event bitmap32: %w", err)
				}
				existingBitmap.Or(newBitmap)
				existingBitmap.RunOptimize()
				finalData, err = existingBitmap.ToBytes()
				if err != nil {
					return nil, fmt.Errorf("failed to serialize merged event bitmap32: %w", err)
				}
			} else {
				finalData = seg.Data
			}

			batch.PutCF(cf, dbKey, finalData)
		}
	}

	// Write/merge ledger maps for each segment that had events.
	ledgerMapData := make(map[uint32][]byte, len(counters))
	for segmentID, counter := range counters {
		lmKey := SegmentLedgerMapKey(segmentID)

		// Read existing ledger map data
		existingData, err := s.db.GetCF(s.ro, s.cfDefault, lmKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read existing ledger map for segment %d: %w", segmentID, err)
		}

		var existingBytes []byte
		if existingData.Size() > 0 {
			existingBytes = make([]byte, existingData.Size())
			copy(existingBytes, existingData.Data())
		}
		existingData.Free()

		// Build new counts map from the counter
		newCounts := make(map[uint16]uint32)
		for i := uint32(0); i < SegmentSize; i++ {
			if counter.eventCounts[i] > 0 {
				newCounts[uint16(i)] = counter.eventCounts[i]
			}
		}

		var finalData []byte
		if len(existingBytes) > 0 {
			finalData = MergeSegmentLedgerMapData(existingBytes, newCounts)
		} else {
			finalData = EncodeSegmentLedgerMap(segmentID, counter.eventCounts)
		}

		ledgerMapData[segmentID] = finalData

		if writeToRocksDB {
			batch.PutCF(s.cfDefault, lmKey, finalData)
		}
	}

	if batch.Count() > 0 {
		if err := s.db.Write(s.wo, batch); err != nil {
			return nil, fmt.Errorf("failed to write event bitmap32 segments: %w", err)
		}
	}

	return ledgerMapData, nil
}

// uint64AddMergeOperator implements a merge operator that adds uint64 values
type uint64AddMergeOperator struct{}

func (m *uint64AddMergeOperator) Name() string {
	return "uint64-add"
}

func (m *uint64AddMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var total uint64

	// Parse existing value if present
	if len(existingValue) == 8 {
		total = binary.BigEndian.Uint64(existingValue)
	}

	// Add all operands
	for _, operand := range operands {
		if len(operand) == 8 {
			total += binary.BigEndian.Uint64(operand)
		}
	}

	// Return new value
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, total)
	return result, true
}

func (m *uint64AddMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
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

// uniqueKey generates a key for the unique index column family
// Format: [type:1][value:N]
func uniqueKey(uniqueType byte, value []byte) []byte {
	key := make([]byte, 1+len(value))
	key[0] = uniqueType
	copy(key[1:], value)
	return key
}

// parseRawXDRToQueryEvent converts binary event bytes to a query.Event.
func parseRawXDRToQueryEvent(data []byte, ledger uint32, tx, op uint32, eventIdx uint16) (*query.Event, error) {
	return event.DecodeBinaryToQueryEvent(data, ledger, eventIdx)
}

// EventStore manages storing events in RocksDB
type EventStore struct {
	db          *grocksdb.DB
	dbPath      string // Store path for filesystem-based stats
	wo          *grocksdb.WriteOptions
	ro          *grocksdb.ReadOptions
	indexes *IndexConfig

	// Column family handles (managed by DB, don't destroy manually)
	cfHandles []*grocksdb.ColumnFamilyHandle
	cfDefault *grocksdb.ColumnFamilyHandle // Metadata
	cfEvents  *grocksdb.ColumnFamilyHandle // Primary event storage
	cfUnique  *grocksdb.ColumnFamilyHandle // Unique value indexes with counts

	// 32-bit bitmap index CFs (event-level, FromBuffer decode)
	cfContractsBM32 *grocksdb.ColumnFamilyHandle // Contract ID bitmap32 index
	cfTopicsBM32    *grocksdb.ColumnFamilyHandle // Topic bitmap32 index

	// Index coordinator (owns in-memory bitmap + delegates to BitmapEventSeqStore)
	indexStore *IndexStore

	// Options that need to be destroyed on Close
	baseOpts *grocksdb.Options
	cfOpts   []*grocksdb.Options
	bbtoList []*grocksdb.BlockBasedTableOptions

	// Keep merge operator alive to prevent GC (RocksDB holds a reference)
	mergeOp grocksdb.MergeOperator

	// Flat file segment directory path (empty = disabled)
	segmentPath   string
	segmentReader *SegmentReader // lazily initialized, reused across queries

	// Segment data writer for flat file event storage (nil = disabled)
	segmentDataWriter *SegmentDataWriter

	// writeRocksDB controls whether events and indexes are written to RocksDB
	writeRocksDB bool
}

// NewEventStoreWithOptions creates a new event store with custom options
func NewEventStoreWithOptions(dbPath string, rocksOpts *RocksDBOptions, indexOpts *IndexConfig) (*EventStore, error) {
	// Create base options
	baseOpts := grocksdb.NewDefaultOptions()
	baseOpts.SetCreateIfMissing(true)
	baseOpts.SetCreateIfMissingColumnFamilies(true)
	applyRocksDBOptions(baseOpts, rocksOpts)

	// Create CF-specific options
	// Default CF - metadata, small values
	defaultOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(defaultOpts, rocksOpts)

	// Events CF - large values, optimized for sequential writes
	eventsOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(eventsOpts, rocksOpts)
	eventsBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	eventsBBTO.SetBlockSize(64 * 1024) // 64KB blocks for better compression
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		eventsBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	eventsOpts.SetBlockBasedTableFactory(eventsBBTO)

	// Unique CF - small values (8-byte counts), optimized for point lookups
	// Uses merge operator for fast counter increments (no read-modify-write)
	uniqueOpts := grocksdb.NewDefaultOptions()
	applyRocksDBOptions(uniqueOpts, rocksOpts)
	mergeOp := &uint64AddMergeOperator{}
	uniqueOpts.SetMergeOperator(mergeOp)
	uniqueBBTO := grocksdb.NewDefaultBlockBasedTableOptions()
	uniqueBBTO.SetBlockSize(4 * 1024) // 4KB blocks
	if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
		uniqueBBTO.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
	}
	uniqueOpts.SetBlockBasedTableFactory(uniqueBBTO)

	// Helper to create bitmap CF options
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

	// 32-bit bitmap CFs (event-level, FromBuffer decode)
	contractsBM32Opts, contractsBM32BBTO := createBitmapCFOpts()
	topicsBM32Opts, topicsBM32BBTO := createBitmapCFOpts()

	cfNames := []string{
		CFDefault, CFEvents, CFUnique,
		CFContractsBM32, CFTopicsBM32,
	}
	cfOpts := []*grocksdb.Options{
		defaultOpts, eventsOpts, uniqueOpts,
		contractsBM32Opts, topicsBM32Opts,
	}
	bbtoList := []*grocksdb.BlockBasedTableOptions{
		eventsBBTO, uniqueBBTO,
		contractsBM32BBTO, topicsBM32BBTO,
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(baseOpts, dbPath, cfNames, cfOpts)
	if err != nil {
		// Note: We intentionally don't destroy cfOpts, bbtoList, or baseOpts here.
		// Options with merge operators (uniqueOpts) crash on Destroy() due to
		// a grocksdb issue with rocksdb_mergeoperator_destroy.
		// These are small allocations that will be cleaned up on process exit.
		return nil, fmt.Errorf("failed to open RocksDB with column families: %w", err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	if rocksOpts != nil && rocksOpts.DisableWAL {
		wo.DisableWAL(true)
	}

	indexes := indexOpts
	if indexes == nil {
		indexes = DefaultIndexConfig()
	}

	// Create event-level bitmap32 index store (32-bit, FromBuffer decode)
	// cfHandles[0] = default, cfHandles[3] = contracts_bm32, cfHandles[4] = topics_bm32
	bitmapStore, err := NewBitmapEventSeqStore(db, cfHandles[0], cfHandles[3], cfHandles[4])
	if err != nil {
		db.Close()
		// Note: We intentionally don't destroy cfOpts, bbtoList, or baseOpts here.
		// See comment above about merge operator crash on Destroy().
		wo.Destroy()
		return nil, fmt.Errorf("failed to create event bitmap32 index store: %w", err)
	}

	return &EventStore{
		db:              db,
		dbPath:          dbPath,
		wo:              wo,
		ro:              grocksdb.NewDefaultReadOptions(),
		indexes:         indexes,
		cfHandles:       cfHandles,
		cfDefault:       cfHandles[0],
		cfEvents:        cfHandles[1],
		cfUnique:        cfHandles[2],
		cfContractsBM32: cfHandles[3],
		cfTopicsBM32:    cfHandles[4],
		indexStore:      NewIndexStore(bitmapStore),
		baseOpts:        baseOpts,
		cfOpts:          cfOpts,
		bbtoList:        bbtoList,
		mergeOp:         mergeOp,
		writeRocksDB:    true,
	}, nil
}

// StoreEvents stores events with optional index updates based on options.
// Returns the number of bytes written.
func (es *EventStore) StoreEvents(events []*event.IngestEvent, opts *StoreOptions) (int64, error) {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	var totalBytes int64

	// Ensure opts is not nil
	if opts == nil {
		opts = &StoreOptions{}
	}

	// Track counts to increment for this batch
	// Map from unique key -> count to add
	countUpdates := make(map[string]uint64)

	for _, ev := range events {
		// Skip diagnostic events if configured
		if opts.ExcludeDiagnostic && ev.EventType == 2 {
			continue
		}

		// Skip events with excluded topic0 values
		if len(opts.ExcludeTopic0) > 0 && len(ev.Topics) > 0 {
			topic0Key := string(ev.Topics[0])
			if _, excluded := opts.ExcludeTopic0[topic0Key]; excluded {
				continue
			}
		}

		// Assign dense local ID and update bitmap + posting list indexes
		if es.indexStore != nil {
			segmentID := SegmentID(ev.LedgerSequence)
			denseLocalID := es.indexStore.AssignDenseLocalID(segmentID, ev.LedgerSequence)

			// Key: [segmentID:4][denseID:4], value: binary event
			key := event.EncodeKey(segmentID, denseLocalID)
			value := event.EncodeBinaryEvent(ev)
			if es.writeRocksDB {
				batch.PutCF(es.cfEvents, key, value)
			}
			totalBytes += int64(len(value))

			// Index contract ID -> dense local ID (bitmap32)
			if len(ev.ContractID) > 0 {
				es.indexStore.AddContractEvent(ev.ContractID, segmentID, denseLocalID)
			}

			// Index topics -> dense local ID (bitmap32, positional)
			for pos, topicBytes := range ev.Topics {
				es.indexStore.AddTopicEvent(pos, topicBytes, segmentID, denseLocalID)
			}

			// Write to segment data if enabled
			if es.segmentDataWriter != nil {
				if !es.segmentDataWriter.IsActive() || segmentID != es.segmentDataWriter.ChunkID() {
					// Finalize previous chunk if active
					if es.segmentDataWriter.IsActive() {
						if err := es.segmentDataWriter.FinalizeChunk(); err != nil {
							return 0, fmt.Errorf("failed to finalize segment data chunk: %w", err)
						}
					}
					if err := es.segmentDataWriter.StartChunk(segmentID); err != nil {
						return 0, fmt.Errorf("failed to start segment data chunk %d: %w", segmentID, err)
					}
				}
				v4Data := event.EncodeBinaryEvent(ev)
				if err := es.segmentDataWriter.AppendEvent(denseLocalID, v4Data); err != nil {
					return 0, fmt.Errorf("failed to append event to file store: %w", err)
				}
			}
		}

		// Optionally update unique indexes with counts
		// Uses pre-extracted fields from IngestEvent (no XDR parsing needed!)
		if opts.UniqueIndexes {
			// Count contract ID
			if len(ev.ContractID) > 0 {
				uk := string(uniqueKey(UniqueTypeContract, ev.ContractID))
				countUpdates[uk]++
			}

			// Count topics (already marshaled as XDR bytes)
			for i, topicBytes := range ev.Topics {
				if i > 3 {
					break // Only index first 4 topics
				}

				var uniqueType byte
				switch i {
				case 0:
					uniqueType = UniqueTypeTopic0
				case 1:
					uniqueType = UniqueTypeTopic1
				case 2:
					uniqueType = UniqueTypeTopic2
				case 3:
					uniqueType = UniqueTypeTopic3
				}
				uk := string(uniqueKey(uniqueType, topicBytes))
				countUpdates[uk]++
			}
		}
	}

	// Apply count updates using merge operator (no reads needed!)
	if opts.UniqueIndexes && len(countUpdates) > 0 {
		for keyStr, addCount := range countUpdates {
			keyBytes := []byte(keyStr)
			countBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(countBytes, addCount)
			batch.MergeCF(es.cfUnique, keyBytes, countBytes)
		}
	}

	if batch.Count() > 0 {
		if err := es.db.Write(es.wo, batch); err != nil {
			return 0, fmt.Errorf("failed to write batch: %w", err)
		}
	}

	return totalBytes, nil
}

// FlushBitmapIndexes flushes hot bitmap segments to disk
func (es *EventStore) FlushBitmapIndexes() error {
	if es.indexStore != nil {
		return es.indexStore.Flush()
	}
	return nil
}


// GetEventsInRangeWithTiming retrieves events in a ledger range with detailed timing.
// Returns query.Event format with disk read and unmarshal timing.
func (es *EventStore) GetEventsInRangeWithTiming(startLedger, endLedger uint32, limit int) (*query.RangeResult, error) {
	result := &query.RangeResult{
		Timing: query.FetchTiming{},
	}

	// Seek to start of segment containing startLedger
	startKey := event.EncodeKey(SegmentID(startLedger), 0)

	// Time iterator creation and seek
	diskStart := time.Now()
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()
	it.Seek(startKey)
	result.Timing.DiskReadTime += time.Since(diskStart)

	var currentSegID uint32 = ^uint32(0)
	var lm *SegmentLedgerMap

	for it.Valid() {
		// Check limit
		if limit > 0 && len(result.Events) >= limit {
			break
		}

		// Time key access (disk read)
		diskStart = time.Now()
		key := it.Key().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		if len(key) < 8 {
			break
		}

		// Decode key: [segmentID:4][denseID:4]
		segID, denseID := event.DecodeKey(key)

		// Stop if past the end segment
		if segID > SegmentID(endLedger) {
			break
		}

		// Load ledger map for new segment
		if segID != currentSegID {
			currentSegID = segID
			lm, _ = es.LoadSegmentLedgerMap(segID)
		}
		if lm == nil {
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}

		ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
		if ledger < startLedger {
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}
		if ledger > endLedger {
			break
		}

		// Time value access (disk read)
		diskStart = time.Now()
		valueData := it.Value().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)
		result.Timing.BytesRead += int64(len(valueData))

		// Time unmarshalling/decoding
		unmarshalStart := time.Now()
		var ev *query.Event
		var err error
		ev, err = parseRawXDRToQueryEvent(valueData, ledger, 0, 0, eventSeq)
		result.Timing.UnmarshalTime += time.Since(unmarshalStart)

		result.EventsScanned++

		if err != nil {
			// Time next iteration
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}
		result.Events = append(result.Events, ev)

		// Time next iteration
		diskStart = time.Now()
		it.Next()
		result.Timing.DiskReadTime += time.Since(diskStart)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return result, nil
}

// GetEventsInLedger retrieves all events in a specific ledger as query.Event format.
// Implements the EventReader interface.
func (es *EventStore) GetEventsInLedger(ledger uint32) ([]*query.Event, error) {
	var events []*query.Event

	segID := SegmentID(ledger)
	startKey := event.EncodeKey(segID, 0)
	lm, _ := es.LoadSegmentLedgerMap(segID)
	if lm == nil {
		return events, nil
	}

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 8 {
			break
		}

		keySegID, denseID := event.DecodeKey(key)
		if keySegID != segID {
			break
		}

		keyLedger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
		if keyLedger < ledger {
			continue
		}
		if keyLedger != ledger {
			break
		}

		valueData := it.Value().Data()

		var ev *query.Event
		var err error
		ev, err = parseRawXDRToQueryEvent(valueData, ledger, 0, 0, eventSeq)
		if err != nil {
			continue
		}
		events = append(events, ev)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsInLedgerWithTiming retrieves all events in a specific ledger with detailed timing.
// Implements the EventReader interface.
func (es *EventStore) GetEventsInLedgerWithTiming(ledger uint32) (*query.FetchResult, error) {
	result := &query.FetchResult{
		Timing: query.FetchTiming{},
	}

	segID := SegmentID(ledger)
	startKey := event.EncodeKey(segID, 0)
	lm, _ := es.LoadSegmentLedgerMap(segID)

	// Time iterator creation and seek
	diskStart := time.Now()
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()
	it.Seek(startKey)
	result.Timing.DiskReadTime += time.Since(diskStart)

	for it.Valid() {
		// Time key access (disk read)
		diskStart = time.Now()
		key := it.Key().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		if len(key) < 8 {
			break
		}

		keySegID, denseID := event.DecodeKey(key)
		if keySegID != segID {
			break
		}

		if lm == nil {
			break
		}

		keyLedger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
		if keyLedger < ledger {
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}
		if keyLedger != ledger {
			break
		}

		// Time value access (disk read)
		diskStart = time.Now()
		valueData := it.Value().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)
		result.Timing.BytesRead += int64(len(valueData))

		// Time unmarshalling/decoding
		unmarshalStart := time.Now()
		var ev *query.Event
		var err error
		ev, err = parseRawXDRToQueryEvent(valueData, keyLedger, 0, 0, eventSeq)
		result.Timing.UnmarshalTime += time.Since(unmarshalStart)

		if err != nil {
			// Time next iteration
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}
		result.Events = append(result.Events, ev)

		// Time next iteration
		diskStart = time.Now()
		it.Next()
		result.Timing.DiskReadTime += time.Since(diskStart)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return result, nil
}

// BuildIndexes scans all events and builds indexes based on options (one-time operation)
// Bitmap indexes are always built. Unique indexes are optional.
// Uses a collector pattern: workers read/extract in parallel, single goroutine updates bitmaps.
func (es *EventStore) BuildIndexes(workers int, opts *BuildIndexOptions, progressFn func(processed int64)) error {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Default options
	if opts == nil {
		opts = &BuildIndexOptions{}
	}

	minLedger, maxLedger, err := es.getLedgerRange()
	if err != nil {
		return fmt.Errorf("failed to get ledger range: %w", err)
	}

	if minLedger == 0 && maxLedger == 0 {
		return nil
	}

	totalLedgers := maxLedger - minLedger + 1
	ledgersPerWorker := totalLedgers / uint32(workers)
	if ledgersPerWorker == 0 {
		ledgersPerWorker = 1
		workers = int(totalLedgers)
	}

	// Create channel for index entries (collector pattern)
	entryCh := make(chan *indexEntry, 100000) // buffered channel
	collectorDone := make(chan error, 1)

	// Start collector goroutine
	go func() {
		collectorDone <- es.indexCollector(entryCh, opts, progressFn)
	}()

	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		startLedger := minLedger + uint32(i)*ledgersPerWorker
		endLedger := startLedger + ledgersPerWorker - 1
		if i == workers-1 {
			endLedger = maxLedger
		}

		wg.Add(1)
		go func(start, end uint32) {
			defer wg.Done()
			if err := es.buildIndexesForRange(start, end, opts, entryCh); err != nil {
				errCh <- err
			}
		}(startLedger, endLedger)
	}

	wg.Wait()

	// Close entry channel and wait for collector to finish
	close(entryCh)
	if err := <-collectorDone; err != nil {
		return fmt.Errorf("index collector failed: %w", err)
	}

	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// buildIndexesForRange reads events for a ledger range and sends index data to collector.
// Unique indexes are still handled locally (no lock contention issue with RocksDB merge).
func (es *EventStore) buildIndexesForRange(startLedger, endLedger uint32, opts *BuildIndexOptions, entryCh chan<- *indexEntry) error {
	startKey := event.EncodeKey(SegmentID(startLedger), 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	// Accumulate unique counts in memory (only if building unique indexes)
	var counts map[string]uint64
	if opts.UniqueIndexes {
		counts = make(map[string]uint64)
	}

	var currentSegID uint32 = ^uint32(0)
	var lm *SegmentLedgerMap

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 8 {
			break
		}

		// Decode key: [segmentID:4][denseID:4]
		segID, denseID := event.DecodeKey(key)
		if segID > SegmentID(endLedger) {
			break
		}

		// Load ledger map for new segment
		if segID != currentSegID {
			currentSegID = segID
			lm, _ = es.LoadSegmentLedgerMap(segID)
		}
		if lm == nil {
			continue
		}

		ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
		if ledger < startLedger {
			continue
		}
		if ledger > endLedger {
			break
		}

		// Strip header, unmarshal DiagnosticEvent XDR
		valueData := it.Value().Data()
		if len(valueData) < event.BinaryHeaderSize {
			continue
		}
		var diagEvent xdr.DiagnosticEvent
		if err := diagEvent.UnmarshalBinary(valueData[event.BinaryHeaderSize:]); err != nil {
			continue
		}
		xdrEvent := diagEvent.Event

		// Extract contract ID
		var contractID []byte
		if xdrEvent.ContractId != nil {
			contractID = xdrEvent.ContractId[:]
		}

		// Extract topics
		var topics [][]byte
		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			for i, topic := range body.Topics {
				if i > 3 {
					break
				}
				topicBytes, err := topic.MarshalBinary()
				if err != nil {
					continue
				}
				topics = append(topics, topicBytes)
			}
		}

		// Build UNIQUE indexes (counts) - handled locally, no contention
		if opts.UniqueIndexes {
			if len(contractID) > 0 {
				uk := string(uniqueKey(UniqueTypeContract, contractID))
				counts[uk]++
			}
			for i, topicBytes := range topics {
				var uniqueType byte
				switch i {
				case 0:
					uniqueType = UniqueTypeTopic0
				case 1:
					uniqueType = UniqueTypeTopic1
				case 2:
					uniqueType = UniqueTypeTopic2
				case 3:
					uniqueType = UniqueTypeTopic3
				default:
					continue
				}
				uk := string(uniqueKey(uniqueType, topicBytes))
				counts[uk]++
			}
		}

		// Send to collector for bitmap indexes (no direct bitmap updates here)
		if entryCh != nil && (len(contractID) > 0 || len(topics) > 0) {
			entryCh <- &indexEntry{
				Ledger:     ledger,
				TxIdx:      0,
				OpIdx:      0,
				EventIdx:   eventSeq,
				ContractID: contractID,
				Topics:     topics,
			}
		}
	}

	// Write unique index counts
	if opts.UniqueIndexes && len(counts) > 0 {
		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		for keyStr, count := range counts {
			keyBytes := []byte(keyStr)
			countBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(countBytes, count)
			batch.MergeCF(es.cfUnique, keyBytes, countBytes)
		}

		if err := es.db.Write(es.wo, batch); err != nil {
			return fmt.Errorf("failed to write unique index batch: %w", err)
		}
	}

	if err := it.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	return nil
}

// indexCollector receives index entries from workers and counts them.
func (es *EventStore) indexCollector(entryCh <-chan *indexEntry, opts *BuildIndexOptions, progressFn func(processed int64)) error {
	var processed int64

	for range entryCh {
		processed++

		// Progress callback every 100k events
		if progressFn != nil && processed%100000 == 0 {
			progressFn(processed)
		}
	}

	// Final progress
	if progressFn != nil {
		progressFn(processed)
	}

	return nil
}

// SetSegmentPath sets the base directory for segment flat files.
func (es *EventStore) SetSegmentPath(path string) {
	es.segmentPath = path
}

// SetLedgerMapWriteConfig configures where ledger maps are written during index flush.
// segmentPath: base directory for segment flat files (empty = file writes disabled).
// writeToRocksDB: if true, bitmaps and ledger maps are written to RocksDB.
func (es *EventStore) SetLedgerMapWriteConfig(segmentPath string, writeToRocksDB bool) {
	if es.indexStore != nil {
		es.indexStore.SetWriteConfig(segmentPath, writeToRocksDB)
	}
}

// SetWriteRocksDB controls whether events and bitmap indexes are written to RocksDB.
func (es *EventStore) SetWriteRocksDB(enabled bool) {
	es.writeRocksDB = enabled
}

// EnableSegmentData initializes the segment data writer for flat file event storage.
// Uses the same base path as segment indexes.
// If compressEvents is true, each event blob is zstd-compressed in events.dat.
// groupSize controls how many events are compressed together (0 or 1 = per-event).
func (es *EventStore) EnableSegmentData(compressEvents bool, groupSize int) {
	if es.segmentPath != "" {
		es.segmentDataWriter = NewSegmentDataWriter(es.segmentPath, compressEvents, groupSize)
		if compressEvents && groupSize > 1 {
			fmt.Fprintf(os.Stderr, "Segment data writer enabled with zstd grouped compression (group=%d, base: %s)\n", groupSize, es.segmentPath)
		} else if compressEvents {
			fmt.Fprintf(os.Stderr, "Segment data writer enabled with zstd compression (base: %s)\n", es.segmentPath)
		} else {
			fmt.Fprintf(os.Stderr, "Segment data writer enabled (base: %s)\n", es.segmentPath)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Warning: EnableSegmentData called but segment path not set\n")
	}
}

// FinalizeSegmentData finalizes any active segment data chunk.
// Called at the end of ingestion to flush the last incomplete chunk.
func (es *EventStore) FinalizeSegmentData() error {
	if es.segmentDataWriter != nil && es.segmentDataWriter.IsActive() {
		return es.segmentDataWriter.FinalizeChunk()
	}
	return nil
}

// WriteSegmentDir writes flat file indexes (.hash + .pack pairs) for a completed segment.
// Scans the bitmap32 CFs to collect term data, partitions topics by position from key format,
// and writes the files atomically using tmp + rename.
func (es *EventStore) WriteSegmentDir(segmentID uint32) error {
	if es.segmentPath == "" {
		return fmt.Errorf("segment path not configured")
	}

	var contractTerms []SegmentTermData
	var topicsByPos [4][]SegmentTermData

	// Try cached data from the most recent Flush() first (used when writeToRocksDB=false)
	if cached := es.indexStore.PopSegmentTerms(segmentID); cached != nil {
		contractTerms = cached.contracts
		topicsByPos = cached.topics
	} else {
		// Fall back to RocksDB scan (for segments flushed in prior runs or with writeToRocksDB=true)
		var err error
		contractTerms, err = es.collectContractTerms(segmentID)
		if err != nil {
			return fmt.Errorf("failed to collect contract terms: %w", err)
		}
		topicsByPos, err = es.collectTopicTerms(segmentID)
		if err != nil {
			return fmt.Errorf("failed to collect topic terms: %w", err)
		}
	}

	// Load ledger map (try IndexStore/RocksDB first, fall back to flat file)
	lm, err := es.indexStore.LoadSegmentLedgerMap(segmentID)
	if err != nil {
		return fmt.Errorf("failed to load ledger map for segment %d: %w", segmentID, err)
	}
	if lm == nil && es.segmentPath != "" {
		// Fallback: load from flat file written by Flush()
		data, readErr := os.ReadFile(filepath.Join(es.segmentPath, fmt.Sprintf("%06d", segmentID), LedgerMapFileName))
		if readErr == nil && len(data) == SegmentLedgerMapSize {
			lm = &SegmentLedgerMap{SegmentID: segmentID, Data: data}
		}
	}

	// Write the segment directory
	if err := WriteSegmentDir(es.segmentPath, segmentID, contractTerms, topicsByPos, lm); err != nil {
		return err
	}

	// Finalize segment data chunk if it matches this segment
	if es.segmentDataWriter != nil && es.segmentDataWriter.IsActive() && es.segmentDataWriter.ChunkID() == segmentID {
		if err := es.segmentDataWriter.FinalizeChunk(); err != nil {
			return fmt.Errorf("failed to finalize segment data chunk %d: %w", segmentID, err)
		}
	}

	return nil
}

// collectContractTerms scans the contract bitmap32 CF for all entries matching a segment ID.
// Contract keys: [termHash:32][segmentID:4] = 36 bytes
func (es *EventStore) collectContractTerms(segmentID uint32) ([]SegmentTermData, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := es.db.NewIteratorCF(ro, es.cfContractsBM32)
	defer iter.Close()

	var results []SegmentTermData

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		if len(key) != IndexKeySize { // 36 bytes
			continue
		}

		// Extract segment ID from key suffix [32:36]
		keySegmentID := binary.BigEndian.Uint32(key[32:36])
		if keySegmentID != segmentID {
			continue
		}

		// Extract full 32-byte term hash
		var termHash [32]byte
		copy(termHash[:], key[0:32])

		// Copy bitmap data
		valData := iter.Value().Data()
		dataCopy := make([]byte, len(valData))
		copy(dataCopy, valData)

		results = append(results, SegmentTermData{
			TermHash:   termHash,
			BitmapData: dataCopy,
		})
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return results, nil
}

// collectTopicTerms scans the topic bitmap32 CF for all entries matching a segment ID.
// Topic keys: [pos:1][termHash:32][segmentID:4] = 37 bytes
// Returns [4][]SegmentTermData pre-partitioned by position from key[0].
func (es *EventStore) collectTopicTerms(segmentID uint32) ([4][]SegmentTermData, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := es.db.NewIteratorCF(ro, es.cfTopicsBM32)
	defer iter.Close()

	var results [4][]SegmentTermData

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		if len(key) != 37 { // [pos:1][termHash:32][segmentID:4]
			continue
		}

		// Extract position from key[0]
		pos := int(key[0])
		if pos > 3 {
			continue
		}

		// Extract segment ID from key suffix [33:37]
		keySegmentID := binary.BigEndian.Uint32(key[33:37])
		if keySegmentID != segmentID {
			continue
		}

		// Extract full 32-byte term hash
		var termHash [32]byte
		copy(termHash[:], key[1:33])

		// Copy bitmap data
		valData := iter.Value().Data()
		dataCopy := make([]byte, len(valData))
		copy(dataCopy, valData)

		results[pos] = append(results[pos], SegmentTermData{
			TermHash:   termHash,
			BitmapData: dataCopy,
		})
	}

	if err := iter.Err(); err != nil {
		return results, fmt.Errorf("iterator error: %w", err)
	}

	return results, nil
}

// Close closes the event store
func (es *EventStore) Close() {
	// Close index stores (flushes any remaining hot segments)
	if es.indexStore != nil {
		es.indexStore.Close()
	}

	// Close segment reader (releases mmap'd files)
	if es.segmentReader != nil {
		es.segmentReader.Close()
	}

	// Destroy read/write options first
	es.wo.Destroy()
	es.ro.Destroy()

	// Close the database
	// Note: CF handles returned from OpenDbColumnFamilies are managed by the DB
	es.db.Close()

	// Note: We intentionally don't destroy cfOpts, bbtoList, or baseOpts here.
	// The merge operator attached to uniqueOpts causes a crash when destroyed
	// after the DB is closed (the C pointer becomes invalid).
	// These are small memory leaks but avoid the crash.
	// A proper fix would require grocksdb to handle this case.
}

// =============================================================================
// Bitmap Stats and Metadata
// =============================================================================

func (es *EventStore) GetBitmapStats() *BitmapStats {
	if es.indexStore == nil {
		return nil
	}
	count, cards, memBytes := es.indexStore.GetHotSegmentStats()
	return &BitmapStats{
		CurrentSegmentID:    es.indexStore.GetCurrentSegmentID(),
		HotSegmentCount:    count,
		HotSegmentCards:    cards,
		HotSegmentMemBytes: memBytes,
	}
}

// SetLastProcessedLedger stores the last processed ledger sequence
func (es *EventStore) SetLastProcessedLedger(sequence uint32) error {
	key := []byte("last_processed_ledger")
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, sequence)
	return es.db.PutCF(es.wo, es.cfDefault, key, value)
}

// GetLastProcessedLedger retrieves the last processed ledger sequence
func (es *EventStore) GetLastProcessedLedger() (uint32, error) {
	key := []byte("last_processed_ledger")
	value, err := es.db.GetCF(es.ro, es.cfDefault, key)
	if err != nil {
		return 0, err
	}
	defer value.Free()

	if value.Size() == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint32(value.Data()), nil
}

// LoadSegmentLedgerMap reads the ledger map for a segment from CFDefault.
func (es *EventStore) LoadSegmentLedgerMap(segmentID uint32) (*SegmentLedgerMap, error) {
	if es.indexStore != nil {
		return es.indexStore.LoadSegmentLedgerMap(segmentID)
	}
	return nil, nil
}

// InitializeDenseCounters loads the ledger map for the current segment from storage
// and initializes the dense ID counter so ingestion continues from the correct offset.
// Should be called on startup after the DB is opened.
func (es *EventStore) InitializeDenseCounters(lastLedger uint32) error {
	if es.indexStore == nil {
		return nil
	}
	segmentID := SegmentID(lastLedger)
	lm, err := es.indexStore.LoadSegmentLedgerMap(segmentID)
	if err != nil {
		return fmt.Errorf("failed to load ledger map on startup: %w", err)
	}
	if lm != nil {
		es.indexStore.InitializeDenseCounter(segmentID, lm.TotalEvents())
	}
	return nil
}

// =============================================================================
// Statistics
// =============================================================================

// CountEvents returns the total number of events in the store
func (es *EventStore) CountEvents() (int64, error) {
	var count int64

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		count++
	}

	if err := it.Err(); err != nil {
		return 0, fmt.Errorf("iterator error: %w", err)
	}

	return count, nil
}

// GetStorageSnapshot returns per-column-family storage statistics.
func (es *EventStore) GetStorageSnapshot() (*StorageSnapshot, error) {
	snapshot := &StorageSnapshot{
		Timestamp:      time.Now(),
		ColumnFamilies: make(map[string]*ColumnFamilyStats),
	}

	cfNames := []string{
		CFDefault, CFEvents, CFUnique,
		CFContractsBM32, CFTopicsBM32,
	}

	// Helper to parse uint64 from RocksDB property string
	parseUint64 := func(val string) uint64 {
		var n uint64
		fmt.Sscanf(val, "%d", &n)
		return n
	}

	// Helper to count files across all levels for a CF
	countFiles := func(cf *grocksdb.ColumnFamilyHandle) int {
		total := 0
		for level := 0; level <= 6; level++ {
			prop := fmt.Sprintf("rocksdb.num-files-at-level%d", level)
			total += int(parseUint64(es.db.GetPropertyCF(prop, cf)))
		}
		return total
	}

	for i, name := range cfNames {
		cf := es.cfHandles[i]
		cfStats := &ColumnFamilyStats{
			Name:           name,
			EstimatedKeys:  parseUint64(es.db.GetPropertyCF("rocksdb.estimate-num-keys", cf)),
			SSTFilesBytes:  parseUint64(es.db.GetPropertyCF("rocksdb.total-sst-files-size", cf)),
			MemtableBytes:  parseUint64(es.db.GetPropertyCF("rocksdb.cur-size-all-mem-tables", cf)),
			PendingCompact: parseUint64(es.db.GetPropertyCF("rocksdb.estimate-pending-compaction-bytes", cf)),
			NumFiles:       countFiles(cf),
		}

		snapshot.ColumnFamilies[name] = cfStats
		snapshot.TotalSST += cfStats.SSTFilesBytes
		snapshot.TotalMemtable += cfStats.MemtableBytes
		snapshot.TotalFiles += cfStats.NumFiles
	}

	return snapshot, nil
}

// Flush forces all memtables to be flushed to SST files
// This should be called before getting accurate storage stats
func (es *EventStore) Flush() error {
	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true)

	// Flush all column families
	for _, cf := range es.cfHandles {
		if err := es.db.FlushCF(cf, flushOpts); err != nil {
			return fmt.Errorf("failed to flush column family: %w", err)
		}
	}
	return nil
}

// GetStats returns statistics about the event database (O(1) - no full scan)
func (es *EventStore) GetStats() (*DBStats, error) {
	stats := &DBStats{}

	// Get min/max ledger using seek (O(1))
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	// Min ledger from first key
	it.SeekToFirst()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			stats.MinLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	// Max ledger from last key
	it.SeekToLast()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			stats.MaxLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Get estimated event count from events column family (instant, approximate)
	estKeys := es.db.GetPropertyCF("rocksdb.estimate-num-keys", es.cfEvents)
	var estCount int64
	fmt.Sscanf(estKeys, "%d", &estCount)
	stats.TotalEvents = estCount

	// Get unique contracts from unique index (instant if index exists)
	counts, _ := es.CountUniqueIndexes()
	if counts != nil {
		stats.UniqueContracts = int(counts.UniqueContracts)
	}

	stats.LastProcessed, _ = es.GetLastProcessedLedger()

	return stats, nil
}

// GetLedgerTxStats scans events to count ledgers with high transaction counts.
func (es *EventStore) GetLedgerTxStats() (*LedgerTxStats, error) {
	stats := &LedgerTxStats{}

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	var currentLedger uint32
	var maxTxIdx uint16
	var first = true

	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 6 {
			continue
		}

		ledger := binary.BigEndian.Uint32(key[0:4])
		txIdx := binary.BigEndian.Uint16(key[4:6])

		if first || ledger != currentLedger {
			// Process previous ledger
			if !first && maxTxIdx >= 10000 {
				stats.LedgersOver10kTx++
			}
			if !first {
				stats.TotalLedgers++
			}

			currentLedger = ledger
			maxTxIdx = txIdx
			first = false
		} else if txIdx > maxTxIdx {
			maxTxIdx = txIdx
		}
	}

	// Process last ledger
	if !first {
		stats.TotalLedgers++
		if maxTxIdx >= 10000 {
			stats.LedgersOver10kTx++
		}
	}

	return stats, it.Err()
}

// CompactAllWithStats runs manual compaction and returns before/after stats per column family.
func (es *EventStore) CompactAllWithStats() (*CompactionSummary, error) {
	before, err := es.GetStorageSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to get pre-compaction stats: %w", err)
	}

	start := time.Now()

	// Create compaction options for full compaction
	compactOpts := grocksdb.NewCompactRangeOptions()
	defer compactOpts.Destroy()

	// Force compaction to bottommost level for maximum compression
	compactOpts.SetBottommostLevelCompaction(grocksdb.KForceOptimized)
	compactOpts.SetExclusiveManualCompaction(true)

	// Compact ALL column families
	fullRange := grocksdb.Range{Start: nil, Limit: nil}
	for _, cf := range es.cfHandles {
		es.db.CompactRangeCFOpt(cf, fullRange, compactOpts)
	}

	duration := time.Since(start)

	after, err := es.GetStorageSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to get post-compaction stats: %w", err)
	}

	// Build per-CF compaction results
	perCF := make(map[string]*CFCompactionResult)
	for name, beforeCF := range before.ColumnFamilies {
		afterCF := after.ColumnFamilies[name]
		var reclaimed uint64
		if beforeCF.SSTFilesBytes > afterCF.SSTFilesBytes {
			reclaimed = beforeCF.SSTFilesBytes - afterCF.SSTFilesBytes
		}
		pct := 0.0
		if beforeCF.SSTFilesBytes > 0 {
			pct = float64(reclaimed) / float64(beforeCF.SSTFilesBytes) * 100
		}
		perCF[name] = &CFCompactionResult{
			Name:           name,
			BeforeBytes:    beforeCF.SSTFilesBytes,
			AfterBytes:     afterCF.SSTFilesBytes,
			Reclaimed:      reclaimed,
			SavingsPercent: pct,
		}
	}

	var totalReclaimed uint64
	if before.TotalSST > after.TotalSST {
		totalReclaimed = before.TotalSST - after.TotalSST
	}
	totalSavings := 0.0
	if before.TotalSST > 0 {
		totalSavings = float64(totalReclaimed) / float64(before.TotalSST) * 100
	}

	return &CompactionSummary{
		Before:         before,
		After:          after,
		Duration:       duration,
		PerCF:          perCF,
		TotalReclaimed: totalReclaimed,
		SavingsPercent: totalSavings,
	}, nil
}

// CountUniqueIndexes counts entries in unique indexes and sums their event counts (parallel)
func (es *EventStore) CountUniqueIndexes() (*UniqueIndexCounts, error) {
	// Use 16 partitions per index type (total 80 goroutines for 5 index types)
	const partitions = 16

	type result struct {
		indexType   byte
		uniqueCount int64
		totalEvents int64
		err         error
	}

	results := make(chan result, 5*partitions)
	var wg sync.WaitGroup

	// For each index type, partition the key space and count in parallel
	for _, indexType := range []byte{UniqueTypeContract, UniqueTypeTopic0, UniqueTypeTopic1, UniqueTypeTopic2, UniqueTypeTopic3} {
		for p := 0; p < partitions; p++ {
			wg.Add(1)
			go func(idxType byte, partition int) {
				defer wg.Done()
				unique, total, err := es.countIndexTypePartition(idxType, partition, partitions)
				results <- result{idxType, unique, total, err}
			}(indexType, p)
		}
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	counts := &UniqueIndexCounts{}
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		switch r.indexType {
		case UniqueTypeContract:
			counts.UniqueContracts += r.uniqueCount
			counts.TotalContractEvents += r.totalEvents
		case UniqueTypeTopic0:
			counts.UniqueTopic0 += r.uniqueCount
			counts.TotalTopic0Events += r.totalEvents
		case UniqueTypeTopic1:
			counts.UniqueTopic1 += r.uniqueCount
			counts.TotalTopic1Events += r.totalEvents
		case UniqueTypeTopic2:
			counts.UniqueTopic2 += r.uniqueCount
			counts.TotalTopic2Events += r.totalEvents
		case UniqueTypeTopic3:
			counts.UniqueTopic3 += r.uniqueCount
			counts.TotalTopic3Events += r.totalEvents
		}
	}

	return counts, nil
}

// countIndexTypePartition counts entries for a partition of an index type
// Partitions are based on the first byte of the value (after the type prefix)
func (es *EventStore) countIndexTypePartition(indexType byte, partition, totalPartitions int) (uniqueCount, totalEvents int64, err error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := es.db.NewIteratorCF(ro, es.cfUnique)
	defer it.Close()

	// Calculate byte range for this partition
	// Each partition handles a range of first-byte values
	bytesPerPartition := 256 / totalPartitions
	startByte := byte(partition * bytesPerPartition)
	endByte := byte((partition + 1) * bytesPerPartition)
	if partition == totalPartitions-1 {
		endByte = 0 // Will wrap, handled in comparison
	}

	// Start key: [indexType][startByte]
	startKey := []byte{indexType, startByte}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 2 || key[0] != indexType {
			break
		}

		// Check if we're still in our partition
		valueByte := key[1]
		if partition < totalPartitions-1 && valueByte >= endByte {
			break
		}

		uniqueCount++
		if it.Value().Size() == 8 {
			totalEvents += int64(binary.BigEndian.Uint64(it.Value().Data()))
		}
	}

	if err := it.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterator error for type %d partition %d: %w", indexType, partition, err)
	}

	return uniqueCount, totalEvents, nil
}

// GetIndexDistribution computes percentile statistics for each index type in parallel
// topN specifies how many top entries to include (0 for none)
func (es *EventStore) GetIndexDistribution(topN int, bottomN ...int) (*IndexDistribution, error) {
	botN := 0
	if len(bottomN) > 0 {
		botN = bottomN[0]
	}

	// Scan each type prefix in parallel
	type result struct {
		indexType byte
		stats     *DistributionStats
		err       error
	}

	results := make(chan result, 5)
	var wg sync.WaitGroup

	// Launch a goroutine for each index type
	for _, indexType := range []byte{UniqueTypeContract, UniqueTypeTopic0, UniqueTypeTopic1, UniqueTypeTopic2, UniqueTypeTopic3} {
		wg.Add(1)
		go func(idxType byte) {
			defer wg.Done()
			stats, err := es.computeDistributionForType(idxType, topN, botN)
			results <- result{idxType, stats, err}
		}(indexType)
	}

	// Close results channel when all goroutines done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	dist := &IndexDistribution{}
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		switch r.indexType {
		case UniqueTypeContract:
			dist.Contracts = r.stats
		case UniqueTypeTopic0:
			dist.Topic0 = r.stats
		case UniqueTypeTopic1:
			dist.Topic1 = r.stats
		case UniqueTypeTopic2:
			dist.Topic2 = r.stats
		case UniqueTypeTopic3:
			dist.Topic3 = r.stats
		}
	}

	return dist, nil
}

// computeDistributionForType computes distribution for a single index type using parallel partitions
func (es *EventStore) computeDistributionForType(indexType byte, topN, bottomN int) (*DistributionStats, error) {
	const partitions = 16

	type partitionResult struct {
		counts      []int64
		total       int64
		over32Bytes int64
		topN        []TopEntry
		bottomN     []TopEntry
		err         error
	}

	results := make(chan partitionResult, partitions)
	var wg sync.WaitGroup

	// Scan each partition in parallel
	for p := 0; p < partitions; p++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			counts, total, over32, top, bot, err := es.scanDistributionPartition(indexType, partition, partitions, topN, bottomN)
			results <- partitionResult{counts, total, over32, top, bot, err}
		}(p)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Merge results from all partitions
	var allCounts []int64
	var totalSum int64
	var over32Sum int64
	mergedTopN := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN)}
	mergedBottomN := &bottomNHeap{maxSize: bottomN, entries: make([]TopEntry, 0, bottomN)}

	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		allCounts = append(allCounts, r.counts...)
		totalSum += r.total
		over32Sum += r.over32Bytes

		// Merge top-N entries
		for _, entry := range r.topN {
			if len(mergedTopN.entries) < topN {
				heap.Push(mergedTopN, entry)
			} else if entry.EventCount > mergedTopN.entries[0].EventCount {
				mergedTopN.entries[0] = entry
				heap.Fix(mergedTopN, 0)
			}
		}

		// Merge bottom-N entries
		for _, entry := range r.bottomN {
			if len(mergedBottomN.entries) < bottomN {
				heap.Push(mergedBottomN, entry)
			} else if entry.EventCount < mergedBottomN.entries[0].EventCount {
				mergedBottomN.entries[0] = entry
				heap.Fix(mergedBottomN, 0)
			}
		}
	}

	if len(allCounts) == 0 {
		return &DistributionStats{}, nil
	}

	// Sort for percentiles
	sort.Slice(allCounts, func(i, j int) bool {
		return allCounts[i] < allCounts[j]
	})

	return &DistributionStats{
		Count:       int64(len(allCounts)),
		Min:         allCounts[0],
		Max:         allCounts[len(allCounts)-1],
		Mean:        float64(totalSum) / float64(len(allCounts)),
		Total:       totalSum,
		P50:         percentile(allCounts, 50),
		P75:         percentile(allCounts, 75),
		P90:         percentile(allCounts, 90),
		P99:         percentile(allCounts, 99),
		TopN:        mergedTopN.getSorted(),
		BottomN:     mergedBottomN.getSorted(),
		Over32Bytes: over32Sum,
	}, nil
}

// scanDistributionPartition scans a partition and returns counts for distribution
// Returns: counts, total, over32Bytes, topN entries, bottomN entries, error
func (es *EventStore) scanDistributionPartition(indexType byte, partition, totalPartitions, topN, bottomN int) ([]int64, int64, int64, []TopEntry, []TopEntry, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := es.db.NewIteratorCF(ro, es.cfUnique)
	defer it.Close()

	// Calculate byte range for this partition
	bytesPerPartition := 256 / totalPartitions
	startByte := byte(partition * bytesPerPartition)
	endByte := byte((partition + 1) * bytesPerPartition)
	if partition == totalPartitions-1 {
		endByte = 0
	}

	startKey := []byte{indexType, startByte}
	var counts []int64
	var total int64
	var over32Bytes int64
	topHeap := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN), indexType: indexType}
	botHeap := &bottomNHeap{maxSize: bottomN, entries: make([]TopEntry, 0, bottomN), indexType: indexType}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 2 || key[0] != indexType {
			break
		}

		valueByte := key[1]
		if partition < totalPartitions-1 && valueByte >= endByte {
			break
		}

		// Count values > 32 bytes (key[1:] is the value portion)
		valueLen := len(key) - 1
		if valueLen > 32 {
			over32Bytes++
		}

		var eventCount int64
		if it.Value().Size() == 8 {
			eventCount = int64(binary.BigEndian.Uint64(it.Value().Data()))
		}

		counts = append(counts, eventCount)
		total += eventCount
		topHeap.tryAdd(key[1:], eventCount)
		botHeap.tryAdd(key[1:], eventCount)
	}

	if err := it.Err(); err != nil {
		return nil, 0, 0, nil, nil, fmt.Errorf("iterator error for type %d partition %d: %w", indexType, partition, err)
	}

	return counts, total, over32Bytes, topHeap.getSorted(), botHeap.getSorted(), nil
}

// getLedgerRange finds the min and max ledger sequences in the database
func (es *EventStore) getLedgerRange() (uint32, uint32, error) {
	var minLedger, maxLedger uint32

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	// Find first key
	it.SeekToFirst()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			minLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	// Find last key
	it.SeekToLast()
	if it.Valid() {
		key := it.Key().Data()
		if len(key) >= 4 {
			maxLedger = binary.BigEndian.Uint32(key[0:4])
		}
	}

	return minLedger, maxLedger, it.Err()
}

// GetLedgerRange returns the min and max ledger sequences in the store.
// Implements the EventReader interface.
func (es *EventStore) GetLedgerRange() (min, max uint32, err error) {
	return es.getLedgerRange()
}

// =============================================================================
// Compute Event Stats (Full Scan)
// =============================================================================

// ComputeEventStats scans all events and computes unique counts.
// Workers controls parallelism: 0 uses NumCPU, 1 for single-threaded, >1 for parallel.
func (es *EventStore) ComputeEventStats(workers int) (*EventStats, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// For single worker, use simple sequential scan (more memory efficient)
	if workers == 1 {
		return es.computeEventStatsSingleThread()
	}

	// Parallel implementation
	minLedger, maxLedger, err := es.getLedgerRange()
	if err != nil {
		return nil, fmt.Errorf("failed to get ledger range: %w", err)
	}

	if minLedger == 0 && maxLedger == 0 {
		return &EventStats{}, nil
	}

	totalLedgers := maxLedger - minLedger + 1
	ledgersPerWorker := totalLedgers / uint32(workers)
	if ledgersPerWorker == 0 {
		ledgersPerWorker = 1
		workers = int(totalLedgers)
	}

	type workerResult struct {
		stats     *EventStats
		contracts map[string]struct{}
		topic0s   map[string]struct{}
		topic1s   map[string]struct{}
		topic2s   map[string]struct{}
		topic3s   map[string]struct{}
		err       error
	}
	results := make(chan workerResult, workers)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		startLedger := minLedger + uint32(i)*ledgersPerWorker
		endLedger := startLedger + ledgersPerWorker - 1
		if i == workers-1 {
			endLedger = maxLedger
		}

		wg.Add(1)
		go func(start, end uint32) {
			defer wg.Done()
			result := es.computeStatsForRange(start, end)
			results <- result
		}(startLedger, endLedger)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	finalStats := &EventStats{}
	contracts := make(map[string]struct{})
	topic0s := make(map[string]struct{})
	topic1s := make(map[string]struct{})
	topic2s := make(map[string]struct{})
	topic3s := make(map[string]struct{})

	for r := range results {
		if r.err != nil {
			return nil, r.err
		}

		finalStats.TotalEvents += r.stats.TotalEvents
		finalStats.ContractEvents += r.stats.ContractEvents
		finalStats.SystemEvents += r.stats.SystemEvents
		finalStats.DiagnosticEvents += r.stats.DiagnosticEvents

		for k := range r.contracts {
			contracts[k] = struct{}{}
		}
		for k := range r.topic0s {
			topic0s[k] = struct{}{}
		}
		for k := range r.topic1s {
			topic1s[k] = struct{}{}
		}
		for k := range r.topic2s {
			topic2s[k] = struct{}{}
		}
		for k := range r.topic3s {
			topic3s[k] = struct{}{}
		}
	}

	finalStats.UniqueContracts = len(contracts)
	finalStats.UniqueTopic0 = len(topic0s)
	finalStats.UniqueTopic1 = len(topic1s)
	finalStats.UniqueTopic2 = len(topic2s)
	finalStats.UniqueTopic3 = len(topic3s)

	return finalStats, nil
}

// computeEventStatsSingleThread is the single-threaded implementation
func (es *EventStore) computeEventStatsSingleThread() (*EventStats, error) {
	stats := &EventStats{}

	contracts := make(map[string]struct{})
	topic0s := make(map[string]struct{})
	topic1s := make(map[string]struct{})
	topic2s := make(map[string]struct{})
	topic3s := make(map[string]struct{})

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		stats.TotalEvents++

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		switch xdrEvent.Type {
		case xdr.ContractEventTypeContract:
			stats.ContractEvents++
		case xdr.ContractEventTypeSystem:
			stats.SystemEvents++
		case xdr.ContractEventTypeDiagnostic:
			stats.DiagnosticEvents++
		}

		if xdrEvent.ContractId != nil {
			contractID := base64.StdEncoding.EncodeToString(xdrEvent.ContractId[:])
			contracts[contractID] = struct{}{}
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			for i, topic := range body.Topics {
				topicBytes, _ := topic.MarshalBinary()
				topicStr := base64.StdEncoding.EncodeToString(topicBytes)
				switch i {
				case 0:
					topic0s[topicStr] = struct{}{}
				case 1:
					topic1s[topicStr] = struct{}{}
				case 2:
					topic2s[topicStr] = struct{}{}
				case 3:
					topic3s[topicStr] = struct{}{}
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	stats.UniqueContracts = len(contracts)
	stats.UniqueTopic0 = len(topic0s)
	stats.UniqueTopic1 = len(topic1s)
	stats.UniqueTopic2 = len(topic2s)
	stats.UniqueTopic3 = len(topic3s)

	return stats, nil
}

// computeStatsForRange computes stats for a specific ledger range
func (es *EventStore) computeStatsForRange(startLedger, endLedger uint32) struct {
	stats     *EventStats
	contracts map[string]struct{}
	topic0s   map[string]struct{}
	topic1s   map[string]struct{}
	topic2s   map[string]struct{}
	topic3s   map[string]struct{}
	err       error
} {
	result := struct {
		stats     *EventStats
		contracts map[string]struct{}
		topic0s   map[string]struct{}
		topic1s   map[string]struct{}
		topic2s   map[string]struct{}
		topic3s   map[string]struct{}
		err       error
	}{
		stats:     &EventStats{},
		contracts: make(map[string]struct{}),
		topic0s:   make(map[string]struct{}),
		topic1s:   make(map[string]struct{}),
		topic2s:   make(map[string]struct{}),
		topic3s:   make(map[string]struct{}),
	}

	startKey := event.EncodeKey(SegmentID(startLedger), 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	var currentSegID uint32 = ^uint32(0)
	var lm *SegmentLedgerMap

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 8 {
			break
		}

		segID, denseID := event.DecodeKey(key)
		if segID > SegmentID(endLedger) {
			break
		}

		// Load ledger map for new segment
		if segID != currentSegID {
			currentSegID = segID
			lm, _ = es.LoadSegmentLedgerMap(segID)
		}
		if lm == nil {
			continue
		}

		ledger, _ := lm.DenseIDToLedgerAndSeq(denseID)
		if ledger < startLedger {
			continue
		}
		if ledger > endLedger {
			break
		}

		result.stats.TotalEvents++

		// Strip header, unmarshal DiagnosticEvent XDR
		valueData := it.Value().Data()
		var xdrEvent xdr.ContractEvent
		if len(valueData) >= event.BinaryHeaderSize && valueData[0] == event.BinaryFormatVersion {
			var diagEvent xdr.DiagnosticEvent
			if err := diagEvent.UnmarshalBinary(valueData[event.BinaryHeaderSize:]); err != nil {
				continue
			}
			xdrEvent = diagEvent.Event
		} else {
			var diagEvent xdr.DiagnosticEvent
			if err := diagEvent.UnmarshalBinary(valueData); err != nil {
				continue
			}
			xdrEvent = diagEvent.Event
		}

		switch xdrEvent.Type {
		case xdr.ContractEventTypeContract:
			result.stats.ContractEvents++
		case xdr.ContractEventTypeSystem:
			result.stats.SystemEvents++
		case xdr.ContractEventTypeDiagnostic:
			result.stats.DiagnosticEvents++
		}

		if xdrEvent.ContractId != nil {
			contractID := base64.StdEncoding.EncodeToString(xdrEvent.ContractId[:])
			result.contracts[contractID] = struct{}{}
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			for i, topic := range body.Topics {
				topicBytes, _ := topic.MarshalBinary()
				topicStr := base64.StdEncoding.EncodeToString(topicBytes)
				switch i {
				case 0:
					result.topic0s[topicStr] = struct{}{}
				case 1:
					result.topic1s[topicStr] = struct{}{}
				case 2:
					result.topic2s[topicStr] = struct{}{}
				case 3:
					result.topic3s[topicStr] = struct{}{}
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		result.err = fmt.Errorf("iterator error: %w", err)
	}

	return result
}

// QueryEventsWithBitmap32EventIndex queries events using the 32-bit event-level bitmap index.
// Uses sequential event IDs + FromBuffer for near-zero-cost index decode.
func (es *EventStore) QueryEventsWithBitmap32EventIndex(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	if es.indexStore == nil {
		return nil, nil, fmt.Errorf("bitmap32 event index not available")
	}

	// Query the bitmap32 index (positional topic matching)
	indexStart := time.Now()
	queryResult, err := es.indexStore.QueryEventKeysWithStats(contractID, topicGroups, startLedger, endLedger)
	if err != nil {
		return nil, nil, fmt.Errorf("bitmap32 event query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLocalIDs = int(queryResult.TotalCount)
	result.IndexBytesRead = queryResult.BytesRead
	result.SegmentsScanned = queryResult.Segments
	result.SegmentsTouched = len(GetSegmentsForRange(startLedger, endLedger))
	result.IndexReadTime = queryResult.ReadTime
	result.IndexDecodeTime = queryResult.DecodeTime
	result.IndexIntersectTime = queryResult.IntersectTime

	if queryResult.TotalCount == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// Collect all keys and metadata from bitmap results, capped at limit.
	// Use ledger map to convert dense local IDs to (ledger, eventSeq) pairs.
	type bitmapEvtMeta struct {
		ledger   uint32
		eventSeq uint16
	}

	segIDs := make([]uint32, 0, len(queryResult.PerSegment))
	for segID := range queryResult.PerSegment {
		segIDs = append(segIDs, segID)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	fetchCap := result.MatchingLocalIDs
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}
	allKeys := make([][]byte, 0, fetchCap)
	allMetas := make([]bitmapEvtMeta, 0, fetchCap)

	// Cache ledger maps per segment
	ledgerMaps := make(map[uint32]*SegmentLedgerMap)

	for _, segID := range segIDs {
		if len(allKeys) >= fetchCap {
			break
		}

		// Load ledger map for this segment
		lm, ok := ledgerMaps[segID]
		if !ok {
			var lmErr error
			lm, lmErr = es.indexStore.LoadSegmentLedgerMap(segID)
			if lmErr != nil {
				return nil, nil, fmt.Errorf("failed to load ledger map for segment %d: %w", segID, lmErr)
			}
			ledgerMaps[segID] = lm
		}
		if lm == nil {
			continue
		}

		// Verify ledger map: count actual keys in this segment
		actualCount := 0
		verifyIter := es.db.NewIteratorCF(es.ro, es.cfEvents)
		verifyIter.Seek(event.EncodeKey(segID, 0))
		for verifyIter.Valid() {
			k := verifyIter.Key()
			if k == nil || len(k.Data()) < 8 {
				break
			}
			kSegID := binary.BigEndian.Uint32(k.Data()[:4])
			if kSegID != segID {
				break
			}
			actualCount++
			verifyIter.Next()
		}
		verifyIter.Close()
		lmTotal := lm.TotalEvents()
		if uint32(actualCount) != lmTotal {
			fmt.Fprintf(os.Stderr, "  [MISMATCH] seg=%d ledgerMap=%d actualKeys=%d (diff=%d)\n",
				segID, lmTotal, actualCount, int(lmTotal)-actualCount)
		} else {
			fmt.Fprintf(os.Stderr, "  [verify] seg=%d ledgerMap=%d actualKeys=%d OK\n",
				segID, lmTotal, actualCount)
		}

		bitmap := queryResult.PerSegment[segID]
		bitmapIter := bitmap.Iterator()
		for bitmapIter.HasNext() {
			if len(allKeys) >= fetchCap {
				break
			}
			denseID := bitmapIter.Next()
			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
			fmt.Fprintf(os.Stderr, "  [dense-debug] seg=%d denseID=%d → ledger=%d seq=%d (totalEvents=%d)\n",
				segID, denseID, ledger, eventSeq, lm.TotalEvents())
			allKeys = append(allKeys, event.EncodeKey(segID, denseID))
			allMetas = append(allMetas, bitmapEvtMeta{ledger: ledger, eventSeq: eventSeq})
		}
	}

	// Pre-compute filter strings for post-filter verification
	var filterContractID string
	if len(contractID) > 0 {
		if encoded, err := strkey.Encode(strkey.VersionByteContract, contractID); err == nil {
			filterContractID = encoded
		}
	}
	var filterTopics [4]string
	for pos, tg := range topicGroups {
		if len(tg) > 0 && len(tg[0]) > 0 {
			filterTopics[pos] = base64.StdEncoding.EncodeToString(tg[0])
		}
	}

	// Parallel fetch using iterators
	const maxWorkers = 4
	numWorkers := maxWorkers
	if len(allKeys) < numWorkers {
		numWorkers = len(allKeys)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	type workerResult struct {
		events     []*query.Event
		fetchTime  time.Duration
		decodeTime time.Duration
		filterTime time.Duration
		bytesRead  int64
		scanned    int
		filtered   int
	}

	results := make([]workerResult, numWorkers)
	chunkSize := (len(allKeys) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > len(allKeys) {
			end = len(allKeys)
		}
		if start >= end {
			continue
		}

		wg.Add(1)
		go func(workerID, start, end int) {
			defer wg.Done()
			r := &results[workerID]
			r.events = make([]*query.Event, 0, end-start)

			dbIter := es.db.NewIteratorCF(es.ro, es.cfEvents)
			defer dbIter.Close()

			for i := start; i < end; i++ {
				key := allKeys[i]
				meta := allMetas[i]

				seekStart := time.Now()
				dbIter.Seek(key)
				if !dbIter.Valid() {
					r.fetchTime += time.Since(seekStart)
					break
				}

				iterKey := dbIter.Key()
				if iterKey == nil || !bytes.Equal(iterKey.Data(), key) {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				iterVal := dbIter.Value()
				if iterVal == nil {
					r.fetchTime += time.Since(seekStart)
					continue
				}
				valData := iterVal.Data()
				if len(valData) == 0 {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				valueCopy := make([]byte, len(valData))
				copy(valueCopy, valData)
				r.fetchTime += time.Since(seekStart)

				r.bytesRead += int64(len(valueCopy))
				r.scanned++

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				ev, decErr = parseRawXDRToQueryEvent(valueCopy, meta.ledger, 0, 0, meta.eventSeq)
				r.decodeTime += time.Since(decStart)

				if decErr != nil {
					continue
				}

				// Post-filter: verify decoded event matches the query filters
				filterStart := time.Now()
				matched := true
				if filterContractID != "" && ev.ContractID != filterContractID {
					matched = false
				}
				if matched {
					for pos, ft := range filterTopics {
						if ft == "" {
							continue
						}
						if pos >= len(ev.Topics) || ev.Topics[pos] != ft {
							matched = false
							break
						}
					}
				}
				r.filterTime += time.Since(filterStart)

				if !matched {
					r.filtered++
					if r.filtered <= 3 {
						fmt.Fprintf(os.Stderr, "  [filter-debug] rejected ledger=%d seq=%d: contract=%s topics=%v\n",
							meta.ledger, meta.eventSeq, ev.ContractID, ev.Topics)
					}
					continue
				}

				r.events = append(r.events, ev)
			}
		}(w, start, end)
	}

	wg.Wait()

	// Merge results - use max of per-worker times since workers run in parallel
	var fetchTime, decodeTime, filterTime time.Duration
	var totalFiltered int
	events := make([]*query.Event, 0, fetchCap)
	for _, r := range results {
		events = append(events, r.events...)
		if r.fetchTime > fetchTime {
			fetchTime = r.fetchTime
		}
		if r.decodeTime > decodeTime {
			decodeTime = r.decodeTime
		}
		if r.filterTime > filterTime {
			filterTime = r.filterTime
		}
		result.EventBytesRead += r.bytesRead
		result.EventsScanned += r.scanned
		totalFiltered += r.filtered
	}
	if totalFiltered > 0 {
		fmt.Fprintf(os.Stderr, "  Post-filter: %d/%d events rejected (index false positives)\n", totalFiltered, result.EventsScanned)
		fmt.Fprintf(os.Stderr, "  Expected: contract=%q topics=%v\n", filterContractID, filterTopics)
	}
	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// QueryEventsWithBitmap32MultiFilter queries events using the 32-bit bitmap index with multi-value OR filters.
// contractIDs: multiple contract IDs (OR within group)
// topicGroups: per-position topic values (OR within position, AND across positions)
func (es *EventStore) QueryEventsWithBitmap32MultiFilter(
	contractIDs [][]byte,
	topicGroups [4][][]byte,
	startLedger, endLedger uint32,
	limit int,
) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	if es.indexStore == nil {
		return nil, nil, fmt.Errorf("bitmap32 event index not available")
	}

	// Query the bitmap32 index with multi-value filters
	indexStart := time.Now()
	queryResult, err := es.indexStore.QueryEventKeysMultiFilter(contractIDs, topicGroups, startLedger, endLedger)
	if err != nil {
		return nil, nil, fmt.Errorf("bitmap32 multi-filter query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLocalIDs = int(queryResult.TotalCount)
	result.IndexBytesRead = queryResult.BytesRead
	result.SegmentsScanned = queryResult.Segments
	result.SegmentsTouched = len(GetSegmentsForRange(startLedger, endLedger))
	result.IndexReadTime = queryResult.ReadTime
	result.IndexDecodeTime = queryResult.DecodeTime
	result.IndexIntersectTime = queryResult.IntersectTime

	if queryResult.TotalCount == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// Collect all keys and metadata from bitmap results, capped at limit.
	// Use ledger map to convert dense local IDs to (ledger, eventSeq) pairs.
	type bitmapEvtMeta struct {
		ledger   uint32
		eventSeq uint16
	}

	segIDs := make([]uint32, 0, len(queryResult.PerSegment))
	for segID := range queryResult.PerSegment {
		segIDs = append(segIDs, segID)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	fetchCap := result.MatchingLocalIDs
	if limit > 0 && fetchCap > limit {
		fetchCap = limit
	}
	allKeys := make([][]byte, 0, fetchCap)
	allMetas := make([]bitmapEvtMeta, 0, fetchCap)

	// Cache ledger maps per segment
	ledgerMaps := make(map[uint32]*SegmentLedgerMap)

	for _, segID := range segIDs {
		if len(allKeys) >= fetchCap {
			break
		}

		// Load ledger map for this segment
		lm, ok := ledgerMaps[segID]
		if !ok {
			var lmErr error
			lm, lmErr = es.indexStore.LoadSegmentLedgerMap(segID)
			if lmErr != nil {
				return nil, nil, fmt.Errorf("failed to load ledger map for segment %d: %w", segID, lmErr)
			}
			ledgerMaps[segID] = lm
		}
		if lm == nil {
			continue
		}

		bitmap := queryResult.PerSegment[segID]
		bitmapIter := bitmap.Iterator()
		for bitmapIter.HasNext() {
			if len(allKeys) >= fetchCap {
				break
			}
			denseID := bitmapIter.Next()
			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID)
			allKeys = append(allKeys, event.EncodeKey(segID, denseID))
			allMetas = append(allMetas, bitmapEvtMeta{ledger: ledger, eventSeq: eventSeq})
		}
	}

	// Parallel fetch using iterators
	const maxWorkers = 4
	numWorkers := maxWorkers
	if len(allKeys) < numWorkers {
		numWorkers = len(allKeys)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	type workerResult struct {
		events     []*query.Event
		fetchTime  time.Duration
		decodeTime time.Duration
		filterTime time.Duration
		bytesRead  int64
		scanned    int
	}

	results := make([]workerResult, numWorkers)
	chunkSize := (len(allKeys) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > len(allKeys) {
			end = len(allKeys)
		}
		if start >= end {
			continue
		}

		wg.Add(1)
		go func(workerID, start, end int) {
			defer wg.Done()
			r := &results[workerID]
			r.events = make([]*query.Event, 0, end-start)

			dbIter := es.db.NewIteratorCF(es.ro, es.cfEvents)
			defer dbIter.Close()

			for i := start; i < end; i++ {
				key := allKeys[i]
				meta := allMetas[i]

				seekStart := time.Now()
				dbIter.Seek(key)
				if !dbIter.Valid() {
					r.fetchTime += time.Since(seekStart)
					break
				}

				iterKey := dbIter.Key()
				if iterKey == nil || !bytes.Equal(iterKey.Data(), key) {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				iterVal := dbIter.Value()
				if iterVal == nil {
					r.fetchTime += time.Since(seekStart)
					continue
				}
				valData := iterVal.Data()
				if len(valData) == 0 {
					r.fetchTime += time.Since(seekStart)
					continue
				}

				valueCopy := make([]byte, len(valData))
				copy(valueCopy, valData)
				r.fetchTime += time.Since(seekStart)

				r.bytesRead += int64(len(valueCopy))
				r.scanned++

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				ev, decErr = parseRawXDRToQueryEvent(valueCopy, meta.ledger, 0, 0, meta.eventSeq)
				r.decodeTime += time.Since(decStart)

				if decErr != nil {
					continue
				}

				r.events = append(r.events, ev)
			}
		}(w, start, end)
	}

	wg.Wait()

	// Merge results - use max of per-worker times since workers run in parallel
	var fetchTime, decodeTime, filterTime time.Duration
	events := make([]*query.Event, 0, fetchCap)
	for _, r := range results {
		events = append(events, r.events...)
		if r.fetchTime > fetchTime {
			fetchTime = r.fetchTime
		}
		if r.decodeTime > decodeTime {
			decodeTime = r.decodeTime
		}
		if r.filterTime > filterTime {
			filterTime = r.filterTime
		}
		result.EventBytesRead += r.bytesRead
		result.EventsScanned += r.scanned
	}
	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// GetSegmentReader returns a SegmentReader for querying flat file indexes.
// The reader is created once and reused across queries so file caches persist.
// Returns nil if segment path is not configured.
func (es *EventStore) GetSegmentReader() *SegmentReader {
	if es.segmentPath == "" {
		return nil
	}
	if es.segmentReader == nil {
		es.segmentReader = NewSegmentReader(es.segmentPath, es)
	}
	return es.segmentReader
}

// QueryEventsWithSegmentIndex queries events using flat file segment indexes (single-value per filter).
func (es *EventStore) QueryEventsWithSegmentIndex(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment path not configured")
	}
	return reader.QueryEvents(contractID, topicGroups, startLedger, endLedger, limit)
}

// QueryEventsWithSegmentIndexMultiFilter queries events using flat file segment indexes with multi-value OR/AND filters.
func (es *EventStore) QueryEventsWithSegmentIndexMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment path not configured")
	}
	return reader.QueryEventsMultiFilter(contractIDs, topicGroups, startLedger, endLedger, limit)
}

// QueryEventsWithSegmentData queries events using flat file indexes + flat file event store (no RocksDB event fetches).
func (es *EventStore) QueryEventsWithSegmentData(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment path not configured")
	}
	return reader.QueryEventsFromSegmentData(contractID, topicGroups, startLedger, endLedger, limit)
}

// QueryEventsWithSegmentDataMultiFilter queries events from file store with multi-value OR/AND filters.
func (es *EventStore) QueryEventsWithSegmentDataMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment path not configured")
	}
	return reader.QueryEventsFromSegmentDataMultiFilter(contractIDs, topicGroups, startLedger, endLedger, limit)
}

// GetEventsInRangeFromSegmentData reads all events in a ledger range from flat file store (no index, no RocksDB).
func (es *EventStore) GetEventsInRangeFromSegmentData(startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment path not configured")
	}
	return reader.GetEventsInRange(startLedger, endLedger, limit)
}
