package store

import (
	"bytes"
	"container/heap"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

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

	// V2 posting list indexes (32-bit local IDs)
	CFContractsPLV2 = "contracts_plv2" // Contract ID V2 posting lists
	CFTopicsPLV2    = "topics_plv2"    // Topic V2 posting lists
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

// parseRawXDRToQueryEvent converts raw XDR bytes to a query.Event
func parseRawXDRToQueryEvent(rawXDR []byte, ledger uint32, tx, op uint32, eventIdx uint16) (*query.Event, error) {
	var xdrEvent xdr.ContractEvent
	if err := xdrEvent.UnmarshalBinary(rawXDR); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR event: %w", err)
	}

	ev := &query.Event{
		LedgerSequence:   ledger,
		TransactionIndex: int(tx),
		OperationIndex:   int(op),
		EventIndex:       int(eventIdx),
	}

	// Extract contract ID if present - encode as strkey (C...)
	if xdrEvent.ContractId != nil {
		if encoded, err := strkey.Encode(strkey.VersionByteContract, xdrEvent.ContractId[:]); err == nil {
			ev.ContractID = encoded
		}
	}

	// Event type
	switch xdrEvent.Type {
	case xdr.ContractEventTypeContract:
		ev.Type = "contract"
	case xdr.ContractEventTypeSystem:
		ev.Type = "system"
	case xdr.ContractEventTypeDiagnostic:
		ev.Type = "diagnostic"
	}

	// Extract topics and data from event body
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()

		// Topics
		for _, topic := range body.Topics {
			topicBytes, _ := topic.MarshalBinary()
			ev.Topics = append(ev.Topics, base64.StdEncoding.EncodeToString(topicBytes))
		}

		// Data
		dataBytes, _ := body.Data.MarshalBinary()
		ev.Data = base64.StdEncoding.EncodeToString(dataBytes)
	}

	return ev, nil
}

// RocksDBEventStore manages storing events in RocksDB
type RocksDBEventStore struct {
	db          *grocksdb.DB
	dbPath      string // Store path for filesystem-based stats
	wo          *grocksdb.WriteOptions
	ro          *grocksdb.ReadOptions
	indexes     *IndexConfig
	eventFormat string // "xdr" or "binary"

	// Column family handles (managed by DB, don't destroy manually)
	cfHandles []*grocksdb.ColumnFamilyHandle
	cfDefault *grocksdb.ColumnFamilyHandle // Metadata
	cfEvents  *grocksdb.ColumnFamilyHandle // Primary event storage
	cfUnique  *grocksdb.ColumnFamilyHandle // Unique value indexes with counts

	// 32-bit bitmap index CFs (event-level, FromBuffer decode)
	cfContractsBM32 *grocksdb.ColumnFamilyHandle // Contract ID bitmap32 index
	cfTopicsBM32    *grocksdb.ColumnFamilyHandle // Topic bitmap32 index

	// V2 posting list index CFs (32-bit local IDs)
	cfContractsPLV2 *grocksdb.ColumnFamilyHandle // Contract ID V2 posting lists
	cfTopicsPLV2    *grocksdb.ColumnFamilyHandle // Topic V2 posting lists

	// Event-level bitmap32 index (32-bit, FromBuffer decode)
	eventIndex32Store *BitmapEventSeqStore

	// Options that need to be destroyed on Close
	baseOpts *grocksdb.Options
	cfOpts   []*grocksdb.Options
	bbtoList []*grocksdb.BlockBasedTableOptions

	// Keep merge operator alive to prevent GC (RocksDB holds a reference)
	mergeOp grocksdb.MergeOperator

	// In-memory posting list accumulation (for efficient batch writes)
	postingsMu sync.Mutex

	// V2 posting list accumulation (32-bit local IDs)
	contractPostingsV2 map[string][]byte // index key -> accumulated 4-byte local IDs
	topicPostingsV2    map[string][]byte // index key -> accumulated 4-byte local IDs

	// Flat file segment directory path (empty = disabled)
	segmentFilePath   string
	segmentFileReader *SegmentFileReader // lazily initialized, reused across queries

	// Event volume writer for flat file event storage (nil = disabled)
	eventVolumeWriter *EventVolumeWriter
}

// NewEventStoreWithOptions creates a new event store with custom options
func NewEventStoreWithOptions(dbPath string, rocksOpts *RocksDBOptions, indexOpts *IndexConfig) (*RocksDBEventStore, error) {
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

	// Helper to create V2 posting list CF options (with V2 merge operator)
	createPostingListV2CFOpts := func() (*grocksdb.Options, *grocksdb.BlockBasedTableOptions) {
		opts := grocksdb.NewDefaultOptions()
		applyRocksDBOptions(opts, rocksOpts)
		opts.SetMergeOperator(&postingListV2MergeOperator{})
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(16 * 1024)
		if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
			bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
		}
		opts.SetBlockBasedTableFactory(bbto)
		return opts, bbto
	}

	// V2 posting list CFs (32-bit local IDs)
	contractsPLV2Opts, contractsPLV2BBTO := createPostingListV2CFOpts()
	topicsPLV2Opts, topicsPLV2BBTO := createPostingListV2CFOpts()

	cfNames := []string{
		CFDefault, CFEvents, CFUnique,
		CFContractsBM32, CFTopicsBM32,
		CFContractsPLV2, CFTopicsPLV2,
	}
	cfOpts := []*grocksdb.Options{
		defaultOpts, eventsOpts, uniqueOpts,
		contractsBM32Opts, topicsBM32Opts,
		contractsPLV2Opts, topicsPLV2Opts,
	}
	bbtoList := []*grocksdb.BlockBasedTableOptions{
		eventsBBTO, uniqueBBTO,
		contractsBM32BBTO, topicsBM32BBTO,
		contractsPLV2BBTO, topicsPLV2BBTO,
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(baseOpts, dbPath, cfNames, cfOpts)
	if err != nil {
		// Note: We intentionally don't destroy cfOpts, bbtoList, or baseOpts here.
		// Options with merge operators (uniqueOpts, postingListV2 opts) crash on
		// Destroy() due to a grocksdb issue with rocksdb_mergeoperator_destroy.
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
	eventIndex32Store, err := NewBitmapEventSeqStore(db, cfHandles[0], cfHandles[3], cfHandles[4])
	if err != nil {
		db.Close()
		// Note: We intentionally don't destroy cfOpts, bbtoList, or baseOpts here.
		// See comment above about merge operator crash on Destroy().
		wo.Destroy()
		return nil, fmt.Errorf("failed to create event bitmap32 index store: %w", err)
	}

	return &RocksDBEventStore{
		db:                 db,
		dbPath:             dbPath,
		wo:                 wo,
		ro:                 grocksdb.NewDefaultReadOptions(),
		indexes:            indexes,
		eventFormat:        "xdr", // Default to XDR format
		cfHandles:          cfHandles,
		cfDefault:          cfHandles[0],
		cfEvents:           cfHandles[1],
		cfUnique:           cfHandles[2],
		cfContractsBM32:    cfHandles[3],
		cfTopicsBM32:       cfHandles[4],
		cfContractsPLV2:    cfHandles[5],
		cfTopicsPLV2:       cfHandles[6],
		eventIndex32Store:  eventIndex32Store,
		baseOpts:           baseOpts,
		cfOpts:             cfOpts,
		bbtoList:           bbtoList,
		mergeOp:            mergeOp,
		contractPostingsV2: make(map[string][]byte),
		topicPostingsV2:    make(map[string][]byte),
	}, nil
}

// StoreEvents stores events with optional index updates based on options.
// Returns the number of bytes written.
func (es *RocksDBEventStore) StoreEvents(events []*event.IngestEvent, opts *StoreOptions) (int64, error) {
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

	// Get bitmap index once outside the loop (avoid per-event method call overhead)
	var eventBitmap32Idx *eventBitmap32Index
	if opts.V2Indexes && es.eventIndex32Store != nil {
		eventBitmap32Idx = es.eventIndex32Store.getEventBitmap32Index()
	}

	// Track event sequence counters per ledger for bitmap32 V2 keys
	var eventSeqCounters map[uint32]uint16
	if opts.V2Indexes {
		eventSeqCounters = make(map[uint32]uint16)
	}

	// Lock posting list maps if we'll be updating them
	if opts.V2Indexes {
		es.postingsMu.Lock()
		defer es.postingsMu.Unlock()
	}

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

		// Use V2 key format when bitmap32 mode is active
		var key []byte
		var eventSeq uint16
		if opts.V2Indexes {
			eventSeq = eventSeqCounters[ev.LedgerSequence]
			key = event.EncodeKeyV2(ev.LedgerSequence, eventSeq)
			eventSeqCounters[ev.LedgerSequence] = eventSeq + 1
		} else {
			key = event.EncodeKeyFromParts(ev.LedgerSequence, uint32(ev.TransactionIndex), uint32(ev.OperationIndex), ev.EventIndex)
		}

		// Encode event based on configured format
		var value []byte
		if es.eventFormat == "binary" {
			value = event.EncodeBinaryEvent(ev)
		} else {
			value = ev.RawXDR
		}
		batch.PutCF(es.cfEvents, key, value)
		totalBytes += int64(len(value))

		// Assign dense local ID and update bitmap + posting list indexes
		if eventBitmap32Idx != nil {
			bucketID := BucketID(ev.LedgerSequence)
			denseLocalID := eventBitmap32Idx.AssignDenseLocalID(bucketID, ev.LedgerSequence)

			// Index contract ID -> dense local ID (bitmap32)
			if len(ev.ContractID) > 0 {
				eventBitmap32Idx.AddContractEvent(ev.ContractID, bucketID, denseLocalID)
			}

			// Index topics -> dense local ID (bitmap32, positional)
			for pos, topicBytes := range ev.Topics {
				eventBitmap32Idx.AddTopicEvent(pos, topicBytes, bucketID, denseLocalID)
			}

			// Write to event volume if enabled
			if es.eventVolumeWriter != nil {
				if !es.eventVolumeWriter.IsActive() || bucketID != es.eventVolumeWriter.ChunkID() {
					// Finalize previous chunk if active
					if es.eventVolumeWriter.IsActive() {
						if err := es.eventVolumeWriter.FinalizeChunk(); err != nil {
							return 0, fmt.Errorf("failed to finalize event volume chunk: %w", err)
						}
					}
					if err := es.eventVolumeWriter.StartChunk(bucketID); err != nil {
						return 0, fmt.Errorf("failed to start event volume chunk %d: %w", bucketID, err)
					}
				}
				v3Data := event.EncodeBinaryEventV3(ev)
				if err := es.eventVolumeWriter.AppendEvent(denseLocalID, v3Data); err != nil {
					return 0, fmt.Errorf("failed to append event to volume: %w", err)
				}
			}
		}

		// Accumulate V2 posting list local IDs using dense IDs
		if opts.V2Indexes {
			bucketID := BucketID(ev.LedgerSequence)
			// Re-use the same dense ID from the bitmap counter
			// The bitmap index already assigned this ID, so we need to compute
			// what it was. Since the bitmap assigns IDs sequentially and we're
			// in the same loop iteration, we can get the last assigned ID.
			var denseLocalID uint32
			if eventBitmap32Idx != nil {
				// The ID was just assigned above; it's nextDenseID - 1
				counter := eventBitmap32Idx.bucketCounters[bucketID]
				denseLocalID = counter.nextDenseID - 1
			}

			localIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(localIDBytes, denseLocalID)

			if len(ev.ContractID) > 0 {
				termKey := ContractTermKey(ev.ContractID)
				indexKey := string(EncodeIndexKey(termKey, ev.LedgerSequence))
				es.contractPostingsV2[indexKey] = append(es.contractPostingsV2[indexKey], localIDBytes...)
			}

			for pos, topicBytes := range ev.Topics {
				termKey := TopicTermKey(pos, topicBytes)
				indexKey := string(EncodeIndexKey(termKey, ev.LedgerSequence))
				es.topicPostingsV2[indexKey] = append(es.topicPostingsV2[indexKey], localIDBytes...)
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

	// Note: V2 posting list indexes are accumulated in memory and flushed
	// periodically via FlushPostingListV2Indexes() for better write efficiency

	if err := es.db.Write(es.wo, batch); err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	return totalBytes, nil
}

// FlushBitmapIndexes flushes hot bitmap segments to disk
func (es *RocksDBEventStore) FlushBitmapIndexes() error {
	// Flush 32-bit event-level bitmap (V2)
	if es.eventIndex32Store != nil {
		if err := es.eventIndex32Store.Flush(); err != nil {
			return fmt.Errorf("failed to flush event-level bitmap32: %w", err)
		}
	}
	return nil
}

// FlushPostingListV2Indexes flushes accumulated V2 posting list entries (32-bit local IDs) to disk.
// Returns the number of contract and topic keys flushed.
func (es *RocksDBEventStore) FlushPostingListV2Indexes() (int, int, error) {
	es.postingsMu.Lock()
	defer es.postingsMu.Unlock()

	if len(es.contractPostingsV2) == 0 && len(es.topicPostingsV2) == 0 {
		return 0, 0, nil
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	contractKeys := 0
	topicKeys := 0

	// Write all accumulated contract postings V2 (convert raw local IDs to delta-varint)
	for keyStr, rawLocalIDs := range es.contractPostingsV2 {
		localIDs := decodeRawLocalIDs(rawLocalIDs)
		localIDs = deduplicateLocalIDs(localIDs)
		encoded := EncodeLocalIDListDeltaVarint(localIDs)
		batch.MergeCF(es.cfContractsPLV2, []byte(keyStr), encoded)
		contractKeys++
	}

	// Write all accumulated topic postings V2 (convert raw local IDs to delta-varint)
	for keyStr, rawLocalIDs := range es.topicPostingsV2 {
		localIDs := decodeRawLocalIDs(rawLocalIDs)
		localIDs = deduplicateLocalIDs(localIDs)
		encoded := EncodeLocalIDListDeltaVarint(localIDs)
		batch.MergeCF(es.cfTopicsPLV2, []byte(keyStr), encoded)
		topicKeys++
	}

	if err := es.db.Write(es.wo, batch); err != nil {
		return 0, 0, fmt.Errorf("failed to flush V2 posting list indexes: %w", err)
	}

	// Clear the maps
	es.contractPostingsV2 = make(map[string][]byte)
	es.topicPostingsV2 = make(map[string][]byte)

	return contractKeys, topicKeys, nil
}

// GetPostingListV2Stats returns statistics about in-memory V2 posting list accumulation.
func (es *RocksDBEventStore) GetPostingListV2Stats() (contractKeys, topicKeys int, contractBytes, topicBytes int64) {
	es.postingsMu.Lock()
	defer es.postingsMu.Unlock()

	contractKeys = len(es.contractPostingsV2)
	topicKeys = len(es.topicPostingsV2)

	for _, v := range es.contractPostingsV2 {
		contractBytes += int64(len(v))
	}
	for _, v := range es.topicPostingsV2 {
		topicBytes += int64(len(v))
	}

	return
}

// GetEventsInRangeWithTiming retrieves events in a ledger range with detailed timing.
// Returns query.Event format with disk read and unmarshal timing.
func (es *RocksDBEventStore) GetEventsInRangeWithTiming(startLedger, endLedger uint32, limit int) (*query.RangeResult, error) {
	result := &query.RangeResult{
		Timing: query.FetchTiming{},
	}

	// Use V2 key (6 bytes) for seeking — works as prefix for both V2 and TOID keys
	startKey := event.EncodeKeyV2(startLedger, 0)

	// Time iterator creation and seek
	diskStart := time.Now()
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()
	it.Seek(startKey)
	result.Timing.DiskReadTime += time.Since(diskStart)

	for it.Valid() {
		// Check limit
		if limit > 0 && len(result.Events) >= limit {
			break
		}

		// Time key access (disk read)
		diskStart = time.Now()
		key := it.Key().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		if len(key) < 6 {
			break
		}

		// Compare ledger (bytes 0-3, same position in both V2 and TOID keys)
		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger > endLedger {
			break
		}

		// Decode key based on format (6 bytes = V2, 10 bytes = TOID)
		var tx, op uint32
		var eventIdx uint16
		if len(key) == 6 {
			eventIdx = binary.BigEndian.Uint16(key[4:6])
		} else if len(key) >= 10 {
			_, tx, op, eventIdx = event.DecodeKeyFull(key)
		} else {
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
		if es.eventFormat == "binary" {
			ev, err = event.DecodeBinaryToQueryEvent(valueData, keyLedger, tx, op, eventIdx)
		} else {
			ev, err = parseRawXDRToQueryEvent(valueData, keyLedger, tx, op, eventIdx)
		}
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
func (es *RocksDBEventStore) GetEventsInLedger(ledger uint32) ([]*query.Event, error) {
	var events []*query.Event

	startKey := event.EncodeKeyV2(ledger, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 6 {
			break
		}

		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger != ledger {
			break
		}

		var tx, op uint32
		var eventIdx uint16
		if len(key) == 6 {
			eventIdx = binary.BigEndian.Uint16(key[4:6])
		} else if len(key) >= 10 {
			_, tx, op, eventIdx = event.DecodeKeyFull(key)
		} else {
			break
		}

		valueData := it.Value().Data()

		var ev *query.Event
		var err error
		if es.eventFormat == "binary" {
			ev, err = event.DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
		} else {
			ev, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
		}
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
func (es *RocksDBEventStore) GetEventsInLedgerWithTiming(ledger uint32) (*query.FetchResult, error) {
	result := &query.FetchResult{
		Timing: query.FetchTiming{},
	}

	startKey := event.EncodeKeyV2(ledger, 0)

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

		if len(key) < 6 {
			break
		}

		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger != ledger {
			break
		}

		var tx, op uint32
		var eventIdx uint16
		if len(key) == 6 {
			eventIdx = binary.BigEndian.Uint16(key[4:6])
		} else if len(key) >= 10 {
			_, tx, op, eventIdx = event.DecodeKeyFull(key)
		} else {
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
		if es.eventFormat == "binary" {
			ev, err = event.DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
		} else {
			ev, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
		}
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
func (es *RocksDBEventStore) BuildIndexes(workers int, opts *BuildIndexOptions, progressFn func(processed int64)) error {
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
func (es *RocksDBEventStore) buildIndexesForRange(startLedger, endLedger uint32, opts *BuildIndexOptions, entryCh chan<- *indexEntry) error {
	startKey := event.EncodeKeyV2(startLedger, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	// Accumulate unique counts in memory (only if building unique indexes)
	var counts map[string]uint64
	if opts.UniqueIndexes {
		counts = make(map[string]uint64)
	}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 6 {
			break
		}

		// Parse key: V2 = [ledger:4][eventSeq:2], TOID = [ledger:4][tx:2][op:2][event:2]
		ledger := binary.BigEndian.Uint32(key[0:4])
		if ledger > endLedger {
			break
		}

		var txIdx, opIdx, eventIdx uint16
		if len(key) == 6 {
			eventIdx = binary.BigEndian.Uint16(key[4:6])
		} else if len(key) >= 10 {
			txIdx = binary.BigEndian.Uint16(key[4:6])
			opIdx = binary.BigEndian.Uint16(key[6:8])
			eventIdx = binary.BigEndian.Uint16(key[8:10])
		} else {
			break
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

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
				TxIdx:      txIdx,
				OpIdx:      opIdx,
				EventIdx:   eventIdx,
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
func (es *RocksDBEventStore) indexCollector(entryCh <-chan *indexEntry, opts *BuildIndexOptions, progressFn func(processed int64)) error {
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

// SetEventFormat sets the event storage format ("xdr" or "binary").
// Must be called before storing events.
func (es *RocksDBEventStore) SetEventFormat(format string) {
	if format == "binary" || format == "xdr" {
		es.eventFormat = format
	}
}

// GetEventFormat returns the current event storage format.
func (es *RocksDBEventStore) GetEventFormat() string {
	return es.eventFormat
}

// SetSegmentFilePath sets the base directory for segment flat files.
func (es *RocksDBEventStore) SetSegmentFilePath(path string) {
	es.segmentFilePath = path
}

// EnableEventVolume initializes the event volume writer for flat file event storage.
// Uses the same base path as segment files.
// If compressEvents is true, each event blob is zstd-compressed in events.dat.
// If dictCompress is true, zstd dictionary compression is used (takes priority over plain zstd).
// groupSize controls how many events are compressed together (0 or 1 = per-event).
func (es *RocksDBEventStore) EnableEventVolume(compressEvents bool, dictCompress bool, dictSampleCount int, groupSize int) {
	if es.segmentFilePath != "" {
		es.eventVolumeWriter = NewEventVolumeWriter(es.segmentFilePath, compressEvents, dictCompress, dictSampleCount, groupSize)
		if dictCompress {
			fmt.Fprintf(os.Stderr, "Event volume writer enabled with zstd dictionary compression (base: %s)\n", es.segmentFilePath)
		} else if compressEvents && groupSize > 1 {
			fmt.Fprintf(os.Stderr, "Event volume writer enabled with zstd grouped compression (group=%d, base: %s)\n", groupSize, es.segmentFilePath)
		} else if compressEvents {
			fmt.Fprintf(os.Stderr, "Event volume writer enabled with zstd compression (base: %s)\n", es.segmentFilePath)
		} else {
			fmt.Fprintf(os.Stderr, "Event volume writer enabled (base: %s)\n", es.segmentFilePath)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Warning: EnableEventVolume called but segment_file_path not set\n")
	}
}

// FinalizeEventVolume finalizes any active event volume chunk.
// Called at the end of ingestion to flush the last incomplete chunk.
func (es *RocksDBEventStore) FinalizeEventVolume() error {
	if es.eventVolumeWriter != nil && es.eventVolumeWriter.IsActive() {
		return es.eventVolumeWriter.FinalizeChunk()
	}
	return nil
}

// WriteBucketDir writes flat file indexes (.idx + .pack pairs) for a completed bucket.
// Scans the bitmap32 CFs to collect term data, partitions topics by position,
// and writes the files atomically using tmp + rename.
func (es *RocksDBEventStore) WriteBucketDir(bucketID uint32) error {
	if es.segmentFilePath == "" {
		return fmt.Errorf("segment file path not configured")
	}

	// Collect contract terms from cfContractsBM32
	contractTerms, err := es.collectBitmapTerms(es.cfContractsBM32, bucketID)
	if err != nil {
		return fmt.Errorf("failed to collect contract terms: %w", err)
	}

	// Collect topic terms from cfTopicsBM32
	topicTerms, err := es.collectBitmapTerms(es.cfTopicsBM32, bucketID)
	if err != nil {
		return fmt.Errorf("failed to collect topic terms: %w", err)
	}

	// Partition topic terms by position using the position map
	topicsByPos := [4][]BucketTermData{}
	var combinedTopics []BucketTermData

	posMap := es.eventIndex32Store.getEventBitmap32Index().GetTopicTermPositions()
	if len(posMap) > 0 {
		for _, t := range topicTerms {
			var termHash [16]byte
			copy(termHash[:], t.TermHash[:])
			if pos, ok := posMap[termHash]; ok && pos < 4 {
				topicsByPos[pos] = append(topicsByPos[pos], t)
			} else {
				// Unknown position — put in combined fallback
				combinedTopics = append(combinedTopics, t)
			}
		}
	} else {
		// No position map (restart fallback) — all topics go to combined
		combinedTopics = topicTerms
	}

	// Load ledger map
	lm, err := es.LoadBucketLedgerMap(bucketID)
	if err != nil {
		return fmt.Errorf("failed to load ledger map for bucket %d: %w", bucketID, err)
	}

	// Write the bucket directory
	if err := WriteBucketDir(es.segmentFilePath, bucketID, contractTerms, topicsByPos, combinedTopics, lm); err != nil {
		return err
	}

	// Finalize event volume chunk if it matches this bucket
	if es.eventVolumeWriter != nil && es.eventVolumeWriter.IsActive() && es.eventVolumeWriter.ChunkID() == bucketID {
		if err := es.eventVolumeWriter.FinalizeChunk(); err != nil {
			return fmt.Errorf("failed to finalize event volume chunk %d: %w", bucketID, err)
		}
	}

	return nil
}

// collectBitmapTerms scans a bitmap32 CF for all entries matching a specific bucket ID.
// Returns a slice of (termHash, bitmapData) pairs.
func (es *RocksDBEventStore) collectBitmapTerms(cf *grocksdb.ColumnFamilyHandle, bucketID uint32) ([]BucketTermData, error) {
	// Create prefix to scan: we need to iterate all keys that end with this bucket ID.
	// Since keys are [termHash:16][bucketID:4], we must scan the entire CF and filter.
	// For efficiency, we use an iterator and check the bucket ID suffix.

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := es.db.NewIteratorCF(ro, cf)
	defer iter.Close()

	var results []BucketTermData

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		if len(key) != IndexKeySize {
			continue
		}

		// Extract bucket ID from key suffix
		keyBucketID := binary.BigEndian.Uint32(key[16:20])
		if keyBucketID != bucketID {
			continue
		}

		// Extract term hash
		var termHash [16]byte
		copy(termHash[:], key[0:16])

		// Copy bitmap data
		valData := iter.Value().Data()
		dataCopy := make([]byte, len(valData))
		copy(dataCopy, valData)

		results = append(results, BucketTermData{
			TermHash:   termHash,
			BitmapData: dataCopy,
		})
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return results, nil
}

// Close closes the event store
func (es *RocksDBEventStore) Close() {
	// Close index stores (flushes any remaining hot segments)
	if es.eventIndex32Store != nil {
		es.eventIndex32Store.Close()
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

func (es *RocksDBEventStore) GetBitmapStats() *BitmapStats {
	if es.eventIndex32Store == nil {
		return nil
	}
	bitmapIdx := es.eventIndex32Store.getEventBitmap32Index()
	if bitmapIdx == nil {
		return nil
	}
	count, cards, memBytes := bitmapIdx.GetHotSegmentStats()
	return &BitmapStats{
		CurrentBucketID:    bitmapIdx.GetCurrentBucketID(),
		HotSegmentCount:    count,
		HotSegmentCards:    cards,
		HotSegmentMemBytes: memBytes,
	}
}

// SetLastProcessedLedger stores the last processed ledger sequence
func (es *RocksDBEventStore) SetLastProcessedLedger(sequence uint32) error {
	key := []byte("last_processed_ledger")
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, sequence)
	return es.db.PutCF(es.wo, es.cfDefault, key, value)
}

// GetLastProcessedLedger retrieves the last processed ledger sequence
func (es *RocksDBEventStore) GetLastProcessedLedger() (uint32, error) {
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

// LoadBucketLedgerMap reads the ledger map for a bucket from CFDefault.
func (es *RocksDBEventStore) LoadBucketLedgerMap(bucketID uint32) (*BucketLedgerMap, error) {
	if es.eventIndex32Store != nil {
		return es.eventIndex32Store.LoadBucketLedgerMap(bucketID)
	}
	return nil, nil
}

// InitializeDenseCounters loads the ledger map for the current bucket from storage
// and initializes the dense ID counter so ingestion continues from the correct offset.
// Should be called on startup after the DB is opened.
func (es *RocksDBEventStore) InitializeDenseCounters(lastLedger uint32) error {
	if es.eventIndex32Store == nil {
		return nil
	}
	bucketID := BucketID(lastLedger)
	lm, err := es.eventIndex32Store.LoadBucketLedgerMap(bucketID)
	if err != nil {
		return fmt.Errorf("failed to load ledger map on startup: %w", err)
	}
	if lm != nil {
		es.eventIndex32Store.bitmap.InitializeDenseCounter(bucketID, lm.TotalEvents())
	}
	return nil
}

// =============================================================================
// Statistics
// =============================================================================

// CountEvents returns the total number of events in the store
func (es *RocksDBEventStore) CountEvents() (int64, error) {
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
func (es *RocksDBEventStore) GetStorageSnapshot() (*StorageSnapshot, error) {
	snapshot := &StorageSnapshot{
		Timestamp:      time.Now(),
		ColumnFamilies: make(map[string]*ColumnFamilyStats),
	}

	cfNames := []string{
		CFDefault, CFEvents, CFUnique,
		CFContractsBM32, CFTopicsBM32,
		CFContractsPLV2, CFTopicsPLV2,
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
func (es *RocksDBEventStore) Flush() error {
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
func (es *RocksDBEventStore) GetStats() (*DBStats, error) {
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
func (es *RocksDBEventStore) GetLedgerTxStats() (*LedgerTxStats, error) {
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
func (es *RocksDBEventStore) CompactAllWithStats() (*CompactionSummary, error) {
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
func (es *RocksDBEventStore) CountUniqueIndexes() (*UniqueIndexCounts, error) {
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
func (es *RocksDBEventStore) countIndexTypePartition(indexType byte, partition, totalPartitions int) (uniqueCount, totalEvents int64, err error) {
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
func (es *RocksDBEventStore) GetIndexDistribution(topN int, bottomN ...int) (*IndexDistribution, error) {
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
func (es *RocksDBEventStore) computeDistributionForType(indexType byte, topN, bottomN int) (*DistributionStats, error) {
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
func (es *RocksDBEventStore) scanDistributionPartition(indexType byte, partition, totalPartitions, topN, bottomN int) ([]int64, int64, int64, []TopEntry, []TopEntry, error) {
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
func (es *RocksDBEventStore) getLedgerRange() (uint32, uint32, error) {
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
func (es *RocksDBEventStore) GetLedgerRange() (min, max uint32, err error) {
	return es.getLedgerRange()
}

// =============================================================================
// Compute Event Stats (Full Scan)
// =============================================================================

// ComputeEventStats scans all events and computes unique counts.
// Workers controls parallelism: 0 uses NumCPU, 1 for single-threaded, >1 for parallel.
func (es *RocksDBEventStore) ComputeEventStats(workers int) (*EventStats, error) {
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
func (es *RocksDBEventStore) computeEventStatsSingleThread() (*EventStats, error) {
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
func (es *RocksDBEventStore) computeStatsForRange(startLedger, endLedger uint32) struct {
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

	startKey := event.EncodeKeyV2(startLedger, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 4 {
			break
		}

		ledger := binary.BigEndian.Uint32(key[0:4])
		if ledger > endLedger {
			break
		}

		result.stats.TotalEvents++

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
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
func (es *RocksDBEventStore) QueryEventsWithBitmap32EventIndex(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	if es.eventIndex32Store == nil {
		return nil, nil, fmt.Errorf("bitmap32 event index not available")
	}

	// Query the bitmap32 index (positional topic matching)
	indexStart := time.Now()
	queryResult, err := es.eventIndex32Store.QueryEventKeysWithStats(contractID, topicGroups, startLedger, endLedger)
	if err != nil {
		return nil, nil, fmt.Errorf("bitmap32 event query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLocalIDs = int(queryResult.TotalCount)
	result.IndexBytesRead = queryResult.BytesRead
	result.SegmentsScanned = queryResult.Segments
	result.BucketsTouched = len(GetBucketsForRange(startLedger, endLedger))
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
	ledgerMaps := make(map[uint32]*BucketLedgerMap)

	for _, segID := range segIDs {
		if len(allKeys) >= fetchCap {
			break
		}

		// Load ledger map for this segment
		lm, ok := ledgerMaps[segID]
		if !ok {
			var lmErr error
			lm, lmErr = es.eventIndex32Store.LoadBucketLedgerMap(segID)
			if lmErr != nil {
				return nil, nil, fmt.Errorf("failed to load ledger map for bucket %d: %w", segID, lmErr)
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
			allKeys = append(allKeys, event.EncodeKeyV2(ledger, eventSeq))
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

				// Filter using binary header (contract ID only; topic filtering is handled by positional index)
				filterStart := time.Now()
				if es.eventFormat == "binary" && len(contractID) > 0 {
					header := event.ParseBinaryHeader(valueCopy)
					if header != nil {
						if !header.MatchesContractID(contractID) {
							r.filterTime += time.Since(filterStart)
							continue
						}
					}
				}
				r.filterTime += time.Since(filterStart)

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				if es.eventFormat == "binary" {
					ev, decErr = event.DecodeBinaryToQueryEventV2(valueCopy, meta.ledger, meta.eventSeq)
				} else {
					ev, decErr = parseRawXDRToQueryEvent(valueCopy, meta.ledger, 0, 0, meta.eventSeq)
				}
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

// QueryEventsWithBitmap32MultiFilter queries events using the 32-bit bitmap index with multi-value OR filters.
// contractIDs: multiple contract IDs (OR within group)
// topicGroups: per-position topic values (OR within position, AND across positions)
func (es *RocksDBEventStore) QueryEventsWithBitmap32MultiFilter(
	contractIDs [][]byte,
	topicGroups [4][][]byte,
	startLedger, endLedger uint32,
	limit int,
) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	if es.eventIndex32Store == nil {
		return nil, nil, fmt.Errorf("bitmap32 event index not available")
	}

	// Query the bitmap32 index with multi-value filters
	indexStart := time.Now()
	queryResult, err := es.eventIndex32Store.QueryEventKeysMultiFilter(contractIDs, topicGroups, startLedger, endLedger)
	if err != nil {
		return nil, nil, fmt.Errorf("bitmap32 multi-filter query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLocalIDs = int(queryResult.TotalCount)
	result.IndexBytesRead = queryResult.BytesRead
	result.SegmentsScanned = queryResult.Segments
	result.BucketsTouched = len(GetBucketsForRange(startLedger, endLedger))
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
	ledgerMaps := make(map[uint32]*BucketLedgerMap)

	for _, segID := range segIDs {
		if len(allKeys) >= fetchCap {
			break
		}

		// Load ledger map for this segment
		lm, ok := ledgerMaps[segID]
		if !ok {
			var lmErr error
			lm, lmErr = es.eventIndex32Store.LoadBucketLedgerMap(segID)
			if lmErr != nil {
				return nil, nil, fmt.Errorf("failed to load ledger map for bucket %d: %w", segID, lmErr)
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
			allKeys = append(allKeys, event.EncodeKeyV2(ledger, eventSeq))
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

				// Post-filter: contract ID only (topic filtering is handled by positional index)
				filterStart := time.Now()
				if es.eventFormat == "binary" && len(contractIDs) > 0 {
					header := event.ParseBinaryHeader(valueCopy)
					if header != nil {
						contractMatch := false
						for _, cid := range contractIDs {
							if header.MatchesContractID(cid) {
								contractMatch = true
								break
							}
						}
						if !contractMatch {
							r.filterTime += time.Since(filterStart)
							continue
						}
					}
				}
				r.filterTime += time.Since(filterStart)

				// Decode to query.Event
				decStart := time.Now()
				var ev *query.Event
				var decErr error
				if es.eventFormat == "binary" {
					ev, decErr = event.DecodeBinaryToQueryEventV2(valueCopy, meta.ledger, meta.eventSeq)
				} else {
					ev, decErr = parseRawXDRToQueryEvent(valueCopy, meta.ledger, 0, 0, meta.eventSeq)
				}
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

// GetSegmentFileReader returns a SegmentFileReader for querying flat file indexes.
// The reader is created once and reused across queries so file caches persist.
// Returns nil if segment file path is not configured.
func (es *RocksDBEventStore) GetSegmentFileReader() *SegmentFileReader {
	if es.segmentFilePath == "" {
		return nil
	}
	if es.segmentFileReader == nil {
		es.segmentFileReader = NewSegmentFileReader(es.segmentFilePath, es)
	}
	return es.segmentFileReader
}

// QueryEventsWithSegmentFile queries events using flat file segment indexes (single-value per filter).
func (es *RocksDBEventStore) QueryEventsWithSegmentFile(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentFileReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment file path not configured")
	}
	return reader.QueryEvents(contractID, topicGroups, startLedger, endLedger, limit)
}

// QueryEventsWithSegmentFileMultiFilter queries events using flat file segment indexes with multi-value OR/AND filters.
func (es *RocksDBEventStore) QueryEventsWithSegmentFileMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentFileReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment file path not configured")
	}
	return reader.QueryEventsMultiFilter(contractIDs, topicGroups, startLedger, endLedger, limit)
}

// QueryEventsWithSegmentVolume queries events using flat file indexes + flat file event volume (no RocksDB event fetches).
func (es *RocksDBEventStore) QueryEventsWithSegmentVolume(contractID []byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentFileReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment file path not configured")
	}
	return reader.QueryEventsFromVolume(contractID, topicGroups, startLedger, endLedger, limit)
}

// QueryEventsWithSegmentVolumeMultiFilter queries events from volume with multi-value OR/AND filters.
func (es *RocksDBEventStore) QueryEventsWithSegmentVolumeMultiFilter(contractIDs [][]byte, topicGroups [4][][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentFileReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment file path not configured")
	}
	return reader.QueryEventsFromVolumeMultiFilter(contractIDs, topicGroups, startLedger, endLedger, limit)
}

// GetEventsInRangeFromVolume reads all events in a ledger range from flat file volumes (no index, no RocksDB).
func (es *RocksDBEventStore) GetEventsInRangeFromVolume(startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	reader := es.GetSegmentFileReader()
	if reader == nil {
		return nil, nil, fmt.Errorf("segment file path not configured")
	}
	return reader.GetEventsInRange(startLedger, endLedger, limit)
}
