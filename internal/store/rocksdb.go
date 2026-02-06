package store

import (
	"container/heap"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/index"
	"github.com/urvisavla/stellar-events/internal/query"
)

// Column family names
const (
	CFDefault = "default" // Metadata (last_processed_ledger, etc.)
	CFEvents  = "events"  // Primary event storage (raw XDR or binary)
	CFUnique  = "unique"  // Unique value indexes with counts

	// Posting list indexes (TOID-based)
	CFContractsPL = "contracts_pl" // Contract ID posting lists
	CFTopicsPL    = "topics_pl"    // Topic posting lists

	// 32-bit bitmap indexes (ledger-level)
	CFContractsBM = "contracts_bm" // Contract ID bitmap index
	CFTopicsBM    = "topics_bm"    // Topic bitmap index

	// 64-bit bitmap indexes (event-level)
	CFContractsBM64 = "contracts_bm64" // Contract ID event bitmap index
	CFTopicsBM64    = "topics_bm64"    // Topic event bitmap index

	// 32-bit bitmap indexes (event-level, FromBuffer decode)
	CFContractsBM32 = "contracts_bm32" // Contract ID bitmap32 index
	CFTopicsBM32    = "topics_bm32"    // Topic bitmap32 index

	// V2 posting list indexes (32-bit local IDs)
	CFContractsPLV2 = "contracts_plv2" // Contract ID V2 posting lists
	CFTopicsPLV2    = "topics_plv2"    // Topic V2 posting lists
)

// Unique index type prefixes within CFUnique
const (
	UniqueTypeContract byte = 0x00 // Contract ID
	UniqueTypeTopic0   byte = 0x01 // Topic 0
	UniqueTypeTopic1   byte = 0x02 // Topic 1
	UniqueTypeTopic2   byte = 0x03 // Topic 2
	UniqueTypeTopic3   byte = 0x04 // Topic 3
)

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

// postingListMergeOperator implements a merge operator for delta-varint encoded TOID lists.
// Decodes, merges (union), and re-encodes the posting lists.
type postingListMergeOperator struct{}

func (m *postingListMergeOperator) Name() string {
	return "posting-list-delta-varint"
}

func (m *postingListMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	// Start with existing value
	var result []uint64
	if len(existingValue) > 0 {
		result = index.DecodeTOIDListDeltaVarint(existingValue)
	}

	// Merge each operand
	for _, operand := range operands {
		if len(operand) == 0 {
			continue
		}
		newTOIDs := index.DecodeTOIDListDeltaVarint(operand)
		if len(result) == 0 {
			result = newTOIDs
		} else {
			result = index.UnionTOIDLists(result, newTOIDs)
		}
	}

	if len(result) == 0 {
		return nil, true
	}

	return index.EncodeTOIDListDeltaVarint(result), true
}

func (m *postingListMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	// Decode both, merge, re-encode
	left := index.DecodeTOIDListDeltaVarint(leftOperand)
	right := index.DecodeTOIDListDeltaVarint(rightOperand)

	if len(left) == 0 {
		return rightOperand, true
	}
	if len(right) == 0 {
		return leftOperand, true
	}

	merged := index.UnionTOIDLists(left, right)
	return index.EncodeTOIDListDeltaVarint(merged), true
}

// postingListV2MergeOperator implements a merge operator for delta-varint encoded local ID lists.
// Same as postingListMergeOperator but decodes/encodes uint32 local IDs.
type postingListV2MergeOperator struct{}

func (m *postingListV2MergeOperator) Name() string {
	return "posting-list-v2-delta-varint"
}

func (m *postingListV2MergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var result []uint32
	if len(existingValue) > 0 {
		result = index.DecodeLocalIDListDeltaVarint(existingValue)
	}

	for _, operand := range operands {
		if len(operand) == 0 {
			continue
		}
		newIDs := index.DecodeLocalIDListDeltaVarint(operand)
		if len(result) == 0 {
			result = newIDs
		} else {
			result = index.UnionLocalIDLists(result, newIDs)
		}
	}

	if len(result) == 0 {
		return nil, true
	}

	return index.EncodeLocalIDListDeltaVarint(result), true
}

func (m *postingListV2MergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	left := index.DecodeLocalIDListDeltaVarint(leftOperand)
	right := index.DecodeLocalIDListDeltaVarint(rightOperand)

	if len(left) == 0 {
		return rightOperand, true
	}
	if len(right) == 0 {
		return leftOperand, true
	}

	merged := index.UnionLocalIDLists(left, right)
	return index.EncodeLocalIDListDeltaVarint(merged), true
}

// eventKey generates a 10-byte binary key for events using TOID format.
// Format: [toid:8][event_index:2]
// TOID = (ledger << 32) | (tx << 12) | op
func eventKey(e *IngestEvent) []byte {
	toid := (uint64(e.LedgerSequence) << 32) |
		(uint64(e.TransactionIndex&0xFFFFF) << 12) |
		uint64(e.OperationIndex&0xFFF)
	key := make([]byte, 10)
	binary.BigEndian.PutUint64(key[0:8], toid)
	binary.BigEndian.PutUint16(key[8:10], e.EventIndex)
	return key
}

// eventKeyFromParts generates a 10-byte binary key from components using TOID format.
// tx supports 20 bits (0-1,048,575), op supports 12 bits (0-4,095)
func eventKeyFromParts(ledger uint32, tx, op uint32, event uint16) []byte {
	toid := (uint64(ledger) << 32) | (uint64(tx&0xFFFFF) << 12) | uint64(op&0xFFF)
	key := make([]byte, 10)
	binary.BigEndian.PutUint64(key[0:8], toid)
	binary.BigEndian.PutUint16(key[8:10], event)
	return key
}

// parseEventKey extracts position info from a 10-byte TOID-format key.
// Returns ledger, tx (20 bits), op (12 bits), and event index.
func parseEventKey(key []byte) (ledger uint32, tx, op uint32, event uint16) {
	if len(key) < 10 {
		return 0, 0, 0, 0
	}
	toid := binary.BigEndian.Uint64(key[0:8])
	ledger = uint32(toid >> 32)
	tx = uint32((toid >> 12) & 0xFFFFF)
	op = uint32(toid & 0xFFF)
	event = binary.BigEndian.Uint16(key[8:10])
	return
}

// uniqueKey generates a key for the unique index column family
// Format: [type:1][value:N]
func uniqueKey(uniqueType byte, value []byte) []byte {
	key := make([]byte, 1+len(value))
	key[0] = uniqueType
	copy(key[1:], value)
	return key
}

// parseRawXDRToEvent converts raw XDR bytes and key info to a ContractEvent
func parseRawXDRToEvent(rawXDR []byte, ledger uint32, tx, op uint32, eventIdx uint16) (*ContractEvent, error) {
	var xdrEvent xdr.ContractEvent
	if err := xdrEvent.UnmarshalBinary(rawXDR); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR event: %w", err)
	}

	event := &ContractEvent{
		LedgerSequence:   ledger,
		TransactionIndex: int(tx),
		OperationIndex:   int(op),
		EventIndex:       int(eventIdx),
	}

	// Extract contract ID if present - encode as strkey (C...)
	if xdrEvent.ContractId != nil {
		if encoded, err := strkey.Encode(strkey.VersionByteContract, xdrEvent.ContractId[:]); err == nil {
			event.ContractID = encoded
		}
	}

	// Event type
	switch xdrEvent.Type {
	case xdr.ContractEventTypeContract:
		event.Type = "contract"
	case xdr.ContractEventTypeSystem:
		event.Type = "system"
	case xdr.ContractEventTypeDiagnostic:
		event.Type = "diagnostic"
	}

	// Extract topics and data from event body
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()

		// Topics
		for _, topic := range body.Topics {
			topicBytes, _ := topic.MarshalBinary()
			event.Topics = append(event.Topics, base64.StdEncoding.EncodeToString(topicBytes))
		}

		// Data
		dataBytes, _ := body.Data.MarshalBinary()
		event.Data = base64.StdEncoding.EncodeToString(dataBytes)
	}

	return event, nil
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

	// Posting list index CFs
	cfContractsPL *grocksdb.ColumnFamilyHandle // Contract ID posting lists
	cfTopicsPL    *grocksdb.ColumnFamilyHandle // Topic posting lists

	// 32-bit bitmap index CFs (ledger-level)
	cfContractsBM *grocksdb.ColumnFamilyHandle // Contract ID bitmap index
	cfTopicsBM    *grocksdb.ColumnFamilyHandle // Topic bitmap index

	// 64-bit bitmap index CFs (event-level)
	cfContractsBM64 *grocksdb.ColumnFamilyHandle // Contract ID event bitmap
	cfTopicsBM64    *grocksdb.ColumnFamilyHandle // Topic event bitmap

	// 32-bit bitmap index CFs (event-level, FromBuffer decode)
	cfContractsBM32 *grocksdb.ColumnFamilyHandle // Contract ID bitmap32 index
	cfTopicsBM32    *grocksdb.ColumnFamilyHandle // Topic bitmap32 index

	// V2 posting list index CFs (32-bit local IDs)
	cfContractsPLV2 *grocksdb.ColumnFamilyHandle // Contract ID V2 posting lists
	cfTopicsPLV2    *grocksdb.ColumnFamilyHandle // Topic V2 posting lists

	// Index store (manages bitmap indexes with RocksDB persistence)
	indexStore *index.RocksDBStore

	// Event-level bitmap index (64-bit, for exact event matching)
	eventIndexStore *index.EventRocksDBStore

	// Event-level bitmap32 index (32-bit, FromBuffer decode)
	eventIndex32Store *index.EventRocksDB32Store

	// true when bitmap32 mode is active (use V2 event keys)
	useEventKeyV2 bool

	// Options that need to be destroyed on Close
	baseOpts *grocksdb.Options
	cfOpts   []*grocksdb.Options
	bbtoList []*grocksdb.BlockBasedTableOptions

	// Keep merge operator alive to prevent GC (RocksDB holds a reference)
	mergeOp grocksdb.MergeOperator

	// Multi-filter optimization toggle (for A/B benchmarking)
	ParallelPostingReads bool // When true, use parallel reads + smallest-first intersection

	// In-memory posting list accumulation (for efficient batch writes)
	postingsMu sync.Mutex
	contractPostings  map[string][]byte // index key -> accumulated TOIDs
	topicPostings     map[string][]byte // index key -> accumulated TOIDs
	postingsLedgerCnt int               // ledgers accumulated since last flush

	// V2 posting list accumulation (32-bit local IDs)
	contractPostingsV2 map[string][]byte // index key -> accumulated 4-byte local IDs
	topicPostingsV2    map[string][]byte // index key -> accumulated 4-byte local IDs
}

// NewEventStore creates a new event store with RocksDB backend
func NewEventStore(dbPath string) (*RocksDBEventStore, error) {
	return NewEventStoreWithOptions(dbPath, nil, nil)
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

	// Helper to create posting list CF options (with merge operator)
	createPostingListCFOpts := func() (*grocksdb.Options, *grocksdb.BlockBasedTableOptions) {
		opts := grocksdb.NewDefaultOptions()
		applyRocksDBOptions(opts, rocksOpts)
		opts.SetMergeOperator(&postingListMergeOperator{})
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(16 * 1024)
		if rocksOpts != nil && rocksOpts.BloomFilterBitsPerKey > 0 {
			bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey)))
		}
		opts.SetBlockBasedTableFactory(bbto)
		return opts, bbto
	}

	// Posting list CFs
	contractsPLOpts, contractsPLBBTO := createPostingListCFOpts()
	topicsPLOpts, topicsPLBBTO := createPostingListCFOpts()

	// 32-bit bitmap CFs (ledger-level)
	contractsBMOpts, contractsBMBBTO := createBitmapCFOpts()
	topicsBMOpts, topicsBMBBTO := createBitmapCFOpts()

	// 64-bit bitmap CFs (event-level)
	contractsBM64Opts, contractsBM64BBTO := createBitmapCFOpts()
	topicsBM64Opts, topicsBM64BBTO := createBitmapCFOpts()

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
		CFContractsPL, CFTopicsPL,
		CFContractsBM, CFTopicsBM,
		CFContractsBM64, CFTopicsBM64,
		CFContractsBM32, CFTopicsBM32,
		CFContractsPLV2, CFTopicsPLV2,
	}
	cfOpts := []*grocksdb.Options{
		defaultOpts, eventsOpts, uniqueOpts,
		contractsPLOpts, topicsPLOpts,
		contractsBMOpts, topicsBMOpts,
		contractsBM64Opts, topicsBM64Opts,
		contractsBM32Opts, topicsBM32Opts,
		contractsPLV2Opts, topicsPLV2Opts,
	}
	bbtoList := []*grocksdb.BlockBasedTableOptions{
		eventsBBTO, uniqueBBTO,
		contractsPLBBTO, topicsPLBBTO,
		contractsBMBBTO, topicsBMBBTO,
		contractsBM64BBTO, topicsBM64BBTO,
		contractsBM32BBTO, topicsBM32BBTO,
		contractsPLV2BBTO, topicsPLV2BBTO,
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(baseOpts, dbPath, cfNames, cfOpts)
	if err != nil {
		for _, opt := range cfOpts {
			opt.Destroy()
		}
		for _, bbto := range bbtoList {
			bbto.Destroy()
		}
		baseOpts.Destroy()
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

	// Create index store (RocksDB-backed bitmap indexes - 32-bit ledger level)
	// cfHandles[5] = contracts_bm, cfHandles[6] = topics_bm
	indexStore, err := index.NewRocksDBStore(db, cfHandles[5], cfHandles[6])
	if err != nil {
		db.Close()
		for _, opt := range cfOpts {
			opt.Destroy()
		}
		for _, bbto := range bbtoList {
			bbto.Destroy()
		}
		baseOpts.Destroy()
		wo.Destroy()
		return nil, fmt.Errorf("failed to create index store: %w", err)
	}

	// Create event-level index store (64-bit event level)
	// cfHandles[7] = contracts_bm64, cfHandles[8] = topics_bm64
	eventIndexStore, err := index.NewEventRocksDBStore(db, cfHandles[7], cfHandles[8])
	if err != nil {
		indexStore.Close()
		db.Close()
		for _, opt := range cfOpts {
			opt.Destroy()
		}
		for _, bbto := range bbtoList {
			bbto.Destroy()
		}
		baseOpts.Destroy()
		wo.Destroy()
		return nil, fmt.Errorf("failed to create event index store: %w", err)
	}

	// Create event-level bitmap32 index store (32-bit, FromBuffer decode)
	// cfHandles[9] = contracts_bm32, cfHandles[10] = topics_bm32
	eventIndex32Store, err := index.NewEventRocksDB32Store(db, cfHandles[9], cfHandles[10])
	if err != nil {
		eventIndexStore.Close()
		indexStore.Close()
		db.Close()
		for _, opt := range cfOpts {
			opt.Destroy()
		}
		for _, bbto := range bbtoList {
			bbto.Destroy()
		}
		baseOpts.Destroy()
		wo.Destroy()
		return nil, fmt.Errorf("failed to create event bitmap32 index store: %w", err)
	}

	return &RocksDBEventStore{
		db:               db,
		dbPath:           dbPath,
		wo:               wo,
		ro:               grocksdb.NewDefaultReadOptions(),
		indexes:          indexes,
		eventFormat:      "xdr", // Default to XDR format
		cfHandles:        cfHandles,
		cfDefault:        cfHandles[0],
		cfEvents:         cfHandles[1],
		cfUnique:         cfHandles[2],
		cfContractsPL:    cfHandles[3],
		cfTopicsPL:       cfHandles[4],
		cfContractsBM:    cfHandles[5],
		cfTopicsBM:       cfHandles[6],
		cfContractsBM64:  cfHandles[7],
		cfTopicsBM64:     cfHandles[8],
		cfContractsBM32:  cfHandles[9],
		cfTopicsBM32:     cfHandles[10],
		cfContractsPLV2:  cfHandles[11],
		cfTopicsPLV2:     cfHandles[12],
		indexStore:       indexStore,
		eventIndexStore:  eventIndexStore,
		eventIndex32Store: eventIndex32Store,
		baseOpts:         baseOpts,
		cfOpts:           cfOpts,
		bbtoList:         bbtoList,
		mergeOp:          mergeOp,
		contractPostings:   make(map[string][]byte),
		topicPostings:      make(map[string][]byte),
		contractPostingsV2: make(map[string][]byte),
		topicPostingsV2:    make(map[string][]byte),
	}, nil
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

// Close closes the event store
func (es *RocksDBEventStore) Close() {
	// Close index stores (flushes any remaining hot segments)
	if es.indexStore != nil {
		es.indexStore.Close()
	}
	if es.eventIndexStore != nil {
		es.eventIndexStore.Close()
	}
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

// StoreEvents stores events with optional index updates based on options.
// Returns the number of bytes written.
func (es *RocksDBEventStore) StoreEvents(events []*IngestEvent, opts *StoreOptions) (int64, error) {
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

	// Get bitmap indexes once outside the loop (avoid per-event method call overhead)
	var bitmapIdx *index.BitmapIndex
	var eventBitmapIdx *index.EventBitmapIndex
	var eventBitmap32Idx *index.EventBitmap32Index
	if opts.BitmapIndexes && es.indexStore != nil {
		bitmapIdx = es.indexStore.GetBitmapIndex()
	}
	if opts.Bitmap64Indexes && es.eventIndexStore != nil {
		eventBitmapIdx = es.eventIndexStore.GetEventBitmapIndex()
	}
	if opts.V2Indexes && es.eventIndex32Store != nil {
		eventBitmap32Idx = es.eventIndex32Store.GetEventBitmap32Index()
	}

	// Track event sequence counters per ledger for bitmap32 V2 keys
	var eventSeqCounters map[uint32]uint16
	if opts.V2Indexes {
		eventSeqCounters = make(map[uint32]uint16)
	}

	// Lock posting list maps if we'll be updating them
	if opts.PostingListIndexes || opts.V2Indexes {
		es.postingsMu.Lock()
		defer es.postingsMu.Unlock()
	}

	for _, event := range events {
		// Skip diagnostic events if configured
		if opts.ExcludeDiagnostic && event.EventType == 2 {
			continue
		}

		// Skip events with excluded topic0 values
		if len(opts.ExcludeTopic0) > 0 && len(event.Topics) > 0 {
			topic0Key := string(event.Topics[0])
			if _, excluded := opts.ExcludeTopic0[topic0Key]; excluded {
				continue
			}
		}

		// Use V2 key format when bitmap32 mode is active
		var key []byte
		var eventSeq uint16
		if opts.V2Indexes {
			eventSeq = eventSeqCounters[event.LedgerSequence]
			key = index.EncodeEventKeyV2(event.LedgerSequence, eventSeq)
			eventSeqCounters[event.LedgerSequence] = eventSeq + 1
		} else {
			key = eventKey(event)
		}

		// Encode event based on configured format
		var value []byte
		if es.eventFormat == "binary" {
			value = EncodeBinaryEvent(event)
		} else {
			value = event.RawXDR
		}
		batch.PutCF(es.cfEvents, key, value)
		totalBytes += int64(len(value))

		// Update bitmap indexes (fast, in-memory operation)
		if bitmapIdx != nil {
			// Index contract ID -> ledger (32-bit ledger-level)
			if len(event.ContractID) > 0 {
				bitmapIdx.AddContractIndex(event.ContractID, event.LedgerSequence)
			}

			// Index topics -> ledger (32-bit ledger-level, non-positional)
			for _, topicBytes := range event.Topics {
				bitmapIdx.AddTopicIndex(topicBytes, event.LedgerSequence)
			}
		}

		// Update 64-bit event-level bitmap indexes
		if eventBitmapIdx != nil {
			tx := uint16(event.TransactionIndex)
			op := uint16(event.OperationIndex)
			evt := event.EventIndex

			// Index contract ID -> event key (64-bit)
			if len(event.ContractID) > 0 {
				eventBitmapIdx.AddContractEvent(event.ContractID, event.LedgerSequence, tx, op, evt)
			}

			// Index topics -> event key (64-bit, non-positional)
			for _, topicBytes := range event.Topics {
				eventBitmapIdx.AddTopicEvent(topicBytes, event.LedgerSequence, tx, op, evt)
			}
		}

		// Update 32-bit event-level bitmap indexes (FromBuffer decode)
		if eventBitmap32Idx != nil {
			// Index contract ID -> local ID (32-bit)
			if len(event.ContractID) > 0 {
				eventBitmap32Idx.AddContractEvent(event.ContractID, event.LedgerSequence, eventSeq)
			}

			// Index topics -> local ID (32-bit, non-positional)
			for _, topicBytes := range event.Topics {
				eventBitmap32Idx.AddTopicEvent(topicBytes, event.LedgerSequence, eventSeq)
			}
		}

		// Accumulate posting list TOIDs in memory (flushed periodically)
		if opts.PostingListIndexes {
			// Compute TOID for this event
			toid := index.EncodeTOID(event.LedgerSequence, uint32(event.TransactionIndex), uint32(event.OperationIndex))
			toidBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(toidBytes, toid)

			// Accumulate contract ID -> TOIDs
			if len(event.ContractID) > 0 {
				termKey := index.ContractTermKey(event.ContractID)
				indexKey := string(index.EncodeIndexKey(termKey, event.LedgerSequence))
				es.contractPostings[indexKey] = append(es.contractPostings[indexKey], toidBytes...)
			}

			// Accumulate all topics (non-positional) -> TOIDs
			for _, topicBytes := range event.Topics {
				termKey := index.TopicTermKey(topicBytes)
				indexKey := string(index.EncodeIndexKey(termKey, event.LedgerSequence))
				es.topicPostings[indexKey] = append(es.topicPostings[indexKey], toidBytes...)
			}
		}

		// Accumulate V2 posting list local IDs when bitmap32 mode is active
		if opts.V2Indexes {
			bucketStart := index.BucketID(event.LedgerSequence) * index.BucketSize
			localID := index.EncodeLocalIDForBucket(event.LedgerSequence, eventSeq, bucketStart)
			localIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(localIDBytes, localID)

			if len(event.ContractID) > 0 {
				termKey := index.ContractTermKey(event.ContractID)
				indexKey := string(index.EncodeIndexKey(termKey, event.LedgerSequence))
				es.contractPostingsV2[indexKey] = append(es.contractPostingsV2[indexKey], localIDBytes...)
			}

			for _, topicBytes := range event.Topics {
				termKey := index.TopicTermKey(topicBytes)
				indexKey := string(index.EncodeIndexKey(termKey, event.LedgerSequence))
				es.topicPostingsV2[indexKey] = append(es.topicPostingsV2[indexKey], localIDBytes...)
			}
		}

		// Optionally update unique indexes with counts
		// Uses pre-extracted fields from IngestEvent (no XDR parsing needed!)
		if opts.UniqueIndexes {
			// Count contract ID
			if len(event.ContractID) > 0 {
				uk := string(uniqueKey(UniqueTypeContract, event.ContractID))
				countUpdates[uk]++
			}

			// Count topics (already marshaled as XDR bytes)
			for i, topicBytes := range event.Topics {
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

	// Note: Posting list indexes are accumulated in memory and flushed
	// periodically via FlushPostingListIndexes() for better write efficiency

	if err := es.db.Write(es.wo, batch); err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	return totalBytes, nil
}

// FlushBitmapIndexes flushes all hot bitmap segments to disk (both 32-bit and 64-bit)
func (es *RocksDBEventStore) FlushBitmapIndexes() error {
	// Flush 32-bit ledger-level bitmap
	if es.indexStore != nil {
		if err := es.indexStore.Flush(); err != nil {
			return fmt.Errorf("failed to flush ledger-level bitmap: %w", err)
		}
	}
	// Flush 64-bit event-level bitmap
	if es.eventIndexStore != nil {
		if err := es.eventIndexStore.Flush(); err != nil {
			return fmt.Errorf("failed to flush event-level bitmap: %w", err)
		}
	}
	// Flush 32-bit event-level bitmap (FromBuffer)
	if es.eventIndex32Store != nil {
		if err := es.eventIndex32Store.Flush(); err != nil {
			return fmt.Errorf("failed to flush event-level bitmap32: %w", err)
		}
	}
	return nil
}

// FlushPostingListIndexes flushes accumulated posting list entries to disk.
// Returns the number of contract and topic keys flushed.
func (es *RocksDBEventStore) FlushPostingListIndexes() (int, int, error) {
	es.postingsMu.Lock()
	defer es.postingsMu.Unlock()

	if len(es.contractPostings) == 0 && len(es.topicPostings) == 0 {
		return 0, 0, nil
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	contractKeys := 0
	topicKeys := 0

	// Write all accumulated contract postings (convert raw TOIDs to delta-varint)
	for keyStr, rawToids := range es.contractPostings {
		// Decode raw 8-byte TOIDs and deduplicate
		// (same TOID may appear multiple times if operation emits multiple events)
		toids := index.DecodeTOIDList(rawToids)
		toids = deduplicateTOIDs(toids)
		// Encode as delta-varint
		encoded := index.EncodeTOIDListDeltaVarint(toids)
		batch.MergeCF(es.cfContractsPL, []byte(keyStr), encoded)
		contractKeys++
	}

	// Write all accumulated topic postings (convert raw TOIDs to delta-varint)
	for keyStr, rawToids := range es.topicPostings {
		// Decode raw 8-byte TOIDs and deduplicate
		toids := index.DecodeTOIDList(rawToids)
		toids = deduplicateTOIDs(toids)
		// Encode as delta-varint
		encoded := index.EncodeTOIDListDeltaVarint(toids)
		batch.MergeCF(es.cfTopicsPL, []byte(keyStr), encoded)
		topicKeys++
	}

	if err := es.db.Write(es.wo, batch); err != nil {
		return 0, 0, fmt.Errorf("failed to flush posting list indexes: %w", err)
	}

	// Clear the maps
	es.contractPostings = make(map[string][]byte)
	es.topicPostings = make(map[string][]byte)
	es.postingsLedgerCnt = 0

	return contractKeys, topicKeys, nil
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
		encoded := index.EncodeLocalIDListDeltaVarint(localIDs)
		batch.MergeCF(es.cfContractsPLV2, []byte(keyStr), encoded)
		contractKeys++
	}

	// Write all accumulated topic postings V2 (convert raw local IDs to delta-varint)
	for keyStr, rawLocalIDs := range es.topicPostingsV2 {
		localIDs := decodeRawLocalIDs(rawLocalIDs)
		localIDs = deduplicateLocalIDs(localIDs)
		encoded := index.EncodeLocalIDListDeltaVarint(localIDs)
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

// decodeRawLocalIDs decodes raw 4-byte big-endian local IDs to a uint32 slice.
func decodeRawLocalIDs(data []byte) []uint32 {
	count := len(data) / 4
	ids := make([]uint32, count)
	for i := 0; i < count; i++ {
		ids[i] = binary.BigEndian.Uint32(data[i*4:])
	}
	return ids
}

// deduplicateLocalIDs removes consecutive duplicate local IDs from a sorted slice.
func deduplicateLocalIDs(ids []uint32) []uint32 {
	if len(ids) <= 1 {
		return ids
	}
	writeIdx := 1
	for readIdx := 1; readIdx < len(ids); readIdx++ {
		if ids[readIdx] != ids[readIdx-1] {
			ids[writeIdx] = ids[readIdx]
			writeIdx++
		}
	}
	return ids[:writeIdx]
}

// deduplicateTOIDs removes consecutive duplicate TOIDs from a sorted slice.
// This is needed because multiple events from the same operation will have the same TOID.
func deduplicateTOIDs(toids []uint64) []uint64 {
	if len(toids) <= 1 {
		return toids
	}
	// In-place deduplication since slice is already sorted
	writeIdx := 1
	for readIdx := 1; readIdx < len(toids); readIdx++ {
		if toids[readIdx] != toids[readIdx-1] {
			toids[writeIdx] = toids[readIdx]
			writeIdx++
		}
	}
	return toids[:writeIdx]
}

// GetPostingListStats returns statistics about in-memory posting list accumulation.
func (es *RocksDBEventStore) GetPostingListStats() (contractKeys, topicKeys int, contractBytes, topicBytes int64) {
	es.postingsMu.Lock()
	defer es.postingsMu.Unlock()

	contractKeys = len(es.contractPostings)
	topicKeys = len(es.topicPostings)

	for _, v := range es.contractPostings {
		contractBytes += int64(len(v))
	}
	for _, v := range es.topicPostings {
		topicBytes += int64(len(v))
	}

	return
}

// GetBitmapStats returns bitmap index statistics
func (es *RocksDBEventStore) GetBitmapStats() *BitmapStats {
	if es.indexStore == nil {
		return nil
	}
	internalStats := es.indexStore.GetStats()
	if internalStats == nil {
		return nil
	}
	return &BitmapStats{
		CurrentSegmentID:   internalStats.CurrentSegmentID,
		HotSegmentCount:    internalStats.HotSegmentCount,
		HotSegmentCards:    internalStats.HotSegmentCards,
		HotSegmentMemBytes: internalStats.HotSegmentMemBytes,
		ContractIndexCount: internalStats.ContractIndexCount,
		TopicIndexCount:    internalStats.TopicIndexCount,
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

// =============================================================================
// Query by Ledger (Primary Index)
// =============================================================================

// GetEventsByLedgerRange retrieves all events within a ledger range
func (es *RocksDBEventStore) GetEventsByLedgerRange(startLedger, endLedger uint32) ([]*ContractEvent, error) {
	var events []*ContractEvent

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		// Compare ledger (bytes 0-3)
		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger > endLedger {
			break
		}

		// Parse key components
		ledger, tx, op, eventIdx := parseEventKey(key)

		// Parse raw XDR value
		event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsByLedger retrieves all events for a specific ledger
func (es *RocksDBEventStore) GetEventsByLedger(ledgerSequence uint32) ([]*ContractEvent, error) {
	return es.GetEventsByLedgerRange(ledgerSequence, ledgerSequence)
}

// GetEventsInRangeWithTiming retrieves events in a ledger range with detailed timing.
// Returns query.Event format with disk read and unmarshal timing.
func (es *RocksDBEventStore) GetEventsInRangeWithTiming(startLedger, endLedger uint32, limit int) (*query.RangeResult, error) {
	result := &query.RangeResult{
		Timing: query.FetchTiming{},
	}

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)

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

		if len(key) < 10 {
			break
		}

		// Compare ledger (bytes 0-3)
		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger > endLedger {
			break
		}

		_, tx, op, eventIdx := parseEventKey(key)

		// Time value access (disk read)
		diskStart = time.Now()
		valueData := it.Value().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)
		result.Timing.BytesRead += int64(len(valueData))

		// Time unmarshalling/decoding
		unmarshalStart := time.Now()
		var event *query.Event
		var err error
		if es.eventFormat == "binary" {
			event, err = DecodeBinaryToQueryEvent(valueData, keyLedger, tx, op, eventIdx)
		} else {
			event, err = parseRawXDRToQueryEvent(valueData, keyLedger, tx, op, eventIdx)
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
		result.Events = append(result.Events, event)

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

// =============================================================================
// Query by Contract ID
// =============================================================================

// GetEventsByContractID retrieves events for a specific contract (scans all events)
func (es *RocksDBEventStore) GetEventsByContractID(contractID []byte, limit int) ([]*ContractEvent, error) {
	var events []*ContractEvent

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if limit > 0 && len(events) >= limit {
			break
		}

		key := it.Key().Data()
		if len(key) < 10 {
			continue
		}

		ledger, tx, op, eventIdx := parseEventKey(key)

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.ContractId != nil && string(xdrEvent.ContractId[:]) == string(contractID) {
			event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
			if err != nil {
				continue
			}
			events = append(events, event)
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsByContractIDInRange retrieves events for a contract within a ledger range
func (es *RocksDBEventStore) GetEventsByContractIDInRange(contractID []byte, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	var events []*ContractEvent

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		ledger, tx, op, eventIdx := parseEventKey(key)
		if ledger > endLedger {
			break
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.ContractId != nil && string(xdrEvent.ContractId[:]) == string(contractID) {
			event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
			if err != nil {
				continue
			}
			events = append(events, event)
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// =============================================================================
// Query by Topic
// =============================================================================

// GetEventsByTopic retrieves events with a specific topic value at the given position
func (es *RocksDBEventStore) GetEventsByTopic(position int, topicValue []byte, limit int) ([]*ContractEvent, error) {
	if position < 0 || position > 3 {
		return nil, fmt.Errorf("topic position must be 0-3, got %d", position)
	}

	var events []*ContractEvent

	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if limit > 0 && len(events) >= limit {
			break
		}

		key := it.Key().Data()
		if len(key) < 10 {
			continue
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			if position < len(body.Topics) {
				topicXDR, _ := body.Topics[position].MarshalBinary()
				if string(topicXDR) == string(topicValue) {
					ledger, tx, op, eventIdx := parseEventKey(key)
					event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
					if err != nil {
						continue
					}
					events = append(events, event)
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// GetEventsByTopicInRange retrieves events with a specific topic within a ledger range
func (es *RocksDBEventStore) GetEventsByTopicInRange(position int, topicValue []byte, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	if position < 0 || position > 3 {
		return nil, fmt.Errorf("topic position must be 0-3, got %d", position)
	}

	var events []*ContractEvent

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		ledger := binary.BigEndian.Uint32(key[0:4])
		if ledger > endLedger {
			break
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			if position < len(body.Topics) {
				topicXDR, _ := body.Topics[position].MarshalBinary()
				if string(topicXDR) == string(topicValue) {
					_, tx, op, eventIdx := parseEventKey(key)
					event, err := parseRawXDRToEvent(it.Value().Data(), ledger, tx, op, eventIdx)
					if err != nil {
						continue
					}
					events = append(events, event)
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
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
		CFContractsPL, CFTopicsPL,
		CFContractsBM, CFTopicsBM,
		CFContractsBM64, CFTopicsBM64,
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

// topNHeap is a min-heap for tracking top N entries by count
type topNHeap struct {
	entries   []TopEntry
	maxSize   int
	indexType byte // Used to determine encoding format (strkey for contracts)
}

func (h *topNHeap) Len() int           { return len(h.entries) }
func (h *topNHeap) Less(i, j int) bool { return h.entries[i].EventCount < h.entries[j].EventCount }
func (h *topNHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

func (h *topNHeap) Push(x interface{}) {
	h.entries = append(h.entries, x.(TopEntry))
}

func (h *topNHeap) Pop() interface{} {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[0 : n-1]
	return x
}

// tryAdd adds an entry if it belongs in top N (min-heap, so smallest is at top)
func (h *topNHeap) tryAdd(value []byte, count int64) {
	if h.maxSize <= 0 {
		return
	}

	// Encode value based on index type
	var encoded string
	if h.indexType == UniqueTypeContract && len(value) == 32 {
		// Contract IDs: encode as strkey (C...)
		if s, err := strkey.Encode(strkey.VersionByteContract, value); err == nil {
			encoded = s
		} else {
			encoded = base64.StdEncoding.EncodeToString(value)
		}
	} else {
		// Topics and other values: use base64
		encoded = base64.StdEncoding.EncodeToString(value)
	}

	if len(h.entries) < h.maxSize {
		heap.Push(h, TopEntry{
			Value:      encoded,
			EventCount: count,
		})
	} else if count > h.entries[0].EventCount {
		// Replace smallest if this is larger
		h.entries[0] = TopEntry{
			Value:      encoded,
			EventCount: count,
		}
		heap.Fix(h, 0)
	}
}

// getSorted returns entries sorted by count descending
func (h *topNHeap) getSorted() []TopEntry {
	if len(h.entries) == 0 {
		return nil
	}
	// Sort descending by count
	result := make([]TopEntry, len(h.entries))
	copy(result, h.entries)
	sort.Slice(result, func(i, j int) bool {
		return result[i].EventCount > result[j].EventCount
	})
	return result
}

// GetIndexDistribution computes percentile statistics for each index type in parallel
// topN specifies how many top entries to include (0 for none)
func (es *RocksDBEventStore) GetIndexDistribution(topN int) (*IndexDistribution, error) {
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
			stats, err := es.computeDistributionForType(idxType, topN)
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
func (es *RocksDBEventStore) computeDistributionForType(indexType byte, topN int) (*DistributionStats, error) {
	const partitions = 16

	type partitionResult struct {
		counts      []int64
		total       int64
		over32Bytes int64
		topN        []TopEntry
		err         error
	}

	results := make(chan partitionResult, partitions)
	var wg sync.WaitGroup

	// Scan each partition in parallel
	for p := 0; p < partitions; p++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			counts, total, over32, top, err := es.scanDistributionPartition(indexType, partition, partitions, topN)
			results <- partitionResult{counts, total, over32, top, err}
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
		Over32Bytes: over32Sum,
	}, nil
}

// scanDistributionPartition scans a partition and returns counts for distribution
// Returns: counts, total, over32Bytes, topN entries, error
func (es *RocksDBEventStore) scanDistributionPartition(indexType byte, partition, totalPartitions, topN int) ([]int64, int64, int64, []TopEntry, error) {
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
	}

	if err := it.Err(); err != nil {
		return nil, 0, 0, nil, fmt.Errorf("iterator error for type %d partition %d: %w", indexType, partition, err)
	}

	return counts, total, over32Bytes, topHeap.getSorted(), nil
}

// percentile calculates the p-th percentile from a sorted slice
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := int(float64(len(sorted)-1) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
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

// GetEventsInLedger retrieves all events in a specific ledger as query.Event format.
// Implements the EventReader interface.
func (es *RocksDBEventStore) GetEventsInLedger(ledger uint32) ([]*query.Event, error) {
	var events []*query.Event

	startKey := eventKeyFromParts(ledger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger != ledger {
			break
		}

		_, tx, op, eventIdx := parseEventKey(key)
		valueData := it.Value().Data()

		var event *query.Event
		var err error
		if es.eventFormat == "binary" {
			event, err = DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
		} else {
			event, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
		}
		if err != nil {
			continue
		}
		events = append(events, event)
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

	startKey := eventKeyFromParts(ledger, 0, 0, 0)

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

		if len(key) < 10 {
			break
		}

		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger != ledger {
			break
		}

		_, tx, op, eventIdx := parseEventKey(key)

		// Time value access (disk read)
		diskStart = time.Now()
		valueData := it.Value().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)
		result.Timing.BytesRead += int64(len(valueData))

		// Time unmarshalling/decoding
		unmarshalStart := time.Now()
		var event *query.Event
		var err error
		if es.eventFormat == "binary" {
			event, err = DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
		} else {
			event, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
		}
		result.Timing.UnmarshalTime += time.Since(unmarshalStart)

		if err != nil {
			// Time next iteration
			diskStart = time.Now()
			it.Next()
			result.Timing.DiskReadTime += time.Since(diskStart)
			continue
		}
		result.Events = append(result.Events, event)

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

// GetEventsInLedgerWithFilter retrieves matching events from a ledger with early filtering.
// For binary format, this filters at the header level before full decode, avoiding
// expensive base64 encoding and object creation for non-matching events.
func (es *RocksDBEventStore) GetEventsInLedgerWithFilter(ledger uint32, contractID []byte, topics [][]byte, limit int) (*query.FilteredFetchResult, error) {
	result := &query.FilteredFetchResult{
		Timing: query.FilteredFetchTiming{},
	}

	startKey := eventKeyFromParts(ledger, 0, 0, 0)

	// Time iterator creation and seek
	diskStart := time.Now()
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()
	it.Seek(startKey)
	result.Timing.DiskReadTime += time.Since(diskStart)

	for it.Valid() {
		// Check limit on returned events
		if limit > 0 && len(result.Events) >= limit {
			break
		}

		// Time key access (disk read)
		diskStart = time.Now()
		key := it.Key().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)

		if len(key) < 10 {
			break
		}

		keyLedger := binary.BigEndian.Uint32(key[0:4])
		if keyLedger != ledger {
			break
		}

		_, tx, op, eventIdx := parseEventKey(key)

		// Time value access (disk read)
		diskStart = time.Now()
		valueData := it.Value().Data()
		result.Timing.DiskReadTime += time.Since(diskStart)
		result.Timing.BytesRead += int64(len(valueData))

		result.EventsScanned++

		// Filter and decode based on format
		if es.eventFormat == "binary" {
			// Binary format: filter at header level BEFORE full decode
			filterStart := time.Now()
			header := ParseBinaryHeader(valueData)
			if header == nil {
				result.Timing.FilterTime += time.Since(filterStart)
				diskStart = time.Now()
				it.Next()
				result.Timing.DiskReadTime += time.Since(diskStart)
				continue
			}

			// Fast filter check using raw bytes - no allocations
			matches := true
			if len(contractID) > 0 && !header.MatchesContractID(contractID) {
				matches = false
			}
			if matches && !header.MatchesTopics(topics) {
				matches = false
			}
			result.Timing.FilterTime += time.Since(filterStart)

			if !matches {
				diskStart = time.Now()
				it.Next()
				result.Timing.DiskReadTime += time.Since(diskStart)
				continue
			}

			// Only decode matching events
			decodeStart := time.Now()
			event, err := DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
			result.Timing.DecodeTime += time.Since(decodeStart)

			if err == nil {
				result.Events = append(result.Events, event)
			}
		} else {
			// XDR format: must unmarshal to filter
			decodeStart := time.Now()
			event, err := parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
			result.Timing.DecodeTime += time.Since(decodeStart)

			if err != nil {
				diskStart = time.Now()
				it.Next()
				result.Timing.DiskReadTime += time.Since(diskStart)
				continue
			}

			// Filter after decode for XDR
			filterStart := time.Now()
			if matchesXDRFilter(event, contractID, topics) {
				result.Events = append(result.Events, event)
			}
			result.Timing.FilterTime += time.Since(filterStart)
		}

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

// matchesXDRFilter checks if a decoded event matches the given filters.
// Note: event.ContractID is in strkey format (C...), contractID filter is raw bytes.
func matchesXDRFilter(event *query.Event, contractID []byte, topics [][]byte) bool {
	// Check contract ID - event stores strkey format, filter is raw bytes
	if len(contractID) > 0 {
		if event.ContractID == "" {
			return false
		}
		// Convert filter bytes to strkey format for comparison
		filterStrkey, err := strkey.Encode(strkey.VersionByteContract, contractID)
		if err != nil || event.ContractID != filterStrkey {
			return false
		}
	}

	// Check topics (non-positional: all filter topics must be present in event)
	for _, topicFilter := range topics {
		if len(topicFilter) == 0 {
			continue
		}
		filterBase64 := base64.StdEncoding.EncodeToString(topicFilter)
		found := false
		for _, eventTopic := range event.Topics {
			if eventTopic == filterBase64 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// parseRawXDRToQueryEvent converts raw XDR bytes to a query.Event
func parseRawXDRToQueryEvent(rawXDR []byte, ledger uint32, tx, op uint32, eventIdx uint16) (*query.Event, error) {
	var xdrEvent xdr.ContractEvent
	if err := xdrEvent.UnmarshalBinary(rawXDR); err != nil {
		return nil, fmt.Errorf("failed to unmarshal XDR event: %w", err)
	}

	event := &query.Event{
		LedgerSequence:   ledger,
		TransactionIndex: int(tx),
		OperationIndex:   int(op),
		EventIndex:       int(eventIdx),
	}

	// Extract contract ID if present - encode as strkey (C...)
	if xdrEvent.ContractId != nil {
		if encoded, err := strkey.Encode(strkey.VersionByteContract, xdrEvent.ContractId[:]); err == nil {
			event.ContractID = encoded
		}
	}

	// Event type
	switch xdrEvent.Type {
	case xdr.ContractEventTypeContract:
		event.Type = "contract"
	case xdr.ContractEventTypeSystem:
		event.Type = "system"
	case xdr.ContractEventTypeDiagnostic:
		event.Type = "diagnostic"
	}

	// Extract topics and data from event body
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()

		// Topics
		for _, topic := range body.Topics {
			topicBytes, _ := topic.MarshalBinary()
			event.Topics = append(event.Topics, base64.StdEncoding.EncodeToString(topicBytes))
		}

		// Data
		dataBytes, _ := body.Data.MarshalBinary()
		event.Data = base64.StdEncoding.EncodeToString(dataBytes)
	}

	return event, nil
}

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

	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
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

// BuildIndexes scans all events and builds indexes based on options (one-time operation)
// Bitmap indexes are always built. Unique indexes are optional.
// Uses a collector pattern: workers read/extract in parallel, single goroutine updates bitmaps.
func (es *RocksDBEventStore) BuildIndexes(workers int, opts *BuildIndexOptions, progressFn func(processed int64)) error {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Default options: L1 bitmap only
	if opts == nil {
		opts = &BuildIndexOptions{
			BitmapIndexes: true,
		}
	}

	// Ensure index store is initialized if we're building bitmap indexes
	if opts.BitmapIndexes && es.indexStore == nil {
		return fmt.Errorf("index store not initialized - cannot build bitmap indexes")
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

	// Create channel for index entries (collector pattern for bitmap indexes)
	var entryCh chan *indexEntry
	var collectorDone chan error

	if opts.BitmapIndexes {
		entryCh = make(chan *indexEntry, 100000) // buffered channel
		collectorDone = make(chan error, 1)

		// Start collector goroutine - single goroutine updates bitmaps (no lock contention)
		go func() {
			collectorDone <- es.indexCollector(entryCh, opts, progressFn)
		}()
	}

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
	if entryCh != nil {
		close(entryCh)
		if err := <-collectorDone; err != nil {
			return fmt.Errorf("index collector failed: %w", err)
		}
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
	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	// Accumulate unique counts in memory (only if building unique indexes)
	var counts map[string]uint64
	if opts.UniqueIndexes {
		counts = make(map[string]uint64)
	}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 10 {
			break
		}

		// Parse key: [ledger:4][tx:2][op:2][event:2]
		ledger := binary.BigEndian.Uint32(key[0:4])
		if ledger > endLedger {
			break
		}

		txIdx := binary.BigEndian.Uint16(key[4:6])
		opIdx := binary.BigEndian.Uint16(key[6:8])
		eventIdx := binary.BigEndian.Uint16(key[8:10])

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

// indexCollector receives index entries from workers and updates bitmaps sequentially.
// This eliminates lock contention - only one goroutine touches the bitmap index.
func (es *RocksDBEventStore) indexCollector(entryCh <-chan *indexEntry, opts *BuildIndexOptions, progressFn func(processed int64)) error {
	var processed int64
	var lastFlushLedger uint32
	var prevLedger uint32

	bitmapIdx := es.indexStore.GetBitmapIndex()

	for entry := range entryCh {
		// Periodic flush on ledger boundary
		if opts.IndexFlushInterval > 0 && entry.Ledger != prevLedger && prevLedger > 0 {
			if (entry.Ledger - lastFlushLedger) >= uint32(opts.IndexFlushInterval) {
				if err := es.indexStore.Flush(); err != nil {
					return fmt.Errorf("failed to flush bitmap indexes: %w", err)
				}
				lastFlushLedger = entry.Ledger
			}
		}
		prevLedger = entry.Ledger

		// Bitmap indexes (contract/topic -> ledger, non-positional)
		if bitmapIdx != nil {
			if len(entry.ContractID) > 0 {
				bitmapIdx.AddContractIndex(entry.ContractID, entry.Ledger)
			}
			for _, topic := range entry.Topics {
				bitmapIdx.AddTopicIndex(topic, entry.Ledger)
			}
		}

		processed++

		// Progress callback every 100k events
		if progressFn != nil && processed%100000 == 0 {
			progressFn(processed)
		}
	}

	// Final flush
	if es.indexStore != nil {
		if err := es.indexStore.Flush(); err != nil {
			return fmt.Errorf("failed to flush bitmap indexes: %w", err)
		}
	}

	// Final progress
	if progressFn != nil {
		progressFn(processed)
	}

	return nil
}

// =============================================================================
// Posting List Index Queries
// =============================================================================

// QueryPostingListByContract queries the contract posting list index for TOIDs.
// Returns TOIDs matching the given contract ID within the ledger range.
func (es *RocksDBEventStore) QueryPostingListByContract(contractID []byte, startLedger, endLedger uint32) ([]uint64, error) {
	if es.cfContractsPL == nil {
		return nil, fmt.Errorf("contracts index not available")
	}

	termKey := index.ContractTermKey(contractID)
	return es.queryPostingList(es.cfContractsPL, termKey, startLedger, endLedger)
}

// QueryPostingListByTopic queries the topic posting list index for TOIDs.
// Returns TOIDs matching the given topic XDR within the ledger range.
// Topics are non-positional - any topic matching the value will be returned.
func (es *RocksDBEventStore) QueryPostingListByTopic(topicXDR []byte, startLedger, endLedger uint32) ([]uint64, error) {
	if es.cfTopicsPL == nil {
		return nil, fmt.Errorf("topics index not available")
	}

	termKey := index.TopicTermKey(topicXDR)
	return es.queryPostingList(es.cfTopicsPL, termKey, startLedger, endLedger)
}

// queryPostingList reads posting lists from a column family across bucket range.
func (es *RocksDBEventStore) queryPostingList(cf *grocksdb.ColumnFamilyHandle, termKey [32]byte, startLedger, endLedger uint32) ([]uint64, error) {
	buckets := index.GetBucketsForRange(startLedger, endLedger)

	var allTOIDs []uint64

	for _, bucketID := range buckets {
		indexKey := index.EncodeIndexKeyWithBucket(termKey, bucketID)

		value, err := es.db.GetCF(es.ro, cf, indexKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read posting list: %w", err)
		}

		if value.Exists() {
			toids := index.DecodeTOIDListDeltaVarint(value.Data())
			// Filter TOIDs to the actual ledger range
			filtered := index.FilterTOIDsByLedgerRange(toids, startLedger, endLedger)
			allTOIDs = append(allTOIDs, filtered...)
		}
		value.Free()
	}

	return allTOIDs, nil
}

// QueryEventsWithPostingList queries events using posting list indexes.
// contractID and topics are ANDed together.
// Returns events matching all filters within the ledger range.
func (es *RocksDBEventStore) QueryEventsWithPostingList(contractID []byte, topics [][]byte, startLedger, endLedger uint32) ([]*query.Event, error) {
	var resultTOIDs []uint64
	var initialized bool

	// Query contract ID posting list if specified
	if len(contractID) > 0 {
		toids, err := es.QueryPostingListByContract(contractID, startLedger, endLedger)
		if err != nil {
			return nil, err
		}
		resultTOIDs = toids
		initialized = true
	}

	// Query topic posting lists and intersect
	for _, topicXDR := range topics {
		if len(topicXDR) == 0 {
			continue
		}

		toids, err := es.QueryPostingListByTopic(topicXDR, startLedger, endLedger)
		if err != nil {
			return nil, err
		}

		if !initialized {
			resultTOIDs = toids
			initialized = true
		} else {
			// Intersect with previous results
			resultTOIDs = index.IntersectTOIDLists(resultTOIDs, toids)
		}

		// Early exit if intersection is empty
		if len(resultTOIDs) == 0 {
			return nil, nil
		}
	}

	// Fetch events by TOID
	return es.fetchEventsByTOIDs(resultTOIDs)
}

// QueryEventsWithPostingListTiming queries events with detailed timing and stats.
// Uses streaming approach: reads posting lists bucket-by-bucket and stops early when limit is reached.
func (es *RocksDBEventStore) QueryEventsWithPostingListTiming(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) (*PostingListQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &PostingListQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	// For single filter queries (contract OR topic, not both), use streaming approach
	// For multi-filter queries, we need all TOIDs to intersect
	hasContract := len(contractID) > 0
	hasTopics := len(topics) > 0
	multiFilter := hasContract && hasTopics

	if !multiFilter && limit > 0 {
		// Single filter with limit - use streaming approach
		return es.queryPostingListStreaming(contractID, topics, startLedger, endLedger, limit, result, totalStart)
	}

	// Multi-filter or no limit
	var resultTOIDs []uint64

	plStart := time.Now()

	if es.ParallelPostingReads {
		// Optimized path: parallel reads → sort by size → intersect with early exit
		plResults, parallelTime := es.queryPostingListsParallel(contractID, topics, startLedger, endLedger)
		result.ParallelReadTime = parallelTime

		// Check for errors and aggregate stats
		for _, plr := range plResults {
			if plr.err != nil {
				return nil, nil, plr.err
			}
			result.BucketsScanned += plr.buckets
			result.PostingListsRead++
			result.PostingListBytes += plr.bytesRead
			result.PostingListReadTime += plr.readTime
			result.PostingListDecodeTime += plr.decodeTime
			if plr.isContract {
				result.TOIDsFromContract = len(plr.toids)
			} else {
				result.TOIDsFromTopics += len(plr.toids)
			}
		}

		// Sort by len(toids) ascending (smallest first for efficient intersection)
		sort.Slice(plResults, func(a, b int) bool {
			return len(plResults[a].toids) < len(plResults[b].toids)
		})

		// Record size metrics
		if len(plResults) > 0 {
			result.SmallestListSize = len(plResults[0].toids)
			result.LargestListSize = len(plResults[len(plResults)-1].toids)
		}

		// Intersect progressively with early exit
		for i, plr := range plResults {
			if i == 0 {
				resultTOIDs = plr.toids
			} else {
				intersectStart := time.Now()
				resultTOIDs = index.IntersectTOIDLists(resultTOIDs, plr.toids)
				result.IntersectTime += time.Since(intersectStart)
			}
			if len(resultTOIDs) == 0 {
				result.PostingListTime = time.Since(plStart) - result.IntersectTime
				result.TotalTime = time.Since(totalStart)
				return result, nil, nil
			}
		}
	} else {
		// Sequential path (original behavior)
		var initialized bool

		if hasContract {
			toids, buckets, bytesRead, readTime, decodeTime, err := es.queryPostingListWithStats(es.cfContractsPL, index.ContractTermKey(contractID), startLedger, endLedger)
			if err != nil {
				return nil, nil, err
			}
			result.BucketsScanned += buckets
			result.PostingListsRead++
			result.PostingListBytes += bytesRead
			result.PostingListReadTime += readTime
			result.PostingListDecodeTime += decodeTime
			result.TOIDsFromContract = len(toids)
			resultTOIDs = toids
			initialized = true
		}

		for _, topicXDR := range topics {
			if len(topicXDR) == 0 {
				continue
			}

			toids, buckets, bytesRead, readTime, decodeTime, err := es.queryPostingListWithStats(es.cfTopicsPL, index.TopicTermKey(topicXDR), startLedger, endLedger)
			if err != nil {
				return nil, nil, err
			}
			result.BucketsScanned += buckets
			result.PostingListsRead++
			result.PostingListBytes += bytesRead
			result.PostingListReadTime += readTime
			result.PostingListDecodeTime += decodeTime
			result.TOIDsFromTopics += len(toids)

			if !initialized {
				resultTOIDs = toids
				initialized = true
			} else {
				intersectStart := time.Now()
				resultTOIDs = index.IntersectTOIDLists(resultTOIDs, toids)
				result.IntersectTime += time.Since(intersectStart)
			}

			if len(resultTOIDs) == 0 {
				result.PostingListTime = time.Since(plStart)
				result.TotalTime = time.Since(totalStart)
				return result, nil, nil
			}
		}
	}

	result.PostingListTime = time.Since(plStart) - result.IntersectTime
	result.TOIDsAfterIntersect = len(resultTOIDs)

	// Count unique ledgers
	ledgerSet := make(map[uint32]struct{})
	for _, toid := range resultTOIDs {
		ledger, _, _ := index.DecodeTOID(toid)
		ledgerSet[ledger] = struct{}{}
	}
	result.UniqueLedgers = len(ledgerSet)

	// Fetch events with filtering (non-positional topic matching for posting list queries)
	fetchStart := time.Now()
	events, bytesRead, scanned, decodeTime, filterTime, err := es.fetchEventsByTOIDsWithStats(resultTOIDs, limit, contractID, topics, true)
	if err != nil {
		return nil, nil, err
	}

	result.EventFetchTime = time.Since(fetchStart) - decodeTime - filterTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventBytesRead = bytesRead
	result.EventsScanned = scanned
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// queryPostingListStreaming reads posting lists bucket-by-bucket and fetches events incrementally.
// Stops early when limit is reached, avoiding reading unnecessary posting list data.
func (es *RocksDBEventStore) queryPostingListStreaming(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int, result *PostingListQueryResult, totalStart time.Time) (*PostingListQueryResult, []*query.Event, error) {
	buckets := index.GetBucketsForRange(startLedger, endLedger)

	// Determine which CF and term key to use
	var cf *grocksdb.ColumnFamilyHandle
	var termKey [32]byte

	if len(contractID) > 0 {
		cf = es.cfContractsPL
		termKey = index.ContractTermKey(contractID)
	} else if len(topics) > 0 && len(topics[0]) > 0 {
		cf = es.cfTopicsPL
		termKey = index.TopicTermKey(topics[0])
	} else {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	var allEvents []*query.Event
	ledgerSet := make(map[uint32]struct{})
	var plTime, fetchTime, decodeTime, filterTime time.Duration
	var plReadTime, plDecodeTime time.Duration

	// Precompute TOID boundaries for ledger range filtering
	startTOID := index.EncodeTOID(startLedger, 0, 0)
	endTOID := index.EncodeTOID(endLedger, 0xFFFFF, 0xFFF) // max tx and op for end ledger

	for _, bucketID := range buckets {
		if len(allEvents) >= limit {
			break
		}

		// Read this bucket's posting list
		plStart := time.Now()
		indexKey := index.EncodeIndexKeyWithBucket(termKey, bucketID)

		t0 := time.Now()
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		plReadTime += time.Since(t0)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read posting list: %w", err)
		}

		if !value.Exists() {
			value.Free()
			result.BucketsScanned++
			continue
		}

		data := value.Data()
		result.PostingListBytes += int64(len(data))
		result.BucketsScanned++
		result.PostingListsRead++

		// Copy data since we need to free value but iterator needs the bytes
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		value.Free()

		// Use incremental iterator for early termination
		t1 := time.Now()
		iter := index.NewDeltaVarintIterator(dataCopy)
		result.TOIDsInPostingList += int(iter.Count())
		remaining := limit - len(allEvents)

		// Collect filtered TOIDs with early termination
		// Buffer: collect up to 3x remaining to account for multi-event TOIDs
		maxToCollect := remaining * 3
		if maxToCollect < 1000 {
			maxToCollect = 1000 // minimum batch size for efficiency
		}

		var filtered []uint64
		toidsDecoded := 0

		for {
			toid, ok := iter.Next()
			if !ok {
				break
			}
			toidsDecoded++

			// Skip if before start ledger (TOIDs are sorted, so we can continue)
			if toid < startTOID {
				continue
			}
			// Stop if past end ledger (TOIDs are sorted, no more matches possible)
			if toid > endTOID {
				break
			}

			filtered = append(filtered, toid)

			// Early termination: stop when we have enough TOIDs
			if len(filtered) >= maxToCollect {
				break
			}
		}
		plDecodeTime += time.Since(t1)

		result.TOIDsDecoded += toidsDecoded
		if len(contractID) > 0 {
			result.TOIDsFromContract += len(filtered)
		} else {
			result.TOIDsFromTopics += len(filtered)
		}
		result.TOIDsAfterIntersect += len(filtered)
		plTime += time.Since(plStart)

		// Count unique ledgers
		for _, toid := range filtered {
			ledger, _, _ := index.DecodeTOID(toid)
			ledgerSet[ledger] = struct{}{}
		}

		// Fetch events for this bucket's TOIDs with filtering (non-positional topic matching)
		fetchStart := time.Now()
		events, bytesRead, scanned, decTime, filtTime, err := es.fetchEventsByTOIDsWithStats(filtered, remaining, contractID, topics, true)
		if err != nil {
			return nil, nil, err
		}

		fetchTime += time.Since(fetchStart) - decTime - filtTime
		decodeTime += decTime
		filterTime += filtTime
		result.EventBytesRead += bytesRead
		result.EventsScanned += scanned

		allEvents = append(allEvents, events...)
	}

	result.PostingListTime = plTime
	result.PostingListReadTime = plReadTime
	result.PostingListDecodeTime = plDecodeTime
	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.UniqueLedgers = len(ledgerSet)
	result.EventsReturned = len(allEvents)
	result.TotalTime = time.Since(totalStart)

	return result, allEvents, nil
}

// queryPostingListWithStats reads posting lists and returns stats.
func (es *RocksDBEventStore) queryPostingListWithStats(cf *grocksdb.ColumnFamilyHandle, termKey [32]byte, startLedger, endLedger uint32) ([]uint64, int, int64, time.Duration, time.Duration, error) {
	buckets := index.GetBucketsForRange(startLedger, endLedger)

	var allTOIDs []uint64
	var bytesRead int64
	var readTime, decodeTime time.Duration

	for _, bucketID := range buckets {
		indexKey := index.EncodeIndexKeyWithBucket(termKey, bucketID)

		t0 := time.Now()
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		readTime += time.Since(t0)
		if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("failed to read posting list: %w", err)
		}

		if value.Exists() {
			data := value.Data()
			bytesRead += int64(len(data))
			t1 := time.Now()
			toids := index.DecodeTOIDListDeltaVarint(data)
			decodeTime += time.Since(t1)
			filtered := index.FilterTOIDsByLedgerRange(toids, startLedger, endLedger)
			allTOIDs = append(allTOIDs, filtered...)
		}
		value.Free()
	}

	return allTOIDs, len(buckets), bytesRead, readTime, decodeTime, nil
}

// =============================================================================
// Multi-Filter Optimization: Phase 1 - Parallel Posting List Reads
// =============================================================================

// postingListResult holds the result of reading a single posting list.
type postingListResult struct {
	toids      []uint64
	buckets    int
	bytesRead  int64
	readTime   time.Duration
	decodeTime time.Duration
	err        error
	isContract bool // true for contract, false for topic
}

// queryPostingListsParallel reads all posting lists (contract + topics) in parallel.
// Returns results in order: contract first (if present), then topics.
func (es *RocksDBEventStore) queryPostingListsParallel(
	contractID []byte,
	topics [][]byte,
	startLedger, endLedger uint32,
) ([]postingListResult, time.Duration) {
	start := time.Now()

	// Count total lists to read
	numLists := 0
	if len(contractID) > 0 {
		numLists++
	}
	for _, t := range topics {
		if len(t) > 0 {
			numLists++
		}
	}

	if numLists == 0 {
		return nil, 0
	}

	results := make([]postingListResult, numLists)
	var wg sync.WaitGroup

	idx := 0

	// Launch contract query
	if len(contractID) > 0 {
		wg.Add(1)
		go func(resultIdx int, cid []byte) {
			defer wg.Done()
			toids, buckets, bytesRead, readTime, decodeTime, err := es.queryPostingListWithStats(
				es.cfContractsPL,
				index.ContractTermKey(cid),
				startLedger, endLedger,
			)
			results[resultIdx] = postingListResult{
				toids:      toids,
				buckets:    buckets,
				bytesRead:  bytesRead,
				readTime:   readTime,
				decodeTime: decodeTime,
				err:        err,
				isContract: true,
			}
		}(idx, contractID)
		idx++
	}

	// Launch topic queries
	for _, topicXDR := range topics {
		if len(topicXDR) == 0 {
			continue
		}
		wg.Add(1)
		go func(resultIdx int, topic []byte) {
			defer wg.Done()
			toids, buckets, bytesRead, readTime, decodeTime, err := es.queryPostingListWithStats(
				es.cfTopicsPL,
				index.TopicTermKey(topic),
				startLedger, endLedger,
			)
			results[resultIdx] = postingListResult{
				toids:      toids,
				buckets:    buckets,
				bytesRead:  bytesRead,
				readTime:   readTime,
				decodeTime: decodeTime,
				err:        err,
				isContract: false,
			}
		}(idx, topicXDR)
		idx++
	}

	wg.Wait()
	return results, time.Since(start)
}

// =============================================================================
// Multi-Filter Optimization: Phase 2 - Smallest-First Intersection with Count Estimation
// =============================================================================

// postingListCountEstimate holds the estimated count for a posting list.
type postingListCountEstimate struct {
	cf         *grocksdb.ColumnFamilyHandle
	termKey    [32]byte
	isContract bool
	count      uint64 // estimated total count across all buckets
	err        error
}

// estimatePostingListCounts reads only the count headers from all posting lists in parallel.
// This is much faster than decoding full lists and helps determine which list to decode first.
func (es *RocksDBEventStore) estimatePostingListCounts(
	contractID []byte,
	topics [][]byte,
	startLedger, endLedger uint32,
) ([]postingListCountEstimate, time.Duration) {
	start := time.Now()

	// Build list of posting lists to estimate
	var estimates []postingListCountEstimate

	if len(contractID) > 0 {
		estimates = append(estimates, postingListCountEstimate{
			cf:         es.cfContractsPL,
			termKey:    index.ContractTermKey(contractID),
			isContract: true,
		})
	}

	for _, topicXDR := range topics {
		if len(topicXDR) == 0 {
			continue
		}
		estimates = append(estimates, postingListCountEstimate{
			cf:         es.cfTopicsPL,
			termKey:    index.TopicTermKey(topicXDR),
			isContract: false,
		})
	}

	if len(estimates) == 0 {
		return nil, 0
	}

	buckets := index.GetBucketsForRange(startLedger, endLedger)

	var wg sync.WaitGroup
	for i := range estimates {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var totalCount uint64

			for _, bucketID := range buckets {
				indexKey := index.EncodeIndexKeyWithBucket(estimates[idx].termKey, bucketID)

				value, err := es.db.GetCF(es.ro, estimates[idx].cf, indexKey)
				if err != nil {
					estimates[idx].err = err
					return
				}

				if value.Exists() {
					data := value.Data()
					count, _ := index.ReadTOIDCount(data)
					totalCount += count
				}
				value.Free()
			}

			estimates[idx].count = totalCount
		}(i)
	}

	wg.Wait()
	return estimates, time.Since(start)
}

// =============================================================================
// Multi-Filter Optimization: Phase 3 - Guided Intersection
// =============================================================================

// guidedPostingListResult holds the result of a guided posting list read.
type guidedPostingListResult struct {
	toids          []uint64
	bytesRead      int64
	bucketsRead    int
	bucketsSkipped int
	err            error
}

// queryPostingListGuided reads a posting list using a guide set to skip buckets.
// Only reads buckets that have potential matches based on the guide set.
func (es *RocksDBEventStore) queryPostingListGuided(
	cf *grocksdb.ColumnFamilyHandle,
	termKey [32]byte,
	startLedger, endLedger uint32,
	guide *index.GuidedIntersector,
) guidedPostingListResult {
	buckets := index.GetBucketsForRange(startLedger, endLedger)

	var result guidedPostingListResult
	var allTOIDs []uint64

	for _, bucketID := range buckets {
		// Phase 3 optimization: skip buckets with no guide TOIDs
		if !guide.HasTOIDsInBucket(bucketID) {
			result.bucketsSkipped++
			continue
		}

		indexKey := index.EncodeIndexKeyWithBucket(termKey, bucketID)
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		if err != nil {
			result.err = err
			return result
		}

		if value.Exists() {
			data := value.Data()
			result.bytesRead += int64(len(data))
			result.bucketsRead++

			// Decode and filter by ledger range
			toids := index.DecodeTOIDListDeltaVarint(data)
			filtered := index.FilterTOIDsByLedgerRange(toids, startLedger, endLedger)
			allTOIDs = append(allTOIDs, filtered...)
		}
		value.Free()
	}

	// Intersect with guide set
	result.toids = guide.IntersectSorted(allTOIDs)
	return result
}

// queryPostingListsWithGuide reads posting lists, using the smallest as a guide for others.
// This implements Phase 2 + Phase 3 optimization combined.
func (es *RocksDBEventStore) queryPostingListsWithGuide(
	contractID []byte,
	topics [][]byte,
	startLedger, endLedger uint32,
) ([]uint64, *PostingListQueryResult, error) {
	result := &PostingListQueryResult{}

	// Phase 2: First estimate counts to find smallest list
	estimates, estTime := es.estimatePostingListCounts(contractID, topics, startLedger, endLedger)
	result.EstimationTime = estTime

	if len(estimates) == 0 {
		return nil, result, nil
	}

	// Check for estimation errors
	for _, est := range estimates {
		if est.err != nil {
			return nil, nil, est.err
		}
	}

	// Find smallest list
	smallestIdx := 0
	for i := 1; i < len(estimates); i++ {
		if estimates[i].count < estimates[smallestIdx].count {
			smallestIdx = i
		}
	}

	// Record size metrics
	result.SmallestListSize = int(estimates[smallestIdx].count)
	result.LargestListSize = int(estimates[smallestIdx].count)
	for _, est := range estimates {
		if int(est.count) > result.LargestListSize {
			result.LargestListSize = int(est.count)
		}
	}

	// Early exit if smallest list is empty
	if estimates[smallestIdx].count == 0 {
		return nil, result, nil
	}

	// Read smallest list fully
	smallestTOIDs, buckets, bytesRead, readTime, decodeTime, err := es.queryPostingListWithStats(
		estimates[smallestIdx].cf,
		estimates[smallestIdx].termKey,
		startLedger, endLedger,
	)
	if err != nil {
		return nil, nil, err
	}

	result.BucketsScanned += buckets
	result.PostingListsRead++
	result.PostingListBytes += bytesRead
	result.PostingListReadTime += readTime
	result.PostingListDecodeTime += decodeTime
	if estimates[smallestIdx].isContract {
		result.TOIDsFromContract = len(smallestTOIDs)
	} else {
		result.TOIDsFromTopics = len(smallestTOIDs)
	}

	if len(smallestTOIDs) == 0 {
		return nil, result, nil
	}

	// Create guide from smallest list
	guide := index.NewGuidedIntersector(smallestTOIDs)
	resultTOIDs := smallestTOIDs

	// Phase 3: Read remaining lists using guided approach
	guidedStart := time.Now()

	for i, est := range estimates {
		if i == smallestIdx {
			continue // Already read
		}

		guidedResult := es.queryPostingListGuided(est.cf, est.termKey, startLedger, endLedger, guide)
		if guidedResult.err != nil {
			return nil, nil, guidedResult.err
		}

		result.BucketsScanned += guidedResult.bucketsRead
		result.SkippedBuckets += guidedResult.bucketsSkipped
		result.PostingListsRead++
		result.PostingListBytes += guidedResult.bytesRead
		if est.isContract {
			result.TOIDsFromContract = len(guidedResult.toids)
		} else {
			result.TOIDsFromTopics += len(guidedResult.toids)
		}

		// Intersect with running result
		resultTOIDs = index.IntersectTOIDLists(resultTOIDs, guidedResult.toids)

		if len(resultTOIDs) == 0 {
			result.GuidedIntersectTime = time.Since(guidedStart)
			return nil, result, nil
		}

		// Update guide for next iteration (progressively smaller)
		guide = index.NewGuidedIntersector(resultTOIDs)
	}

	result.GuidedIntersectTime = time.Since(guidedStart)
	return resultTOIDs, result, nil
}

// fetchEventsByTOIDsWithStats fetches events with detailed stats.
// Filters events by contractID and topics if provided.
// If nonPositionalTopics is true, uses non-positional topic matching (topic can be at any position).
func (es *RocksDBEventStore) fetchEventsByTOIDsWithStats(toids []uint64, limit int, contractID []byte, topics [][]byte, nonPositionalTopics bool) ([]*query.Event, int64, int, time.Duration, time.Duration, error) {
	events := make([]*query.Event, 0, len(toids))
	var bytesRead int64
	var scanned int
	var decodeTime time.Duration
	var filterTime time.Duration

	for _, toid := range toids {
		if limit > 0 && len(events) >= limit {
			break
		}

		ledger, tx, op := index.DecodeTOID(toid)
		startKey := index.EncodeEventKey(toid, 0)

		it := es.db.NewIteratorCF(es.ro, es.cfEvents)

		for it.Seek(startKey); it.Valid(); it.Next() {
			if limit > 0 && len(events) >= limit {
				break
			}

			key := it.Key().Data()
			if len(key) < 10 {
				break
			}

			keyTOID := binary.BigEndian.Uint64(key[0:8])
			if keyTOID > toid {
				break
			}

			eventIdx := binary.BigEndian.Uint16(key[8:10])
			valueData := it.Value().Data()
			bytesRead += int64(len(valueData))
			scanned++

			// Filter using binary header for fast rejection
			filterStart := time.Now()
			if es.eventFormat == "binary" && (len(contractID) > 0 || len(topics) > 0) {
				header := ParseBinaryHeader(valueData)
				if header != nil {
					matches := true
					if len(contractID) > 0 && !header.MatchesContractID(contractID) {
						matches = false
					}
					if matches && len(topics) > 0 {
						if nonPositionalTopics {
							if !header.MatchesTopicsNonPositional(topics) {
								matches = false
							}
						} else {
							if !header.MatchesTopics(topics) {
								matches = false
							}
						}
					}
					filterTime += time.Since(filterStart)
					if !matches {
						continue
					}
				}
			} else {
				filterTime += time.Since(filterStart)
			}

			decodeStart := time.Now()
			var event *query.Event
			var err error

			if es.eventFormat == "binary" {
				event, err = DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
			} else {
				event, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
			}
			decodeTime += time.Since(decodeStart)

			if err != nil {
				continue
			}

			events = append(events, event)
		}
		it.Close()
	}

	return events, bytesRead, scanned, decodeTime, filterTime, nil
}

// fetchEventsByTOIDs fetches events from primary storage by TOID.
func (es *RocksDBEventStore) fetchEventsByTOIDs(toids []uint64) ([]*query.Event, error) {
	events := make([]*query.Event, 0, len(toids))

	for _, toid := range toids {
		ledger, tx, op := index.DecodeTOID(toid)

		// We need to scan for events with this TOID (could have multiple event indices)
		startKey := index.EncodeEventKey(toid, 0)

		it := es.db.NewIteratorCF(es.ro, es.cfEvents)
		defer it.Close()

		for it.Seek(startKey); it.Valid(); it.Next() {
			key := it.Key().Data()
			if len(key) < 10 {
				break
			}

			// Check if we're past the end key
			keyTOID := binary.BigEndian.Uint64(key[0:8])
			if keyTOID > toid {
				break
			}

			eventIdx := binary.BigEndian.Uint16(key[8:10])
			valueData := it.Value().Data()

			var event *query.Event
			var err error

			if es.eventFormat == "binary" {
				event, err = DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
			} else {
				event, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
			}

			if err != nil {
				continue
			}

			events = append(events, event)
		}
	}

	return events, nil
}

// =============================================================================
// 32-bit Bitmap Query (Ledger-Level Granularity)
// =============================================================================

// QueryEventsWithBitmap queries events using the 32-bit ledger-level bitmap index.
// Returns matching ledgers from index, then scans those ledgers for matching events.
// Uses non-positional topic matching (same semantics as posting list).
func (es *RocksDBEventStore) QueryEventsWithBitmap(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) (*BitmapQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &BitmapQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	if es.indexStore == nil {
		return nil, nil, fmt.Errorf("32-bit bitmap index not available")
	}

	// Query the 32-bit bitmap index
	indexStart := time.Now()
	queryResult, err := es.indexStore.QueryLedgersWithStats(contractID, topics, startLedger, endLedger)
	if err != nil {
		return nil, nil, fmt.Errorf("bitmap index query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLedgers = int(queryResult.Bitmap.GetCardinality())
	result.IndexBytesRead = queryResult.BytesRead
	result.SegmentsScanned = queryResult.Segments
	matchingLedgers := queryResult.Bitmap

	if matchingLedgers.IsEmpty() {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// Fetch events from matching ledgers
	fetchStart := time.Now()
	var events []*query.Event

	iter := matchingLedgers.Iterator()
	for iter.HasNext() && (limit <= 0 || len(events) < limit) {
		ledger := iter.Next()
		remaining := limit - len(events)
		if limit <= 0 {
			remaining = 0
		}

		ledgerEvents, err := es.GetEventsInLedgerWithFilter(ledger, contractID, topics, remaining)
		if err != nil {
			continue
		}

		result.EventsScanned += int(ledgerEvents.EventsScanned)
		result.EventBytesRead += ledgerEvents.Timing.BytesRead
		result.DiskReadTime += ledgerEvents.Timing.DiskReadTime
		result.DecodeTime += ledgerEvents.Timing.DecodeTime
		result.FilterTime += ledgerEvents.Timing.FilterTime
		events = append(events, ledgerEvents.Events...)
	}
	result.EventFetchTime = time.Since(fetchStart)
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// =============================================================================
// V2 Posting List Query (32-bit Local IDs)
// =============================================================================

// postingListV2Result holds the result of reading a single V2 posting list.
type postingListV2Result struct {
	localIDs   []uint32
	buckets    int
	bytesRead  int64
	readTime   time.Duration
	decodeTime time.Duration
	err        error
	isContract bool
}

// QueryEventsWithPostingListV2Timing queries events using V2 posting lists (32-bit local IDs).
// Uses point gets with V2 event keys instead of TOID-based range scans.
// Supports parallel reads, guided intersection (smallest-first), and streaming for single-filter queries.
// Requires data ingested with V2Indexes: true (V2 event keys in cfEvents).
func (es *RocksDBEventStore) QueryEventsWithPostingListV2Timing(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) (*PostingListV2QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &PostingListV2QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	hasContract := len(contractID) > 0
	hasTopics := len(topics) > 0
	multiFilter := hasContract && hasTopics

	if !hasContract && !hasTopics {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// For single filter queries with limit, use streaming approach
	if !multiFilter && limit > 0 {
		return es.queryPostingListV2Streaming(contractID, topics, startLedger, endLedger, limit, result, totalStart)
	}

	// Multi-filter: use parallel reads + guided intersection
	buckets := index.GetBucketsForRange(startLedger, endLedger)

	plStart := time.Now()

	// Parallel read all posting lists
	plResults := es.queryPostingListsV2Parallel(contractID, topics, buckets, startLedger, endLedger)

	// Check for errors and aggregate stats
	for _, plr := range plResults {
		if plr.err != nil {
			return nil, nil, plr.err
		}
		result.BucketsScanned += plr.buckets
		result.PostingListsRead++
		result.PostingListBytes += plr.bytesRead
		result.PostingListReadTime += plr.readTime
		result.PostingListDecodeTime += plr.decodeTime
		result.LocalIDsInPostingList += len(plr.localIDs)
	}

	// Sort by size ascending (smallest first for efficient intersection)
	sort.Slice(plResults, func(a, b int) bool {
		return len(plResults[a].localIDs) < len(plResults[b].localIDs)
	})

	// Intersect progressively (guided by smallest list)
	var resultLocalIDs []uint32
	for i, plr := range plResults {
		if i == 0 {
			resultLocalIDs = plr.localIDs
		} else {
			intersectStart := time.Now()
			resultLocalIDs = index.IntersectLocalIDLists(resultLocalIDs, plr.localIDs)
			result.IntersectTime += time.Since(intersectStart)
		}
		if len(resultLocalIDs) == 0 {
			result.PostingListTime = time.Since(plStart) - result.IntersectTime
			result.TotalTime = time.Since(totalStart)
			return result, nil, nil
		}
	}

	result.PostingListTime = time.Since(plStart) - result.IntersectTime
	result.LocalIDsAfterIntersect = len(resultLocalIDs)

	// Fetch events using point gets
	events, fetchTime, decodeTime, filterTime, bytesRead, scanned := es.fetchEventsByLocalIDsV2(
		resultLocalIDs, buckets, startLedger, endLedger, limit, contractID, topics)

	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventBytesRead = bytesRead
	result.EventsScanned = scanned
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// queryPostingListsV2Parallel reads all V2 posting lists in parallel.
func (es *RocksDBEventStore) queryPostingListsV2Parallel(contractID []byte, topics [][]byte, buckets []uint32, startLedger, endLedger uint32) []postingListV2Result {
	// Count how many posting lists to read
	numLists := 0
	if len(contractID) > 0 {
		numLists++
	}
	for _, t := range topics {
		if len(t) > 0 {
			numLists++
		}
	}

	results := make([]postingListV2Result, numLists)
	var wg sync.WaitGroup

	idx := 0

	// Read contract posting list
	if len(contractID) > 0 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			termKey := index.ContractTermKey(contractID)
			ids, bucketsRead, bytesRead, readT, decodeT, err := es.queryPostingListV2WithStats(es.cfContractsPLV2, termKey, buckets, startLedger, endLedger)
			results[i] = postingListV2Result{
				localIDs:   ids,
				buckets:    bucketsRead,
				bytesRead:  bytesRead,
				readTime:   readT,
				decodeTime: decodeT,
				err:        err,
				isContract: true,
			}
		}(idx)
		idx++
	}

	// Read topic posting lists
	for _, topicXDR := range topics {
		if len(topicXDR) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int, topic []byte) {
			defer wg.Done()
			termKey := index.TopicTermKey(topic)
			ids, bucketsRead, bytesRead, readT, decodeT, err := es.queryPostingListV2WithStats(es.cfTopicsPLV2, termKey, buckets, startLedger, endLedger)
			results[i] = postingListV2Result{
				localIDs:   ids,
				buckets:    bucketsRead,
				bytesRead:  bytesRead,
				readTime:   readT,
				decodeTime: decodeT,
				err:        err,
				isContract: false,
			}
		}(idx, topicXDR)
		idx++
	}

	wg.Wait()
	return results
}

// queryPostingListV2Streaming reads posting lists bucket-by-bucket and fetches events incrementally.
// Stops early when limit is reached.
func (es *RocksDBEventStore) queryPostingListV2Streaming(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int, result *PostingListV2QueryResult, totalStart time.Time) (*PostingListV2QueryResult, []*query.Event, error) {
	buckets := index.GetBucketsForRange(startLedger, endLedger)

	// Determine which CF and term key to use
	var cf *grocksdb.ColumnFamilyHandle
	var termKey [32]byte

	if len(contractID) > 0 {
		cf = es.cfContractsPLV2
		termKey = index.ContractTermKey(contractID)
	} else if len(topics) > 0 && len(topics[0]) > 0 {
		cf = es.cfTopicsPLV2
		termKey = index.TopicTermKey(topics[0])
	} else {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	var allEvents []*query.Event
	var plTime, plReadTime, plDecodeTime time.Duration
	var fetchTime, decodeTime, filterTime time.Duration

	for _, bucketID := range buckets {
		if len(allEvents) >= limit {
			break
		}

		bucketStart := bucketID * index.BucketSize

		// Read this bucket's posting list
		plStart := time.Now()
		indexKey := index.EncodeIndexKeyWithBucket(termKey, bucketID)

		t0 := time.Now()
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		plReadTime += time.Since(t0)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read V2 posting list: %w", err)
		}

		if !value.Exists() {
			value.Free()
			result.BucketsScanned++
			continue
		}

		data := value.Data()
		result.PostingListBytes += int64(len(data))
		result.BucketsScanned++
		result.PostingListsRead++

		// Copy data since we need to free value
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		value.Free()

		// Decode local IDs
		t1 := time.Now()
		localIDs := index.DecodeLocalIDListDeltaVarint(dataCopy)
		plDecodeTime += time.Since(t1)

		// Filter by ledger range
		filtered := index.FilterLocalIDsByLedgerRange(localIDs, bucketStart, startLedger, endLedger)
		result.LocalIDsInPostingList += len(filtered)
		result.LocalIDsAfterIntersect += len(filtered)
		plTime += time.Since(plStart)

		// Fetch events for this bucket's local IDs
		remaining := limit - len(allEvents)
		events, ft, dt, flt, bytesRead, scanned := es.fetchEventsByLocalIDsV2ForBucket(
			filtered, bucketStart, remaining, contractID, topics)

		fetchTime += ft
		decodeTime += dt
		filterTime += flt
		result.EventBytesRead += bytesRead
		result.EventsScanned += scanned

		allEvents = append(allEvents, events...)
	}

	result.PostingListTime = plTime
	result.PostingListReadTime = plReadTime
	result.PostingListDecodeTime = plDecodeTime
	result.EventFetchTime = fetchTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventsReturned = len(allEvents)
	result.TotalTime = time.Since(totalStart)

	return result, allEvents, nil
}

// fetchEventsByLocalIDsV2 fetches events by local IDs using point gets with V2 keys.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2(localIDs []uint32, buckets []uint32, startLedger, endLedger uint32, limit int, contractID []byte, topics [][]byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {
	fetchStart := time.Now()
	var decodeTime, filterTime time.Duration
	var bytesRead int64
	var scanned int
	events := make([]*query.Event, 0, min(limit, len(localIDs)))

	// Group local IDs by bucket
	localIDsByBucket := make(map[uint32][]uint32)
	for _, localID := range localIDs {
		ledgerOffset := localID >> 16
		// Find which bucket this belongs to
		for _, bucketID := range buckets {
			bucketStart := bucketID * index.BucketSize
			ledger := bucketStart + ledgerOffset
			_, bucketEnd := index.BucketLedgerRange(bucketID)
			if ledger >= startLedger && ledger <= endLedger && ledger <= bucketEnd && ledgerOffset < index.BucketSize {
				localIDsByBucket[bucketID] = append(localIDsByBucket[bucketID], localID)
				break
			}
		}
	}

	// Process buckets in order
	for _, bucketID := range buckets {
		if limit > 0 && len(events) >= limit {
			break
		}

		bucketLocalIDs := localIDsByBucket[bucketID]
		if len(bucketLocalIDs) == 0 {
			continue
		}

		bucketStart := bucketID * index.BucketSize
		remaining := limit - len(events)
		if limit <= 0 {
			remaining = len(bucketLocalIDs)
		}

		evts, _, dt, flt, br, sc := es.fetchEventsByLocalIDsV2ForBucket(
			bucketLocalIDs, bucketStart, remaining, contractID, topics)

		decodeTime += dt
		filterTime += flt
		bytesRead += br
		scanned += sc
		events = append(events, evts...)
	}

	return events, time.Since(fetchStart) - decodeTime - filterTime, decodeTime, filterTime, bytesRead, scanned
}

// fetchEventsByLocalIDsV2ForBucket fetches events for local IDs within a single bucket.
func (es *RocksDBEventStore) fetchEventsByLocalIDsV2ForBucket(localIDs []uint32, bucketStart uint32, limit int, contractID []byte, topics [][]byte) ([]*query.Event, time.Duration, time.Duration, time.Duration, int64, int) {
	fetchStart := time.Now()
	var decodeTime, filterTime time.Duration
	var bytesRead int64
	var scanned int
	events := make([]*query.Event, 0, min(limit, len(localIDs)))

	for _, localID := range localIDs {
		if limit > 0 && len(events) >= limit {
			break
		}

		ledger, eventSeq := index.DecodeLocalIDForBucket(localID, bucketStart)
		v2Key := index.EncodeEventKeyV2(ledger, eventSeq)

		data, err := es.db.GetCF(es.ro, es.cfEvents, v2Key)
		if err != nil {
			continue
		}
		if data.Size() == 0 {
			data.Free()
			continue
		}

		valueData := data.Data()
		valueCopy := make([]byte, len(valueData))
		copy(valueCopy, valueData)
		data.Free()

		bytesRead += int64(len(valueCopy))
		scanned++

		// Filter using binary header
		filterStart := time.Now()
		if es.eventFormat == "binary" && (len(contractID) > 0 || len(topics) > 0) {
			header := ParseBinaryHeader(valueCopy)
			if header != nil {
				matches := true
				if len(contractID) > 0 && !header.MatchesContractID(contractID) {
					matches = false
				}
				if matches && len(topics) > 0 && !header.MatchesTopicsNonPositional(topics) {
					matches = false
				}
				filterTime += time.Since(filterStart)
				if !matches {
					continue
				}
			}
		} else {
			filterTime += time.Since(filterStart)
		}

		// Decode to query.Event
		decStart := time.Now()
		var event *query.Event
		if es.eventFormat == "binary" {
			event, err = DecodeBinaryToQueryEventV2(valueCopy, ledger, eventSeq)
		} else {
			event, err = parseRawXDRToQueryEvent(valueCopy, ledger, 0, 0, eventSeq)
		}
		decodeTime += time.Since(decStart)

		if err != nil {
			continue
		}

		events = append(events, event)
	}

	return events, time.Since(fetchStart) - decodeTime - filterTime, decodeTime, filterTime, bytesRead, scanned
}

// queryPostingListV2WithStats reads V2 posting lists (32-bit local IDs) and returns stats.
func (es *RocksDBEventStore) queryPostingListV2WithStats(cf *grocksdb.ColumnFamilyHandle, termKey [32]byte, buckets []uint32, startLedger, endLedger uint32) ([]uint32, int, int64, time.Duration, time.Duration, error) {
	var allLocalIDs []uint32
	var bytesRead int64
	var readTime, decodeTime time.Duration

	for _, bucketID := range buckets {
		indexKey := index.EncodeIndexKeyWithBucket(termKey, bucketID)

		t0 := time.Now()
		value, err := es.db.GetCF(es.ro, cf, indexKey)
		readTime += time.Since(t0)
		if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("failed to read V2 posting list: %w", err)
		}

		if value.Exists() {
			data := value.Data()
			bytesRead += int64(len(data))
			t1 := time.Now()
			localIDs := index.DecodeLocalIDListDeltaVarint(data)
			decodeTime += time.Since(t1)

			bucketStart := bucketID * index.BucketSize
			filtered := index.FilterLocalIDsByLedgerRange(localIDs, bucketStart, startLedger, endLedger)
			allLocalIDs = append(allLocalIDs, filtered...)
		}
		value.Free()
	}

	return allLocalIDs, len(buckets), bytesRead, readTime, decodeTime, nil
}

// =============================================================================
// 64-bit Bitmap Query (Event-Level Granularity)
// =============================================================================

// QueryEventsWithBitmap64 queries events using the 64-bit event-level bitmap index.
// This provides exact event matching (no ledger scanning) for better performance.
// Uses non-positional topic matching (same semantics as posting list).
func (es *RocksDBEventStore) QueryEventsWithBitmap64(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) (*Bitmap64QueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap64QueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	if es.eventIndexStore == nil {
		return nil, nil, fmt.Errorf("64-bit bitmap index not available")
	}

	// Query the 64-bit bitmap index (non-positional topic matching)
	indexStart := time.Now()
	queryResult, err := es.eventIndexStore.QueryEventKeysWithStats(contractID, topics, startLedger, endLedger)
	if err != nil {
		return nil, nil, fmt.Errorf("64-bit bitmap query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingTOIDs = int(queryResult.Bitmap.GetCardinality())
	result.IndexBytesRead = queryResult.BytesRead
	result.SegmentsScanned = queryResult.Segments
	result.IndexReadTime = queryResult.ReadTime
	result.IndexDecodeTime = queryResult.DecodeTime
	eventKeys := queryResult.Bitmap

	if eventKeys.IsEmpty() {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// Fetch events by TOIDs (bitmap64 now stores TOIDs, not event keys)
	// Same logic as posting list - iterate all events for each TOID with filtering
	fetchStart := time.Now()
	var decodeTime, filterTime time.Duration
	events := make([]*query.Event, 0, min(limit, result.MatchingTOIDs))

	iter := eventKeys.Iterator()
	for iter.HasNext() && (limit <= 0 || len(events) < limit) {
		toid := iter.Next() // Bitmap now stores TOIDs directly

		ledger, tx, op := index.DecodeTOID(toid)
		startKey := index.EncodeEventKey(toid, 0)

		// Iterate all events for this TOID (same as posting list)
		it := es.db.NewIteratorCF(es.ro, es.cfEvents)
		for it.Seek(startKey); it.Valid(); it.Next() {
			if limit > 0 && len(events) >= limit {
				break
			}

			key := it.Key().Data()
			if len(key) < 10 {
				break
			}

			keyTOID := binary.BigEndian.Uint64(key[0:8])
			if keyTOID > toid {
				break
			}

			eventIdx := binary.BigEndian.Uint16(key[8:10])
			valueData := it.Value().Data()
			result.EventBytesRead += int64(len(valueData))
			result.EventsScanned++

			// Filter using binary header for fast rejection (non-positional topic matching for bitmap64)
			filterStart := time.Now()
			if es.eventFormat == "binary" && (len(contractID) > 0 || len(topics) > 0) {
				header := ParseBinaryHeader(valueData)
				if header != nil {
					matches := true
					if len(contractID) > 0 && !header.MatchesContractID(contractID) {
						matches = false
					}
					if matches && len(topics) > 0 && !header.MatchesTopicsNonPositional(topics) {
						matches = false
					}
					filterTime += time.Since(filterStart)
					if !matches {
						continue
					}
				}
			} else {
				filterTime += time.Since(filterStart)
			}

			decStart := time.Now()
			var event *query.Event
			var err error
			if es.eventFormat == "binary" {
				event, err = DecodeBinaryToQueryEvent(valueData, ledger, tx, op, eventIdx)
			} else {
				event, err = parseRawXDRToQueryEvent(valueData, ledger, tx, op, eventIdx)
			}
			decodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, event)
		}
		it.Close()
	}

	result.EventFetchTime = time.Since(fetchStart) - decodeTime - filterTime
	result.DecodeTime = decodeTime
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// GetEventIndexStore returns the 64-bit event bitmap index store.
func (es *RocksDBEventStore) GetEventIndexStore() *index.EventRocksDBStore {
	return es.eventIndexStore
}

// GetEventIndex32Store returns the 32-bit event bitmap index store (FromBuffer decode).
func (es *RocksDBEventStore) GetEventIndex32Store() *index.EventRocksDB32Store {
	return es.eventIndex32Store
}

// SetUseEventKeyV2 enables V2 event key format for reads.
// This should be set after ingesting with V2Indexes=true.
func (es *RocksDBEventStore) SetUseEventKeyV2(enabled bool) {
	es.useEventKeyV2 = enabled
}

// =============================================================================
// Bitmap32 Event-Level Queries (FromBuffer decode)
// =============================================================================

// QueryEventsWithBitmap32EventIndex queries events using the 32-bit event-level bitmap index.
// Uses sequential event IDs + FromBuffer for near-zero-cost index decode.
func (es *RocksDBEventStore) QueryEventsWithBitmap32EventIndex(contractID []byte, topics [][]byte, startLedger, endLedger uint32, limit int) (*Bitmap32EventQueryResult, []*query.Event, error) {
	totalStart := time.Now()
	result := &Bitmap32EventQueryResult{
		LedgerRange: endLedger - startLedger + 1,
	}

	if es.eventIndex32Store == nil {
		return nil, nil, fmt.Errorf("bitmap32 event index not available")
	}

	// Query the bitmap32 index (non-positional topic matching)
	indexStart := time.Now()
	queryResult, err := es.eventIndex32Store.QueryEventKeysWithStats(contractID, topics, startLedger, endLedger)
	if err != nil {
		return nil, nil, fmt.Errorf("bitmap32 event query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLocalIDs = int(queryResult.TotalCount)
	result.IndexBytesRead = queryResult.BytesRead
	result.SegmentsScanned = queryResult.Segments
	result.IndexReadTime = queryResult.ReadTime
	result.IndexDecodeTime = queryResult.DecodeTime

	if queryResult.TotalCount == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil, nil
	}

	// Fetch events by local IDs from each segment
	fetchStart := time.Now()
	var decodeTime, filterTime time.Duration
	events := make([]*query.Event, 0, min(limit, result.MatchingLocalIDs))

	// Sort segment IDs for ordered output
	segIDs := make([]uint32, 0, len(queryResult.PerSegment))
	for segID := range queryResult.PerSegment {
		segIDs = append(segIDs, segID)
	}
	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	for _, segID := range segIDs {
		localIDs := queryResult.PerSegment[segID]
		segmentStart := segID * index.SegmentSize

		iter := localIDs.Iterator()
		for iter.HasNext() && (limit <= 0 || len(events) < limit) {
			localID := iter.Next()

			// Convert local ID to V2 key
			v2Key := index.LocalIDToEventKeyV2(segmentStart, localID)

			// Point-get from cfEvents
			data, err := es.db.GetCF(es.ro, es.cfEvents, v2Key)
			if err != nil {
				continue
			}
			if data.Size() == 0 {
				data.Free()
				continue
			}

			valueData := data.Data()
			valueCopy := make([]byte, len(valueData))
			copy(valueCopy, valueData)
			data.Free()

			result.EventBytesRead += int64(len(valueCopy))
			result.EventsScanned++

			// Filter using binary header for fast rejection (non-positional topic matching)
			filterStart := time.Now()
			if es.eventFormat == "binary" && (len(contractID) > 0 || len(topics) > 0) {
				header := ParseBinaryHeader(valueCopy)
				if header != nil {
					matches := true
					if len(contractID) > 0 && !header.MatchesContractID(contractID) {
						matches = false
					}
					if matches && len(topics) > 0 && !header.MatchesTopicsNonPositional(topics) {
						matches = false
					}
					filterTime += time.Since(filterStart)
					if !matches {
						continue
					}
				}
			} else {
				filterTime += time.Since(filterStart)
			}

			// Decode to query.Event
			ledgerOffset, eventSeq := index.DecodeBitmap32LocalID(localID)
			ledger := segmentStart + uint32(ledgerOffset)

			decStart := time.Now()
			var event *query.Event
			if es.eventFormat == "binary" {
				event, err = DecodeBinaryToQueryEventV2(valueCopy, ledger, eventSeq)
			} else {
				event, err = parseRawXDRToQueryEvent(valueCopy, ledger, 0, 0, eventSeq)
			}
			decodeTime += time.Since(decStart)

			if err != nil {
				continue
			}

			events = append(events, event)
		}

		if limit > 0 && len(events) >= limit {
			break
		}
	}

	result.EventFetchTime = time.Since(fetchStart) - decodeTime - filterTime
	result.DecodeTime = decodeTime
	result.FilterTime = filterTime
	result.EventsReturned = len(events)
	result.TotalTime = time.Since(totalStart)

	return result, events, nil
}

// =============================================================================
// Compile-time Interface Checks
// =============================================================================

// Verify RocksDBEventStore implements EventStore interface
var _ EventStore = (*RocksDBEventStore)(nil)

// Verify RocksDBEventStore implements EventReader interface
var _ EventReader = (*RocksDBEventStore)(nil)

// GetIndexReader returns the index store as an index.IndexReader for use with query.Engine.
// Returns nil if the index store is not available.
func (es *RocksDBEventStore) GetIndexReader() index.IndexReader {
	return es.indexStore
}
