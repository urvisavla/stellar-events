package store

import (
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
	"sync/atomic"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// RocksDBOptions contains tuning parameters for RocksDB
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
}

// Column family names
const (
	CFDefault = "default" // Metadata (last_processed_ledger, etc.)
	CFEvents  = "events"  // Primary event storage (raw XDR)
	CFUnique  = "unique"  // Unique value indexes with counts
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

// ContractEvent represents a contract event extracted from a ledger (full JSON format)
type ContractEvent struct {
	LedgerSequence   uint32   `json:"ledger_sequence"`
	TransactionIndex int      `json:"transaction_index"`
	OperationIndex   int      `json:"operation_index,omitempty"`
	EventIndex       int      `json:"event_index"`
	ContractID       string   `json:"contract_id,omitempty"`
	Type             string   `json:"type"`
	EventStage       string   `json:"event_stage,omitempty"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	TransactionHash  string   `json:"transaction_hash"`
	Successful       bool     `json:"successful"`
}

// MinimalEvent represents an event with just position info and raw XDR
// Used for fast ingestion - no JSON serialization overhead
type MinimalEvent struct {
	LedgerSequence   uint32
	TransactionIndex uint16
	OperationIndex   uint16
	EventIndex       uint16
	RawXDR           []byte

	// Pre-extracted fields for indexing (avoids re-parsing XDR)
	ContractID []byte   // 32 bytes if present, nil otherwise
	Topics     [][]byte // Pre-marshaled topic XDR bytes
}

// eventKey generates a 10-byte binary key for events
// Format: [ledger:4][tx:2][op:2][event:2]
func eventKey(e *MinimalEvent) []byte {
	key := make([]byte, 10)
	binary.BigEndian.PutUint32(key[0:4], e.LedgerSequence)
	binary.BigEndian.PutUint16(key[4:6], e.TransactionIndex)
	binary.BigEndian.PutUint16(key[6:8], e.OperationIndex)
	binary.BigEndian.PutUint16(key[8:10], e.EventIndex)
	return key
}

// eventKeyFromParts generates a 10-byte binary key from components
func eventKeyFromParts(ledger uint32, tx, op, event uint16) []byte {
	key := make([]byte, 10)
	binary.BigEndian.PutUint32(key[0:4], ledger)
	binary.BigEndian.PutUint16(key[4:6], tx)
	binary.BigEndian.PutUint16(key[6:8], op)
	binary.BigEndian.PutUint16(key[8:10], event)
	return key
}

// parseEventKey extracts position info from a binary key
func parseEventKey(key []byte) (ledger uint32, tx, op, event uint16) {
	if len(key) < 10 {
		return 0, 0, 0, 0
	}
	ledger = binary.BigEndian.Uint32(key[0:4])
	tx = binary.BigEndian.Uint16(key[4:6])
	op = binary.BigEndian.Uint16(key[6:8])
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
func parseRawXDRToEvent(rawXDR []byte, ledger uint32, tx, op, eventIdx uint16) (*ContractEvent, error) {
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

	// Extract contract ID if present
	if xdrEvent.ContractId != nil {
		event.ContractID = base64.StdEncoding.EncodeToString(xdrEvent.ContractId[:])
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

// IndexConfig controls which secondary indexes to create
type IndexConfig struct {
	ContractID bool
	Topics     bool // enables topic0-3
}

// DefaultIndexConfig returns config with all indexes enabled
func DefaultIndexConfig() *IndexConfig {
	return &IndexConfig{
		ContractID: true,
		Topics:     true,
	}
}

// EventStore manages storing events in RocksDB
type EventStore struct {
	db      *grocksdb.DB
	dbPath  string // Store path for filesystem-based stats
	wo      *grocksdb.WriteOptions
	ro      *grocksdb.ReadOptions
	indexes *IndexConfig

	// Column family handles (managed by DB, don't destroy manually)
	cfHandles []*grocksdb.ColumnFamilyHandle
	cfDefault *grocksdb.ColumnFamilyHandle // Metadata
	cfEvents  *grocksdb.ColumnFamilyHandle // Primary event storage
	cfUnique  *grocksdb.ColumnFamilyHandle // Unique value indexes with counts

	// Options that need to be destroyed on Close
	baseOpts *grocksdb.Options
	cfOpts   []*grocksdb.Options
	bbtoList []*grocksdb.BlockBasedTableOptions

	// Keep merge operator alive to prevent GC (RocksDB holds a reference)
	mergeOp grocksdb.MergeOperator
}

// NewEventStore creates a new event store with RocksDB backend
func NewEventStore(dbPath string) (*EventStore, error) {
	return NewEventStoreWithOptions(dbPath, nil, nil)
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

	cfNames := []string{CFDefault, CFEvents, CFUnique}
	cfOpts := []*grocksdb.Options{defaultOpts, eventsOpts, uniqueOpts}
	bbtoList := []*grocksdb.BlockBasedTableOptions{eventsBBTO, uniqueBBTO}

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

	return &EventStore{
		db:        db,
		dbPath:    dbPath,
		wo:        wo,
		ro:        grocksdb.NewDefaultReadOptions(),
		indexes:   indexes,
		cfHandles: cfHandles,
		cfDefault: cfHandles[0],
		cfEvents:  cfHandles[1],
		cfUnique:  cfHandles[2],
		baseOpts:  baseOpts,
		cfOpts:    cfOpts,
		bbtoList:  bbtoList,
		mergeOp:   mergeOp,
	}, nil
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
func (es *EventStore) Close() {
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

// StoreMinimalEventsWithIndexes stores events and optionally updates unique indexes with counts
func (es *EventStore) StoreMinimalEventsWithIndexes(events []*MinimalEvent, updateUniqueIndexes bool) (int64, error) {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	var totalBytes int64

	// Track counts to increment for this batch
	// Map from unique key -> count to add
	countUpdates := make(map[string]uint64)

	for _, event := range events {
		key := eventKey(event)
		batch.PutCF(es.cfEvents, key, event.RawXDR)
		totalBytes += int64(len(event.RawXDR))

		// Optionally update unique indexes with counts
		// Uses pre-extracted fields from MinimalEvent (no XDR parsing needed!)
		if updateUniqueIndexes {
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
	if updateUniqueIndexes && len(countUpdates) > 0 {
		for keyStr, addCount := range countUpdates {
			keyBytes := []byte(keyStr)
			countBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(countBytes, addCount)
			batch.MergeCF(es.cfUnique, keyBytes, countBytes)
		}
	}

	if err := es.db.Write(es.wo, batch); err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	return totalBytes, nil
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

// =============================================================================
// Query by Ledger (Primary Index)
// =============================================================================

// GetEventsByLedgerRange retrieves all events within a ledger range
func (es *EventStore) GetEventsByLedgerRange(startLedger, endLedger uint32) ([]*ContractEvent, error) {
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
func (es *EventStore) GetEventsByLedger(ledgerSequence uint32) ([]*ContractEvent, error) {
	return es.GetEventsByLedgerRange(ledgerSequence, ledgerSequence)
}

// =============================================================================
// Query by Contract ID
// =============================================================================

// GetEventsByContractID retrieves events for a specific contract (scans all events)
func (es *EventStore) GetEventsByContractID(contractIDBase64 string, limit int) ([]*ContractEvent, error) {
	contractBytes, err := base64.StdEncoding.DecodeString(contractIDBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid contract ID: %w", err)
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

		ledger, tx, op, eventIdx := parseEventKey(key)

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.ContractId != nil && string(xdrEvent.ContractId[:]) == string(contractBytes) {
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
func (es *EventStore) GetEventsByContractIDInRange(contractIDBase64 string, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	contractBytes, err := base64.StdEncoding.DecodeString(contractIDBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid contract ID: %w", err)
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

		ledger, tx, op, eventIdx := parseEventKey(key)
		if ledger > endLedger {
			break
		}

		var xdrEvent xdr.ContractEvent
		if err := xdrEvent.UnmarshalBinary(it.Value().Data()); err != nil {
			continue
		}

		if xdrEvent.ContractId != nil && string(xdrEvent.ContractId[:]) == string(contractBytes) {
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
func (es *EventStore) GetEventsByTopic(position int, topicValueBase64 string, limit int) ([]*ContractEvent, error) {
	if position < 0 || position > 3 {
		return nil, fmt.Errorf("topic position must be 0-3, got %d", position)
	}

	topicBytes, err := base64.StdEncoding.DecodeString(topicValueBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid topic value: %w", err)
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
				if string(topicXDR) == string(topicBytes) {
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
func (es *EventStore) GetEventsByTopicInRange(position int, topicValueBase64 string, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	if position < 0 || position > 3 {
		return nil, fmt.Errorf("topic position must be 0-3, got %d", position)
	}

	topicBytes, err := base64.StdEncoding.DecodeString(topicValueBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid topic value: %w", err)
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
				if string(topicXDR) == string(topicBytes) {
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

// DBStats holds statistics about the event database
type DBStats struct {
	TotalEvents     int64  `json:"total_events"`
	MinLedger       uint32 `json:"min_ledger"`
	MaxLedger       uint32 `json:"max_ledger"`
	LastProcessed   uint32 `json:"last_processed_ledger"`
	UniqueContracts int    `json:"unique_contracts"`
}

// StorageStats holds RocksDB storage statistics
type StorageStats struct {
	EstimatedNumKeys     string `json:"estimated_num_keys"`
	EstimateLiveDataSize string `json:"estimate_live_data_size"`
	TotalSstFilesSize    string `json:"total_sst_files_size"`
	LiveSstFilesSize     string `json:"live_sst_files_size"`
	SizeAllMemTables     string `json:"size_all_mem_tables"`
	CurSizeAllMemTables  string `json:"cur_size_all_mem_tables"`

	EstimatePendingCompactionBytes string `json:"estimate_pending_compaction_bytes"`
	NumRunningCompactions          string `json:"num_running_compactions"`
	NumRunningFlushes              string `json:"num_running_flushes"`

	NumFilesAtLevel0 string `json:"num_files_at_level0"`
	NumFilesAtLevel1 string `json:"num_files_at_level1"`
	NumFilesAtLevel2 string `json:"num_files_at_level2"`
	NumFilesAtLevel3 string `json:"num_files_at_level3"`

	BlockCacheUsage       string `json:"block_cache_usage"`
	BlockCachePinnedUsage string `json:"block_cache_pinned_usage"`
	BackgroundErrors      string `json:"background_errors"`
	NumLiveVersions       string `json:"num_live_versions"`
	NumSnapshots          string `json:"num_snapshots"`
}

// GetStorageStats retrieves RocksDB storage statistics
func (es *EventStore) GetStorageStats() *StorageStats {
	stats := &StorageStats{}

	stats.EstimatedNumKeys = es.db.GetProperty("rocksdb.estimate-num-keys")
	stats.EstimateLiveDataSize = es.db.GetProperty("rocksdb.estimate-live-data-size")
	stats.TotalSstFilesSize = es.db.GetProperty("rocksdb.total-sst-files-size")
	stats.LiveSstFilesSize = es.db.GetProperty("rocksdb.live-sst-files-size")
	stats.SizeAllMemTables = es.db.GetProperty("rocksdb.size-all-mem-tables")
	stats.CurSizeAllMemTables = es.db.GetProperty("rocksdb.cur-size-all-mem-tables")

	// Fallback to filesystem scan - RocksDB properties are unreliable
	diskSize := es.getSSTFileSizeFromDisk()
	if diskSize > 0 {
		diskSizeStr := fmt.Sprintf("%d", diskSize)
		stats.TotalSstFilesSize = diskSizeStr
		stats.LiveSstFilesSize = diskSizeStr
		stats.EstimateLiveDataSize = diskSizeStr
	}

	// Note: rocksdb.estimate-num-keys is often inaccurate with column families
	// The actual event count is shown separately via GetStats()

	stats.EstimatePendingCompactionBytes = es.db.GetProperty("rocksdb.estimate-pending-compaction-bytes")
	stats.NumRunningCompactions = es.db.GetProperty("rocksdb.num-running-compactions")
	stats.NumRunningFlushes = es.db.GetProperty("rocksdb.num-running-flushes")

	stats.NumFilesAtLevel0 = es.db.GetProperty("rocksdb.num-files-at-level0")
	stats.NumFilesAtLevel1 = es.db.GetProperty("rocksdb.num-files-at-level1")
	stats.NumFilesAtLevel2 = es.db.GetProperty("rocksdb.num-files-at-level2")
	stats.NumFilesAtLevel3 = es.db.GetProperty("rocksdb.num-files-at-level3")

	stats.BlockCacheUsage = es.db.GetProperty("rocksdb.block-cache-usage")
	stats.BlockCachePinnedUsage = es.db.GetProperty("rocksdb.block-cache-pinned-usage")

	stats.BackgroundErrors = es.db.GetProperty("rocksdb.background-errors")
	stats.NumLiveVersions = es.db.GetProperty("rocksdb.num-live-versions")
	stats.NumSnapshots = es.db.GetProperty("rocksdb.num-snapshots")

	return stats
}

// SnapshotStats holds numeric RocksDB stats for progress snapshots
type SnapshotStats struct {
	SSTFilesSizeBytes   int64 `json:"sst_files_size_bytes"`
	MemtableSizeBytes   int64 `json:"memtable_size_bytes"`
	EstimatedNumKeys    int64 `json:"estimated_num_keys"`
	PendingCompactBytes int64 `json:"pending_compact_bytes"`

	L0Files int `json:"l0_files"`
	L1Files int `json:"l1_files"`
	L2Files int `json:"l2_files"`
	L3Files int `json:"l3_files"`
	L4Files int `json:"l4_files"`
	L5Files int `json:"l5_files"`
	L6Files int `json:"l6_files"`

	RunningCompactions int  `json:"running_compactions"`
	CompactionPending  bool `json:"compaction_pending"`
}

// GetSnapshotStats returns numeric RocksDB stats for progress snapshots
func (es *EventStore) GetSnapshotStats() *SnapshotStats {
	stats := &SnapshotStats{}

	// Parse int64 from string property
	parseIntProp := func(val string) int64 {
		var n int64
		fmt.Sscanf(val, "%d", &n)
		return n
	}

	// Parse int from string property
	parseIntPropSmall := func(val string) int {
		var n int
		fmt.Sscanf(val, "%d", &n)
		return n
	}

	// RocksDB properties are unreliable - always use filesystem scan for SST size
	stats.SSTFilesSizeBytes = es.getSSTFileSizeFromDisk()
	stats.MemtableSizeBytes = parseIntProp(es.db.GetProperty("rocksdb.cur-size-all-mem-tables"))
	stats.EstimatedNumKeys = parseIntProp(es.db.GetProperty("rocksdb.estimate-num-keys"))
	stats.PendingCompactBytes = parseIntProp(es.db.GetProperty("rocksdb.estimate-pending-compaction-bytes"))

	// Level file counts - aggregate from all CFs
	for _, cf := range es.cfHandles {
		stats.L0Files += parseIntPropSmall(es.db.GetPropertyCF("rocksdb.num-files-at-level0", cf))
		stats.L1Files += parseIntPropSmall(es.db.GetPropertyCF("rocksdb.num-files-at-level1", cf))
		stats.L2Files += parseIntPropSmall(es.db.GetPropertyCF("rocksdb.num-files-at-level2", cf))
		stats.L3Files += parseIntPropSmall(es.db.GetPropertyCF("rocksdb.num-files-at-level3", cf))
		stats.L4Files += parseIntPropSmall(es.db.GetPropertyCF("rocksdb.num-files-at-level4", cf))
		stats.L5Files += parseIntPropSmall(es.db.GetPropertyCF("rocksdb.num-files-at-level5", cf))
		stats.L6Files += parseIntPropSmall(es.db.GetPropertyCF("rocksdb.num-files-at-level6", cf))
	}

	stats.RunningCompactions = parseIntPropSmall(es.db.GetProperty("rocksdb.num-running-compactions"))
	stats.CompactionPending = es.db.GetProperty("rocksdb.compaction-pending") == "1"

	return stats
}

// getSSTFileSizeFromDisk scans the database directory for SST files and returns total size
func (es *EventStore) getSSTFileSizeFromDisk() int64 {
	// Convert to absolute path for reliability
	absPath, err := filepath.Abs(es.dbPath)
	if err != nil {
		absPath = es.dbPath
	}

	var totalSize int64
	filepath.Walk(absPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sst") {
			totalSize += info.Size()
		}
		return nil
	})
	return totalSize
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

// CompactionResult holds before/after metrics from manual compaction
type CompactionResult struct {
	BeforeSSTBytes      int64   `json:"before_sst_bytes"`
	BeforeL0Files       int     `json:"before_l0_files"`
	BeforeTotalFiles    int     `json:"before_total_files"`
	AfterSSTBytes       int64   `json:"after_sst_bytes"`
	AfterL0Files        int     `json:"after_l0_files"`
	AfterTotalFiles     int     `json:"after_total_files"`
	BytesReclaimed      int64   `json:"bytes_reclaimed"`
	SpaceSavingsPercent float64 `json:"space_savings_percent"`
}

// CompactAll runs manual compaction on the entire database
func (es *EventStore) CompactAll() *CompactionResult {
	beforeStats := es.GetSnapshotStats()
	beforeFiles := beforeStats.L0Files + beforeStats.L1Files + beforeStats.L2Files +
		beforeStats.L3Files + beforeStats.L4Files + beforeStats.L5Files + beforeStats.L6Files

	result := &CompactionResult{
		BeforeSSTBytes:   beforeStats.SSTFilesSizeBytes,
		BeforeL0Files:    beforeStats.L0Files,
		BeforeTotalFiles: beforeFiles,
	}

	es.db.CompactRange(grocksdb.Range{Start: nil, Limit: nil})

	afterStats := es.GetSnapshotStats()
	afterFiles := afterStats.L0Files + afterStats.L1Files + afterStats.L2Files +
		afterStats.L3Files + afterStats.L4Files + afterStats.L5Files + afterStats.L6Files

	result.AfterSSTBytes = afterStats.SSTFilesSizeBytes
	result.AfterL0Files = afterStats.L0Files
	result.AfterTotalFiles = afterFiles
	result.BytesReclaimed = beforeStats.SSTFilesSizeBytes - afterStats.SSTFilesSizeBytes

	if beforeStats.SSTFilesSizeBytes > 0 {
		result.SpaceSavingsPercent = float64(result.BytesReclaimed) / float64(beforeStats.SSTFilesSizeBytes) * 100
	}

	return result
}

// UniqueIndexCounts holds counts from unique indexes
type UniqueIndexCounts struct {
	UniqueContracts int64 `json:"unique_contracts"`
	UniqueTopic0    int64 `json:"unique_topic0"`
	UniqueTopic1    int64 `json:"unique_topic1"`
	UniqueTopic2    int64 `json:"unique_topic2"`
	UniqueTopic3    int64 `json:"unique_topic3"`

	// Total event counts for each type
	TotalContractEvents int64 `json:"total_contract_events"`
	TotalTopic0Events   int64 `json:"total_topic0_events"`
	TotalTopic1Events   int64 `json:"total_topic1_events"`
	TotalTopic2Events   int64 `json:"total_topic2_events"`
	TotalTopic3Events   int64 `json:"total_topic3_events"`
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

// DistributionStats holds percentile statistics for event counts
type DistributionStats struct {
	Count int64      `json:"count"`           // Number of unique values
	Min   int64      `json:"min"`             // Minimum event count
	Max   int64      `json:"max"`             // Maximum event count
	Mean  float64    `json:"mean"`            // Average event count
	P50   int64      `json:"p50"`             // Median (50th percentile)
	P75   int64      `json:"p75"`             // 75th percentile
	P90   int64      `json:"p90"`             // 90th percentile
	P99   int64      `json:"p99"`             // 99th percentile
	Total int64      `json:"total"`           // Total events across all values
	TopN  []TopEntry `json:"top_n,omitempty"` // Top N by event count
}

// TopEntry represents a top item by event count
type TopEntry struct {
	Value      string `json:"value"`       // Base64-encoded value (contract ID or topic)
	EventCount int64  `json:"event_count"` // Number of events
}

// IndexDistribution holds distribution stats for all index types
type IndexDistribution struct {
	Contracts *DistributionStats `json:"contracts"`
	Topic0    *DistributionStats `json:"topic0"`
	Topic1    *DistributionStats `json:"topic1"`
	Topic2    *DistributionStats `json:"topic2"`
	Topic3    *DistributionStats `json:"topic3"`
}

// topNHeap is a min-heap for tracking top N entries by count
type topNHeap struct {
	entries []TopEntry
	maxSize int
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
	if len(h.entries) < h.maxSize {
		heap.Push(h, TopEntry{
			Value:      base64.StdEncoding.EncodeToString(value),
			EventCount: count,
		})
	} else if count > h.entries[0].EventCount {
		// Replace smallest if this is larger
		h.entries[0] = TopEntry{
			Value:      base64.StdEncoding.EncodeToString(value),
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
func (es *EventStore) GetIndexDistribution(topN int) (*IndexDistribution, error) {
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
func (es *EventStore) computeDistributionForType(indexType byte, topN int) (*DistributionStats, error) {
	const partitions = 16

	type partitionResult struct {
		counts []int64
		total  int64
		topN   []TopEntry
		err    error
	}

	results := make(chan partitionResult, partitions)
	var wg sync.WaitGroup

	// Scan each partition in parallel
	for p := 0; p < partitions; p++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			counts, total, top, err := es.scanDistributionPartition(indexType, partition, partitions, topN)
			results <- partitionResult{counts, total, top, err}
		}(p)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Merge results from all partitions
	var allCounts []int64
	var totalSum int64
	mergedTopN := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN)}

	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		allCounts = append(allCounts, r.counts...)
		totalSum += r.total

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
		Count: int64(len(allCounts)),
		Min:   allCounts[0],
		Max:   allCounts[len(allCounts)-1],
		Mean:  float64(totalSum) / float64(len(allCounts)),
		Total: totalSum,
		P50:   percentile(allCounts, 50),
		P75:   percentile(allCounts, 75),
		P90:   percentile(allCounts, 90),
		P99:   percentile(allCounts, 99),
		TopN:  mergedTopN.getSorted(),
	}, nil
}

// scanDistributionPartition scans a partition and returns counts for distribution
func (es *EventStore) scanDistributionPartition(indexType byte, partition, totalPartitions, topN int) ([]int64, int64, []TopEntry, error) {
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
	topHeap := &topNHeap{maxSize: topN, entries: make([]TopEntry, 0, topN)}

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 2 || key[0] != indexType {
			break
		}

		valueByte := key[1]
		if partition < totalPartitions-1 && valueByte >= endByte {
			break
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
		return nil, 0, nil, fmt.Errorf("iterator error for type %d partition %d: %w", indexType, partition, err)
	}

	return counts, total, topHeap.getSorted(), nil
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

// EventStats holds computed statistics from scanning all events
type EventStats struct {
	TotalEvents      int64 `json:"total_events"`
	UniqueContracts  int   `json:"unique_contracts"`
	UniqueTopic0     int   `json:"unique_topic0"`
	UniqueTopic1     int   `json:"unique_topic1"`
	UniqueTopic2     int   `json:"unique_topic2"`
	UniqueTopic3     int   `json:"unique_topic3"`
	ContractEvents   int64 `json:"contract_events"`
	SystemEvents     int64 `json:"system_events"`
	DiagnosticEvents int64 `json:"diagnostic_events"`
}

// ComputeEventStats scans all events and computes unique counts
func (es *EventStore) ComputeEventStats() (*EventStats, error) {
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

// ComputeEventStatsParallel computes event statistics using multiple goroutines
func (es *EventStore) ComputeEventStatsParallel(workers int) (*EventStats, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

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

// BuildUniqueIndexes scans all events and builds unique indexes with counts (one-time operation)
func (es *EventStore) BuildUniqueIndexes(workers int, progressFn func(processed int64)) error {
	if workers <= 0 {
		workers = runtime.NumCPU()
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

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	var totalProcessed int64

	for i := 0; i < workers; i++ {
		startLedger := minLedger + uint32(i)*ledgersPerWorker
		endLedger := startLedger + ledgersPerWorker - 1
		if i == workers-1 {
			endLedger = maxLedger
		}

		wg.Add(1)
		go func(start, end uint32) {
			defer wg.Done()
			processed, err := es.buildIndexesForRange(start, end)
			if err != nil {
				errCh <- err
				return
			}
			newTotal := atomic.AddInt64(&totalProcessed, processed)
			if progressFn != nil {
				progressFn(newTotal)
			}
		}(startLedger, endLedger)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// buildIndexesForRange builds unique indexes with counts for a ledger range
func (es *EventStore) buildIndexesForRange(startLedger, endLedger uint32) (int64, error) {
	startKey := eventKeyFromParts(startLedger, 0, 0, 0)
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	defer it.Close()

	var processed int64

	// Accumulate counts in memory
	counts := make(map[string]uint64)

	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if len(key) < 4 {
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

		if xdrEvent.ContractId != nil {
			uk := string(uniqueKey(UniqueTypeContract, xdrEvent.ContractId[:]))
			counts[uk]++
		}

		if xdrEvent.Body.V == 0 {
			body := xdrEvent.Body.MustV0()
			for i, topic := range body.Topics {
				topicBytes, err := topic.MarshalBinary()
				if err != nil {
					continue
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
				default:
					continue
				}
				uk := string(uniqueKey(uniqueType, topicBytes))
				counts[uk]++
			}
		}

		processed++
	}

	// Write all counts using merge operator (no reads needed!)
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for keyStr, count := range counts {
		keyBytes := []byte(keyStr)
		countBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(countBytes, count)
		batch.MergeCF(es.cfUnique, keyBytes, countBytes)
	}

	if err := es.db.Write(es.wo, batch); err != nil {
		return processed, fmt.Errorf("failed to write batch: %w", err)
	}

	if err := it.Err(); err != nil {
		return processed, fmt.Errorf("iterator error: %w", err)
	}

	return processed, nil
}

// AllStats combines all statistics
type AllStats struct {
	Database *DBStats      `json:"database"`
	Storage  *StorageStats `json:"storage"`
	Indexes  *IndexStats   `json:"indexes"`
}

// IndexStats holds per-index entry counts (for compatibility)
type IndexStats struct {
	EventsCount   int64 `json:"events_count"`
	ContractCount int64 `json:"contract_index_count"`
	TxHashCount   int64 `json:"txhash_index_count"`
	TypeCount     int64 `json:"type_index_count"`
	Topic0Count   int64 `json:"topic0_index_count"`
	Topic1Count   int64 `json:"topic1_index_count"`
	Topic2Count   int64 `json:"topic2_index_count"`
	Topic3Count   int64 `json:"topic3_index_count"`
}

// GetIndexStats counts entries in each index
func (es *EventStore) GetIndexStats() *IndexStats {
	stats := &IndexStats{}

	// Count events
	it := es.db.NewIteratorCF(es.ro, es.cfEvents)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		stats.EventsCount++
	}
	it.Close()

	return stats
}
