package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/linxGnu/grocksdb"
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

// Index key prefixes
const (
	PrefixEvents   = "events:"   // Primary: events:<ledger>:<tx>:<op>:<event>
	PrefixContract = "contract:" // contract:<contract_id>:<ledger>:<tx>:<op>:<event>
	PrefixTxHash   = "txhash:"   // txhash:<hash>:<op>:<event>
	PrefixType     = "type:"     // type:<event_type>:<ledger>:<tx>:<op>:<event>
	PrefixTopic0   = "topic0:"   // topic0:<topic_value>:<ledger>:<tx>:<op>:<event>
	PrefixTopic1   = "topic1:"   // topic1:<topic_value>:<ledger>:<tx>:<op>:<event>
	PrefixTopic2   = "topic2:"   // topic2:<topic_value>:<ledger>:<tx>:<op>:<event>
	PrefixTopic3   = "topic3:"   // topic3:<topic_value>:<ledger>:<tx>:<op>:<event>
	PrefixMeta     = "meta:"     // meta:<key>
)

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
}

// PrefixRawEvents is the prefix for minimal/raw event storage
const PrefixRawEvents = "raw:"

// minimalEventKey generates a 10-byte binary key for minimal events
// Format: [ledger:4][tx:2][op:2][event:2]
func minimalEventKey(e *MinimalEvent) []byte {
	key := make([]byte, 4+10) // "raw:" prefix + 10 bytes
	copy(key, PrefixRawEvents)
	binary.BigEndian.PutUint32(key[4:8], e.LedgerSequence)
	binary.BigEndian.PutUint16(key[8:10], e.TransactionIndex)
	binary.BigEndian.PutUint16(key[10:12], e.OperationIndex)
	binary.BigEndian.PutUint16(key[12:14], e.EventIndex)
	return key
}

// parseMinimalEventKey extracts position info from a binary key
func parseMinimalEventKey(key []byte) (ledger uint32, tx, op, event uint16) {
	if len(key) < 14 {
		return 0, 0, 0, 0
	}
	ledger = binary.BigEndian.Uint32(key[4:8])
	tx = binary.BigEndian.Uint16(key[8:10])
	op = binary.BigEndian.Uint16(key[10:12])
	event = binary.BigEndian.Uint16(key[12:14])
	return
}

// IndexConfig controls which secondary indexes to create
type IndexConfig struct {
	ContractID      bool
	TransactionHash bool
	EventType       bool
	Topics          bool // enables topic0-3
}

// DefaultIndexConfig returns config with all indexes enabled
func DefaultIndexConfig() *IndexConfig {
	return &IndexConfig{
		ContractID:      true,
		TransactionHash: true,
		EventType:       true,
		Topics:          true,
	}
}

// EventStore manages storing events in RocksDB
type EventStore struct {
	db      *grocksdb.DB
	wo      *grocksdb.WriteOptions
	ro      *grocksdb.ReadOptions
	indexes *IndexConfig
}

// NewEventStore creates a new event store with RocksDB backend using default options
func NewEventStore(dbPath string) (*EventStore, error) {
	return NewEventStoreWithOptions(dbPath, nil, nil)
}

// NewEventStoreWithOptions creates a new event store with custom RocksDB options
func NewEventStoreWithOptions(dbPath string, rocksOpts *RocksDBOptions, indexOpts *IndexConfig) (*EventStore, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// Apply custom options if provided
	if rocksOpts != nil {
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

		// Read performance - Block cache and bloom filter
		if rocksOpts.BlockCacheSizeMB > 0 || rocksOpts.BloomFilterBitsPerKey > 0 {
			bbto := grocksdb.NewDefaultBlockBasedTableOptions()

			if rocksOpts.BlockCacheSizeMB > 0 {
				cache := grocksdb.NewLRUCache(uint64(rocksOpts.BlockCacheSizeMB) * 1024 * 1024)
				bbto.SetBlockCache(cache)
			}

			if rocksOpts.BloomFilterBitsPerKey > 0 {
				filter := grocksdb.NewBloomFilter(float64(rocksOpts.BloomFilterBitsPerKey))
				bbto.SetFilterPolicy(filter)
			}

			bbto.SetCacheIndexAndFilterBlocks(rocksOpts.CacheIndexAndFilterBlocks)

			opts.SetBlockBasedTableFactory(bbto)
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
	} else {
		// Default compression
		opts.SetCompression(grocksdb.LZ4Compression)
	}

	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	// Create write options
	wo := grocksdb.NewDefaultWriteOptions()
	if rocksOpts != nil && rocksOpts.DisableWAL {
		wo.DisableWAL(true)
	}

	// Use provided index config or default
	indexes := indexOpts
	if indexes == nil {
		indexes = DefaultIndexConfig()
	}

	return &EventStore{
		db:      db,
		wo:      wo,
		ro:      grocksdb.NewDefaultReadOptions(),
		indexes: indexes,
	}, nil
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
	es.wo.Destroy()
	es.ro.Destroy()
	es.db.Close()
}

// primaryKey generates the primary key for an event
func primaryKey(event *ContractEvent) string {
	return fmt.Sprintf("%s%010d:%04d:%04d:%04d", PrefixEvents, event.LedgerSequence, event.TransactionIndex, event.OperationIndex, event.EventIndex)
}

// StoreEvents stores events with just binary key and raw XDR value
// This is the fastest ingestion mode - no JSON, no secondary indexes
func (es *EventStore) StoreEvents(events []*MinimalEvent) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, event := range events {
		key := minimalEventKey(event)
		batch.Put(key, event.RawXDR)
	}

	if err := es.db.Write(es.wo, batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	return nil
}

// StoreMinimalEventsMultiLedger stores events from multiple ledgers in a single batch
// Returns the total number of bytes written (for tracking raw data size)
func (es *EventStore) StoreMinimalEventsMultiLedger(events []*MinimalEvent) (int64, error) {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	var totalBytes int64
	for _, event := range events {
		key := minimalEventKey(event)
		batch.Put(key, event.RawXDR)
		totalBytes += int64(len(event.RawXDR))
	}

	if err := es.db.Write(es.wo, batch); err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	return totalBytes, nil
}

// getEventByPrimaryKey retrieves an event by its primary key
func (es *EventStore) getEventByPrimaryKey(pk string) (*ContractEvent, error) {
	value, err := es.db.Get(es.ro, []byte(pk))
	if err != nil {
		return nil, err
	}
	defer value.Free()

	if value.Size() == 0 {
		return nil, nil
	}

	var event ContractEvent
	if err := json.Unmarshal(value.Data(), &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}
	return &event, nil
}

// getEventsByIndex retrieves events using a secondary index prefix
func (es *EventStore) getEventsByIndex(prefix string, limit int) ([]*ContractEvent, error) {
	var events []*ContractEvent

	it := es.db.NewIterator(es.ro)
	defer it.Close()

	for it.Seek([]byte(prefix)); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		if !strings.HasPrefix(key, prefix) {
			break
		}

		// Value is the primary key
		pk := string(it.Value().Data())
		event, err := es.getEventByPrimaryKey(pk)
		if err != nil {
			return nil, err
		}
		if event != nil {
			events = append(events, event)
		}

		if limit > 0 && len(events) >= limit {
			break
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// SetLastProcessedLedger stores the last processed ledger sequence
func (es *EventStore) SetLastProcessedLedger(sequence uint32) error {
	key := []byte(PrefixMeta + "last_processed_ledger")
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, sequence)
	return es.db.Put(es.wo, key, value)
}

// GetLastProcessedLedger retrieves the last processed ledger sequence
func (es *EventStore) GetLastProcessedLedger() (uint32, error) {
	key := []byte(PrefixMeta + "last_processed_ledger")
	value, err := es.db.Get(es.ro, key)
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

	startKey := fmt.Sprintf("%s%010d:", PrefixEvents, startLedger)
	endKey := fmt.Sprintf("%s%010d:", PrefixEvents, endLedger+1)

	it := es.db.NewIterator(es.ro)
	defer it.Close()

	for it.Seek([]byte(startKey)); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		if key >= endKey {
			break
		}

		var event ContractEvent
		if err := json.Unmarshal(it.Value().Data(), &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %w", err)
		}
		events = append(events, &event)
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
// Query by Contract ID (Secondary Index)
// =============================================================================

// GetEventsByContractID retrieves all events for a specific contract ID
func (es *EventStore) GetEventsByContractID(contractID string, limit int) ([]*ContractEvent, error) {
	prefix := fmt.Sprintf("%s%s:", PrefixContract, contractID)
	return es.getEventsByIndex(prefix, limit)
}

// GetEventsByContractIDInRange retrieves events for a contract within a ledger range
func (es *EventStore) GetEventsByContractIDInRange(contractID string, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	var events []*ContractEvent

	startKey := fmt.Sprintf("%s%s:%010d:", PrefixContract, contractID, startLedger)
	endKey := fmt.Sprintf("%s%s:%010d:", PrefixContract, contractID, endLedger+1)

	it := es.db.NewIterator(es.ro)
	defer it.Close()

	for it.Seek([]byte(startKey)); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		if key >= endKey || !strings.HasPrefix(key, PrefixContract+contractID+":") {
			break
		}

		pk := string(it.Value().Data())
		event, err := es.getEventByPrimaryKey(pk)
		if err != nil {
			return nil, err
		}
		if event != nil {
			events = append(events, event)
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// =============================================================================
// Query by Transaction Hash (Secondary Index)
// =============================================================================

// GetEventsByTxHash retrieves all events for a specific transaction
func (es *EventStore) GetEventsByTxHash(txHash string) ([]*ContractEvent, error) {
	prefix := fmt.Sprintf("%s%s:", PrefixTxHash, txHash)
	return es.getEventsByIndex(prefix, 0)
}

// =============================================================================
// Query by Event Type (Secondary Index)
// =============================================================================

// GetEventsByType retrieves events by type (contract, system, diagnostic)
func (es *EventStore) GetEventsByType(eventType string, limit int) ([]*ContractEvent, error) {
	prefix := fmt.Sprintf("%s%s:", PrefixType, eventType)
	return es.getEventsByIndex(prefix, limit)
}

// GetEventsByTypeInRange retrieves events by type within a ledger range
func (es *EventStore) GetEventsByTypeInRange(eventType string, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	var events []*ContractEvent

	startKey := fmt.Sprintf("%s%s:%010d:", PrefixType, eventType, startLedger)
	endKey := fmt.Sprintf("%s%s:%010d:", PrefixType, eventType, endLedger+1)

	it := es.db.NewIterator(es.ro)
	defer it.Close()

	for it.Seek([]byte(startKey)); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		if key >= endKey || !strings.HasPrefix(key, PrefixType+eventType+":") {
			break
		}

		pk := string(it.Value().Data())
		event, err := es.getEventByPrimaryKey(pk)
		if err != nil {
			return nil, err
		}
		if event != nil {
			events = append(events, event)
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return events, nil
}

// =============================================================================
// Query by Topic (Secondary Index)
// =============================================================================

// GetEventsByTopic retrieves events by topic value at a specific position
// topicPosition: 0-3 for topic0-topic3
func (es *EventStore) GetEventsByTopic(topicPosition int, topicValue string, limit int) ([]*ContractEvent, error) {
	if topicPosition < 0 || topicPosition > 3 {
		return nil, fmt.Errorf("invalid topic position: must be 0-3")
	}

	prefixes := []string{PrefixTopic0, PrefixTopic1, PrefixTopic2, PrefixTopic3}
	prefix := fmt.Sprintf("%s%s:", prefixes[topicPosition], topicValue)
	return es.getEventsByIndex(prefix, limit)
}

// GetEventsByTopicInRange retrieves events by topic within a ledger range
func (es *EventStore) GetEventsByTopicInRange(topicPosition int, topicValue string, startLedger, endLedger uint32) ([]*ContractEvent, error) {
	if topicPosition < 0 || topicPosition > 3 {
		return nil, fmt.Errorf("invalid topic position: must be 0-3")
	}

	prefixes := []string{PrefixTopic0, PrefixTopic1, PrefixTopic2, PrefixTopic3}
	topicPrefix := prefixes[topicPosition]

	var events []*ContractEvent

	startKey := fmt.Sprintf("%s%s:%010d:", topicPrefix, topicValue, startLedger)
	endKey := fmt.Sprintf("%s%s:%010d:", topicPrefix, topicValue, endLedger+1)

	it := es.db.NewIterator(es.ro)
	defer it.Close()

	for it.Seek([]byte(startKey)); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		if key >= endKey || !strings.HasPrefix(key, topicPrefix+topicValue+":") {
			break
		}

		pk := string(it.Value().Data())
		event, err := es.getEventByPrimaryKey(pk)
		if err != nil {
			return nil, err
		}
		if event != nil {
			events = append(events, event)
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

	it := es.db.NewIterator(es.ro)
	defer it.Close()

	for it.Seek([]byte(PrefixEvents)); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		if !strings.HasPrefix(key, PrefixEvents) {
			break
		}
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
	// Size metrics
	EstimatedNumKeys     string `json:"estimated_num_keys"`
	EstimateLiveDataSize string `json:"estimate_live_data_size"`
	TotalSstFilesSize    string `json:"total_sst_files_size"`
	LiveSstFilesSize     string `json:"live_sst_files_size"`
	SizeAllMemTables     string `json:"size_all_mem_tables"`
	CurSizeAllMemTables  string `json:"cur_size_all_mem_tables"`

	// Compaction metrics
	EstimatePendingCompactionBytes string `json:"estimate_pending_compaction_bytes"`
	NumRunningCompactions          string `json:"num_running_compactions"`
	NumRunningFlushes              string `json:"num_running_flushes"`

	// Level metrics
	NumFilesAtLevel0 string `json:"num_files_at_level0"`
	NumFilesAtLevel1 string `json:"num_files_at_level1"`
	NumFilesAtLevel2 string `json:"num_files_at_level2"`
	NumFilesAtLevel3 string `json:"num_files_at_level3"`

	// Cache metrics
	BlockCacheUsage       string `json:"block_cache_usage"`
	BlockCachePinnedUsage string `json:"block_cache_pinned_usage"`

	// Other
	BackgroundErrors string `json:"background_errors"`
	NumLiveVersions  string `json:"num_live_versions"`
	NumSnapshots     string `json:"num_snapshots"`
}

// IndexStats holds per-index entry counts
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

// GetStorageStats retrieves RocksDB storage statistics
func (es *EventStore) GetStorageStats() *StorageStats {
	stats := &StorageStats{}

	// Size metrics
	stats.EstimatedNumKeys = es.db.GetProperty("rocksdb.estimate-num-keys")
	stats.EstimateLiveDataSize = es.db.GetProperty("rocksdb.estimate-live-data-size")
	stats.TotalSstFilesSize = es.db.GetProperty("rocksdb.total-sst-files-size")
	stats.LiveSstFilesSize = es.db.GetProperty("rocksdb.live-sst-files-size")
	stats.SizeAllMemTables = es.db.GetProperty("rocksdb.size-all-mem-tables")
	stats.CurSizeAllMemTables = es.db.GetProperty("rocksdb.cur-size-all-mem-tables")

	// Compaction metrics
	stats.EstimatePendingCompactionBytes = es.db.GetProperty("rocksdb.estimate-pending-compaction-bytes")
	stats.NumRunningCompactions = es.db.GetProperty("rocksdb.num-running-compactions")
	stats.NumRunningFlushes = es.db.GetProperty("rocksdb.num-running-flushes")

	// Level metrics
	stats.NumFilesAtLevel0 = es.db.GetProperty("rocksdb.num-files-at-level0")
	stats.NumFilesAtLevel1 = es.db.GetProperty("rocksdb.num-files-at-level1")
	stats.NumFilesAtLevel2 = es.db.GetProperty("rocksdb.num-files-at-level2")
	stats.NumFilesAtLevel3 = es.db.GetProperty("rocksdb.num-files-at-level3")

	// Cache metrics
	stats.BlockCacheUsage = es.db.GetProperty("rocksdb.block-cache-usage")
	stats.BlockCachePinnedUsage = es.db.GetProperty("rocksdb.block-cache-pinned-usage")

	// Other
	stats.BackgroundErrors = es.db.GetProperty("rocksdb.background-errors")
	stats.NumLiveVersions = es.db.GetProperty("rocksdb.num-live-versions")
	stats.NumSnapshots = es.db.GetProperty("rocksdb.num-snapshots")

	return stats
}

// GetIndexStats counts entries in each index
func (es *EventStore) GetIndexStats() *IndexStats {
	stats := &IndexStats{}

	prefixes := []struct {
		prefix string
		count  *int64
	}{
		{PrefixEvents, &stats.EventsCount},
		{PrefixContract, &stats.ContractCount},
		{PrefixTxHash, &stats.TxHashCount},
		{PrefixType, &stats.TypeCount},
		{PrefixTopic0, &stats.Topic0Count},
		{PrefixTopic1, &stats.Topic1Count},
		{PrefixTopic2, &stats.Topic2Count},
		{PrefixTopic3, &stats.Topic3Count},
	}

	for _, p := range prefixes {
		it := es.db.NewIterator(es.ro)
		for it.Seek([]byte(p.prefix)); it.Valid(); it.Next() {
			key := string(it.Key().Data())
			if !strings.HasPrefix(key, p.prefix) {
				break
			}
			*p.count++
		}
		it.Close()
	}

	return stats
}

// AllStats combines all statistics
type AllStats struct {
	Database *DBStats      `json:"database"`
	Storage  *StorageStats `json:"storage"`
	Indexes  *IndexStats   `json:"indexes"`
}

// GetStats returns statistics about the event database
func (es *EventStore) GetStats() (*DBStats, error) {
	stats := &DBStats{}

	it := es.db.NewIterator(es.ro)
	defer it.Close()

	contracts := make(map[string]struct{})
	firstEvent := true

	for it.Seek([]byte(PrefixEvents)); it.Valid(); it.Next() {
		key := string(it.Key().Data())
		if !strings.HasPrefix(key, PrefixEvents) {
			break
		}

		stats.TotalEvents++

		// Parse ledger sequence from key: events:0000012345:0001:0001:0001
		var ledgerSeq uint32
		if _, err := fmt.Sscanf(key, PrefixEvents+"%010d:", &ledgerSeq); err == nil {
			if firstEvent {
				stats.MinLedger = ledgerSeq
				stats.MaxLedger = ledgerSeq
				firstEvent = false
			} else {
				if ledgerSeq < stats.MinLedger {
					stats.MinLedger = ledgerSeq
				}
				if ledgerSeq > stats.MaxLedger {
					stats.MaxLedger = ledgerSeq
				}
			}
		}

		// Count unique contracts
		var event ContractEvent
		if err := json.Unmarshal(it.Value().Data(), &event); err == nil {
			if event.ContractID != "" {
				contracts[event.ContractID] = struct{}{}
			}
		}
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	stats.UniqueContracts = len(contracts)
	stats.LastProcessed, _ = es.GetLastProcessedLedger()

	return stats, nil
}

// SnapshotStats holds numeric RocksDB stats for progress snapshots
type SnapshotStats struct {
	// Size metrics (in bytes)
	SSTFilesSizeBytes   int64 `json:"sst_files_size_bytes"`
	MemtableSizeBytes   int64 `json:"memtable_size_bytes"`
	EstimatedNumKeys    int64 `json:"estimated_num_keys"`
	PendingCompactBytes int64 `json:"pending_compact_bytes"`

	// Level metrics
	L0Files int `json:"l0_files"`
	L1Files int `json:"l1_files"`
	L2Files int `json:"l2_files"`
	L3Files int `json:"l3_files"`
	L4Files int `json:"l4_files"`
	L5Files int `json:"l5_files"`
	L6Files int `json:"l6_files"`

	// Compaction state
	RunningCompactions int  `json:"running_compactions"`
	CompactionPending  bool `json:"compaction_pending"`
}

// GetSnapshotStats returns numeric RocksDB stats for progress snapshots
func (es *EventStore) GetSnapshotStats() *SnapshotStats {
	stats := &SnapshotStats{}

	// Parse string properties to int64
	parseIntProp := func(prop string) int64 {
		val := es.db.GetProperty(prop)
		var n int64
		fmt.Sscanf(val, "%d", &n)
		return n
	}

	parseIntPropSmall := func(prop string) int {
		val := es.db.GetProperty(prop)
		var n int
		fmt.Sscanf(val, "%d", &n)
		return n
	}

	// Size metrics
	stats.SSTFilesSizeBytes = parseIntProp("rocksdb.total-sst-files-size")
	stats.MemtableSizeBytes = parseIntProp("rocksdb.cur-size-all-mem-tables")
	stats.EstimatedNumKeys = parseIntProp("rocksdb.estimate-num-keys")
	stats.PendingCompactBytes = parseIntProp("rocksdb.estimate-pending-compaction-bytes")

	// Level metrics
	stats.L0Files = parseIntPropSmall("rocksdb.num-files-at-level0")
	stats.L1Files = parseIntPropSmall("rocksdb.num-files-at-level1")
	stats.L2Files = parseIntPropSmall("rocksdb.num-files-at-level2")
	stats.L3Files = parseIntPropSmall("rocksdb.num-files-at-level3")
	stats.L4Files = parseIntPropSmall("rocksdb.num-files-at-level4")
	stats.L5Files = parseIntPropSmall("rocksdb.num-files-at-level5")
	stats.L6Files = parseIntPropSmall("rocksdb.num-files-at-level6")

	// Compaction state
	stats.RunningCompactions = parseIntPropSmall("rocksdb.num-running-compactions")
	stats.CompactionPending = es.db.GetProperty("rocksdb.compaction-pending") == "1"

	return stats
}

// CompactionResult holds before/after metrics from manual compaction
type CompactionResult struct {
	// Before compaction
	BeforeSSTBytes   int64 `json:"before_sst_bytes"`
	BeforeL0Files    int   `json:"before_l0_files"`
	BeforeTotalFiles int   `json:"before_total_files"`

	// After compaction
	AfterSSTBytes   int64 `json:"after_sst_bytes"`
	AfterL0Files    int   `json:"after_l0_files"`
	AfterTotalFiles int   `json:"after_total_files"`

	// Savings
	BytesReclaimed      int64   `json:"bytes_reclaimed"`
	SpaceSavingsPercent float64 `json:"space_savings_percent"`
}

// CompactAll runs manual compaction on the entire database
// This is useful after bulk ingestion to optimize for reads
func (es *EventStore) CompactAll() *CompactionResult {
	// Get before stats
	beforeStats := es.GetSnapshotStats()
	beforeFiles := beforeStats.L0Files + beforeStats.L1Files + beforeStats.L2Files +
		beforeStats.L3Files + beforeStats.L4Files + beforeStats.L5Files + beforeStats.L6Files

	result := &CompactionResult{
		BeforeSSTBytes:   beforeStats.SSTFilesSizeBytes,
		BeforeL0Files:    beforeStats.L0Files,
		BeforeTotalFiles: beforeFiles,
	}

	// Run compaction on full range (nil, nil = entire database)
	es.db.CompactRange(grocksdb.Range{Start: nil, Limit: nil})

	// Get after stats
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
