// Package store provides event storage interfaces and implementations.
package store

import "time"

// =============================================================================
// Event Types (Write Path)
// =============================================================================

// IngestEvent represents an event captured during ingestion.
// Used for fast ingestion - no JSON serialization overhead.
// Pre-extracted fields avoid re-parsing XDR during indexing.
type IngestEvent struct {
	LedgerSequence   uint32
	TransactionIndex uint16
	OperationIndex   uint16
	EventIndex       uint16
	RawXDR           []byte

	// Pre-extracted fields for indexing (avoids re-parsing XDR)
	ContractID []byte   // 32 bytes if present, nil otherwise
	Topics     [][]byte // Pre-marshaled topic XDR bytes
	TxHash     []byte   // 32 bytes - transaction hash
}

// StoreOptions configures what indexes to update when storing events.
type StoreOptions struct {
	UniqueIndexes bool // Maintain unique value indexes with counts
	BitmapIndexes bool // Maintain roaring bitmap indexes for fast queries
	L2Indexes     bool // Maintain L2 hierarchical indexes for precise lookups
}

// =============================================================================
// Event Types (Read Path)
// =============================================================================

// ContractEvent represents a fully decoded contract event for JSON output.
// This is the read-path structure returned by queries.
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

// =============================================================================
// Query Types
// =============================================================================

// QueryFilter defines a filter for bitmap-accelerated queries.
type QueryFilter struct {
	ContractID []byte // Filter by contract ID (32 bytes)
	Topic0     []byte // Filter by topic at position 0
	Topic1     []byte // Filter by topic at position 1
	Topic2     []byte // Filter by topic at position 2
	Topic3     []byte // Filter by topic at position 3
}

// QueryResult holds the result of a bitmap-accelerated query.
type QueryResult struct {
	Events          []*ContractEvent
	MatchingLedgers uint64 // Number of ledgers that matched the filter
	EventsScanned   int64  // Number of events scanned
	EventsReturned  int64  // Number of events returned

	// Timing breakdown
	BitmapLookupTime time.Duration // Time to query bitmap index
	EventFetchTime   time.Duration // Time to fetch events from matching ledgers
	FilterTime       time.Duration // Time spent filtering events
	TotalTime        time.Duration // Total query time

	// Additional stats
	LedgerRange        uint32   // Number of ledgers in query range
	SegmentsQueried    int      // Number of bitmap segments queried
	MatchingLedgerSeqs []uint32 // Actual ledger sequences that matched
}

// HierarchicalQueryResult holds the result of a hierarchical bitmap query.
type HierarchicalQueryResult struct {
	Events []*ContractEvent

	// Timing breakdown
	L1LookupTime   time.Duration // Level 1: find matching ledgers
	L2LookupTime   time.Duration // Level 2: find matching events per ledger
	EventFetchTime time.Duration // Fetch events by exact keys
	TotalTime      time.Duration

	// Stats
	MatchingLedgers int
	MatchingEvents  int
	EventsFetched   int
}

// =============================================================================
// Statistics Types
// =============================================================================

// DBStats holds statistics about the event database.
type DBStats struct {
	TotalEvents     int64  `json:"total_events"`
	MinLedger       uint32 `json:"min_ledger"`
	MaxLedger       uint32 `json:"max_ledger"`
	LastProcessed   uint32 `json:"last_processed_ledger"`
	UniqueContracts int    `json:"unique_contracts"`
}

// ColumnFamilyStats holds storage stats for a single column family.
type ColumnFamilyStats struct {
	Name           string `json:"name"`
	EstimatedKeys  uint64 `json:"estimated_keys"`
	SSTFilesBytes  uint64 `json:"sst_files_bytes"`
	MemtableBytes  uint64 `json:"memtable_bytes"`
	PendingCompact uint64 `json:"pending_compact_bytes"`
	NumFiles       int    `json:"num_files"`
}

// StorageSnapshot holds storage stats for all column families at a point in time.
type StorageSnapshot struct {
	Timestamp      time.Time                     `json:"timestamp"`
	ColumnFamilies map[string]*ColumnFamilyStats `json:"column_families"`
	TotalSST       uint64                        `json:"total_sst_bytes"`
	TotalMemtable  uint64                        `json:"total_memtable_bytes"`
	TotalFiles     int                           `json:"total_files"`
}

// CFCompactionResult holds compaction results for one column family.
type CFCompactionResult struct {
	Name           string  `json:"name"`
	BeforeBytes    uint64  `json:"before_bytes"`
	AfterBytes     uint64  `json:"after_bytes"`
	Reclaimed      uint64  `json:"reclaimed_bytes"`
	SavingsPercent float64 `json:"savings_percent"`
}

// CompactionSummary holds complete compaction results with per-CF breakdown.
type CompactionSummary struct {
	Before         *StorageSnapshot               `json:"before"`
	After          *StorageSnapshot               `json:"after"`
	Duration       time.Duration                  `json:"duration"`
	PerCF          map[string]*CFCompactionResult `json:"per_cf"`
	TotalReclaimed uint64                         `json:"total_reclaimed"`
	SavingsPercent float64                        `json:"savings_percent"`
}

// =============================================================================
// Index Statistics Types
// =============================================================================

// UniqueIndexCounts holds counts from unique indexes.
type UniqueIndexCounts struct {
	UniqueContracts int64 `json:"unique_contracts"`
	UniqueTopic0    int64 `json:"unique_topic0"`
	UniqueTopic1    int64 `json:"unique_topic1"`
	UniqueTopic2    int64 `json:"unique_topic2"`
	UniqueTopic3    int64 `json:"unique_topic3"`

	TotalContractEvents int64 `json:"total_contract_events"`
	TotalTopic0Events   int64 `json:"total_topic0_events"`
	TotalTopic1Events   int64 `json:"total_topic1_events"`
	TotalTopic2Events   int64 `json:"total_topic2_events"`
	TotalTopic3Events   int64 `json:"total_topic3_events"`
}

// DistributionStats holds percentile statistics for event counts.
type DistributionStats struct {
	Count int64      `json:"count"`
	Min   int64      `json:"min"`
	Max   int64      `json:"max"`
	Mean  float64    `json:"mean"`
	P50   int64      `json:"p50"`
	P75   int64      `json:"p75"`
	P90   int64      `json:"p90"`
	P99   int64      `json:"p99"`
	Total int64      `json:"total"`
	TopN  []TopEntry `json:"top_n,omitempty"`
}

// TopEntry represents a top item by event count.
type TopEntry struct {
	Value      string `json:"value"`
	EventCount int64  `json:"event_count"`
}

// IndexDistribution holds distribution stats for all index types.
type IndexDistribution struct {
	Contracts *DistributionStats `json:"contracts"`
	Topic0    *DistributionStats `json:"topic0"`
	Topic1    *DistributionStats `json:"topic1"`
	Topic2    *DistributionStats `json:"topic2"`
	Topic3    *DistributionStats `json:"topic3"`
}

// EventStats holds computed statistics from scanning all events.
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

// BitmapStats holds statistics about bitmap indexes.
type BitmapStats struct {
	CurrentSegmentID   uint32 `json:"current_segment_id"`
	HotSegmentCount    int    `json:"hot_segment_count"`
	HotSegmentCards    uint64 `json:"hot_segment_cards"`
	HotSegmentMemBytes uint64 `json:"hot_segment_mem_bytes"`
	ContractIndexCount int64  `json:"contract_index_count"`
	Topic0IndexCount   int64  `json:"topic0_index_count"`
	Topic1IndexCount   int64  `json:"topic1_index_count"`
	Topic2IndexCount   int64  `json:"topic2_index_count"`
	Topic3IndexCount   int64  `json:"topic3_index_count"`
}

// =============================================================================
// Benchmark Types
// =============================================================================

// FetchBenchmarkResult holds comparison results between storage backends.
type FetchBenchmarkResult struct {
	// Bitmap phase (same for both)
	BitmapLookupTime time.Duration
	MatchingLedgers  int
	LedgerSeqs       []uint32

	// Primary storage fetch
	StoreFetchTime time.Duration
	StoreEvents    int
	StoreBytesRead int64

	// Ledger file fetch (for comparison)
	LedgerFetchTime      time.Duration
	LedgerEvents         int
	LedgerBytesRead      int64
	LedgerDiskReadTime   time.Duration
	LedgerDecompressTime time.Duration
	LedgerUnmarshalTime  time.Duration
}

// =============================================================================
// Configuration Types
// =============================================================================

// IndexConfig controls which secondary indexes to create.
type IndexConfig struct {
	ContractID bool
	Topics     bool // enables topic0-3
}

// BuildIndexOptions controls which indexes to build during rebuild.
type BuildIndexOptions struct {
	UniqueIndexes       bool // Build unique value counts (for stats)
	BitmapIndexes       bool // Build L1 bitmap indexes (ledger-level)
	L2Indexes           bool // Build L2 bitmap indexes (event-level)
	BitmapFlushInterval int  // Ledgers between bitmap flushes (0 = only at end)
}

// DefaultIndexConfig returns config with all indexes enabled.
func DefaultIndexConfig() *IndexConfig {
	return &IndexConfig{
		ContractID: true,
		Topics:     true,
	}
}

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
