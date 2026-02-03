// Package store provides event storage interfaces and implementations.
package store

import "time"

// =============================================================================
// Event Types (Write Path)
// =============================================================================

// IngestEvent represents an event captured during ingestion.
// Used for fast ingestion - no JSON serialization overhead.
// Pre-extracted fields avoid re-parsing XDR during indexing and querying.
type IngestEvent struct {
	LedgerSequence   uint32
	TransactionIndex uint32 // Changed from uint16: TOID uses 20 bits (0-1,048,575)
	OperationIndex   uint16 // TOID uses 12 bits (0-4,095)
	EventIndex       uint16
	RawXDR           []byte

	// Pre-extracted fields for indexing (avoids re-parsing XDR)
	ContractID []byte   // 32 bytes if present, nil otherwise
	Topics     [][]byte // Pre-marshaled topic XDR bytes
	TxHash     []byte   // 32 bytes - transaction hash

	// Pre-extracted fields for binary format storage (avoids XDR parsing at query time)
	EventType      int       // 0=contract, 1=system, 2=diagnostic
	DataBytes      []byte    // Pre-marshaled SCVal data bytes
	LedgerClosedAt time.Time // Ledger close time for the event
	Successful     bool      // True if event is from a successful contract call
}

// StoreOptions configures what indexes to update when storing events.
type StoreOptions struct {
	UniqueIndexes      bool // Maintain unique value indexes with counts
	BitmapIndexes      bool // Maintain 32-bit roaring bitmap indexes (ledger-level)
	Bitmap64Indexes    bool // Maintain 64-bit roaring bitmap indexes (event-level)
	PostingListIndexes bool // Maintain posting list indexes for contracts and topics

	// ExcludeTopic0 is a set of topic0 values (as strings) to skip during ingestion.
	// Events with matching topic0 will not be stored.
	ExcludeTopic0 map[string]struct{}

	// ExcludeDiagnostic skips diagnostic events during ingestion.
	ExcludeDiagnostic bool
}

// =============================================================================
// Event Types (Read Path)
// =============================================================================

// ContractEvent represents a fully decoded contract event for JSON output.
// This is the read-path structure returned by queries.
type ContractEvent struct {
	LedgerSequence   uint32   `json:"ledger_sequence"`
	TransactionIndex int      `json:"transaction_index"`
	OperationIndex   int      `json:"operation_index"`
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

	// Key size metrics (for bitmap index truncation analysis)
	Over32Bytes int64 `json:"over_32_bytes"` // Count of unique values > 32 bytes
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
	TopicIndexCount    int64  `json:"topic_index_count"` // All topics (non-positional)
}

// LedgerTxStats holds statistics about transactions per ledger.
type LedgerTxStats struct {
	TotalLedgers      int64 `json:"total_ledgers"`
	LedgersOver10kTx  int64 `json:"ledgers_over_10k_tx"`
}

// =============================================================================
// Unified Query Result (for all index types)
// =============================================================================

// UnifiedQueryResult holds query results in a common format for all index types.
// This enables consistent output and comparison across Posting List, Bitmap32, and Bitmap64.
type UnifiedQueryResult struct {
	// Index identification
	IndexType string // "posting", "bitmap32", or "bitmap64"

	// Ledger range
	LedgerRange   uint32 // endLedger - startLedger + 1
	IndexMatches  int    // Matches from index (ledgers, keys, or TOIDs depending on type)
	MatchUnitName string // "ledgers", "event keys", or "TOIDs" - describes what IndexMatches counts

	// Event stats
	EventsScanned  int // Events scanned from storage
	EventsReturned int // Events returned after filtering

	// I/O stats
	IndexBytesRead int64 // Bytes read from index
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	IndexLookupTime time.Duration // Time querying index
	EventFetchTime  time.Duration // Time fetching events from storage
	DecodeTime      time.Duration // Time decoding/unmarshalling events
	FilterTime      time.Duration // Time filtering events (post-fetch)
	TotalTime       time.Duration // Total query time
}

// =============================================================================
// Bitmap Query Result (32-bit, ledger-level)
// =============================================================================

// BitmapQueryResult holds detailed results from a 32-bit bitmap query.
type BitmapQueryResult struct {
	// Ledger range
	LedgerRange     uint32 // endLedger - startLedger + 1
	MatchingLedgers int    // Ledgers matching index query

	// Index stats
	SegmentsScanned int   // Number of segments scanned
	IndexBytesRead  int64 // Bytes read from bitmap index

	// Event fetch stats
	EventsScanned  int   // Events scanned from storage
	EventsReturned int   // Events returned after filtering
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	IndexLookupTime time.Duration // Time querying bitmap index
	EventFetchTime  time.Duration // Time fetching events (total)
	DiskReadTime    time.Duration // Time reading from disk
	DecodeTime      time.Duration // Time decoding/unmarshalling
	FilterTime      time.Duration // Time filtering events
	TotalTime       time.Duration // Total query time
}

// ToUnified converts BitmapQueryResult to UnifiedQueryResult
func (r *BitmapQueryResult) ToUnified() *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:       "bitmap32",
		LedgerRange:     r.LedgerRange,
		IndexMatches:    r.MatchingLedgers,
		MatchUnitName:   "ledgers",
		EventsScanned:   r.EventsScanned,
		EventsReturned:  r.EventsReturned,
		IndexBytesRead:  r.IndexBytesRead,
		EventBytesRead:  r.EventBytesRead,
		IndexLookupTime: r.IndexLookupTime,
		EventFetchTime:  r.EventFetchTime,
		DecodeTime:      r.DecodeTime,
		FilterTime:      r.FilterTime,
		TotalTime:       r.TotalTime,
	}
}

// =============================================================================
// Bitmap64 Query Result (64-bit, TOID-level)
// =============================================================================

// Bitmap64QueryResult holds detailed results from a 64-bit bitmap query.
// Bitmap64 now stores TOIDs (operation-level granularity), same as posting list.
type Bitmap64QueryResult struct {
	// Ledger range
	LedgerRange   uint32 // endLedger - startLedger + 1
	MatchingTOIDs int    // TOIDs matching index query

	// Index stats
	SegmentsScanned int   // Number of segments scanned
	IndexBytesRead  int64 // Bytes read from bitmap index

	// Event fetch stats
	EventsScanned  int   // Events scanned from storage
	EventsReturned int   // Events returned
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	IndexLookupTime time.Duration // Time querying bitmap index
	EventFetchTime  time.Duration // Time fetching events
	DecodeTime      time.Duration // Time decoding events
	TotalTime       time.Duration // Total query time
}

// ToUnified converts Bitmap64QueryResult to UnifiedQueryResult
func (r *Bitmap64QueryResult) ToUnified() *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:       "bitmap64",
		LedgerRange:     r.LedgerRange,
		IndexMatches:    r.MatchingTOIDs,
		MatchUnitName:   "TOIDs",
		EventsScanned:   r.EventsScanned,
		EventsReturned:  r.EventsReturned,
		IndexBytesRead:  r.IndexBytesRead,
		EventBytesRead:  r.EventBytesRead,
		IndexLookupTime: r.IndexLookupTime,
		EventFetchTime:  r.EventFetchTime,
		DecodeTime:      r.DecodeTime,
		FilterTime:      0, // bitmap64 doesn't have separate filter time
		TotalTime:       r.TotalTime,
	}
}

// =============================================================================
// Posting List Query Result
// =============================================================================

// PostingListQueryResult holds detailed results from a posting list query.
type PostingListQueryResult struct {
	// Ledger range
	LedgerRange uint32 // endLedger - startLedger + 1

	// Posting list stats
	BucketsScanned      int   // Number of bucket ranges scanned
	PostingListsRead    int   // Number of posting list keys read
	PostingListBytes    int64 // Total bytes read from posting lists
	TOIDsInPostingList  int   // Total TOIDs in posting lists (before decoding)
	TOIDsDecoded        int   // TOIDs actually decoded (shows early termination effect)
	TOIDsFromContract   int   // TOIDs from contract posting list (in range)
	TOIDsFromTopics     int   // TOIDs from topic posting lists (in range)
	TOIDsAfterIntersect int   // TOIDs after intersection (final count)

	// Event fetch stats
	UniqueLedgers  int   // Unique ledgers containing matching events
	EventsScanned  int   // Events scanned from storage
	EventsReturned int   // Events returned after filtering
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	PostingListTime time.Duration // Time reading posting lists
	IntersectTime   time.Duration // Time intersecting TOID lists
	EventFetchTime  time.Duration // Time fetching events
	DecodeTime      time.Duration // Time decoding events
	FilterTime      time.Duration // Time filtering events by contract/topics
	TotalTime       time.Duration // Total query time
}

// ToUnified converts PostingListQueryResult to UnifiedQueryResult
func (r *PostingListQueryResult) ToUnified() *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:       "posting",
		LedgerRange:     r.LedgerRange,
		IndexMatches:    r.TOIDsAfterIntersect,
		MatchUnitName:   "TOIDs",
		EventsScanned:   r.EventsScanned,
		EventsReturned:  r.EventsReturned,
		IndexBytesRead:  r.PostingListBytes,
		EventBytesRead:  r.EventBytesRead,
		IndexLookupTime: r.PostingListTime + r.IntersectTime, // Include intersect time in index lookup
		EventFetchTime:  r.EventFetchTime,
		DecodeTime:      r.DecodeTime,
		FilterTime:      r.FilterTime,
		TotalTime:       r.TotalTime,
	}
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
	BitmapIndexes       bool // Build bitmap indexes (ledger-level)
	IndexFlushInterval int  // Ledgers between index flushes (0 = only at end)
}

// indexEntry holds extracted data for index updates (used by collector pattern)
type indexEntry struct {
	Ledger     uint32
	TxIdx      uint16
	OpIdx      uint16
	EventIdx   uint16
	ContractID []byte   // nil if no contract
	Topics     [][]byte // up to 4 topics
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
