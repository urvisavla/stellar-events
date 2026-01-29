package store

// Store defines the interface for event storage backends.
// Implementations must be safe for concurrent use.
type Store interface {
	// ==========================================================================
	// Lifecycle
	// ==========================================================================

	// Close releases all resources held by the store.
	Close()

	// Flush forces any buffered data to persistent storage.
	Flush() error

	// ==========================================================================
	// Write Operations
	// ==========================================================================

	// StoreEvents stores events with optional index updates based on options.
	// Returns the number of bytes written.
	StoreEvents(events []*IngestEvent, opts *StoreOptions) (int64, error)

	// SetLastProcessedLedger stores the last processed ledger sequence.
	SetLastProcessedLedger(sequence uint32) error

	// FlushBitmapIndexes flushes all hot bitmap segments to disk.
	FlushBitmapIndexes() error

	// ==========================================================================
	// Read Operations - Primary Index (Ledger)
	// ==========================================================================

	// GetLastProcessedLedger retrieves the last processed ledger sequence.
	GetLastProcessedLedger() (uint32, error)

	// GetEventsByLedger retrieves all events for a specific ledger.
	GetEventsByLedger(ledgerSequence uint32) ([]*ContractEvent, error)

	// GetEventsByLedgerRange retrieves all events within a ledger range.
	GetEventsByLedgerRange(startLedger, endLedger uint32) ([]*ContractEvent, error)

	// ==========================================================================
	// Read Operations - Secondary Indexes
	// ==========================================================================

	// GetEventsByContractID retrieves events for a contract (full scan).
	GetEventsByContractID(contractID []byte, limit int) ([]*ContractEvent, error)

	// GetEventsByContractIDInRange retrieves events for a contract within a ledger range.
	GetEventsByContractIDInRange(contractID []byte, startLedger, endLedger uint32) ([]*ContractEvent, error)

	// GetEventsByTopic retrieves events with a specific topic (full scan).
	GetEventsByTopic(position int, topicValue []byte, limit int) ([]*ContractEvent, error)

	// GetEventsByTopicInRange retrieves events with a specific topic within a ledger range.
	GetEventsByTopicInRange(position int, topicValue []byte, startLedger, endLedger uint32) ([]*ContractEvent, error)

	// ==========================================================================
	// Bitmap-Accelerated Queries
	// ==========================================================================

	// GetEventsByContractIDBitmap retrieves events using bitmap index.
	GetEventsByContractIDBitmap(contractID []byte, startLedger, endLedger uint32, limit int) (*QueryResult, error)

	// GetEventsByTopicBitmap retrieves events with a specific topic using bitmap index.
	GetEventsByTopicBitmap(position int, topicValue []byte, startLedger, endLedger uint32, limit int) (*QueryResult, error)

	// GetEventsWithFilter retrieves events matching a filter using bitmap indexes.
	GetEventsWithFilter(filter *QueryFilter, startLedger, endLedger uint32, limit int) (*QueryResult, error)

	// GetEventsWithFilterHierarchical uses two-level bitmap for precise event lookup.
	GetEventsWithFilterHierarchical(filter *QueryFilter, startLedger, endLedger uint32, limit int) (*HierarchicalQueryResult, error)

	// CountEventsByContractBitmap counts events for a contract using bitmap index.
	CountEventsByContractBitmap(contractID []byte, startLedger, endLedger uint32) (uint64, error)

	// CountEventsByTopicBitmap counts events with a topic using bitmap index.
	CountEventsByTopicBitmap(position int, topicValue []byte, startLedger, endLedger uint32) (uint64, error)

	// ==========================================================================
	// Statistics
	// ==========================================================================

	// CountEvents returns the total number of events stored.
	CountEvents() (int64, error)

	// GetStats returns database statistics.
	GetStats() (*DBStats, error)

	// GetStorageStats returns storage-level statistics.
	GetStorageStats() *StorageStats

	// GetSnapshotStats returns numeric stats for progress snapshots (fast path, L0 only).
	GetSnapshotStats() *SnapshotStats

	// GetDetailedSnapshotStats returns full stats including all level file counts.
	GetDetailedSnapshotStats() *SnapshotStats

	// GetIndexStats returns per-index entry counts.
	GetIndexStats() *IndexStats

	// GetBitmapStats returns bitmap index statistics (may return nil if not available).
	GetBitmapStats() *BitmapStats

	// CountUniqueIndexes counts entries in unique indexes.
	CountUniqueIndexes() (*UniqueIndexCounts, error)

	// GetIndexDistribution returns distribution stats for all index types.
	GetIndexDistribution(topN int) (*IndexDistribution, error)

	// ComputeEventStats scans all events and computes unique counts.
	// Workers controls parallelism: 0 or 1 for single-threaded, >1 for parallel.
	ComputeEventStats(workers int) (*EventStats, error)

	// ==========================================================================
	// Maintenance
	// ==========================================================================

	// CompactAll runs manual compaction on the entire database.
	CompactAll() *CompactionResult

	// BuildUniqueIndexes rebuilds unique indexes from existing events.
	BuildUniqueIndexes(workers int, progressFn func(processed int64)) error
}
