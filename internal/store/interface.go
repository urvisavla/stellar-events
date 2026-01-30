package store

import (
	"github.com/urvisavla/stellar-events/internal/query"
)

// =============================================================================
// EventStore Interface
// =============================================================================

// EventStore defines the interface for event storage backends.
// It focuses on event storage and retrieval - query orchestration is handled
// by query.Engine using the EventReader interface.
//
// Implementations must be safe for concurrent use.
type EventStore interface {
	// =========================================================================
	// Lifecycle
	// =========================================================================

	// Close releases all resources held by the store.
	Close()

	// Flush forces any buffered data to persistent storage.
	Flush() error

	// =========================================================================
	// Write Operations
	// =========================================================================

	// StoreEvents stores events with optional index updates based on options.
	// Returns the number of bytes written.
	StoreEvents(events []*IngestEvent, opts *StoreOptions) (int64, error)

	// SetLastProcessedLedger stores the last processed ledger sequence.
	SetLastProcessedLedger(sequence uint32) error

	// =========================================================================
	// Read Operations - By Ledger
	// =========================================================================

	// GetLastProcessedLedger retrieves the last processed ledger sequence.
	GetLastProcessedLedger() (uint32, error)

	// GetEventsByLedger retrieves all events for a specific ledger.
	GetEventsByLedger(ledgerSequence uint32) ([]*ContractEvent, error)

	// GetEventsByLedgerRange retrieves all events within a ledger range.
	GetEventsByLedgerRange(startLedger, endLedger uint32) ([]*ContractEvent, error)

	// GetEventsInRangeWithTiming retrieves events in a range with detailed timing.
	GetEventsInRangeWithTiming(startLedger, endLedger uint32, limit int) (*query.RangeResult, error)

	// =========================================================================
	// Read Operations - For Query Engine
	// =========================================================================

	// GetEventsInLedger retrieves all events in a ledger (for query.EventReader).
	GetEventsInLedger(ledger uint32) ([]*query.Event, error)

	// GetEventsInLedgerWithTiming retrieves all events in a ledger with detailed timing.
	GetEventsInLedgerWithTiming(ledger uint32) (*query.FetchResult, error)

	// GetLedgerRange returns the min and max ledger sequences in the store.
	GetLedgerRange() (min, max uint32, err error)

	// =========================================================================
	// Statistics
	// =========================================================================

	// CountEvents returns the total number of events stored.
	CountEvents() (int64, error)

	// GetStats returns database statistics.
	GetStats() (*DBStats, error)

	// GetStorageSnapshot returns per-column-family storage statistics.
	GetStorageSnapshot() (*StorageSnapshot, error)

	// CountUniqueIndexes counts entries in unique indexes.
	CountUniqueIndexes() (*UniqueIndexCounts, error)

	// GetIndexDistribution returns distribution stats for all index types.
	GetIndexDistribution(topN int) (*IndexDistribution, error)

	// ComputeEventStats scans all events and computes unique counts.
	// Workers controls parallelism: 0 or 1 for single-threaded, >1 for parallel.
	ComputeEventStats(workers int) (*EventStats, error)

	// =========================================================================
	// Maintenance
	// =========================================================================

	// CompactAllWithStats runs manual compaction and returns before/after stats per column family.
	CompactAllWithStats() (*CompactionSummary, error)
}

// =============================================================================
// EventReader Interface (for query.Engine)
// =============================================================================

// EventReader is the interface that query.Engine uses to fetch events.
// EventStore implementations satisfy this interface.
type EventReader interface {
	// GetEventsInLedger retrieves all events in a specific ledger.
	GetEventsInLedger(ledger uint32) ([]*query.Event, error)

	// GetEventsInLedgerWithTiming retrieves all events in a ledger with detailed timing.
	GetEventsInLedgerWithTiming(ledger uint32) (*query.FetchResult, error)

	// GetLedgerRange returns the min and max ledger sequences in the store.
	GetLedgerRange() (min, max uint32, err error)
}

// Verify EventStore satisfies EventReader at compile time
var _ EventReader = (EventStore)(nil)
