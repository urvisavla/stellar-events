package query

import (
	"fmt"
	"time"

	"github.com/urvisavla/stellar-events/internal/index"
)

// =============================================================================
// Interfaces (dependencies injected into Engine)
// =============================================================================

// EventReader provides raw event retrieval.
// Implementations can be backed by RocksDB, S3, PostgreSQL, etc.
type EventReader interface {
	// GetEventsInLedger retrieves all events in a specific ledger.
	GetEventsInLedger(ledger uint32) ([]*Event, error)

	// GetEventsInLedgerWithTiming retrieves all events in a ledger with detailed timing.
	GetEventsInLedgerWithTiming(ledger uint32) (*FetchResult, error)

	// GetEventsInLedgerWithFilter retrieves matching events from a ledger with early filtering.
	// This allows the store to filter at the storage level (e.g., binary header) before full decode.
	// Returns events that match, plus timing that separates filtering from decoding.
	GetEventsInLedgerWithFilter(ledger uint32, contractID []byte, topics [][]byte, limit int) (*FilteredFetchResult, error)

	// GetLedgerRange returns the min and max ledger sequences in the store.
	GetLedgerRange() (min, max uint32, err error)
}

// =============================================================================
// Query Engine
// =============================================================================

// Engine orchestrates index lookups and event fetches.
// It is completely storage-agnostic.
type Engine struct {
	indexReader index.IndexReader
	eventReader EventReader
}

// NewEngine creates a new query engine with the given readers.
func NewEngine(indexReader index.IndexReader, eventReader EventReader) *Engine {
	return &Engine{
		indexReader: indexReader,
		eventReader: eventReader,
	}
}

// Query executes a filtered query.
// It uses the index to find matching ledgers, then fetches and filters events from those ledgers.
func (e *Engine) Query(filter *Filter, startLedger, endLedger uint32, opts *Options) (*Result, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	if filter.IsEmpty() {
		return nil, fmt.Errorf("at least one filter must be specified")
	}

	totalStart := time.Now()
	result := &Result{
		LedgerRange: endLedger - startLedger + 1,
	}

	// Convert filter to raw parameters
	topics := filter.TopicFilters()

	// Phase 1: Index lookup - find matching ledgers
	indexStart := time.Now()
	matchingLedgers, err := e.indexReader.QueryLedgers(filter.ContractID, topics, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("index query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLedgers = int(matchingLedgers.GetCardinality())
	result.MatchingLedgerSeqs = matchingLedgers.ToArray()

	if matchingLedgers.IsEmpty() {
		result.TotalTime = time.Since(totalStart)
		return result, nil
	}

	// Count only mode - skip event fetch
	if opts.CountOnly {
		result.TotalTime = time.Since(totalStart)
		return result, nil
	}

	// Phase 2: Fetch events from matching ledgers with early filtering
	// This allows the store to filter at the storage level before full decode
	fetchStart := time.Now()
	limit := opts.Limit
	remainingLimit := limit

	iter := matchingLedgers.Iterator()
	for iter.HasNext() {
		if limit > 0 && len(result.Events) >= limit {
			break
		}

		ledger := iter.Next()

		// Calculate per-ledger limit
		ledgerLimit := 0
		if limit > 0 {
			remainingLimit = limit - len(result.Events)
			ledgerLimit = remainingLimit
		}

		// Use filter-aware fetch - allows binary format to filter at header level
		fetchResult, err := e.eventReader.GetEventsInLedgerWithFilter(ledger, filter.ContractID, topics, ledgerLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch events from ledger %d: %w", ledger, err)
		}

		// Aggregate timing from this ledger
		result.DiskReadTime += fetchResult.Timing.DiskReadTime
		result.FilterTime += fetchResult.Timing.FilterTime
		result.UnmarshalTime += fetchResult.Timing.DecodeTime
		result.BytesRead += fetchResult.Timing.BytesRead

		result.EventsScanned += fetchResult.EventsScanned

		// Events are already filtered, just append
		result.Events = append(result.Events, fetchResult.Events...)
	}

	result.EventFetchTime = time.Since(fetchStart)
	result.EventsReturned = len(result.Events)
	result.TotalTime = time.Since(totalStart)

	return result, nil
}

