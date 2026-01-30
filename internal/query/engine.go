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

	// GetEventsByKeys retrieves events by their precise keys (batch fetch).
	GetEventsByKeys(keys []index.EventKey) ([]*Event, error)

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
// It uses the index to find matching ledgers, then fetches events from those ledgers.
func (e *Engine) Query(filter *Filter, startLedger, endLedger uint32, opts *Options) (*Result, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	if filter.IsEmpty() {
		return nil, fmt.Errorf("at least one filter must be specified")
	}

	// Use hierarchical query if L2 index is available and requested
	if opts.UseL2Index {
		return e.queryHierarchical(filter, startLedger, endLedger, opts)
	}

	return e.queryL1(filter, startLedger, endLedger, opts)
}

// queryL1 performs a Level 1 query (ledger-level bitmap + post-filter).
func (e *Engine) queryL1(filter *Filter, startLedger, endLedger uint32, opts *Options) (*Result, error) {
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
	result.L1LookupTime = time.Since(indexStart)
	result.IndexLookupTime = result.L1LookupTime
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

	// Phase 2: Fetch events from matching ledgers
	fetchStart := time.Now()
	limit := opts.Limit

	iter := matchingLedgers.Iterator()
	for iter.HasNext() {
		if limit > 0 && len(result.Events) >= limit {
			break
		}

		ledger := iter.Next()
		events, err := e.eventReader.GetEventsInLedger(ledger)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch events from ledger %d: %w", ledger, err)
		}

		result.EventsScanned += int64(len(events))

		// Post-filter events
		for _, event := range events {
			if limit > 0 && len(result.Events) >= limit {
				break
			}

			if matchesFilter(event, filter.ContractID, topics) {
				result.Events = append(result.Events, event)
			}
		}
	}

	result.EventFetchTime = time.Since(fetchStart)
	result.EventsReturned = len(result.Events)
	result.TotalTime = time.Since(totalStart)

	return result, nil
}

// queryHierarchical performs a Level 2 query (precise event keys from index).
func (e *Engine) queryHierarchical(filter *Filter, startLedger, endLedger uint32, opts *Options) (*Result, error) {
	totalStart := time.Now()
	result := &Result{
		LedgerRange: endLedger - startLedger + 1,
	}

	// Convert filter to raw parameters
	topics := filter.TopicFilters()

	// Phase 1: Index lookup - get precise event keys
	indexStart := time.Now()
	limit := opts.Limit
	if opts.CountOnly {
		limit = 0 // No limit when counting
	}

	eventKeys, matchingLedgers, err := e.indexReader.QueryEvents(filter.ContractID, topics, startLedger, endLedger, limit)
	if err != nil {
		return nil, fmt.Errorf("index query failed: %w", err)
	}
	result.IndexLookupTime = time.Since(indexStart)
	result.MatchingLedgers = matchingLedgers
	result.MatchingEvents = len(eventKeys)

	if len(eventKeys) == 0 {
		result.TotalTime = time.Since(totalStart)
		return result, nil
	}

	// Count only mode - skip event fetch
	if opts.CountOnly {
		result.TotalTime = time.Since(totalStart)
		return result, nil
	}

	// Phase 2: Batch fetch events by keys
	fetchStart := time.Now()
	events, err := e.eventReader.GetEventsByKeys(eventKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch events: %w", err)
	}

	result.Events = events
	result.EventFetchTime = time.Since(fetchStart)
	result.EventsReturned = len(result.Events)
	result.TotalTime = time.Since(totalStart)

	return result, nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// matchesFilter checks if an event matches the given filters.
// Used for post-filtering when L1 index gives ledger-level granularity.
func matchesFilter(event *Event, contractID []byte, topics [][]byte) bool {
	// Check contract ID
	if len(contractID) > 0 {
		if event.ContractID == "" {
			return false
		}
		// ContractID in event is base64 encoded, need to compare appropriately
		// For now, assume the caller has already encoded contractID to match
		// This will need adjustment based on actual encoding
	}

	// Check topics
	for i, topicFilter := range topics {
		if len(topicFilter) == 0 {
			continue
		}
		if i >= len(event.Topics) {
			return false
		}
		// Topics in event are base64 encoded
		// Similar encoding consideration as contractID
	}

	return true
}
