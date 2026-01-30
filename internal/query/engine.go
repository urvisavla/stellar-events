package query

import (
	"encoding/base64"
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

	// Phase 2: Fetch events from matching ledgers and post-filter
	fetchStart := time.Now()
	limit := opts.Limit

	iter := matchingLedgers.Iterator()
	for iter.HasNext() {
		if limit > 0 && len(result.Events) >= limit {
			break
		}

		ledger := iter.Next()
		fetchResult, err := e.eventReader.GetEventsInLedgerWithTiming(ledger)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch events from ledger %d: %w", ledger, err)
		}

		// Aggregate timing from this ledger
		result.DiskReadTime += fetchResult.Timing.DiskReadTime
		result.UnmarshalTime += fetchResult.Timing.UnmarshalTime

		result.EventsScanned += int64(len(fetchResult.Events))

		// Post-filter events
		filterStart := time.Now()
		for _, event := range fetchResult.Events {
			if limit > 0 && len(result.Events) >= limit {
				break
			}

			if matchesFilter(event, filter.ContractID, topics) {
				result.Events = append(result.Events, event)
			}
		}
		result.FilterTime += time.Since(filterStart)
	}

	result.EventFetchTime = time.Since(fetchStart)
	result.EventsReturned = len(result.Events)
	result.TotalTime = time.Since(totalStart)

	return result, nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// matchesFilter checks if an event matches the given filters.
// Used for post-filtering when index gives ledger-level granularity.
func matchesFilter(event *Event, contractID []byte, topics [][]byte) bool {
	// Check contract ID
	if len(contractID) > 0 {
		if event.ContractID == "" {
			return false
		}
		// Event.ContractID is base64 encoded, contractID filter is raw bytes
		// Encode filter to base64 for comparison
		filterBase64 := base64.StdEncoding.EncodeToString(contractID)
		if event.ContractID != filterBase64 {
			return false
		}
	}

	// Check topics
	for i, topicFilter := range topics {
		if len(topicFilter) == 0 {
			continue
		}
		if i >= len(event.Topics) {
			return false
		}
		// Event.Topics are base64 encoded, topicFilter is raw bytes
		filterBase64 := base64.StdEncoding.EncodeToString(topicFilter)
		if event.Topics[i] != filterBase64 {
			return false
		}
	}

	return true
}
