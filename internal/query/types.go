// Package query provides query execution logic for contract events.
// It orchestrates index lookups and event fetches, independent of storage backend.
package query

import "time"

// =============================================================================
// Filter Types
// =============================================================================

// Filter specifies criteria for querying events.
// All non-nil fields are ANDed together.
type Filter struct {
	ContractID []byte // Filter by contract ID (32 bytes)
	Topic0     []byte // Filter by topic at position 0
	Topic1     []byte // Filter by topic at position 1
	Topic2     []byte // Filter by topic at position 2
	Topic3     []byte // Filter by topic at position 3
}

// IsEmpty returns true if no filters are specified.
func (f *Filter) IsEmpty() bool {
	return len(f.ContractID) == 0 &&
		len(f.Topic0) == 0 &&
		len(f.Topic1) == 0 &&
		len(f.Topic2) == 0 &&
		len(f.Topic3) == 0
}

// HasContractFilter returns true if contract ID filter is set.
func (f *Filter) HasContractFilter() bool {
	return len(f.ContractID) > 0
}

// HasTopicFilter returns true if any topic filter is set.
func (f *Filter) HasTopicFilter() bool {
	return len(f.Topic0) > 0 || len(f.Topic1) > 0 ||
		len(f.Topic2) > 0 || len(f.Topic3) > 0
}

// TopicFilters returns topics as a slice for iteration.
func (f *Filter) TopicFilters() [][]byte {
	return [][]byte{f.Topic0, f.Topic1, f.Topic2, f.Topic3}
}

// =============================================================================
// Query Options
// =============================================================================

// Options configures query execution behavior.
type Options struct {
	Limit      int  // Maximum events to return (0 = no limit)
	IncludeXDR bool // Include raw XDR in results
	CountOnly  bool // Only count matches, don't fetch events
}

// DefaultOptions returns default query options.
func DefaultOptions() *Options {
	return &Options{
		Limit: 100,
	}
}

// =============================================================================
// Result Types
// =============================================================================

// Result holds the result of a query execution.
type Result struct {
	// Events returned by the query
	Events []*Event

	// Counts
	MatchingLedgers int   // Number of ledgers that matched the filter
	EventsScanned   int64 // Events scanned during fetch phase
	EventsReturned  int   // Events returned (len(Events))

	// Ledger range info
	LedgerRange        uint32   // Number of ledgers in query range
	MatchingLedgerSeqs []uint32 // Actual ledger sequences that matched

	// High-level timing breakdown
	IndexLookupTime time.Duration // Time for index lookup
	EventFetchTime  time.Duration // Time to fetch events from store (includes all sub-timings below)
	TotalTime       time.Duration // Total query time

	// Detailed fetch timing breakdown
	DiskReadTime   time.Duration // Time spent reading from RocksDB (iterator operations)
	UnmarshalTime  time.Duration // Time spent unmarshalling XDR
	FilterTime     time.Duration // Time spent post-filtering events
	BytesRead      int64         // Total bytes read from disk
}

// FetchTiming holds detailed timing for a single ledger fetch operation.
type FetchTiming struct {
	DiskReadTime  time.Duration // Time spent in RocksDB iterator operations
	UnmarshalTime time.Duration // Time spent unmarshalling XDR to events
	BytesRead     int64         // Total bytes read from disk
}

// FetchResult holds the result of fetching events from a ledger with timing info.
type FetchResult struct {
	Events []*Event
	Timing FetchTiming
}

// FilteredFetchResult holds the result of fetching and filtering events with detailed timing.
type FilteredFetchResult struct {
	Events        []*Event
	EventsScanned int64 // Total events examined
	Timing        FilteredFetchTiming
}

// FilteredFetchTiming holds timing for filtered fetch operations.
type FilteredFetchTiming struct {
	DiskReadTime  time.Duration // Time spent reading from storage
	FilterTime    time.Duration // Time spent filtering (at header level for binary)
	DecodeTime    time.Duration // Time spent decoding matching events only
	BytesRead     int64         // Total bytes read from disk
}

// RangeResult holds the result of fetching events from a ledger range with timing info.
type RangeResult struct {
	Events        []*Event
	EventsScanned int64 // Total events scanned
	Timing        FetchTiming
}

// =============================================================================
// Event Types (Query Output)
// =============================================================================

// Event represents a contract event returned by queries.
// This is the decoded, JSON-serializable representation.
type Event struct {
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

	// Optional raw XDR (only if Options.IncludeXDR is true)
	RawXDR []byte `json:"raw_xdr,omitempty"`
}
