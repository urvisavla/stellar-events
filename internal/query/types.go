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
	UseL2Index bool // Use hierarchical L2 index for precise lookups
	IncludeXDR bool // Include raw XDR in results
	CountOnly  bool // Only count matches, don't fetch events
}

// DefaultOptions returns default query options.
func DefaultOptions() *Options {
	return &Options{
		Limit:      100,
		UseL2Index: true,
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
	MatchingEvents  int   // Total matching events (may be > len(Events) if limited)
	EventsScanned   int64 // Events scanned during fetch phase
	EventsReturned  int   // Events returned (len(Events))

	// Ledger range info
	LedgerRange        uint32   // Number of ledgers in query range
	SegmentsQueried    int      // Number of bitmap segments queried
	MatchingLedgerSeqs []uint32 // Actual ledger sequences that matched

	// Timing breakdown
	IndexLookupTime time.Duration // Time for index lookup (L1 or L1+L2)
	L1LookupTime    time.Duration // L1 bitmap lookup time
	L2LookupTime    time.Duration // L2 bitmap lookup time (if hierarchical)
	EventFetchTime  time.Duration // Time to fetch events from store
	FilterTime      time.Duration // Time spent post-filtering events
	TotalTime       time.Duration // Total query time
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

// EventKey uniquely identifies an event in storage.
type EventKey struct {
	LedgerSeq uint32
	TxIdx     uint16
	OpIdx     uint16
	EventIdx  uint16
}
