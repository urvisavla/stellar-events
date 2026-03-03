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

// TopicGroups returns topics as positional groups for index queries.
// Each position has 0 or 1 entries (single-value filter per position).
func (f *Filter) TopicGroups() [4][][]byte {
	var tg [4][][]byte
	if len(f.Topic0) > 0 {
		tg[0] = [][]byte{f.Topic0}
	}
	if len(f.Topic1) > 0 {
		tg[1] = [][]byte{f.Topic1}
	}
	if len(f.Topic2) > 0 {
		tg[2] = [][]byte{f.Topic2}
	}
	if len(f.Topic3) > 0 {
		tg[3] = [][]byte{f.Topic3}
	}
	return tg
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
	IndexBytesRead int64         // Bytes read from index
	BytesRead      int64         // Bytes read from event storage
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
	LedgerSequence           uint32    `json:"ledger"`
	LedgerClosedAt           time.Time `json:"ledgerClosedAt"`
	TransactionIndex         int       `json:"transactionIndex"`
	OperationIndex           int       `json:"operationIndex"`
	EventIndex               int       `json:"eventIndex,omitempty"`
	ContractID               string    `json:"contractId,omitempty"`
	Type                     string    `json:"type"`
	EventStage               string    `json:"event_stage,omitempty"`
	Topics                   []string  `json:"topics"`
	Data                     string    `json:"data"`
	TransactionHash          string    `json:"txHash"`
	InSuccessfulContractCall bool      `json:"inSuccessfulContractCall"`

	// Computed ID field (format: "{ledger}-{eventIndex}")
	ID string `json:"id,omitempty"`

	// Optional raw XDR (only if Options.IncludeXDR is true)
	RawXDR []byte `json:"raw_xdr,omitempty"`
}
