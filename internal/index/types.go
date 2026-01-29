// Package index provides bitmap indexing for fast event queries.
package index

import "time"

// =============================================================================
// Constants
// =============================================================================

const (
	// SegmentSize is the number of ledgers per bitmap segment.
	// 1M ledgers ≈ 2 months of Stellar data.
	// Small enough to limit write amplification, large enough to amortize overhead.
	SegmentSize uint32 = 1_000_000
)

// Level 1 index prefixes (ledger-level granularity)
const (
	PrefixContractIndex byte = 0x01
	PrefixTopic0Index   byte = 0x02
	PrefixTopic1Index   byte = 0x03
	PrefixTopic2Index   byte = 0x04
	PrefixTopic3Index   byte = 0x05
)

// Level 2 index prefixes (event-level granularity within ledger)
// Key format: [L2prefix][keyValue:32][ledger:4] -> bitmap of event indices
const (
	PrefixContractL2 byte = 0x11
	PrefixTopic0L2   byte = 0x12
	PrefixTopic1L2   byte = 0x13
	PrefixTopic2L2   byte = 0x14
	PrefixTopic3L2   byte = 0x15
)

// =============================================================================
// Types
// =============================================================================

// EventKey represents a precise event location within a ledger.
type EventKey struct {
	LedgerSeq  uint32
	EventIndex uint32 // Encoded index within ledger: [tx:10bits][op:10bits][event:12bits]
}

// BitmapIndexStats holds statistics about the bitmap index.
type BitmapIndexStats struct {
	HotSegmentCount    int
	HotSegmentCards    uint64
	HotSegmentMemBytes uint64
	CurrentSegmentID   uint32
	ContractIndexCount int64
	Topic0IndexCount   int64
	Topic1IndexCount   int64
	Topic2IndexCount   int64
	Topic3IndexCount   int64
}

// IndexQueryResult holds the result of a hierarchical index query.
// This is the index-level result containing event positions, not decoded events.
type IndexQueryResult struct {
	// EventKeys contains precise event locations
	EventKeys []EventKey

	// Timing breakdown
	L1LookupTime time.Duration // Time for Level 1 bitmap lookup
	L2LookupTime time.Duration // Time for Level 2 bitmap lookups
	TotalTime    time.Duration

	// Stats
	MatchingLedgers int // Number of ledgers with matches (from L1)
	TotalEvents     int // Total events found (from L2)
}

// =============================================================================
// Helper Functions
// =============================================================================

// SegmentID returns the segment ID for a given ledger sequence.
func SegmentID(ledgerSeq uint32) uint32 {
	return ledgerSeq / SegmentSize
}

// SegmentRange returns the ledger range for a segment.
func SegmentRange(segmentID uint32) (start, end uint32) {
	start = segmentID * SegmentSize
	end = start + SegmentSize - 1
	return
}

// EncodeEventIndex encodes tx, op, and event indices into a single uint32.
// Format: [tx:10bits][op:10bits][event:12bits] - supports tx<1024, op<1024, event<4096
func EncodeEventIndex(txIndex, opIndex, eventIndex uint16) uint32 {
	return (uint32(txIndex&0x3FF) << 22) | (uint32(opIndex&0x3FF) << 12) | uint32(eventIndex&0xFFF)
}

// DecodeEventIndex decodes a uint32 back to tx, op, and event indices.
func DecodeEventIndex(encoded uint32) (txIndex, opIndex, eventIndex uint16) {
	txIndex = uint16((encoded >> 22) & 0x3FF)
	opIndex = uint16((encoded >> 12) & 0x3FF)
	eventIndex = uint16(encoded & 0xFFF)
	return
}
