// Package index provides bitmap indexing for fast event queries.
package index

// =============================================================================
// Constants
// =============================================================================

const (
	// SegmentSize is the number of ledgers per bitmap segment.
	// Matches posting list BucketSize (10,000 ledgers ≈ 14 hours) for fair comparison.
	SegmentSize uint32 = 10_000
)

// Level 1 index prefixes (ledger-level granularity)
// Simplified to match posting list structure: one index for contracts, one for topics
const (
	PrefixContractIndex byte = 0x01 // Contract ID → ledger bitmap
	PrefixTopicIndex    byte = 0x02 // Topic (any position) → ledger bitmap
)


// =============================================================================
// Types
// =============================================================================

// BitmapIndexStats holds statistics about the bitmap index.
type BitmapIndexStats struct {
	HotSegmentCount    int
	HotSegmentCards    uint64
	HotSegmentMemBytes uint64
	CurrentSegmentID   uint32
	ContractIndexCount int64
	TopicIndexCount    int64 // All topics (non-positional)
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
