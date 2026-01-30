// Package index provides bitmap indexing for fast event queries.
package index

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
	Topic0IndexCount   int64
	Topic1IndexCount   int64
	Topic2IndexCount   int64
	Topic3IndexCount   int64
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
