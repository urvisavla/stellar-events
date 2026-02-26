package store

import "time"

// =============================================================================
// Configuration Types
// =============================================================================

// IndexConfig controls which secondary indexes to create.
type IndexConfig struct {
	ContractID bool
	Topics     bool // enables topic0-3
}

// BuildIndexOptions controls which indexes to build during rebuild.
type BuildIndexOptions struct {
	UniqueIndexes bool // Build unique value counts (for stats)
}

// indexEntry holds extracted data for index updates (used by collector pattern)
type indexEntry struct {
	Ledger     uint32
	TxIdx      uint16
	OpIdx      uint16
	EventIdx   uint16
	ContractID []byte   // nil if no contract
	Topics     [][]byte // up to 4 topics
}

// DefaultIndexConfig returns config with all indexes enabled.
func DefaultIndexConfig() *IndexConfig {
	return &IndexConfig{
		ContractID: true,
		Topics:     true,
	}
}

// Unique index type prefixes within CFUnique
const (
	UniqueTypeContract byte = 0x00 // Contract ID
	UniqueTypeTopic0   byte = 0x01 // Topic 0
	UniqueTypeTopic1   byte = 0x02 // Topic 1
	UniqueTypeTopic2   byte = 0x03 // Topic 2
	UniqueTypeTopic3   byte = 0x04 // Topic 3
)

// StoreOptions configures what indexes to update when storing events.
type StoreOptions struct {
	UniqueIndexes bool // Maintain unique value indexes with counts

	// ExcludeTopic0 is a set of topic0 values (as strings) to skip during ingestion.
	// Events with matching topic0 will not be stored.
	ExcludeTopic0 map[string]struct{}

	// ExcludeDiagnostic skips diagnostic events during ingestion.
	ExcludeDiagnostic bool
}

const (
	// SegmentSize is the number of ledgers per index segment.
	// At ~5 seconds per ledger, 10,000 ledgers ≈ 14 hours.
	SegmentSize uint32 = 10_000
)

// =============================================================================
// Segment Functions (shared by bitmap and segment-index indexes)
// =============================================================================

// SegmentID calculates the segment ID for a given ledger sequence.
func SegmentID(ledgerSeq uint32) uint32 {
	return ledgerSeq / SegmentSize
}

// SegmentRange returns the ledger range covered by a segment.
func SegmentRange(segmentID uint32) (start, end uint32) {
	start = segmentID * SegmentSize
	end = start + SegmentSize - 1
	return
}

// GetSegmentsForRange returns all segment IDs that cover the given ledger range.
func GetSegmentsForRange(startLedger, endLedger uint32) []uint32 {
	startSegment := SegmentID(startLedger)
	endSegment := SegmentID(endLedger)

	segments := make([]uint32, 0, endSegment-startSegment+1)
	for s := startSegment; s <= endSegment; s++ {
		segments = append(segments, s)
	}
	return segments
}

// =============================================================================
// Bitmap32 Event-Level Query Result (FromBuffer decode)
// =============================================================================

// Bitmap32EventQueryResult holds detailed results from a bitmap32 event-level query.
// Uses sequential event IDs + FromBuffer for near-zero-cost decode.
type Bitmap32EventQueryResult struct {
	// Ledger range
	LedgerRange      uint32 // endLedger - startLedger + 1
	MatchingLocalIDs int    // Local IDs matching index query

	// Index stats
	SegmentsTouched int   // Number of segments touched
	SegmentsScanned int   // Number of segments scanned
	IndexBytesRead  int64 // Bytes read from bitmap index

	// Event fetch stats
	EventsScanned  int   // Events scanned from storage
	EventsReturned int   // Events returned after filtering
	EventBytesRead int64 // Bytes read from event storage

	// Timing breakdown
	IndexLookupTime    time.Duration // Time querying bitmap index
	IndexReadTime      time.Duration // Time reading bitmap segments from storage (I/O)
	IndexDecodeTime    time.Duration // Time decoding bitmap segments (CPU - near zero with FromBuffer)
	IndexIntersectTime time.Duration // Time spent on bitmap OR/AND operations
	EventFetchTime      time.Duration // Time fetching events
	DecompressTime      time.Duration // Time spent decompressing event blobs (zstd/dict)
	EventDiskReadTime   time.Duration // Time spent on disk I/O for event data
	GroupsDecompressed  int           // Number of group blocks decompressed
	DecodeTime         time.Duration // Time decoding events
	FilterTime         time.Duration // Time filtering events
	TotalTime          time.Duration // Total query time
}

// ToUnified converts Bitmap32EventQueryResult to UnifiedQueryResult
func (r *Bitmap32EventQueryResult) ToUnified() *UnifiedQueryResult {
	return &UnifiedQueryResult{
		IndexType:         "bitmap32-event",
		LedgerRange:       r.LedgerRange,
		SegmentsTouched:   r.SegmentsTouched,
		IndexMatches:      r.MatchingLocalIDs,
		MatchUnitName:     "local IDs",
		EventsScanned:     r.EventsScanned,
		EventsReturned:    r.EventsReturned,
		IndexBytesRead:    r.IndexBytesRead,
		EventBytesRead:    r.EventBytesRead,
		IndexLookupTime:   r.IndexLookupTime,
		EventFetchTime:    r.EventFetchTime,
		DecompressTime:     r.DecompressTime,
		EventDiskReadTime:  r.EventDiskReadTime,
		GroupsDecompressed: r.GroupsDecompressed,
		DecodeTime:        r.DecodeTime,
		FilterTime:        r.FilterTime,
		TotalTime:         r.TotalTime,
	}
}
