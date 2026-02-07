package store

import (
	"encoding/binary"
)

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
	UniqueIndexes      bool // Build unique value counts (for stats)
	IndexFlushInterval int  // Ledgers between index flushes (0 = only at end)
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
	V2Indexes     bool // Maintain V2 indexes: bitmap32-event + posting-v2 (uses 6-byte event keys)

	// ExcludeTopic0 is a set of topic0 values (as strings) to skip during ingestion.
	// Events with matching topic0 will not be stored.
	ExcludeTopic0 map[string]struct{}

	// ExcludeDiagnostic skips diagnostic events during ingestion.
	ExcludeDiagnostic bool
}

// decodeRawLocalIDs decodes raw 4-byte big-endian local IDs to a uint32 slice.
func decodeRawLocalIDs(data []byte) []uint32 {
	count := len(data) / 4
	ids := make([]uint32, count)
	for i := 0; i < count; i++ {
		ids[i] = binary.BigEndian.Uint32(data[i*4:])
	}
	return ids
}

// deduplicateLocalIDs removes consecutive duplicate local IDs from a sorted slice.
func deduplicateLocalIDs(ids []uint32) []uint32 {
	if len(ids) <= 1 {
		return ids
	}
	writeIdx := 1
	for readIdx := 1; readIdx < len(ids); readIdx++ {
		if ids[readIdx] != ids[readIdx-1] {
			ids[writeIdx] = ids[readIdx]
			writeIdx++
		}
	}
	return ids[:writeIdx]
}

const (
	// BucketSize is the number of ledgers per index bucket.
	// At ~5 seconds per ledger, 10,000 ledgers ≈ 14 hours.
	// Used by both bitmap and posting list indexes.
	BucketSize uint32 = 10_000
)

// =============================================================================
// Bucket Functions (shared by bitmap and posting list indexes)
// =============================================================================

// BucketID calculates the bucket ID for a given ledger sequence.
func BucketID(ledgerSeq uint32) uint32 {
	return ledgerSeq / BucketSize
}

// BucketRange returns the ledger range covered by a bucket.
func BucketRange(bucketID uint32) (start, end uint32) {
	start = bucketID * BucketSize
	end = start + BucketSize - 1
	return
}

// GetBucketsForRange returns all bucket IDs that cover the given ledger range.
func GetBucketsForRange(startLedger, endLedger uint32) []uint32 {
	startBucket := BucketID(startLedger)
	endBucket := BucketID(endLedger)

	buckets := make([]uint32, 0, endBucket-startBucket+1)
	for b := startBucket; b <= endBucket; b++ {
		buckets = append(buckets, b)
	}
	return buckets
}
