package store

import (
	"encoding/binary"
	"sort"
)

// BucketLedgerMap maps dense sequential local IDs to (ledger, eventSeq) pairs
// within a bucket. Stored as a fixed-size cumulative array of event counts.
//
// Entry i = total events in ledgers at offsets 0 through i (inclusive cumulative sum).
// The array has exactly BucketSize entries (10,000), stored as little-endian uint32s.
//
// Key format in CFDefault: [0xFF][bucketID:4] — 5 bytes
// Value: [cumulative_count:4] × BucketSize — exactly 40,000 bytes (fixed)
type BucketLedgerMap struct {
	BucketID uint32
	Data     []byte // raw 40,000-byte cumulative array (zero-copy from RocksDB)
}

const (
	// BucketLedgerMapSize is the size of the cumulative array in bytes.
	BucketLedgerMapSize = int(BucketSize) * 4 // 40,000 bytes

	// bucketLedgerMapPrefix is the key prefix used to avoid collision with text keys in CFDefault.
	bucketLedgerMapPrefix = 0xFF
)

// BucketLedgerMapKey returns the CFDefault key for a bucket's ledger map.
// Format: [0xFF][bucketID:4] — 5 bytes
func BucketLedgerMapKey(bucketID uint32) []byte {
	key := make([]byte, 5)
	key[0] = bucketLedgerMapPrefix
	binary.BigEndian.PutUint32(key[1:5], bucketID)
	return key
}

// TotalEvents returns the total number of events in this bucket (last cumulative entry).
func (m *BucketLedgerMap) TotalEvents() uint32 {
	if len(m.Data) < BucketLedgerMapSize {
		return 0
	}
	return m.getCumulative(int(BucketSize) - 1)
}

// LedgerRangeToIDRange converts a ledger offset range to a dense ID range. O(1).
// startOff and endOff are ledger offsets within the bucket (0-based, inclusive).
// Returns (startID, endID) where endID is inclusive. If the range has no events,
// returns (0, 0) with startID > endID being impossible, so caller should check endID >= startID.
func (m *BucketLedgerMap) LedgerRangeToIDRange(startOff, endOff uint16) (uint32, uint32) {
	if len(m.Data) < BucketLedgerMapSize {
		return 0, 0
	}

	var startID uint32
	if startOff > 0 {
		startID = m.getCumulative(int(startOff) - 1)
	}

	endID := m.getCumulative(int(endOff))
	if endID == 0 {
		return 0, 0
	}
	return startID, endID - 1
}

// DenseIDToLedgerAndSeq converts a dense local ID to an absolute ledger sequence
// and event sequence within that ledger. O(log N) via binary search.
func (m *BucketLedgerMap) DenseIDToLedgerAndSeq(denseID uint32) (ledger uint32, seq uint16) {
	if len(m.Data) < BucketLedgerMapSize {
		return 0, 0
	}

	bucketStart := m.BucketID * BucketSize
	n := int(BucketSize)

	// Binary search for smallest i where cumulative[i] > denseID
	i := sort.Search(n, func(j int) bool {
		return m.getCumulative(j) > denseID
	})

	if i >= n {
		// denseID is out of range
		return 0, 0
	}

	ledger = bucketStart + uint32(i)
	if i == 0 {
		seq = uint16(denseID)
	} else {
		seq = uint16(denseID - m.getCumulative(i-1))
	}
	return ledger, seq
}

// getCumulative reads the cumulative count at offset i from the raw data.
func (m *BucketLedgerMap) getCumulative(i int) uint32 {
	off := i * 4
	return binary.LittleEndian.Uint32(m.Data[off : off+4])
}

// EncodeBucketLedgerMap builds a cumulative array from per-ledger event counts.
// ledgerEventCounts[i] = number of events at ledger offset i.
// Returns the raw 40,000-byte cumulative array.
func EncodeBucketLedgerMap(bucketID uint32, ledgerEventCounts [BucketSize]uint32) []byte {
	data := make([]byte, BucketLedgerMapSize)
	var cumulative uint32
	for i := uint32(0); i < BucketSize; i++ {
		cumulative += ledgerEventCounts[i]
		off := i * 4
		binary.LittleEndian.PutUint32(data[off:off+4], cumulative)
	}
	return data
}

// MergeBucketLedgerMapData merges new per-ledger event counts into an existing
// cumulative array. The existing data is read, counts are added at the specified
// offsets, and the cumulative sums are recomputed from the point of first change.
// Returns a new 40,000-byte cumulative array.
func MergeBucketLedgerMapData(existing []byte, newCounts map[uint16]uint32) []byte {
	if len(existing) < BucketLedgerMapSize {
		// No existing data — build from scratch
		var counts [BucketSize]uint32
		for off, count := range newCounts {
			counts[off] = count
		}
		// We don't know bucketID here, but EncodeBucketLedgerMap only uses it
		// for nothing — it just builds the array from counts.
		return EncodeBucketLedgerMap(0, counts)
	}

	// Decode existing cumulative into per-ledger counts
	var counts [BucketSize]uint32
	prev := uint32(0)
	for i := uint32(0); i < BucketSize; i++ {
		off := i * 4
		cum := binary.LittleEndian.Uint32(existing[off : off+4])
		counts[i] = cum - prev
		prev = cum
	}

	// Add new counts
	for off, count := range newCounts {
		counts[off] += count
	}

	// Re-encode
	return EncodeBucketLedgerMap(0, counts)
}

// bucketEventCounter tracks per-ledger event counts during ingestion for a single bucket.
type bucketEventCounter struct {
	eventCounts [BucketSize]uint32 // per-ledger-offset event count
	nextDenseID uint32             // running total = next ID to assign
}

// assignDenseID assigns the next dense local ID for a ledger within this bucket.
func (c *bucketEventCounter) assignDenseID(ledgerOffset uint16) uint32 {
	id := c.nextDenseID
	c.nextDenseID++
	c.eventCounts[ledgerOffset]++
	return id
}
