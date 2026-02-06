package index

import (
	"crypto/sha256"
	"encoding/binary"
)

// Posting list index for contract and topic lookups.
//
// Index Key Format (36 bytes):
//   [term_key:32][bucket_id:4]
//   - term_key: SHA-256 hash of the indexed value (contract_id or topic_xdr)
//   - bucket_id: ledger_seq / BucketSize (groups ~14 hours of ledgers)
//
// Index Value Format:
//   Concatenated TOIDs (8 bytes each), sorted by TOID value.
//   No length prefix - value length / 8 = number of TOIDs.
//
// Query flow:
//   1. Compute term_key = SHA-256(search_value)
//   2. Determine bucket range from ledger range
//   3. For each bucket: read posting list, filter TOIDs by ledger range
//   4. Intersect posting lists if multiple filters
//   5. Fetch events by TOID from primary storage

const (
	// BucketSize is the number of ledgers per index bucket.
	// At ~5 seconds per ledger, 10,000 ledgers ≈ 14 hours.
	BucketSize = 10_000

	// IndexKeySize is the size of an index key in bytes.
	IndexKeySize = 36

	// TOIDSize is the size of a TOID in bytes.
	TOIDSize = 8
)

// BucketID calculates the bucket ID for a given ledger sequence.
func BucketID(ledgerSeq uint32) uint32 {
	return ledgerSeq / BucketSize
}

// BucketLedgerRange returns the ledger range covered by a bucket.
func BucketLedgerRange(bucketID uint32) (start, end uint32) {
	start = bucketID * BucketSize
	end = start + BucketSize - 1
	return
}

// EncodeIndexKey creates a 36-byte index key from term key and ledger sequence.
func EncodeIndexKey(termKey [32]byte, ledgerSeq uint32) []byte {
	bucketID := BucketID(ledgerSeq)
	key := make([]byte, IndexKeySize)
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], bucketID)
	return key
}

// EncodeIndexKeyWithBucket creates a 36-byte index key from term key and bucket ID directly.
func EncodeIndexKeyWithBucket(termKey [32]byte, bucketID uint32) []byte {
	key := make([]byte, IndexKeySize)
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], bucketID)
	return key
}

// DecodeIndexKey extracts term key and bucket ID from a 36-byte index key.
func DecodeIndexKey(key []byte) (termKey [32]byte, bucketID uint32) {
	if len(key) < IndexKeySize {
		return
	}
	copy(termKey[:], key[0:32])
	bucketID = binary.BigEndian.Uint32(key[32:36])
	return
}

// ContractTermKey computes the term key (SHA-256) for a contract ID.
func ContractTermKey(contractID []byte) [32]byte {
	return sha256.Sum256(contractID)
}

// TopicTermKey computes the term key (SHA-256) for a topic XDR value.
func TopicTermKey(topicXDR []byte) [32]byte {
	return sha256.Sum256(topicXDR)
}

// TOIDToBytes encodes a single TOID to 8 bytes (big-endian).
func TOIDToBytes(toid uint64) []byte {
	buf := make([]byte, TOIDSize)
	binary.BigEndian.PutUint64(buf, toid)
	return buf
}

// TOIDFromBytes decodes a single TOID from 8 bytes.
func TOIDFromBytes(data []byte) uint64 {
	if len(data) < TOIDSize {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// EncodeTOIDList encodes a list of TOIDs to bytes.
// Each TOID is 8 bytes, big-endian.
func EncodeTOIDList(toids []uint64) []byte {
	buf := make([]byte, len(toids)*TOIDSize)
	for i, toid := range toids {
		binary.BigEndian.PutUint64(buf[i*TOIDSize:], toid)
	}
	return buf
}

// DecodeTOIDList decodes bytes to a list of TOIDs.
func DecodeTOIDList(data []byte) []uint64 {
	count := len(data) / TOIDSize
	toids := make([]uint64, count)
	for i := 0; i < count; i++ {
		toids[i] = binary.BigEndian.Uint64(data[i*TOIDSize:])
	}
	return toids
}

// =============================================================================
// Delta-Varint Encoding for Posting Lists
// =============================================================================
//
// Delta-varint encoding dramatically reduces posting list size by:
// 1. Storing deltas between consecutive TOIDs (usually small numbers)
// 2. Using variable-length integer encoding (small numbers = fewer bytes)
//
// Example: TOIDs [1000000, 1000001, 1000005, 1000006]
// - Raw: 4 × 8 = 32 bytes
// - Delta: [1000000, 1, 4, 1]
// - Varint: ~5 + 1 + 1 + 1 = ~8 bytes (4x compression)
//
// Format: [count:varint][first_toid:8bytes][delta1:varint][delta2:varint]...

// EncodeTOIDListDeltaVarint encodes TOIDs using delta-varint compression.
// TOIDs must be sorted in ascending order.
func EncodeTOIDListDeltaVarint(toids []uint64) []byte {
	if len(toids) == 0 {
		return nil
	}

	// Estimate size: count (5) + first toid (8) + deltas (avg 2 bytes each)
	buf := make([]byte, 0, 13+len(toids)*2)

	// Write count as varint
	buf = appendVarint(buf, uint64(len(toids)))

	// Write first TOID as full 8 bytes (big-endian)
	firstBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstBytes, toids[0])
	buf = append(buf, firstBytes...)

	// Write deltas as varints
	prev := toids[0]
	for i := 1; i < len(toids); i++ {
		delta := toids[i] - prev
		buf = appendVarint(buf, delta)
		prev = toids[i]
	}

	return buf
}

// ReadTOIDCount reads only the count from delta-varint encoded data without decoding TOIDs.
// This is useful for estimating posting list sizes before deciding which to decode first.
// Returns count and number of bytes consumed for the count header.
func ReadTOIDCount(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}
	count, n := readVarint(data)
	if n <= 0 {
		return 0, 0
	}
	return count, n
}

// DecodeTOIDListDeltaVarint decodes delta-varint encoded TOIDs.
func DecodeTOIDListDeltaVarint(data []byte) []uint64 {
	if len(data) == 0 {
		return nil
	}

	// Read count
	count, n := readVarint(data)
	if n <= 0 || count == 0 {
		return nil
	}
	data = data[n:]

	// Need at least 8 bytes for first TOID
	if len(data) < 8 {
		return nil
	}

	toids := make([]uint64, count)

	// Read first TOID
	toids[0] = binary.BigEndian.Uint64(data[:8])
	data = data[8:]

	// Read deltas
	prev := toids[0]
	for i := uint64(1); i < count; i++ {
		delta, n := readVarint(data)
		if n <= 0 {
			// Truncated data, return what we have
			return toids[:i]
		}
		data = data[n:]
		toids[i] = prev + delta
		prev = toids[i]
	}

	return toids
}

// DeltaVarintIterator allows incremental decoding of delta-varint TOIDs.
type DeltaVarintIterator struct {
	data      []byte
	count     uint64
	index     uint64
	prevTOID  uint64
	exhausted bool
}

// NewDeltaVarintIterator creates an iterator for delta-varint encoded data.
func NewDeltaVarintIterator(data []byte) *DeltaVarintIterator {
	it := &DeltaVarintIterator{data: data}
	if len(data) == 0 {
		it.exhausted = true
		return it
	}

	// Read count
	count, n := readVarint(data)
	if n <= 0 || count == 0 {
		it.exhausted = true
		return it
	}
	it.count = count
	it.data = data[n:]

	// Need at least 8 bytes for first TOID
	if len(it.data) < 8 {
		it.exhausted = true
		return it
	}

	// Read first TOID but don't advance yet
	it.prevTOID = binary.BigEndian.Uint64(it.data[:8])
	return it
}

// Next returns the next TOID and whether it exists.
func (it *DeltaVarintIterator) Next() (uint64, bool) {
	if it.exhausted || it.index >= it.count {
		return 0, false
	}

	if it.index == 0 {
		// First TOID
		it.data = it.data[8:]
		it.index++
		return it.prevTOID, true
	}

	// Read delta
	delta, n := readVarint(it.data)
	if n <= 0 {
		it.exhausted = true
		return 0, false
	}
	it.data = it.data[n:]
	it.prevTOID += delta
	it.index++
	return it.prevTOID, true
}

// Count returns total number of TOIDs in the data.
func (it *DeltaVarintIterator) Count() uint64 {
	return it.count
}

// Remaining returns how many TOIDs are left to read.
func (it *DeltaVarintIterator) Remaining() uint64 {
	if it.index >= it.count {
		return 0
	}
	return it.count - it.index
}

// =============================================================================
// Phase 3: Guided Intersection
// =============================================================================

// GuidedIntersector performs intersection using a guide set to skip non-matching TOIDs.
// The guide set is the smallest posting list result. When decoding larger lists,
// we can skip TOIDs that fall outside the range of guide set TOIDs.
type GuidedIntersector struct {
	guideSet     []uint64            // sorted guide set (smallest list)
	guideIdx     int                 // current position in guide set
	guideBuckets map[uint32]struct{} // bucket IDs that have guide set TOIDs
}

// NewGuidedIntersector creates a new guided intersector from the smallest list.
func NewGuidedIntersector(guideSet []uint64) *GuidedIntersector {
	gi := &GuidedIntersector{
		guideSet:     guideSet,
		guideBuckets: make(map[uint32]struct{}),
	}

	// Build set of buckets that have guide TOIDs
	for _, toid := range guideSet {
		ledger, _, _ := DecodeTOID(toid)
		bucketID := BucketID(ledger)
		gi.guideBuckets[bucketID] = struct{}{}
	}

	return gi
}

// HasTOIDsInBucket returns true if the guide set has TOIDs in the given bucket.
// This allows skipping entire buckets when reading larger posting lists.
func (gi *GuidedIntersector) HasTOIDsInBucket(bucketID uint32) bool {
	_, ok := gi.guideBuckets[bucketID]
	return ok
}

// BucketCount returns the number of buckets that have guide TOIDs.
func (gi *GuidedIntersector) BucketCount() int {
	return len(gi.guideBuckets)
}

// IntersectWithIterator intersects the guide set with TOIDs from an iterator.
// Returns matching TOIDs and the number of TOIDs skipped.
func (gi *GuidedIntersector) IntersectWithIterator(it *DeltaVarintIterator) ([]uint64, int) {
	if len(gi.guideSet) == 0 || it == nil {
		return nil, 0
	}

	var result []uint64
	skipped := 0
	gi.guideIdx = 0

	for {
		toid, ok := it.Next()
		if !ok {
			break
		}

		// Advance guide index to catch up
		for gi.guideIdx < len(gi.guideSet) && gi.guideSet[gi.guideIdx] < toid {
			gi.guideIdx++
		}

		if gi.guideIdx >= len(gi.guideSet) {
			// No more guide TOIDs, remaining iterator TOIDs won't match
			skipped += int(it.Remaining())
			break
		}

		if gi.guideSet[gi.guideIdx] == toid {
			result = append(result, toid)
			gi.guideIdx++
		} else {
			skipped++
		}
	}

	return result, skipped
}

// IntersectSorted intersects the guide set with a sorted TOID list.
// More efficient than IntersectTOIDLists when guide set is much smaller.
func (gi *GuidedIntersector) IntersectSorted(toids []uint64) []uint64 {
	if len(gi.guideSet) == 0 || len(toids) == 0 {
		return nil
	}

	// Use two-pointer intersection (same as IntersectTOIDLists)
	return IntersectTOIDLists(gi.guideSet, toids)
}

// appendVarint appends a varint-encoded uint64 to buf.
func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

// readVarint reads a varint from data, returns value and bytes consumed.
func readVarint(data []byte) (uint64, int) {
	var v uint64
	var shift uint
	for i, b := range data {
		if i >= 10 { // max 10 bytes for uint64 varint
			return 0, -1
		}
		v |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return v, i + 1
		}
		shift += 7
	}
	return 0, -1 // incomplete varint
}

// MergeTOIDListsDeltaVarint merges two delta-varint encoded posting lists.
// Both inputs must be sorted. Output is sorted delta-varint encoded.
func MergeTOIDListsDeltaVarint(existing, new []byte) []byte {
	// Decode both lists
	existingTOIDs := DecodeTOIDListDeltaVarint(existing)
	newTOIDs := DecodeTOIDListDeltaVarint(new)

	if len(existingTOIDs) == 0 {
		return new
	}
	if len(newTOIDs) == 0 {
		return existing
	}

	// Merge sorted lists (union, preserving order)
	merged := UnionTOIDLists(existingTOIDs, newTOIDs)

	// Re-encode
	return EncodeTOIDListDeltaVarint(merged)
}

// FilterTOIDsByLedgerRange filters TOIDs to only include those within the ledger range.
// TOIDs are assumed to be sorted.
func FilterTOIDsByLedgerRange(toids []uint64, startLedger, endLedger uint32) []uint64 {
	if len(toids) == 0 {
		return nil
	}

	// Create range boundaries as TOIDs (uses EncodeTOID from event_key.go)
	startTOID := EncodeTOID(startLedger, 0, 0)
	endTOID := EncodeTOID(endLedger, 0xFFFFF, 0xFFF) // max tx and op for end ledger

	var result []uint64
	for _, toid := range toids {
		if toid >= startTOID && toid <= endTOID {
			result = append(result, toid)
		}
	}
	return result
}

// IntersectTOIDLists returns the intersection of two sorted TOID lists.
func IntersectTOIDLists(a, b []uint64) []uint64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	var result []uint64
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}

	return result
}

// UnionTOIDLists returns the union of two sorted TOID lists (for OR queries).
func UnionTOIDLists(a, b []uint64) []uint64 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	result := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	// Append remaining elements
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}

// MergeTOIDLists merges new TOIDs into an existing list (for merge operator).
// Since events arrive in ledger order, new TOIDs should be >= existing TOIDs.
// Simply concatenates the lists (assumes append-only writes).
func MergeTOIDLists(existing, new []byte) []byte {
	if len(existing) == 0 {
		return new
	}
	if len(new) == 0 {
		return existing
	}

	result := make([]byte, len(existing)+len(new))
	copy(result, existing)
	copy(result[len(existing):], new)
	return result
}

// =============================================================================
// V2 Posting List Functions (32-bit Local IDs)
// =============================================================================
//
// V2 posting lists store compact 32-bit local IDs instead of 64-bit TOIDs.
// Local ID format: (ledger_offset << 16) | event_seq (same as bitmap32)
// This enables point gets instead of range scans when fetching events.

// EncodeLocalIDForBucket computes the 32-bit local ID for a posting list bucket.
// Same format as bitmap32: (ledger_offset << 16) | event_seq
func EncodeLocalIDForBucket(ledger uint32, eventSeq uint16, bucketStart uint32) uint32 {
	ledgerOffset := uint16(ledger - bucketStart)
	return (uint32(ledgerOffset) << 16) | uint32(eventSeq)
}

// DecodeLocalIDForBucket extracts ledger and eventSeq from a local ID and bucket start.
func DecodeLocalIDForBucket(localID uint32, bucketStart uint32) (ledger uint32, eventSeq uint16) {
	ledgerOffset := uint16(localID >> 16)
	eventSeq = uint16(localID & 0xFFFF)
	ledger = bucketStart + uint32(ledgerOffset)
	return
}

// FilterLocalIDsByLedgerRange filters local IDs to only include those within the ledger range.
func FilterLocalIDsByLedgerRange(localIDs []uint32, bucketStart, startLedger, endLedger uint32) []uint32 {
	if len(localIDs) == 0 {
		return nil
	}

	// Compute local ID boundaries
	var startOffset, endOffset uint32
	if startLedger > bucketStart {
		startOffset = startLedger - bucketStart
	}
	endOffset = endLedger - bucketStart
	_, bucketEnd := BucketLedgerRange(BucketID(bucketStart))
	if endLedger > bucketEnd {
		endOffset = bucketEnd - bucketStart
	}

	startLocalID := startOffset << 16 // min local ID for start ledger
	endLocalID := (endOffset << 16) | 0xFFFF // max local ID for end ledger

	var result []uint32
	for _, id := range localIDs {
		if id >= startLocalID && id <= endLocalID {
			result = append(result, id)
		}
	}
	return result
}

// IntersectLocalIDLists returns the intersection of two sorted local ID lists.
func IntersectLocalIDLists(a, b []uint32) []uint32 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	var result []uint32
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}

	return result
}

// UnionLocalIDLists returns the union of two sorted local ID lists.
func UnionLocalIDLists(a, b []uint32) []uint32 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	result := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}

// EncodeLocalIDListDeltaVarint encodes local IDs using delta-varint compression.
// Local IDs are cast to uint64 internally for code reuse with varint encoding.
// IDs must be sorted in ascending order.
func EncodeLocalIDListDeltaVarint(ids []uint32) []byte {
	if len(ids) == 0 {
		return nil
	}

	// Convert to uint64 for encoding
	toids := make([]uint64, len(ids))
	for i, id := range ids {
		toids[i] = uint64(id)
	}
	return EncodeTOIDListDeltaVarint(toids)
}

// DecodeLocalIDListDeltaVarint decodes delta-varint encoded local IDs.
func DecodeLocalIDListDeltaVarint(data []byte) []uint32 {
	toids := DecodeTOIDListDeltaVarint(data)
	if len(toids) == 0 {
		return nil
	}

	ids := make([]uint32, len(toids))
	for i, t := range toids {
		ids[i] = uint32(t)
	}
	return ids
}

// MergeLocalIDListsDeltaVarint merges two delta-varint encoded local ID lists.
// Both inputs must be sorted. Output is sorted delta-varint encoded.
func MergeLocalIDListsDeltaVarint(existing, new []byte) []byte {
	existingIDs := DecodeLocalIDListDeltaVarint(existing)
	newIDs := DecodeLocalIDListDeltaVarint(new)

	if len(existingIDs) == 0 {
		return new
	}
	if len(newIDs) == 0 {
		return existing
	}

	merged := UnionLocalIDLists(existingIDs, newIDs)
	return EncodeLocalIDListDeltaVarint(merged)
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
