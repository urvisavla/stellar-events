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
