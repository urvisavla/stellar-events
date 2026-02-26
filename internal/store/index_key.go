package store

import (
	"crypto/sha256"
	"encoding/binary"
)

// Index Key Format (36 bytes):
//   [term_key:32][segment_id:4]
//   - term_key: full SHA-256 hash (32 bytes) of the indexed value
//   - segment_id: ledger_seq / SegmentSize (groups ~14 hours of ledgers)
//
// Used by bitmap and segment-index indexes.
// Column families separate contract vs topic data.

const (
	// IndexKeySize is the size of an index key in bytes (32 hash + 4 segment ID).
	IndexKeySize = 36
)

// EncodeIndexKeyWithSegment creates a 36-byte index key from term key and segment ID directly.
func EncodeIndexKeyWithSegment(termKey [32]byte, segmentID uint32) []byte {
	key := make([]byte, IndexKeySize)
	copy(key[0:32], termKey[:])
	binary.BigEndian.PutUint32(key[32:36], segmentID)
	return key
}

// EncodeTopicIndexKey creates a 37-byte RocksDB key for topic CF entries.
// Format: [pos:1][termHash:32][segmentID:4]
// The position byte is needed because TopicTermKey no longer embeds position.
func EncodeTopicIndexKey(pos int, termKey [32]byte, segmentID uint32) []byte {
	key := make([]byte, 37)
	key[0] = byte(pos)
	copy(key[1:33], termKey[:])
	binary.BigEndian.PutUint32(key[33:37], segmentID)
	return key
}

// ContractTermKey computes the full SHA-256 term key for a contract ID.
func ContractTermKey(contractID []byte) [32]byte {
	return sha256.Sum256(contractID)
}

// TopicTermKey computes the full SHA-256 term key for a topic XDR value.
// Position-implicit: the position is NOT embedded in the hash.
// Instead, position is tracked by separate per-field maps/files.
func TopicTermKey(topicXDR []byte) [32]byte {
	return sha256.Sum256(topicXDR)
}
