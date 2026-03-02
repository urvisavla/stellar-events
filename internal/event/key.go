// Package event provides event key encoding/decoding and binary format handling.
package event

import "encoding/binary"

// =============================================================================
// Event Key Functions (Dense ID Keys)
// =============================================================================
// Key format: [segmentID:4][denseID:4] = 8 bytes (big-endian)
// Used with bitmap32 indexes — dense ID comes directly from bitmap, no ledger map indirection.
// Metadata (ledger, eventSeq) derived from ledger map after fetch.

// EncodeKey creates an 8-byte RocksDB key from segment ID and dense local ID.
// Format: [segmentID:4][denseID:4] (big-endian)
func EncodeKey(segmentID uint32, denseID uint32) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[0:4], segmentID)
	binary.BigEndian.PutUint32(key[4:8], denseID)
	return key
}

// DecodeKey decodes an 8-byte key into segment ID and dense local ID.
func DecodeKey(key []byte) (segmentID uint32, denseID uint32) {
	segmentID = binary.BigEndian.Uint32(key[0:4])
	denseID = binary.BigEndian.Uint32(key[4:8])
	return
}
