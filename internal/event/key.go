// Package event provides event key encoding/decoding and binary format handling.
package event

import "encoding/binary"

// =============================================================================
// TOID (Transaction Order ID) Format
// =============================================================================
// TOID Format (64 bits):
//   (ledger_seq << 32) | (tx_order << 12) | op_index
//   - ledger_seq: 32 bits (0 - 4,294,967,295)
//   - tx_order:   20 bits (0 - 1,048,575)
//   - op_index:   12 bits (0 - 4,095)

// EncodeTOID encodes ledger, transaction, and operation into a 64-bit TOID.
func EncodeTOID(ledger uint32, tx uint32, op uint32) uint64 {
	return (uint64(ledger) << 32) | (uint64(tx&0xFFFFF) << 12) | uint64(op&0xFFF)
}

// DecodeTOID extracts ledger, transaction, and operation from a TOID.
func DecodeTOID(toid uint64) (ledger uint32, tx uint32, op uint32) {
	ledger = uint32(toid >> 32)
	tx = uint32((toid >> 12) & 0xFFFFF)
	op = uint32(toid & 0xFFF)
	return
}

// =============================================================================
// 10-byte RocksDB Key Format (TOID + Event Index)
// =============================================================================
// Format: [toid:8][event_index:2] (big-endian)
//   - toid:        64-bit TOID, big-endian
//   - event_index: 16-bit event index within operation, big-endian

// EncodeKey creates a 10-byte RocksDB key from TOID and event index.
func EncodeKey(toid uint64, eventIndex uint16) []byte {
	key := make([]byte, 10)
	binary.BigEndian.PutUint64(key[0:8], toid)
	binary.BigEndian.PutUint16(key[8:10], eventIndex)
	return key
}

// DecodeKey extracts TOID and event index from a 10-byte RocksDB key.
func DecodeKey(key []byte) (toid uint64, eventIndex uint16) {
	if len(key) < 10 {
		return 0, 0
	}
	toid = binary.BigEndian.Uint64(key[0:8])
	eventIndex = binary.BigEndian.Uint16(key[8:10])
	return
}

// EncodeKeyFromParts creates a 10-byte key from individual components.
// This is a convenience function that combines EncodeTOID and EncodeKey.
func EncodeKeyFromParts(ledger uint32, tx uint32, op uint32, eventIndex uint16) []byte {
	toid := EncodeTOID(ledger, tx, op)
	return EncodeKey(toid, eventIndex)
}

// DecodeKeyFull extracts all components from a 10-byte RocksDB key.
// Returns ledger, tx, op, and event index.
func DecodeKeyFull(key []byte) (ledger uint32, tx uint32, op uint32, eventIndex uint16) {
	toid, eventIndex := DecodeKey(key)
	ledger, tx, op = DecodeTOID(toid)
	return
}

// =============================================================================
// V2 Event Key Functions (Sequential Event IDs)
// =============================================================================
// V2 key format: [ledger:4][event_seq:2] = 6 bytes
// Used with bitmap32 indexes for near-zero-cost deserialization via FromBuffer.
// event_seq is assigned sequentially per ledger during ingestion (0, 1, 2, ...).

// EncodeKeyV2 creates a 6-byte RocksDB key from ledger and event sequence.
// Format: [ledger:4][event_seq:2] (big-endian)
func EncodeKeyV2(ledger uint32, eventSeq uint16) []byte {
	key := make([]byte, 6)
	binary.BigEndian.PutUint32(key[0:4], ledger)
	binary.BigEndian.PutUint16(key[4:6], eventSeq)
	return key
}

// =============================================================================
// Bitmap32 Local ID Functions
// =============================================================================
// Local ID within a segment: (ledger_offset << 16) | event_seq = 32 bits
// ledger_offset = ledger - segment_start (0–9999, fits in 14 bits of uint16)
// event_seq = 0–65535 (16 bits)

// EncodeBitmap32LocalID encodes a ledger offset and event sequence into a 32-bit local ID.
func EncodeBitmap32LocalID(ledgerOffset uint16, eventSeq uint16) uint32 {
	return (uint32(ledgerOffset) << 16) | uint32(eventSeq)
}

// DecodeBitmap32LocalID decodes a 32-bit local ID into ledger offset and event sequence.
func DecodeBitmap32LocalID(localID uint32) (ledgerOffset uint16, eventSeq uint16) {
	ledgerOffset = uint16(localID >> 16)
	eventSeq = uint16(localID & 0xFFFF)
	return
}

// LocalIDToKeyV2 converts a bitmap32 local ID and segment start to a V2 event key.
func LocalIDToKeyV2(segmentStart uint32, localID uint32) []byte {
	ledgerOffset, eventSeq := DecodeBitmap32LocalID(localID)
	ledger := segmentStart + uint32(ledgerOffset)
	return EncodeKeyV2(ledger, eventSeq)
}

// =============================================================================
// Posting List Bucket Local ID Functions
// =============================================================================
// For posting lists using bucket-based storage with 32-bit local IDs.

// EncodeLocalIDForBucket computes the 32-bit local ID for a posting list bucket.
func EncodeLocalIDForBucket(ledger uint32, eventSeq uint16, bucketStart uint32) uint32 {
	ledgerOffset := uint16(ledger - bucketStart)
	return EncodeBitmap32LocalID(ledgerOffset, eventSeq)
}

// DecodeLocalIDForBucket extracts ledger and eventSeq from a local ID and bucket start.
func DecodeLocalIDForBucket(localID uint32, bucketStart uint32) (ledger uint32, eventSeq uint16) {
	ledgerOffset, eventSeq := DecodeBitmap32LocalID(localID)
	ledger = bucketStart + uint32(ledgerOffset)
	return
}
