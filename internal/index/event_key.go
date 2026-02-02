package index

import "encoding/binary"

// Event key encoding/decoding using TOID (Transaction Order ID) format.
//
// TOID Format (64 bits):
//   (ledger_seq << 32) | (tx_order << 12) | op_index
//   - ledger_seq: 32 bits (0 - 4,294,967,295)
//   - tx_order:   20 bits (0 - 1,048,575)
//   - op_index:   12 bits (0 - 4,095)
//
// 10-byte RocksDB Key Format:
//   [toid:8][event_index:2]
//   - toid:        64-bit TOID, big-endian
//   - event_index: 16-bit event index within operation, big-endian

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

// EncodeEventKey creates a 10-byte RocksDB key from TOID and event index.
// Format: [toid:8][event_index:2] (big-endian)
func EncodeEventKey(toid uint64, eventIndex uint16) []byte {
	key := make([]byte, 10)
	binary.BigEndian.PutUint64(key[0:8], toid)
	binary.BigEndian.PutUint16(key[8:10], eventIndex)
	return key
}

// DecodeEventKey extracts TOID and event index from a 10-byte RocksDB key.
func DecodeEventKey(key []byte) (toid uint64, eventIndex uint16) {
	if len(key) < 10 {
		return 0, 0
	}
	toid = binary.BigEndian.Uint64(key[0:8])
	eventIndex = binary.BigEndian.Uint16(key[8:10])
	return
}

// EncodeEventKeyFromParts creates a 10-byte key from individual components.
// This is a convenience function that combines EncodeTOID and EncodeEventKey.
func EncodeEventKeyFromParts(ledger uint32, tx uint32, op uint32, eventIndex uint16) []byte {
	toid := EncodeTOID(ledger, tx, op)
	return EncodeEventKey(toid, eventIndex)
}

// DecodeEventKeyFull extracts all components from a 10-byte RocksDB key.
// Returns ledger, tx, op, and event index.
func DecodeEventKeyFull(key []byte) (ledger uint32, tx uint32, op uint32, eventIndex uint16) {
	toid, eventIndex := DecodeEventKey(key)
	ledger, tx, op = DecodeTOID(toid)
	return
}

// LedgerFromKey extracts just the ledger sequence from a 10-byte key.
// This is optimized for range scans where only ledger comparison is needed.
func LedgerFromKey(key []byte) uint32 {
	if len(key) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(key[0:4])
}

// =============================================================================
// Legacy Bitmap Functions (for backward compatibility with event_bitmap.go)
// =============================================================================
// These use the old 64-bit format: [ledger:32][tx:16][op:8][evt:8]
// Kept for bitmap index comparison purposes.

// EncodeBitmapKey encodes event position into a 64-bit bitmap key.
// Format: [ledger:32][tx:16][op:8][evt:8] = 64 bits
// DEPRECATED: Use TOID-based functions for new code.
func EncodeBitmapKey(ledger uint32, tx, op, evt uint16) uint64 {
	return uint64(ledger)<<32 | uint64(tx)<<16 | uint64(op&0xFF)<<8 | uint64(evt&0xFF)
}

// DecodeBitmapKey decodes a 64-bit bitmap key into components.
// DEPRECATED: Use TOID-based functions for new code.
func DecodeBitmapKey(key uint64) (ledger uint32, tx, op, evt uint16) {
	ledger = uint32(key >> 32)
	tx = uint16((key >> 16) & 0xFFFF)
	op = uint16((key >> 8) & 0xFF)
	evt = uint16(key & 0xFF)
	return
}
