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
// Bitmap64 Key Functions (TOID-based, operation-level granularity)
// =============================================================================
// Bitmap64 now stores TOIDs directly (not event keys), making it comparable
// to posting lists. Both index at operation-level, not event-level.
//
// Format: TOID = [ledger:32][tx:20][op:12] = 64 bits
//
// Note: Event index is NOT stored in bitmap. When fetching events, all events
// for a TOID are retrieved (same as posting list behavior).

// EncodeBitmapKey encodes event position into a 64-bit TOID for bitmap storage.
// The evt parameter is ignored - bitmap64 stores at TOID (operation) level.
func EncodeBitmapKey(ledger uint32, tx, op, evt uint16) uint64 {
	// Use EncodeTOID - evt is ignored, bitmap stores at operation level
	return EncodeTOID(ledger, uint32(tx), uint32(op))
}

// DecodeBitmapKey decodes a 64-bit TOID from bitmap into components.
// evt is always 0 since bitmap stores at operation level.
func DecodeBitmapKey(key uint64) (ledger uint32, tx, op, evt uint16) {
	l, t, o := DecodeTOID(key)
	return l, uint16(t), uint16(o), 0
}

// =============================================================================
// V2 Event Key Functions (Sequential Event IDs)
// =============================================================================
// V2 key format: [ledger:4][event_seq:2] = 6 bytes
// Used with bitmap32 indexes for near-zero-cost deserialization via FromBuffer.
// event_seq is assigned sequentially per ledger during ingestion (0, 1, 2, ...).

// EncodeEventKeyV2 creates a 6-byte RocksDB key from ledger and event sequence.
// Format: [ledger:4][event_seq:2] (big-endian)
func EncodeEventKeyV2(ledger uint32, eventSeq uint16) []byte {
	key := make([]byte, 6)
	binary.BigEndian.PutUint32(key[0:4], ledger)
	binary.BigEndian.PutUint16(key[4:6], eventSeq)
	return key
}

// DecodeEventKeyV2 extracts ledger and event sequence from a 6-byte V2 key.
func DecodeEventKeyV2(key []byte) (ledger uint32, eventSeq uint16) {
	if len(key) < 6 {
		return 0, 0
	}
	ledger = binary.BigEndian.Uint32(key[0:4])
	eventSeq = binary.BigEndian.Uint16(key[4:6])
	return
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

// LocalIDToEventKeyV2 converts a bitmap32 local ID and segment start to a V2 event key.
func LocalIDToEventKeyV2(segmentStart uint32, localID uint32) []byte {
	ledgerOffset, eventSeq := DecodeBitmap32LocalID(localID)
	ledger := segmentStart + uint32(ledgerOffset)
	return EncodeEventKeyV2(ledger, eventSeq)
}
