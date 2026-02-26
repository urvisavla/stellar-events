// Package event provides event key encoding/decoding and binary format handling.
package event

import "encoding/binary"

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
// Posting List Segment Local ID Functions
// =============================================================================
// For posting lists using segment-based storage with 32-bit local IDs.

// EncodeLocalIDForSegment computes the 32-bit local ID for a posting list segment.
func EncodeLocalIDForSegment(ledger uint32, eventSeq uint16, segmentStart uint32) uint32 {
	ledgerOffset := uint16(ledger - segmentStart)
	return EncodeBitmap32LocalID(ledgerOffset, eventSeq)
}

// DecodeLocalIDForSegment extracts ledger and eventSeq from a local ID and segment start.
func DecodeLocalIDForSegment(localID uint32, segmentStart uint32) (ledger uint32, eventSeq uint16) {
	ledgerOffset, eventSeq := DecodeBitmap32LocalID(localID)
	ledger = segmentStart + uint32(ledgerOffset)
	return
}
