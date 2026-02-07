package event

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/urvisavla/stellar-events/internal/query"
)

// =============================================================================
// Event Types (Write Path)
// =============================================================================

// IngestEvent represents an event captured during ingestion.
// Used for fast ingestion - no JSON serialization overhead.
// Pre-extracted fields avoid re-parsing XDR during indexing and querying.
type IngestEvent struct {
	LedgerSequence   uint32
	TransactionIndex uint32 // Changed from uint16: TOID uses 20 bits (0-1,048,575)
	OperationIndex   uint16 // TOID uses 12 bits (0-4,095)
	EventIndex       uint16
	RawXDR           []byte

	// Pre-extracted fields for indexing (avoids re-parsing XDR)
	ContractID []byte   // 32 bytes if present, nil otherwise
	Topics     [][]byte // Pre-marshaled topic XDR bytes
	TxHash     []byte   // 32 bytes - transaction hash

	// Pre-extracted fields for binary format storage (avoids XDR parsing at query time)
	EventType      int       // 0=contract, 1=system, 2=diagnostic
	DataBytes      []byte    // Pre-marshaled SCVal data bytes
	LedgerClosedAt time.Time // Ledger close time for the event
	Successful     bool      // True if event is from a successful contract call
}

// Binary event format v2 for fast queries - NO XDR parsing at query time.
//
// Format (80-byte fixed header + variable topics and data):
//   [version:1][type:1][flags:1][topic_count:1][contract_id:32][tx_hash:32]
//   [ledger_closed_at:8][value_len:4][topics...][value]
//
// Fields derived from key (NOT stored in value):
//   - ledger: from TOID (key bytes 0-3)
//   - tx_index: from TOID ((toid >> 12) & 0xFFFFF)
//   - op_index: from TOID (toid & 0xFFF)
//   - event_index: from key bytes 8-10
//
// Version byte allows future format changes.
// Flags: bit 0 = has_contract_id, bit 1 = successful (inSuccessfulContractCall)
//
// Topics section:
//   For each topic: [topic_len:2][topic_bytes:var]
//
// Value section:
//   Raw SCVal bytes (pre-extracted from XDR during ingestion)
//
// This format allows:
//   - Fast contract ID filtering (read bytes 4-36)
//   - Fast topic filtering (parse topic section)
//   - Zero XDR unmarshalling at query time
//   - O(1) access to all fixed fields

const (
	binaryFormatVersion   = 0x02 // Version 2 with tx_hash and ledger_closed_at
	binaryFormatVersionV1 = 0x01 // Legacy version 1

	// Header size for v2 format
	binaryHeaderSizeV2 = 80

	// BinaryEventType* constants match XDR ContractEventType
	// XDR: SYSTEM=0, CONTRACT=1, DIAGNOSTIC=2
	BinaryEventTypeSystem     = 0
	BinaryEventTypeContract   = 1
	BinaryEventTypeDiagnostic = 2

	// Flags
	flagHasContractID = 0x01
	flagSuccessful    = 0x02
)

// EncodeBinaryEvent encodes an IngestEvent to binary format v2.
// Uses pre-extracted fields from the IngestEvent struct.
func EncodeBinaryEvent(event *IngestEvent) []byte {
	// Calculate total size
	// Header: version(1) + type(1) + flags(1) + topic_count(1) + contract_id(32) +
	//         tx_hash(32) + ledger_closed_at(8) + value_len(4) = 80 bytes
	headerSize := binaryHeaderSizeV2

	topicsSize := 0
	for _, topic := range event.Topics {
		topicsSize += 2 + len(topic) // length prefix + data
	}

	totalSize := headerSize + topicsSize + len(event.DataBytes)
	buf := make([]byte, totalSize)

	// Write header
	buf[0] = binaryFormatVersion
	buf[1] = byte(event.EventType)

	// Flags
	flags := byte(0)
	if len(event.ContractID) > 0 {
		flags |= flagHasContractID
	}
	if event.Successful {
		flags |= flagSuccessful
	}
	buf[2] = flags
	buf[3] = byte(len(event.Topics))

	// Contract ID (32 bytes, zero-padded if not present)
	if len(event.ContractID) >= 32 {
		copy(buf[4:36], event.ContractID[:32])
	}

	// Transaction hash (32 bytes)
	if len(event.TxHash) >= 32 {
		copy(buf[36:68], event.TxHash[:32])
	}

	// Ledger close time (8 bytes, Unix timestamp as int64)
	binary.BigEndian.PutUint64(buf[68:76], uint64(event.LedgerClosedAt.Unix()))

	// Data/value length (4 bytes)
	binary.BigEndian.PutUint32(buf[76:80], uint32(len(event.DataBytes)))

	// Topics
	offset := 80
	for _, topic := range event.Topics {
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(topic)))
		copy(buf[offset+2:], topic)
		offset += 2 + len(topic)
	}

	// Data/value (pre-extracted SCVal bytes)
	copy(buf[offset:], event.DataBytes)

	return buf
}

// BinaryEventHeader contains pre-parsed header fields for fast filtering.
type BinaryEventHeader struct {
	Version        byte
	Type           byte
	Flags          byte
	NumTopics      byte
	ContractID     []byte    // 32 bytes slice into original buffer
	TxHash         []byte    // 32 bytes slice into original buffer
	LedgerClosedAt time.Time // Parsed from Unix timestamp
	DataLen        uint32
	TopicsData     []byte // Slice containing all topics data
	Data           []byte // Slice containing the pre-extracted SCVal data bytes
}

// ParseBinaryHeader parses just the header for fast filtering.
// Returns nil if the format is not binary (for backward compatibility with XDR).
func ParseBinaryHeader(data []byte) *BinaryEventHeader {
	if len(data) < 4 {
		return nil
	}

	version := data[0]

	// Handle v2 format
	if version == binaryFormatVersion {
		if len(data) < binaryHeaderSizeV2 {
			return nil
		}

		h := &BinaryEventHeader{
			Version:        version,
			Type:           data[1],
			Flags:          data[2],
			NumTopics:      data[3],
			ContractID:     data[4:36],
			TxHash:         data[36:68],
			LedgerClosedAt: time.Unix(int64(binary.BigEndian.Uint64(data[68:76])), 0).UTC(),
			DataLen:        binary.BigEndian.Uint32(data[76:80]),
		}

		// Calculate topics section size
		offset := 80
		for i := 0; i < int(h.NumTopics); i++ {
			if offset+2 > len(data) {
				return nil // Corrupt data
			}
			topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			offset += 2 + topicLen
		}

		h.TopicsData = data[80:offset]
		h.Data = data[offset:]

		return h
	}

	// Handle v1 format for backward compatibility
	if version == binaryFormatVersionV1 {
		if len(data) < 40 {
			return nil
		}

		h := &BinaryEventHeader{
			Version:    version,
			Type:       data[1],
			Flags:      data[2],
			NumTopics:  data[3],
			ContractID: data[4:36],
			DataLen:    binary.BigEndian.Uint32(data[36:40]),
		}

		// Calculate topics section size
		offset := 40
		for i := 0; i < int(h.NumTopics); i++ {
			if offset+2 > len(data) {
				return nil // Corrupt data
			}
			topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			offset += 2 + topicLen
		}

		h.TopicsData = data[40:offset]
		h.Data = data[offset:]

		return h
	}

	return nil // Unknown version or not binary format
}

// HasContractID returns true if the event has a contract ID.
func (h *BinaryEventHeader) HasContractID() bool {
	return h.Flags&flagHasContractID != 0
}

// IsSuccessful returns true if the event is from a successful contract call.
func (h *BinaryEventHeader) IsSuccessful() bool {
	return h.Flags&flagSuccessful != 0
}

// GetTopics parses and returns all topics from the header.
func (h *BinaryEventHeader) GetTopics() [][]byte {
	topics := make([][]byte, 0, h.NumTopics)
	offset := 0
	data := h.TopicsData

	for i := 0; i < int(h.NumTopics); i++ {
		if offset+2 > len(data) {
			break
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		if offset+2+topicLen > len(data) {
			break
		}
		topics = append(topics, data[offset+2:offset+2+topicLen])
		offset += 2 + topicLen
	}

	return topics
}

// MatchesContractID checks if the event matches the given contract ID.
// This is a fast check that doesn't require full parsing.
func (h *BinaryEventHeader) MatchesContractID(contractID []byte) bool {
	if !h.HasContractID() {
		return len(contractID) == 0
	}
	if len(contractID) == 0 {
		return true // No filter
	}
	if len(contractID) > 32 {
		contractID = contractID[:32]
	}

	// Compare bytes directly
	for i := 0; i < len(contractID); i++ {
		if h.ContractID[i] != contractID[i] {
			return false
		}
	}
	return true
}

// MatchesTopics checks if the event matches the given topic filters (positional).
// filter[0] must match topics[0], filter[1] must match topics[1], etc.
// Empty filter entries are wildcards.
func (h *BinaryEventHeader) MatchesTopics(topicFilters [][]byte) bool {
	topics := h.GetTopics()

	for i, filter := range topicFilters {
		if len(filter) == 0 {
			continue // Wildcard
		}
		if i >= len(topics) {
			return false // Topic doesn't exist
		}
		// Compare bytes
		if len(filter) != len(topics[i]) {
			return false
		}
		for j := 0; j < len(filter); j++ {
			if filter[j] != topics[i][j] {
				return false
			}
		}
	}
	return true
}

// MatchesTopicsNonPositional checks if all filter topics appear somewhere in the event's topics.
// Each filter must match at least one topic at any position (AND logic).
// This is used for posting list and bitmap64 queries with --topics flag.
func (h *BinaryEventHeader) MatchesTopicsNonPositional(topicFilters [][]byte) bool {
	topics := h.GetTopics()

	for _, filter := range topicFilters {
		if len(filter) == 0 {
			continue // Wildcard
		}

		found := false
		for _, topic := range topics {
			if len(filter) == len(topic) {
				match := true
				for j := 0; j < len(filter); j++ {
					if filter[j] != topic[j] {
						match = false
						break
					}
				}
				if match {
					found = true
					break
				}
			}
		}

		if !found {
			return false
		}
	}
	return true
}

// DecodeBinaryToQueryEvent converts binary format to query.Event.
// Zero XDR parsing - all fields are directly accessible.
func DecodeBinaryToQueryEvent(data []byte, ledger uint32, tx, op uint32, eventIdx uint16) (*query.Event, error) {
	h := ParseBinaryHeader(data)
	if h == nil {
		return nil, fmt.Errorf("invalid binary format")
	}

	event := &query.Event{
		LedgerSequence:   ledger,
		TransactionIndex: int(tx),
		OperationIndex:   int(op),
		EventIndex:       int(eventIdx),
	}

	// Contract ID - encode as strkey (C...)
	if h.HasContractID() {
		if encoded, err := strkey.Encode(strkey.VersionByteContract, h.ContractID); err == nil {
			event.ContractID = encoded
		}
	}

	// Transaction hash - encode as hex (v2 format only)
	if h.Version >= binaryFormatVersion && len(h.TxHash) == 32 {
		event.TransactionHash = hex.EncodeToString(h.TxHash)
	}

	// Ledger closed at (v2 format only)
	if h.Version >= binaryFormatVersion {
		event.LedgerClosedAt = h.LedgerClosedAt
	}

	// Successful flag (v2 format only)
	event.InSuccessfulContractCall = h.IsSuccessful()

	// Type - direct byte access
	switch h.Type {
	case BinaryEventTypeContract:
		event.Type = "contract"
	case BinaryEventTypeSystem:
		event.Type = "system"
	case BinaryEventTypeDiagnostic:
		event.Type = "diagnostic"
	}

	// Topics - direct byte access, just base64 encode
	topics := h.GetTopics()
	event.Topics = make([]string, len(topics))
	for i, topic := range topics {
		event.Topics[i] = base64.StdEncoding.EncodeToString(topic)
	}

	// Data - direct byte access, already pre-extracted SCVal bytes
	if len(h.Data) > 0 {
		event.Data = base64.StdEncoding.EncodeToString(h.Data)
	}

	return event, nil
}

// DecodeBinaryToQueryEventV2 converts binary format to query.Event using V2 key format.
// With V2 keys, TransactionIndex and OperationIndex are not available (set to 0).
// EventIndex is set to the eventSeq from the V2 key.
func DecodeBinaryToQueryEventV2(data []byte, ledger uint32, eventSeq uint16) (*query.Event, error) {
	return DecodeBinaryToQueryEvent(data, ledger, 0, 0, eventSeq)
}
