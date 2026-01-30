package store

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/urvisavla/stellar-events/internal/query"
)

// Binary event format for fast queries - NO XDR parsing at query time.
//
// Format:
//   [version:1][type:1][flags:1][num_topics:1][contract_id:32][data_len:4][topics...][data]
//
// Version byte allows future format changes.
// Flags: bit 0 = has contract ID
//
// Topics section:
//   For each topic: [topic_len:2][topic_bytes:var]
//
// Data section:
//   Raw SCVal bytes (pre-extracted from XDR during ingestion)
//
// This format allows:
//   - Fast contract ID filtering (read bytes 4-36)
//   - Fast topic filtering (parse topic section)
//   - Zero XDR unmarshalling at query time

const (
	binaryFormatVersion = 0x01

	// BinaryEventType* constants match XDR ContractEventType
	BinaryEventTypeContract   = 0
	BinaryEventTypeSystem     = 1
	BinaryEventTypeDiagnostic = 2

	// Flags
	flagHasContractID = 0x01
)

// EncodeBinaryEvent encodes an IngestEvent to binary format.
// The eventType and dataBytes must be extracted from XDR during ingestion.
func EncodeBinaryEvent(event *IngestEvent, eventType int, dataBytes []byte) []byte {
	// Calculate total size
	// Header: version(1) + type(1) + flags(1) + num_topics(1) + contract_id(32) + data_len(4) = 40 bytes
	headerSize := 40

	topicsSize := 0
	for _, topic := range event.Topics {
		topicsSize += 2 + len(topic) // length prefix + data
	}

	totalSize := headerSize + topicsSize + len(dataBytes)
	buf := make([]byte, totalSize)

	// Write header
	buf[0] = binaryFormatVersion
	buf[1] = byte(eventType)

	flags := byte(0)
	if len(event.ContractID) > 0 {
		flags |= flagHasContractID
	}
	buf[2] = flags
	buf[3] = byte(len(event.Topics))

	// Contract ID (32 bytes, zero-padded if not present)
	if len(event.ContractID) >= 32 {
		copy(buf[4:36], event.ContractID[:32])
	}

	// Data length
	binary.BigEndian.PutUint32(buf[36:40], uint32(len(dataBytes)))

	// Topics
	offset := 40
	for _, topic := range event.Topics {
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(topic)))
		copy(buf[offset+2:], topic)
		offset += 2 + len(topic)
	}

	// Data (pre-extracted SCVal bytes)
	copy(buf[offset:], dataBytes)

	return buf
}

// BinaryEventHeader contains pre-parsed header fields for fast filtering.
type BinaryEventHeader struct {
	Version    byte
	Type       byte
	Flags      byte
	NumTopics  byte
	ContractID []byte // 32 bytes slice into original buffer
	DataLen    uint32
	TopicsData []byte // Slice containing all topics data
	Data       []byte // Slice containing the pre-extracted SCVal data bytes
}

// ParseBinaryHeader parses just the header for fast filtering.
// Returns nil if the format is not binary (for backward compatibility with XDR).
func ParseBinaryHeader(data []byte) *BinaryEventHeader {
	if len(data) < 40 {
		return nil
	}

	if data[0] != binaryFormatVersion {
		return nil // Not binary format (probably XDR)
	}

	h := &BinaryEventHeader{
		Version:    data[0],
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

// HasContractID returns true if the event has a contract ID.
func (h *BinaryEventHeader) HasContractID() bool {
	return h.Flags&flagHasContractID != 0
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

// MatchesTopics checks if the event matches the given topic filters.
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

// DecodeBinaryToQueryEvent converts binary format to query.Event.
// Zero XDR parsing - all fields are directly accessible.
func DecodeBinaryToQueryEvent(data []byte, ledger uint32, tx, op, eventIdx uint16) (*query.Event, error) {
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

	// Contract ID - direct byte access
	if h.HasContractID() {
		event.ContractID = base64.StdEncoding.EncodeToString(h.ContractID)
	}

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