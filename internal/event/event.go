package event

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

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
	RawXDR           []byte // DiagnosticEvent XDR (includes InSuccessfulContractCall + ContractEvent)

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

// Binary event format V4 stores raw DiagnosticEvent XDR as-is with a small
// metadata header containing fields NOT present in the XDR.
//
// Format (47-byte fixed header + raw XDR):
//
//	[version:1]           = 0x04
//	[tx_hash:32]          = transaction hash
//	[ledger_closed_at:8]  = Unix timestamp (int64 BE)
//	[tx_index:4]          = TransactionIndex (uint32 BE)
//	[op_index:2]          = OperationIndex (uint16 BE)
//	[raw_xdr_bytes...]    = DiagnosticEvent XDR (contains InSuccessfulContractCall + ContractEvent)
//
// Fields derived from key (NOT stored in value):
//   - ledger: from key bytes 0-3
//   - event_index: from key bytes 4-5
//
// Fields extracted from XDR at decode time (NOT in header):
//   - contractID, eventType, topics, data, inSuccessfulContractCall
const (
	BinaryFormatVersionV4 = 0x04
	BinaryHeaderSizeV4    = 47 // version(1) + tx_hash(32) + ledger_closed_at(8) + tx_index(4) + op_index(2)
)

// EncodeBinaryEventV4 encodes an IngestEvent to binary format V4.
// Stores a 47-byte metadata header followed by the raw DiagnosticEvent XDR bytes.
func EncodeBinaryEventV4(ev *IngestEvent) []byte {
	totalSize := BinaryHeaderSizeV4 + len(ev.RawXDR)
	buf := make([]byte, totalSize)

	// Version
	buf[0] = BinaryFormatVersionV4

	// Transaction hash (32 bytes)
	if len(ev.TxHash) >= 32 {
		copy(buf[1:33], ev.TxHash[:32])
	}

	// Ledger close time (8 bytes, Unix timestamp as int64 BE)
	binary.BigEndian.PutUint64(buf[33:41], uint64(ev.LedgerClosedAt.Unix()))

	// TransactionIndex (4 bytes)
	binary.BigEndian.PutUint32(buf[41:45], ev.TransactionIndex)

	// OperationIndex (2 bytes)
	binary.BigEndian.PutUint16(buf[45:47], ev.OperationIndex)

	// Raw DiagnosticEvent XDR bytes
	copy(buf[47:], ev.RawXDR)

	return buf
}

// DecodeBinaryToQueryEventV4 converts V4 binary format to query.Event.
// Parses the 47-byte header for metadata and unmarshals the DiagnosticEvent XDR
// payload for contractID, eventType, topics, data, and inSuccessfulContractCall.
func DecodeBinaryToQueryEventV4(data []byte, ledger uint32, eventSeq uint16) (*query.Event, error) {
	if len(data) < BinaryHeaderSizeV4 {
		return nil, fmt.Errorf("V4 data too short: %d < %d", len(data), BinaryHeaderSizeV4)
	}

	if data[0] != BinaryFormatVersionV4 {
		return nil, fmt.Errorf("expected V4 format (0x%02x), got 0x%02x", BinaryFormatVersionV4, data[0])
	}

	txHash := data[1:33]
	ledgerClosedAt := time.Unix(int64(binary.BigEndian.Uint64(data[33:41])), 0).UTC()
	txIndex := binary.BigEndian.Uint32(data[41:45])
	opIndex := binary.BigEndian.Uint16(data[45:47])

	ev := &query.Event{
		LedgerSequence:   ledger,
		TransactionIndex: int(txIndex),
		OperationIndex:   int(opIndex),
		EventIndex:       int(eventSeq),
		TransactionHash:  hex.EncodeToString(txHash),
		LedgerClosedAt:   ledgerClosedAt,
	}

	// Unmarshal DiagnosticEvent XDR payload
	rawXDR := data[BinaryHeaderSizeV4:]
	var diagEvent xdr.DiagnosticEvent
	if err := diagEvent.UnmarshalBinary(rawXDR); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DiagnosticEvent XDR: %w", err)
	}

	ev.InSuccessfulContractCall = diagEvent.InSuccessfulContractCall
	xdrEvent := diagEvent.Event

	// Contract ID
	if xdrEvent.ContractId != nil {
		if encoded, err := strkey.Encode(strkey.VersionByteContract, xdrEvent.ContractId[:]); err == nil {
			ev.ContractID = encoded
		}
	}

	// Event type
	switch xdrEvent.Type {
	case xdr.ContractEventTypeContract:
		ev.Type = "contract"
	case xdr.ContractEventTypeSystem:
		ev.Type = "system"
	case xdr.ContractEventTypeDiagnostic:
		ev.Type = "diagnostic"
	}

	// Topics and data from event body
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()

		for _, topic := range body.Topics {
			topicBytes, _ := topic.MarshalBinary()
			ev.Topics = append(ev.Topics, base64.StdEncoding.EncodeToString(topicBytes))
		}

		dataBytes, _ := body.Data.MarshalBinary()
		ev.Data = base64.StdEncoding.EncodeToString(dataBytes)
	}

	return ev, nil
}
