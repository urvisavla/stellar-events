package event

import (
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestEncodeBinaryEventV4_RoundTrip(t *testing.T) {
	// Build a ContractEvent with known fields
	contractID := xdr.ContractId{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
	}

	sym := xdr.ScSymbol("transfer")
	topic1 := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	topic2 := xdr.ScVal{Type: xdr.ScValTypeScvBool, B: boolPtr(true)}
	dataVal := xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: 0, Lo: 1000}}

	contractEvent := xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{topic1, topic2},
				Data:   dataVal,
			},
		},
	}

	// Wrap in DiagnosticEvent (as the ingest path now does)
	diagEvent := xdr.DiagnosticEvent{
		InSuccessfulContractCall: true,
		Event:                    contractEvent,
	}
	rawXDR, err := diagEvent.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal DiagnosticEvent: %v", err)
	}

	txHash := make([]byte, 32)
	for i := range txHash {
		txHash[i] = byte(0xaa + i)
	}

	closedAt := time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC)

	ev := &IngestEvent{
		LedgerSequence:   100000,
		TransactionIndex: 42,
		OperationIndex:   7,
		EventIndex:       3,
		RawXDR:           rawXDR,
		TxHash:           txHash,
		LedgerClosedAt:   closedAt,
		Successful:       true,
		ContractID:       contractID[:],
	}

	// Encode
	encoded := EncodeBinaryEventV4(ev)

	// Verify header size
	if len(encoded) != BinaryHeaderSizeV4+len(rawXDR) {
		t.Fatalf("encoded size: got %d, want %d", len(encoded), BinaryHeaderSizeV4+len(rawXDR))
	}

	// Verify version byte
	if encoded[0] != BinaryFormatVersionV4 {
		t.Fatalf("version byte: got 0x%02x, want 0x%02x", encoded[0], BinaryFormatVersionV4)
	}

	// Decode
	qev, err := DecodeBinaryToQueryEventV4(encoded, ev.LedgerSequence, ev.EventIndex)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify all fields
	if qev.LedgerSequence != ev.LedgerSequence {
		t.Errorf("LedgerSequence: got %d, want %d", qev.LedgerSequence, ev.LedgerSequence)
	}
	if qev.TransactionIndex != int(ev.TransactionIndex) {
		t.Errorf("TransactionIndex: got %d, want %d", qev.TransactionIndex, ev.TransactionIndex)
	}
	if qev.OperationIndex != int(ev.OperationIndex) {
		t.Errorf("OperationIndex: got %d, want %d", qev.OperationIndex, ev.OperationIndex)
	}
	if qev.EventIndex != int(ev.EventIndex) {
		t.Errorf("EventIndex: got %d, want %d", qev.EventIndex, ev.EventIndex)
	}
	if qev.InSuccessfulContractCall != true {
		t.Error("InSuccessfulContractCall: got false, want true")
	}
	if qev.Type != "contract" {
		t.Errorf("Type: got %q, want %q", qev.Type, "contract")
	}
	if qev.ContractID == "" {
		t.Error("ContractID: got empty, want non-empty strkey")
	}
	if len(qev.Topics) != 2 {
		t.Fatalf("Topics: got %d, want 2", len(qev.Topics))
	}
	if qev.Data == "" {
		t.Error("Data: got empty, want non-empty base64")
	}
	if !qev.LedgerClosedAt.Equal(closedAt) {
		t.Errorf("LedgerClosedAt: got %v, want %v", qev.LedgerClosedAt, closedAt)
	}
	if qev.TransactionHash == "" {
		t.Error("TransactionHash: got empty, want hex string")
	}
}

func TestEncodeBinaryEventV4_Unsuccessful(t *testing.T) {
	contractEvent := xdr.ContractEvent{
		Type: xdr.ContractEventTypeSystem,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}

	diagEvent := xdr.DiagnosticEvent{
		InSuccessfulContractCall: false,
		Event:                    contractEvent,
	}
	rawXDR, err := diagEvent.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	ev := &IngestEvent{
		LedgerSequence: 200,
		RawXDR:         rawXDR,
		TxHash:         make([]byte, 32),
		LedgerClosedAt: time.Now().UTC().Truncate(time.Second),
		Successful:     false,
	}

	encoded := EncodeBinaryEventV4(ev)
	qev, err := DecodeBinaryToQueryEventV4(encoded, ev.LedgerSequence, 0)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if qev.InSuccessfulContractCall != false {
		t.Error("InSuccessfulContractCall: got true, want false")
	}
	if qev.Type != "system" {
		t.Errorf("Type: got %q, want %q", qev.Type, "system")
	}
	if qev.ContractID != "" {
		t.Errorf("ContractID: got %q, want empty", qev.ContractID)
	}
}

func TestDecodeBinaryToQueryEventV4_TooShort(t *testing.T) {
	_, err := DecodeBinaryToQueryEventV4(make([]byte, 10), 0, 0)
	if err == nil {
		t.Fatal("expected error for short data")
	}
}

func TestDecodeBinaryToQueryEventV4_WrongVersion(t *testing.T) {
	data := make([]byte, 100)
	data[0] = 0x02 // wrong version
	_, err := DecodeBinaryToQueryEventV4(data, 0, 0)
	if err == nil {
		t.Fatal("expected error for wrong version")
	}
}

func boolPtr(b bool) *bool {
	return &b
}
