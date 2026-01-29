package ingest

import (
	"fmt"
	"io"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/store"
)

// ExtractEvents extracts events as raw XDR with minimal overhead
// This is the fastest extraction mode - no field parsing, just raw bytes
func ExtractEvents(xdrBytes []byte, networkPassphrase string, stats *LedgerStats) ([]*store.IngestEvent, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(xdrBytes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal LedgerCloseMeta: %w", err)
	}

	if stats != nil {
		stats.TotalLedgers++
	}

	txCount := lcm.CountTransactions()
	if txCount == 0 {
		return nil, nil
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader for ledger %d: %w", lcm.LedgerSequence(), err)
	}
	defer txReader.Close()

	var events []*store.IngestEvent
	ledgerSeq := lcm.LedgerSequence()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction: %w", err)
		}

		if stats != nil {
			stats.TotalTransactions++
		}

		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			return nil, fmt.Errorf("failed to get transaction events: %w", err)
		}

		// Capture transaction hash once for all events in this transaction
		// IMPORTANT: Copy the hash to avoid keeping the entire tx structure alive in memory
		txHash := make([]byte, 32)
		copy(txHash, tx.Hash[:])

		// Process transaction-level events
		for eventIndex, event := range txEvents.TransactionEvents {
			if stats != nil {
				stats.TransactionEvents++
				stats.TotalEvents++
			}

			rawXDR, err := event.Event.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal event XDR: %w", err)
			}

			// Extract contract ID and topics for indexing (avoids re-parsing later)
			// IMPORTANT: Copy contractID to avoid keeping the entire XDR structure alive
			var contractID []byte
			var topics [][]byte
			if event.Event.ContractId != nil {
				contractID = make([]byte, 32)
				copy(contractID, event.Event.ContractId[:])
			}
			if event.Event.Body.V == 0 {
				body := event.Event.Body.MustV0()
				for _, topic := range body.Topics {
					topicBytes, _ := topic.MarshalBinary()
					topics = append(topics, topicBytes)
				}
			}

			events = append(events, &store.IngestEvent{
				LedgerSequence:   ledgerSeq,
				TransactionIndex: uint16(tx.Index),
				OperationIndex:   0, // Transaction-level events have no operation
				EventIndex:       uint16(eventIndex),
				RawXDR:           rawXDR,
				ContractID:       contractID,
				Topics:           topics,
				TxHash:           txHash,
			})
		}

		// Process operation-level events
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIndex, event := range opEvents {
				if stats != nil {
					stats.OperationEvents++
					stats.TotalEvents++
				}

				rawXDR, err := event.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("failed to marshal event XDR: %w", err)
				}

				// Extract contract ID and topics for indexing (avoids re-parsing later)
				// IMPORTANT: Copy contractID to avoid keeping the entire XDR structure alive
				var contractID []byte
				var topics [][]byte
				if event.ContractId != nil {
					contractID = make([]byte, 32)
					copy(contractID, event.ContractId[:])
				}
				if event.Body.V == 0 {
					body := event.Body.MustV0()
					for _, topic := range body.Topics {
						topicBytes, _ := topic.MarshalBinary()
						topics = append(topics, topicBytes)
					}
				}

				events = append(events, &store.IngestEvent{
					LedgerSequence:   ledgerSeq,
					TransactionIndex: uint16(tx.Index),
					OperationIndex:   uint16(opIndex),
					EventIndex:       uint16(eventIndex),
					RawXDR:           rawXDR,
					ContractID:       contractID,
					Topics:           topics,
					TxHash:           txHash,
				})
			}
		}
	}

	return events, nil
}

// LedgerStats tracks statistics about processed ledgers for debugging
type LedgerStats struct {
	TotalLedgers      int
	TotalTransactions int
	TotalEvents       int
	OperationEvents   int
	TransactionEvents int
}

// NewLedgerStats creates a new stats tracker
func NewLedgerStats() *LedgerStats {
	return &LedgerStats{}
}
