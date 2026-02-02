package ingest

import (
	"fmt"
	"io"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/store"
)

// ExtractEvents extracts events as raw XDR with minimal overhead.
// This is the fastest extraction mode - no field parsing, just raw bytes.
// Uses defensive copies to prevent memory leaks (safe for long-lived events).
func ExtractEvents(xdrBytes []byte, networkPassphrase string, stats *LedgerStats) ([]*store.IngestEvent, error) {
	return ExtractEventsWithOptions(xdrBytes, networkPassphrase, stats, false, false)
}

// ExtractEventsFast extracts events without defensive memory copies.
// Use this for bulk ingestion where events are written immediately and not held in memory.
// The returned events are only valid until the next call or until xdrBytes is modified.
func ExtractEventsFast(xdrBytes []byte, networkPassphrase string, stats *LedgerStats) ([]*store.IngestEvent, error) {
	return ExtractEventsWithOptions(xdrBytes, networkPassphrase, stats, true, false)
}

// ExtractEventsFastFiltered extracts events without defensive memory copies, with optional diagnostic filtering.
func ExtractEventsFastFiltered(xdrBytes []byte, networkPassphrase string, stats *LedgerStats, excludeDiagnostic bool) ([]*store.IngestEvent, error) {
	return ExtractEventsWithOptions(xdrBytes, networkPassphrase, stats, true, excludeDiagnostic)
}

// ExtractEventsWithOptions extracts events with configurable memory behavior.
// When fastMode is true, skips defensive copies for better performance during bulk ingestion.
// When excludeDiagnostic is true, skips diagnostic events (type=2).
func ExtractEventsWithOptions(xdrBytes []byte, networkPassphrase string, stats *LedgerStats, fastMode bool, excludeDiagnostic bool) ([]*store.IngestEvent, error) {
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

	// Extract ledger close time (Unix timestamp to time.Time)
	ledgerCloseTime := time.Unix(lcm.LedgerCloseTime(), 0).UTC()

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

		// Check if transaction was successful
		txSuccessful := tx.Result.Successful()

		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			return nil, fmt.Errorf("failed to get transaction events: %w", err)
		}

		// Capture transaction hash once for all events in this transaction
		var txHash []byte
		if fastMode {
			txHash = tx.Hash[:]
		} else {
			txHash = make([]byte, 32)
			copy(txHash, tx.Hash[:])
		}

		// Process transaction-level events
		for eventIndex, event := range txEvents.TransactionEvents {
			// Skip diagnostic events if configured
			if excludeDiagnostic && event.Event.Type == xdr.ContractEventTypeDiagnostic {
				continue
			}

			if stats != nil {
				stats.TransactionEvents++
				stats.TotalEvents++
			}

			rawXDR, err := event.Event.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal event XDR: %w", err)
			}

			// Extract contract ID, topics, type, and data for indexing and binary storage
			var contractID []byte
			var topics [][]byte
			var dataBytes []byte
			if event.Event.ContractId != nil {
				if fastMode {
					contractID = event.Event.ContractId[:]
				} else {
					contractID = make([]byte, 32)
					copy(contractID, event.Event.ContractId[:])
				}
			}
			if event.Event.Body.V == 0 {
				body := event.Event.Body.MustV0()
				for _, topic := range body.Topics {
					topicBytes, _ := topic.MarshalBinary()
					topics = append(topics, topicBytes)
				}
				dataBytes, _ = body.Data.MarshalBinary()
			}

			events = append(events, &store.IngestEvent{
				LedgerSequence:   ledgerSeq,
				TransactionIndex: uint32(tx.Index),
				OperationIndex:   0xFFFF, // Transaction-level events use sentinel value
				EventIndex:       uint16(eventIndex),
				RawXDR:           rawXDR,
				ContractID:       contractID,
				Topics:           topics,
				TxHash:           txHash,
				EventType:        int(event.Event.Type),
				DataBytes:        dataBytes,
				LedgerClosedAt:   ledgerCloseTime,
				Successful:       txSuccessful,
			})
		}

		// Process operation-level events
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIndex, event := range opEvents {
				// Skip diagnostic events if configured
				if excludeDiagnostic && event.Type == xdr.ContractEventTypeDiagnostic {
					continue
				}

				if stats != nil {
					stats.OperationEvents++
					stats.TotalEvents++
				}

				rawXDR, err := event.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("failed to marshal event XDR: %w", err)
				}

				// Extract contract ID, topics, type, and data for indexing and binary storage
				var contractID []byte
				var topics [][]byte
				var dataBytes []byte
				if event.ContractId != nil {
					if fastMode {
						contractID = event.ContractId[:]
					} else {
						contractID = make([]byte, 32)
						copy(contractID, event.ContractId[:])
					}
				}
				if event.Body.V == 0 {
					body := event.Body.MustV0()
					for _, topic := range body.Topics {
						topicBytes, _ := topic.MarshalBinary()
						topics = append(topics, topicBytes)
					}
					dataBytes, _ = body.Data.MarshalBinary()
				}

				events = append(events, &store.IngestEvent{
					LedgerSequence:   ledgerSeq,
					TransactionIndex: uint32(tx.Index),
					OperationIndex:   uint16(opIndex),
					EventIndex:       uint16(eventIndex),
					RawXDR:           rawXDR,
					ContractID:       contractID,
					Topics:           topics,
					TxHash:           txHash,
					EventType:        int(event.Type),
					DataBytes:        dataBytes,
					LedgerClosedAt:   ledgerCloseTime,
					Successful:       txSuccessful,
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
