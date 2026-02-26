package ingest

import (
	"fmt"
	"io"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/event"
)

// ExtractEventsFastFiltered extracts events without defensive memory copies, with optional diagnostic filtering.
func ExtractEventsFastFiltered(xdrBytes []byte, networkPassphrase string, stats *LedgerStats, excludeDiagnostic bool) ([]*event.IngestEvent, error) {
	return ExtractEventsWithOptions(xdrBytes, networkPassphrase, stats, true, excludeDiagnostic)
}

// ExtractEventsWithOptions extracts events with configurable memory behavior.
// When fastMode is true, skips defensive copies for better performance during bulk ingestion.
// When excludeDiagnostic is true, skips diagnostic events (type=2).
func ExtractEventsWithOptions(xdrBytes []byte, networkPassphrase string, stats *LedgerStats, fastMode bool, excludeDiagnostic bool) ([]*event.IngestEvent, error) {
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

	var events []*event.IngestEvent
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
		for eventIndex, ev := range txEvents.TransactionEvents {
			// Skip diagnostic events if configured
			if excludeDiagnostic && ev.Event.Type == xdr.ContractEventTypeDiagnostic {
				continue
			}

			if stats != nil {
				stats.TransactionEvents++
				stats.TotalEvents++
			}

			// Marshal as DiagnosticEvent XDR (wraps ContractEvent + InSuccessfulContractCall)
			diagEvent := xdr.DiagnosticEvent{
				InSuccessfulContractCall: txSuccessful,
				Event:                    ev.Event,
			}
			rawXDR, err := diagEvent.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal DiagnosticEvent XDR: %w", err)
			}

			// Extract contract ID, topics, type, and data for indexing and binary storage
			var contractID []byte
			var topics [][]byte
			var dataBytes []byte
			if ev.Event.ContractId != nil {
				if fastMode {
					contractID = ev.Event.ContractId[:]
				} else {
					contractID = make([]byte, 32)
					copy(contractID, ev.Event.ContractId[:])
				}
			}
			if ev.Event.Body.V == 0 {
				body := ev.Event.Body.MustV0()
				for _, topic := range body.Topics {
					topicBytes, _ := topic.MarshalBinary()
					topics = append(topics, topicBytes)
				}
				dataBytes, _ = body.Data.MarshalBinary()
			}

			events = append(events, &event.IngestEvent{
				LedgerSequence:   ledgerSeq,
				TransactionIndex: uint32(tx.Index),
				OperationIndex:   0xFFFF, // Transaction-level events use sentinel value
				EventIndex:       uint16(eventIndex),
				RawXDR:           rawXDR,
				ContractID:       contractID,
				Topics:           topics,
				TxHash:           txHash,
				EventType:        int(ev.Event.Type),
				DataBytes:        dataBytes,
				LedgerClosedAt:   ledgerCloseTime,
				Successful:       txSuccessful,
			})
		}

		// Process operation-level events
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIndex, ev := range opEvents {
				// Skip diagnostic events if configured
				if excludeDiagnostic && ev.Type == xdr.ContractEventTypeDiagnostic {
					continue
				}

				if stats != nil {
					stats.OperationEvents++
					stats.TotalEvents++
				}

				// Marshal as DiagnosticEvent XDR (wraps ContractEvent + InSuccessfulContractCall)
				diagEvent := xdr.DiagnosticEvent{
					InSuccessfulContractCall: txSuccessful,
					Event:                    ev,
				}
				rawXDR, err := diagEvent.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("failed to marshal DiagnosticEvent XDR: %w", err)
				}

				// Extract contract ID, topics, type, and data for indexing and binary storage
				var contractID []byte
				var topics [][]byte
				var dataBytes []byte
				if ev.ContractId != nil {
					if fastMode {
						contractID = ev.ContractId[:]
					} else {
						contractID = make([]byte, 32)
						copy(contractID, ev.ContractId[:])
					}
				}
				if ev.Body.V == 0 {
					body := ev.Body.MustV0()
					for _, topic := range body.Topics {
						topicBytes, _ := topic.MarshalBinary()
						topics = append(topics, topicBytes)
					}
					dataBytes, _ = body.Data.MarshalBinary()
				}

				events = append(events, &event.IngestEvent{
					LedgerSequence:   ledgerSeq,
					TransactionIndex: uint32(tx.Index),
					OperationIndex:   uint16(opIndex),
					EventIndex:       uint16(eventIndex),
					RawXDR:           rawXDR,
					ContractID:       contractID,
					Topics:           topics,
					TxHash:           txHash,
					EventType:        int(ev.Type),
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
