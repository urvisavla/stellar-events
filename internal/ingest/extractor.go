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
func ExtractEvents(xdrBytes []byte, networkPassphrase string, stats *LedgerStats) ([]*store.MinimalEvent, error) {
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

	var events []*store.MinimalEvent
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

			events = append(events, &store.MinimalEvent{
				LedgerSequence:   ledgerSeq,
				TransactionIndex: uint16(tx.Index),
				OperationIndex:   0, // Transaction-level events have no operation
				EventIndex:       uint16(eventIndex),
				RawXDR:           rawXDR,
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

				events = append(events, &store.MinimalEvent{
					LedgerSequence:   ledgerSeq,
					TransactionIndex: uint16(tx.Index),
					OperationIndex:   uint16(opIndex),
					EventIndex:       uint16(eventIndex),
					RawXDR:           rawXDR,
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
