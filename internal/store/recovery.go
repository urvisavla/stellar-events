package store

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/urvisavla/stellar-events/internal/event"
)

// RecoveryResult holds the result of a crash recovery operation.
type RecoveryResult struct {
	ResumeLedger uint32 // Next ledger to ingest (lastProcessedLedger + 1)
	SegmentID    uint32 // The recovered segment ID
	EventCount   int    // Number of events rebuilt from RocksDB
}

// Recover rebuilds in-memory state from RocksDB after a crash.
// It detects incomplete segments (where events exist in RocksDB but flat files
// aren't finalized), rebuilds bitmap indexes from the stored events, and prepares
// the store to resume ingestion.
//
// Returns nil if no recovery is needed (no events in RocksDB or segment already finalized).
func (es *Store) Recover() (*RecoveryResult, error) {
	if es.rocksDB == nil {
		return nil, fmt.Errorf("recovery requires RocksDB backend")
	}

	// Step 1: Read last processed ledger
	lastLedger, err := es.rocksDB.GetLastProcessedLedger()
	if err != nil {
		return nil, fmt.Errorf("failed to read last processed ledger: %w", err)
	}
	if lastLedger == 0 {
		return nil, nil // No events in RocksDB, nothing to recover
	}

	incompleteSegmentID := SegmentID(lastLedger)

	// Step 2: Check if the segment is already finalized
	if es.segmentPath != "" && isSegmentFullyFinalized(es.segmentPath, incompleteSegmentID) {
		// Segment is finalized — check if we're at a segment boundary
		// If lastLedger is the last ledger in a segment, finalization is complete
		nextLedger := lastLedger + 1
		nextSegmentID := SegmentID(nextLedger)
		if nextSegmentID != incompleteSegmentID {
			// We finished exactly at a segment boundary, nothing to recover
			return nil, nil
		}
	}

	// Step 3: Delete partial flat files for the incomplete segment
	if es.segmentPath != "" {
		if err := deletePartialSegmentFiles(es.segmentPath, incompleteSegmentID); err != nil {
			return nil, fmt.Errorf("failed to clean partial segment files: %w", err)
		}
	}

	// Step 4: Scan RocksDB events for the incomplete segment and rebuild bitmaps
	eventCount, err := es.rebuildFromRocksDB(incompleteSegmentID)
	if err != nil {
		return nil, fmt.Errorf("failed to rebuild segment %d from RocksDB: %w", incompleteSegmentID, err)
	}

	// Step 5: Re-open SegmentDataWriter for this segment
	if es.segmentDataWriter != nil {
		if err := es.segmentDataWriter.StartChunk(incompleteSegmentID); err != nil {
			return nil, fmt.Errorf("failed to restart segment data writer for segment %d: %w", incompleteSegmentID, err)
		}

		// Re-write events from RocksDB to the flat file writer
		if err := es.rewriteSegmentData(incompleteSegmentID); err != nil {
			return nil, fmt.Errorf("failed to rewrite segment data for segment %d: %w", incompleteSegmentID, err)
		}
	}

	// Step 6: Set store state so ingestion continues correctly
	es.lastSegmentID = incompleteSegmentID
	es.hasLastSegment = true

	return &RecoveryResult{
		ResumeLedger: lastLedger + 1,
		SegmentID:    incompleteSegmentID,
		EventCount:   eventCount,
	}, nil
}

// rebuildFromRocksDB scans all events for a segment in RocksDB and rebuilds
// the in-memory bitmap indexes and segment counters.
func (es *Store) rebuildFromRocksDB(segmentID uint32) (int, error) {
	startKey := EncodeKey(segmentID, 0)
	endKey := EncodeKey(segmentID+1, 0)

	iter := NewRocksDBEventFetcher(es.rocksDB.db, es.rocksDB.ro, es.rocksDB.cfEvents).NewEventIterator()
	defer iter.Close()

	iter.Seek(startKey)

	eventCount := 0
	for iter.Valid() {
		key := iter.Key()
		if len(key) < 8 {
			break
		}

		// Check if we've passed the segment boundary
		iterSegID, _ := DecodeKey(key)
		if iterSegID != segmentID {
			break
		}

		// Ensure we haven't gone past endKey
		if compareKeys(key, endKey) >= 0 {
			break
		}

		value := iter.Value()
		if len(value) < event.BinaryHeaderSize {
			iter.Next()
			continue
		}

		// Extract indexing fields from the binary event
		contractID, topics, err := extractIndexFields(value)
		if err != nil {
			// Skip corrupt events but continue recovery
			fmt.Fprintf(os.Stderr, "  [recovery] warning: skipping corrupt event in segment %d: %v\n", segmentID, err)
			iter.Next()
			continue
		}

		// Decode the key to get the dense ID for verification
		_, denseID := DecodeKey(key)

		// Assign dense local ID (rebuilds segmentCounters)
		// We need to know the ledger for this event — resolve via the counter
		// Since events are stored in dense ID order within a segment,
		// we need to reconstruct the ledger mapping.
		// The dense IDs are sequential, so we assign them in order.
		// We'll resolve the ledger from the RocksDB ledger offsets if available,
		// otherwise we just assign sequentially which rebuilds the same mapping.
		lm, _ := es.rocksDB.bitmapStore.LoadSegmentLedgerOffsets(segmentID)
		var ledger uint32
		if lm != nil {
			ledger, _ = lm.DenseIDToLedgerAndSeq(denseID)
		} else {
			// Fallback: compute from segment start (best effort)
			ledger = segmentID * SegmentSize
		}

		assignedID := es.indexStore.AssignDenseLocalID(segmentID, ledger)
		_ = assignedID // Should match denseID if counter starts from 0

		// Rebuild bitmap indexes
		if len(contractID) > 0 {
			es.indexStore.AddContractEvent(contractID, segmentID, denseID)
		}
		for pos, topicBytes := range topics {
			es.indexStore.AddTopicEvent(pos, topicBytes, segmentID, denseID)
		}

		eventCount++
		iter.Next()
	}

	return eventCount, nil
}

// rewriteSegmentData reads events from RocksDB for the given segment and
// re-appends them to the SegmentDataWriter (flat file event data).
func (es *Store) rewriteSegmentData(segmentID uint32) error {
	startKey := EncodeKey(segmentID, 0)
	endKey := EncodeKey(segmentID+1, 0)

	iter := NewRocksDBEventFetcher(es.rocksDB.db, es.rocksDB.ro, es.rocksDB.cfEvents).NewEventIterator()
	defer iter.Close()

	iter.Seek(startKey)

	for iter.Valid() {
		key := iter.Key()
		if len(key) < 8 {
			break
		}
		if compareKeys(key, endKey) >= 0 {
			break
		}

		_, denseID := DecodeKey(key)
		value := iter.Value()

		// Copy value since iterator data is transient
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		if err := es.segmentDataWriter.AppendEvent(denseID, valueCopy); err != nil {
			return fmt.Errorf("failed to append event %d: %w", denseID, err)
		}

		iter.Next()
	}

	return nil
}

// extractIndexFields extracts the raw contract ID and topic bytes from a binary event
// for rebuilding bitmap indexes during recovery.
func extractIndexFields(data []byte) (contractID []byte, topics [][]byte, err error) {
	if len(data) < event.BinaryHeaderSize {
		return nil, nil, fmt.Errorf("data too short: %d < %d", len(data), event.BinaryHeaderSize)
	}

	if data[0] != event.BinaryFormatVersion {
		return nil, nil, fmt.Errorf("unexpected format version: 0x%02x", data[0])
	}

	// Unmarshal DiagnosticEvent XDR payload
	rawXDR := data[event.BinaryHeaderSize:]
	var diagEvent xdr.DiagnosticEvent
	if err := diagEvent.UnmarshalBinary(rawXDR); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal DiagnosticEvent: %w", err)
	}

	xdrEvent := diagEvent.Event

	// Extract raw contract ID (32 bytes)
	if xdrEvent.ContractId != nil {
		contractID = xdrEvent.ContractId[:]
	}

	// Extract raw topic bytes (pre-marshaled XDR)
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()
		for _, topic := range body.Topics {
			topicBytes, marshalErr := topic.MarshalBinary()
			if marshalErr != nil {
				continue
			}
			topics = append(topics, topicBytes)
		}
	}

	return contractID, topics, nil
}

// isSegmentFullyFinalized checks if all required flat files exist for a segment.
func isSegmentFullyFinalized(basePath string, segmentID uint32) bool {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(basePath, dirName)

	requiredFiles := []string{
		"index.hash",
		EventsFileName, // events.pack (ledger offsets embedded as appData)
	}

	for _, f := range requiredFiles {
		if _, err := os.Stat(filepath.Join(dirPath, f)); err != nil {
			return false
		}
	}
	return true
}

// deletePartialSegmentFiles removes incomplete flat files for a segment.
// This is safe because we're about to rebuild the segment from RocksDB.
func deletePartialSegmentFiles(basePath string, segmentID uint32) error {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(basePath, dirName)

	// Check if directory exists
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil // Nothing to clean
	}

	// Remove the entire segment directory (it will be recreated during rebuild)
	fmt.Fprintf(os.Stderr, "  [recovery] removing partial segment directory: %s\n", dirPath)
	return os.RemoveAll(dirPath)
}

// compareKeys compares two byte slices lexicographically.
func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
