package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/linxGnu/grocksdb"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/progress"
)

// RocksDBHotSegmentWriter writes hot segment data into RocksDB column families
// instead of flat files. Parallel to HotSegmentWriter for benchmarking.
type RocksDBHotSegmentWriter struct {
	backend     *RocksDBBackend
	segmentPath string // for cold output path
	segmentID   uint32

	nextEventID      uint32
	cumulativeEvents uint32
	ledgersWritten   uint32
	ledgerOffsData   []byte // accumulated cumulative counts (4 bytes per ledger)
	eventBytesWritten int64
	nextDeltaSeqNum  uint32
}

var _ HotWriter = (*RocksDBHotSegmentWriter)(nil)

// NewRocksDBHotSegmentWriter creates a new RocksDB-backed hot segment writer.
func NewRocksDBHotSegmentWriter(backend *RocksDBBackend, segmentPath string, segmentID uint32) *RocksDBHotSegmentWriter {
	return &RocksDBHotSegmentWriter{
		backend:     backend,
		segmentPath: segmentPath,
		segmentID:   segmentID,
	}
}

// hotDeltaKey builds an 8-byte key for the index_deltas CF: [segID:4 BE][seqNum:4 BE].
func hotDeltaKey(segmentID, seqNum uint32) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key[0:4], segmentID)
	binary.BigEndian.PutUint32(key[4:8], seqNum)
	return key
}

// WriteLedger processes a batch of events for a single ledger, writing to RocksDB.
func (w *RocksDBHotSegmentWriter) WriteLedger(events []*event.IngestEvent, indexStore *IndexStore) error {
	if len(events) == 0 {
		// Record cumulative count in memory; defer RocksDB write to next
		// non-empty ledger or Fsync to avoid 40 KB PutCF per empty ledger.
		w.ledgersWritten++
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], w.cumulativeEvents)
		w.ledgerOffsData = append(w.ledgerOffsData, buf[:]...)
		return nil
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	startID := w.nextEventID

	for i, ev := range events {
		eventID := startID + uint32(i)

		// Write event to events CF
		encoded := event.EncodeBinaryEvent(ev)
		batch.PutCF(w.backend.cfEvents, EncodeKey(w.segmentID, eventID), encoded)
		w.eventBytesWritten += int64(len(encoded))

		// Write index deltas
		if len(ev.ContractID) > 0 {
			termHash := ContractTermKey(ev.ContractID)
			var deltaVal [indexDeltaSize]byte
			deltaVal[0] = 0x00
			copy(deltaVal[1:17], termHash[:])
			binary.LittleEndian.PutUint32(deltaVal[17:21], eventID)
			batch.PutCF(w.backend.cfIndexDeltas, hotDeltaKey(w.segmentID, w.nextDeltaSeqNum), deltaVal[:])
			w.nextDeltaSeqNum++
		}
		for pos, topicBytes := range ev.Topics {
			if pos > 3 {
				break
			}
			termHash := TopicTermKey(topicBytes)
			var deltaVal [indexDeltaSize]byte
			deltaVal[0] = byte(pos + 1)
			copy(deltaVal[1:17], termHash[:])
			binary.LittleEndian.PutUint32(deltaVal[17:21], eventID)
			batch.PutCF(w.backend.cfIndexDeltas, hotDeltaKey(w.segmentID, w.nextDeltaSeqNum), deltaVal[:])
			w.nextDeltaSeqNum++
		}

		// Update in-memory bitmaps
		if len(ev.ContractID) > 0 {
			indexStore.AddContractEvent(ev.ContractID, w.segmentID, eventID)
		}
		for pos, topicBytes := range ev.Topics {
			indexStore.AddTopicEvent(pos, topicBytes, w.segmentID, eventID)
		}
	}

	w.nextEventID = startID + uint32(len(events))

	// Update cumulative counts
	w.cumulativeEvents += uint32(len(events))
	w.ledgersWritten++
	var cumBuf [4]byte
	binary.LittleEndian.PutUint32(cumBuf[:], w.cumulativeEvents)
	w.ledgerOffsData = append(w.ledgerOffsData, cumBuf[:]...)

	// Write padded ledger offsets to default CF via batch
	paddedOffs := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedOffs, w.ledgerOffsData)
	batch.PutCF(w.backend.cfDefault, SegmentLedgerOffsetsKey(w.segmentID), paddedOffs)

	if err := w.backend.db.Write(w.backend.wo, batch); err != nil {
		return fmt.Errorf("write hot RocksDB batch: %w", err)
	}

	return nil
}

// writeLedgerOffsets writes the padded ledger offsets to default CF (for empty ledger case).
func (w *RocksDBHotSegmentWriter) writeLedgerOffsets() error {
	paddedOffs := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedOffs, w.ledgerOffsData)
	return w.backend.db.PutCF(w.backend.wo, w.backend.cfDefault, SegmentLedgerOffsetsKey(w.segmentID), paddedOffs)
}

// FlushBuffers is a no-op for RocksDB (data goes through WriteBatch).
func (w *RocksDBHotSegmentWriter) FlushBuffers() error {
	return nil
}

// Fsync writes pending ledger offsets to RocksDB, then flushes hot CFs to SST files.
func (w *RocksDBHotSegmentWriter) Fsync() error {
	if err := w.writeLedgerOffsets(); err != nil {
		return fmt.Errorf("write ledger offsets before fsync: %w", err)
	}
	return w.backend.FlushHotCFs()
}

// CommittedLengths returns approximate sizes from tracked counters.
func (w *RocksDBHotSegmentWriter) CommittedLengths() HotSegmentMeta {
	return HotSegmentMeta{
		EventsDatLen:   w.eventBytesWritten,
		EventsIdxLen:   int64(w.nextEventID) * 8,
		IndexDeltasLen: int64(w.nextDeltaSeqNum) * indexDeltaSize,
		LedgerOffsLen:  int64(w.ledgersWritten) * 4,
	}
}

// ConvertToCold converts hot RocksDB data to cold flat files, then deletes hot data.
func (w *RocksDBHotSegmentWriter) ConvertToCold(indexStore *IndexStore, sdw *SegmentDataWriter, stats *progress.SegmentStats) error {
	totalStart := time.Now()
	segID := w.segmentID
	coldBasePath := filepath.Join(w.segmentPath, "cold")

	fmt.Fprintf(os.Stderr, "\n[hot→cold %06d] starting conversion (rocksdb, %d events, %d ledgers)\n",
		segID, w.nextEventID, w.ledgersWritten)

	// Empty segment
	if w.nextEventID == 0 {
		fmt.Fprintf(os.Stderr, "  [hot→cold %06d] empty segment, skipping\n", segID)
		w.Cleanup()
		return nil
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Step 1: Flush in-memory bitmaps
	t0 := time.Now()
	if err := indexStore.Flush(); err != nil {
		return fmt.Errorf("flush bitmap indexes: %w", err)
	}
	flushTime := time.Since(t0)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] flush bitmaps: %v\n", segID, flushTime)

	cached := indexStore.PopSegmentTerms(segID)
	if cached == nil {
		return fmt.Errorf("no cached terms for segment %d after flush", segID)
	}

	// Step 2a: Build events.pack from RocksDB events CF
	t1 := time.Now()

	paddedLedgerOffs := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedLedgerOffs, w.ledgerOffsData)

	if sdw != nil {
		if err := sdw.StartChunk(segID); err != nil {
			return fmt.Errorf("start cold chunk: %w", err)
		}

		// Iterate events CF for this segment
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()
		it := w.backend.db.NewIteratorCF(ro, w.backend.cfEvents)
		defer it.Close()

		startKey := EncodeKey(segID, 0)
		endKey := EncodeKey(segID+1, 0)

		eventIdx := uint32(0)
		for it.Seek(startKey); it.Valid(); it.Next() {
			key := it.Key()
			keyData := key.Data()
			if len(keyData) < 8 {
				key.Free()
				break
			}
			// Check if we've gone past this segment
			if bytes.Compare(keyData, endKey) >= 0 {
				key.Free()
				break
			}
			key.Free()

			value := it.Value()
			eventData := make([]byte, value.Size())
			copy(eventData, value.Data())
			value.Free()

			if err := sdw.AppendEvent(eventIdx, eventData); err != nil {
				return fmt.Errorf("append event %d to cold pack: %w", eventIdx, err)
			}
			eventIdx++
		}
		if err := it.Err(); err != nil {
			return fmt.Errorf("iterate events CF for segment %d: %w", segID, err)
		}

		if err := sdw.FinalizeChunk(paddedLedgerOffs); err != nil {
			return fmt.Errorf("finalize cold events.pack: %w", err)
		}
	}

	eventsPackTime := time.Since(t1)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] events.pack (%d events): %v\n", segID, w.nextEventID, eventsPackTime)

	// Step 2b: Build index files from flushed bitmaps
	t2 := time.Now()
	if err := WriteSegmentDir(coldBasePath, segID, cached.Contracts, cached.Topics); err != nil {
		return fmt.Errorf("write cold index files: %w", err)
	}
	mphfTime := time.Since(t2)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] MPHF index build: %v\n", segID, mphfTime)

	// Step 3: Delete hot data from RocksDB
	t3 := time.Now()
	if err := w.backend.DeleteHotSegment(segID); err != nil {
		fmt.Fprintf(os.Stderr, "  [hot→cold %06d] warning: cleanup failed: %v\n", segID, err)
	}
	cleanupTime := time.Since(t3)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] cleanup: %v\n", segID, cleanupTime)

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	beforeMB := memBefore.HeapAlloc / (1024 * 1024)
	afterMB := memAfter.HeapAlloc / (1024 * 1024)
	freedMB := int64(beforeMB) - int64(afterMB)

	totalTime := time.Since(totalStart)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] total: %v (flush=%v events.pack=%v mphf=%v cleanup=%v) heap freed %d MB (%d→%d)\n",
		segID, totalTime, flushTime, eventsPackTime, mphfTime, cleanupTime, freedMB, beforeMB, afterMB)

	// Fill stats
	if stats != nil {
		stats.FreezeWallMs = float64(totalTime.Microseconds()) / 1000
		stats.FlushMs = float64(flushTime.Microseconds()) / 1000
		stats.EventsPackMs = float64(eventsPackTime.Microseconds()) / 1000
		stats.MphfMs = float64(mphfTime.Microseconds()) / 1000
		stats.CleanupMs = float64(cleanupTime.Microseconds()) / 1000
		stats.HeapFreedMB = freedMB
		stats.ContractTerms = len(cached.Contracts)
		for i, t := range cached.Topics {
			switch i {
			case 0:
				stats.Topic0Terms = len(t)
			case 1:
				stats.Topic1Terms = len(t)
			case 2:
				stats.Topic2Terms = len(t)
			case 3:
				stats.Topic3Terms = len(t)
			}
		}
		stats.IndexTerms = stats.ContractTerms + stats.Topic0Terms + stats.Topic1Terms + stats.Topic2Terms + stats.Topic3Terms

		coldDir := filepath.Join(coldBasePath, fmt.Sprintf("%06d", segID))
		for _, name := range []string{"index.hash", "index.pack"} {
			if fi, err := os.Stat(filepath.Join(coldDir, name)); err == nil {
				stats.ColdIndexBytes += fi.Size()
			}
		}
		if fi, err := os.Stat(filepath.Join(coldDir, "events.pack")); err == nil {
			stats.ColdEventBytes = fi.Size()
		}
	}

	return nil
}

// Close is a no-op for RocksDB (backend lifecycle is managed externally).
func (w *RocksDBHotSegmentWriter) Close() error {
	return nil
}

// Cleanup deletes the hot segment data from RocksDB.
func (w *RocksDBHotSegmentWriter) Cleanup() error {
	return w.backend.DeleteHotSegment(w.segmentID)
}
