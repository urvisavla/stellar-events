package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/packfile"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/progress"
)

// LiveHotSegmentWriter writes hot segment events directly to a packfile via
// packfile.LiveWriter. This builds events.pack incrementally during ingestion,
// eliminating the need to re-read and re-pack events during ConvertToCold.
//
// Index deltas are written to a separate flat file (index_deltas.dat) and
// in-memory bitmaps are maintained via IndexStore, same as HotSegmentWriter.
type LiveHotSegmentWriter struct {
	segmentPath string // top-level segment path (e.g. <basePath>)
	segmentID   uint32
	coldDir     string // full path: <basePath>/cold/NNNNNN
	hotDir      string // full path: <basePath>/hot/NNNNNN (for index_deltas + ledger_offsets)

	lw *packfile.LiveWriter

	nextEventID      uint32
	cumulativeEvents uint32
	ledgersWritten   uint32
	ledgerOffsData   []byte
	eventBytesWritten int64

	// Index deltas file (same format as HotSegmentWriter)
	indexDeltas    *os.File
	indexDeltasBuf []byte // manual buffer to reduce syscalls

	// Ledger offsets file (for crash recovery)
	ledgerOffs *os.File

	compressEvents bool
	blockSize      int
}

var _ HotWriter = (*LiveHotSegmentWriter)(nil)

// NewLiveHotSegmentWriter creates a new LiveWriter-backed hot segment writer.
func NewLiveHotSegmentWriter(basePath string, segmentID uint32, compressEvents bool, blockSize int) (*LiveHotSegmentWriter, error) {
	if blockSize <= 0 {
		blockSize = eventstore.DefaultRecordSize
	}

	dirName := fmt.Sprintf("%06d", segmentID)
	coldDir := filepath.Join(basePath, "cold", dirName)
	hotDir := filepath.Join(basePath, hotDirName, dirName)

	if err := os.MkdirAll(coldDir, 0755); err != nil {
		return nil, fmt.Errorf("create cold dir %s: %w", coldDir, err)
	}
	if err := os.MkdirAll(hotDir, 0755); err != nil {
		return nil, fmt.Errorf("create hot dir %s: %w", hotDir, err)
	}

	var format packfile.RecordFormat
	if !compressEvents {
		format = packfile.Uncompressed
	}

	lw, err := packfile.CreateLive(filepath.Join(coldDir, EventsFileName), packfile.WriterOptions{
		RecordSize:   blockSize,
		Format:       format,
		BytesPerSync: 1 << 20, // 1 MB
	})
	if err != nil {
		return nil, fmt.Errorf("create live packfile: %w", err)
	}

	// Open index_deltas.dat for appending (same format as HotSegmentWriter)
	indexDeltas, err := os.OpenFile(filepath.Join(hotDir, hotIndexDeltasFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		lw.Close()
		return nil, fmt.Errorf("open index_deltas.dat: %w", err)
	}

	// Open ledger_offsets.dat for crash recovery
	ledgerOffs, err := os.OpenFile(filepath.Join(hotDir, hotLedgerOffsFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		lw.Close()
		indexDeltas.Close()
		return nil, fmt.Errorf("open ledger_offsets.dat: %w", err)
	}

	return &LiveHotSegmentWriter{
		segmentPath:    basePath,
		segmentID:      segmentID,
		coldDir:        coldDir,
		hotDir:         hotDir,
		lw:             lw,
		indexDeltas:     indexDeltas,
		ledgerOffs:     ledgerOffs,
		compressEvents: compressEvents,
		blockSize:      blockSize,
	}, nil
}

// WriteLedger appends events to the live packfile and writes index deltas.
func (w *LiveHotSegmentWriter) WriteLedger(events []*event.IngestEvent, indexStore *IndexStore) error {
	if len(events) == 0 {
		// Still record the cumulative count for empty ledgers
		w.ledgersWritten++
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], w.cumulativeEvents)
		w.ledgerOffsData = append(w.ledgerOffsData, buf[:]...)
		if _, err := w.ledgerOffs.Write(buf[:]); err != nil {
			return fmt.Errorf("write ledger_offsets.dat: %w", err)
		}
		return nil
	}

	startID := w.nextEventID

	for i, ev := range events {
		eventID := startID + uint32(i)

		// Append encoded event to live packfile
		encoded := event.EncodeBinaryEvent(ev)
		if err := w.lw.Append(encoded); err != nil {
			return fmt.Errorf("append event to live packfile: %w", err)
		}
		w.eventBytesWritten += int64(len(encoded))

		// Write index deltas to flat file (same format as HotSegmentWriter)
		if len(ev.ContractID) > 0 {
			termHash := ContractTermKey(ev.ContractID)
			w.writeIndexDelta(0x00, termHash, eventID)
		}
		for pos, topicBytes := range ev.Topics {
			if pos > 3 {
				break
			}
			termHash := TopicTermKey(topicBytes)
			w.writeIndexDelta(byte(pos+1), termHash, eventID)
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
	if _, err := w.ledgerOffs.Write(cumBuf[:]); err != nil {
		return fmt.Errorf("write ledger_offsets.dat: %w", err)
	}

	// Flush index deltas buffer
	if len(w.indexDeltasBuf) > 0 {
		if _, err := w.indexDeltas.Write(w.indexDeltasBuf); err != nil {
			return fmt.Errorf("write index_deltas.dat: %w", err)
		}
		w.indexDeltasBuf = w.indexDeltasBuf[:0]
	}

	return nil
}

// writeIndexDelta appends a single [fieldIndex:1][termHash:16][eventID:4] entry to the buffer.
func (w *LiveHotSegmentWriter) writeIndexDelta(fieldIndex byte, termHash [16]byte, eventID uint32) {
	var buf [indexDeltaSize]byte
	buf[0] = fieldIndex
	copy(buf[1:17], termHash[:])
	binary.LittleEndian.PutUint32(buf[17:21], eventID)
	w.indexDeltasBuf = append(w.indexDeltasBuf, buf[:]...)
}

// FlushBuffers is a no-op — the LiveWriter handles its own buffering.
func (w *LiveHotSegmentWriter) FlushBuffers() error {
	return nil
}

// Fsync syncs the live packfile and auxiliary files.
func (w *LiveHotSegmentWriter) Fsync() error {
	if _, err := w.lw.Sync(); err != nil {
		return fmt.Errorf("sync live packfile: %w", err)
	}
	if err := w.indexDeltas.Sync(); err != nil {
		return fmt.Errorf("fsync index_deltas.dat: %w", err)
	}
	if err := w.ledgerOffs.Sync(); err != nil {
		return fmt.Errorf("fsync ledger_offsets.dat: %w", err)
	}
	return nil
}

// CommittedLengths returns approximate sizes for stats logging.
func (w *LiveHotSegmentWriter) CommittedLengths() HotSegmentMeta {
	return HotSegmentMeta{
		EventsDatLen:   w.eventBytesWritten,
		EventsIdxLen:   int64(w.nextEventID) * 8,
		IndexDeltasLen: 0, // not tracked per-event for LiveWriter
		LedgerOffsLen:  int64(w.ledgersWritten) * 4,
	}
}

// ConvertToCold finalizes the live packfile in-place and builds MPHF indexes.
// The events.pack file is already at cold/NNNNNN/events.pack — Freeze just
// writes the index/trailer. No event re-reading or re-packing needed.
func (w *LiveHotSegmentWriter) ConvertToCold(indexStore *IndexStore, sdw *SegmentDataWriter, stats *progress.SegmentStats) error {
	totalStart := time.Now()
	segID := w.segmentID
	coldBasePath := filepath.Join(w.segmentPath, "cold")

	fmt.Fprintf(os.Stderr, "\n[hot→cold %06d] starting conversion (live packfile, %d events, %d ledgers)\n",
		segID, w.nextEventID, w.ledgersWritten)

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

	// Step 2: Freeze the live packfile with ledger offsets as appData.
	// This writes the packfile index + trailer in-place — no event re-reading.
	t1 := time.Now()
	paddedLedgerOffs := make([]byte, SegmentLedgerOffsetsSize)
	copy(paddedLedgerOffs, w.ledgerOffsData)

	if err := w.lw.Freeze(paddedLedgerOffs); err != nil {
		return fmt.Errorf("freeze live packfile: %w", err)
	}
	freezeTime := time.Since(t1)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] freeze events.pack (%d events): %v\n", segID, w.nextEventID, freezeTime)

	// Step 3: Build MPHF index from flushed bitmaps (same as other writers)
	t2 := time.Now()
	if err := WriteSegmentDir(coldBasePath, segID, cached.Contracts, cached.Topics); err != nil {
		return fmt.Errorf("write cold index files: %w", err)
	}
	mphfTime := time.Since(t2)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] MPHF index build: %v\n", segID, mphfTime)

	// Step 4: Clean up hot directory (index_deltas.dat, ledger_offsets.dat)
	t3 := time.Now()
	w.closeFiles()
	os.RemoveAll(w.hotDir)
	cleanupTime := time.Since(t3)

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	beforeMB := memBefore.HeapAlloc / (1024 * 1024)
	afterMB := memAfter.HeapAlloc / (1024 * 1024)
	freedMB := int64(beforeMB) - int64(afterMB)

	totalTime := time.Since(totalStart)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] total: %v (flush=%v freeze=%v mphf=%v cleanup=%v) heap freed %d MB (%d→%d)\n",
		segID, totalTime, flushTime, freezeTime, mphfTime, cleanupTime, freedMB, beforeMB, afterMB)

	if stats != nil {
		stats.FreezeWallMs = float64(totalTime.Microseconds()) / 1000
		stats.FlushMs = float64(flushTime.Microseconds()) / 1000
		stats.EventsPackMs = float64(freezeTime.Microseconds()) / 1000
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
		if fi, err := os.Stat(filepath.Join(coldDir, EventsFileName)); err == nil {
			stats.ColdEventBytes = fi.Size()
		}
	}

	return nil
}

// Close closes the LiveWriter and auxiliary files without deleting.
func (w *LiveHotSegmentWriter) Close() error {
	w.closeFiles()
	if w.lw != nil {
		return w.lw.Close()
	}
	return nil
}

// Cleanup closes and removes all hot segment files.
func (w *LiveHotSegmentWriter) Cleanup() error {
	w.Close()
	os.RemoveAll(w.hotDir)
	// Don't remove coldDir — events.pack may be needed if frozen
	return nil
}

func (w *LiveHotSegmentWriter) closeFiles() {
	if w.indexDeltas != nil {
		w.indexDeltas.Close()
		w.indexDeltas = nil
	}
	if w.ledgerOffs != nil {
		w.ledgerOffs.Close()
		w.ledgerOffs = nil
	}
}
