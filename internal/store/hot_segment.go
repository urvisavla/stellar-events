package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/urvisavla/stellar-events/internal/event"
	"github.com/urvisavla/stellar-events/internal/progress"
)

const hotBufSize = 256 * 1024 // 256 KB write buffer per file

// =============================================================================
// Hot Segment Writer — append-only files for per-ledger durability
// =============================================================================
//
// Directory layout (under <basePath>/hot/NNNNNN/):
//   events.dat          - append-only event binary data (47-byte header + XDR per event)
//   events.idx          - fixed-size offset index: entry i = uint64 LE byte offset of event i in events.dat
//   index_deltas.dat    - append-only term deltas: [fieldIndex:1][termHash:16][eventID:4] = 21 bytes each
//   ledger_offsets.dat  - append-only: one uint32 LE per ledger = cumulative event count

const (
	hotDirName         = "hot"
	hotEventsDatFile   = "events.dat"
	hotEventsIdxFile   = "events.idx"
	hotIndexDeltasFile = "index_deltas.dat"
	hotLedgerOffsFile  = "ledger_offsets.dat"

	// indexDeltaSize is the fixed size of one index_deltas.dat entry:
	// [fieldIndex:1][termHash:16][eventID:4] = 21 bytes
	indexDeltaSize = 21
)

// HotSegmentMeta holds current file lengths for potential RocksDB commit checkpointing.
type HotSegmentMeta struct {
	EventsDatLen   int64
	EventsIdxLen   int64
	IndexDeltasLen int64
	LedgerOffsLen  int64
}

// HotSegmentWriter writes incoming ledger data into hot append-only files
// within a single segment directory. It tracks the next event ID and writes
// to four files per ledger batch. All writes go through bufio.Writer to
// reduce syscalls from ~7/event to ~1 per 256KB buffer fill.
type HotSegmentWriter struct {
	basePath  string // parent directory (e.g. <segmentPath>)
	segmentID uint32
	hotDir    string // full path: <basePath>/hot/NNNNNN

	nextEventID      uint32 // next dense local ID to assign
	cumulativeEvents uint32 // running cumulative event count for ledger_offsets.dat
	ledgersWritten   uint32 // number of ledger offset entries written
	eventsDatPos     int64  // tracked in memory to avoid Seek syscall

	// Raw file handles (for fsync)
	eventsDat   *os.File
	eventsIdx   *os.File
	indexDeltas *os.File
	ledgerOffs  *os.File

	// Buffered writers — reduce per-event syscalls to buffered memcpys
	eventsDatBuf   *bufio.Writer
	eventsIdxBuf   *bufio.Writer
	indexDeltasBuf *bufio.Writer
	ledgerOffsBuf  *bufio.Writer
}

// NewHotSegmentWriter opens (or creates) a hot segment directory and its four files.
// basePath is the top-level segment path (hot/ subdirectory is appended automatically).
func NewHotSegmentWriter(basePath string, segmentID uint32) (*HotSegmentWriter, error) {
	dirName := fmt.Sprintf("%06d", segmentID)
	hotDir := filepath.Join(basePath, hotDirName, dirName)

	if err := os.MkdirAll(hotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create hot segment dir %s: %w", hotDir, err)
	}

	openAppend := func(name string) (*os.File, error) {
		p := filepath.Join(hotDir, name)
		return os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	}

	eventsDat, err := openAppend(hotEventsDatFile)
	if err != nil {
		return nil, err
	}
	eventsIdx, err := openAppend(hotEventsIdxFile)
	if err != nil {
		eventsDat.Close()
		return nil, err
	}
	indexDeltas, err := openAppend(hotIndexDeltasFile)
	if err != nil {
		eventsDat.Close()
		eventsIdx.Close()
		return nil, err
	}
	ledgerOffs, err := openAppend(hotLedgerOffsFile)
	if err != nil {
		eventsDat.Close()
		eventsIdx.Close()
		indexDeltas.Close()
		return nil, err
	}

	return &HotSegmentWriter{
		basePath:       basePath,
		segmentID:      segmentID,
		hotDir:         hotDir,
		eventsDat:      eventsDat,
		eventsIdx:      eventsIdx,
		indexDeltas:    indexDeltas,
		ledgerOffs:     ledgerOffs,
		eventsDatBuf:   bufio.NewWriterSize(eventsDat, hotBufSize),
		eventsIdxBuf:   bufio.NewWriterSize(eventsIdx, hotBufSize),
		indexDeltasBuf: bufio.NewWriterSize(indexDeltas, hotBufSize),
		ledgerOffsBuf:  bufio.NewWriterSize(ledgerOffs, hotBufSize),
	}, nil
}

// WriteLedger processes a batch of events for a single ledger:
//  1. Assigns event IDs (startID = nextEventID; nextEventID += len(events))
//  2. Serializes + appends events to events.dat, records byte offsets in events.idx
//  3. Appends term deltas to index_deltas.dat for each contract/topic
//  4. Updates in-memory bitmaps via indexStore
//  5. Appends cumulative event count to ledger_offsets.dat
func (w *HotSegmentWriter) WriteLedger(events []*event.IngestEvent, indexStore *IndexStore) error {
	if len(events) == 0 {
		// Still write the cumulative count for empty ledgers
		w.ledgersWritten++
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], w.cumulativeEvents)
		if _, err := w.ledgerOffsBuf.Write(buf[:]); err != nil {
			return fmt.Errorf("write ledger_offsets.dat: %w", err)
		}
		return nil
	}

	datPos := w.eventsDatPos
	startID := w.nextEventID

	for i, ev := range events {
		eventID := startID + uint32(i)

		// Step 2: Serialize event and append to events.dat
		encoded := event.EncodeBinaryEvent(ev)

		// Record byte offset in events.idx (uint64 LE)
		var offBuf [8]byte
		binary.LittleEndian.PutUint64(offBuf[:], uint64(datPos))
		if _, err := w.eventsIdxBuf.Write(offBuf[:]); err != nil {
			return fmt.Errorf("write events.idx: %w", err)
		}

		if _, err := w.eventsDatBuf.Write(encoded); err != nil {
			return fmt.Errorf("write events.dat: %w", err)
		}
		datPos += int64(len(encoded))

		// Step 3: Write term deltas to index_deltas.dat
		if len(ev.ContractID) > 0 {
			termHash := ContractTermKey(ev.ContractID)
			if err := w.writeIndexDelta(0x00, termHash, eventID); err != nil {
				return err
			}
		}
		for pos, topicBytes := range ev.Topics {
			if pos > 3 {
				break
			}
			termHash := TopicTermKey(topicBytes)
			if err := w.writeIndexDelta(byte(pos+1), termHash, eventID); err != nil {
				return err
			}
		}

		// Step 4: Update in-memory bitmaps
		if len(ev.ContractID) > 0 {
			indexStore.AddContractEvent(ev.ContractID, w.segmentID, eventID)
		}
		for pos, topicBytes := range ev.Topics {
			indexStore.AddTopicEvent(pos, topicBytes, w.segmentID, eventID)
		}
	}

	w.eventsDatPos = datPos
	w.nextEventID = startID + uint32(len(events))

	// Step 5: Append cumulative event count to ledger_offsets.dat
	w.cumulativeEvents += uint32(len(events))
	w.ledgersWritten++
	var cumBuf [4]byte
	binary.LittleEndian.PutUint32(cumBuf[:], w.cumulativeEvents)
	if _, err := w.ledgerOffsBuf.Write(cumBuf[:]); err != nil {
		return fmt.Errorf("write ledger_offsets.dat: %w", err)
	}

	return nil
}

// writeIndexDelta appends a single [fieldIndex:1][termHash:16][eventID:4] entry.
func (w *HotSegmentWriter) writeIndexDelta(fieldIndex byte, termHash [16]byte, eventID uint32) error {
	var buf [indexDeltaSize]byte
	buf[0] = fieldIndex
	copy(buf[1:17], termHash[:])
	binary.LittleEndian.PutUint32(buf[17:21], eventID)
	if _, err := w.indexDeltasBuf.Write(buf[:]); err != nil {
		return fmt.Errorf("write index_deltas.dat: %w", err)
	}
	return nil
}

// Fsync flushes all bufio writers then fsyncs all four hot files.
func (w *HotSegmentWriter) Fsync() error {
	// Flush buffered data to OS
	for _, bw := range []*bufio.Writer{w.eventsDatBuf, w.eventsIdxBuf, w.indexDeltasBuf, w.ledgerOffsBuf} {
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("flush buffer: %w", err)
		}
	}
	// Fsync to disk
	for _, f := range []*os.File{w.eventsDat, w.eventsIdx, w.indexDeltas, w.ledgerOffs} {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("fsync %s: %w", f.Name(), err)
		}
	}
	return nil
}

// CommittedLengths returns the current file sizes for checkpointing.
func (w *HotSegmentWriter) CommittedLengths() HotSegmentMeta {
	stat := func(f *os.File) int64 {
		fi, err := f.Stat()
		if err != nil {
			return 0
		}
		return fi.Size()
	}
	return HotSegmentMeta{
		EventsDatLen:   stat(w.eventsDat),
		EventsIdxLen:   stat(w.eventsIdx),
		IndexDeltasLen: stat(w.indexDeltas),
		LedgerOffsLen:  stat(w.ledgerOffs),
	}
}

// ConvertToCold converts this hot segment to cold format with per-step timing.
//  1. Flush in-memory bitmaps to get []SegmentTermData per field
//  2. Build cold segment in cold/NNNNNN/:
//     - events.pack: read events from hot events.dat using events.idx offsets
//     - index.hash + index.pack: build from flushed bitmaps
//  3. Delete hot directory
//  4. Log detailed timing for each step
func (w *HotSegmentWriter) ConvertToCold(indexStore *IndexStore, sdw *SegmentDataWriter) (*progress.FreezeSegmentStats, error) {
	totalStart := time.Now()
	segID := w.segmentID
	coldBasePath := filepath.Join(w.basePath, "cold")

	fmt.Fprintf(os.Stderr, "\n[hot→cold %06d] starting conversion (%d events, %d ledgers)\n",
		segID, w.nextEventID, w.ledgersWritten)

	// Empty segment — nothing to convert, just clean up hot dir
	if w.nextEventID == 0 {
		fmt.Fprintf(os.Stderr, "  [hot→cold %06d] empty segment, skipping\n", segID)
		w.Cleanup()
		return nil, nil
	}

	// Snapshot heap before flush to measure memory freed
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Step 1: Flush in-memory bitmaps
	t0 := time.Now()
	if err := indexStore.Flush(); err != nil {
		return nil, fmt.Errorf("flush bitmap indexes: %w", err)
	}
	flushTime := time.Since(t0)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] flush bitmaps: %v\n", segID, flushTime)

	cached := indexStore.PopSegmentTerms(segID)
	if cached == nil {
		return nil, fmt.Errorf("no cached terms for segment %d after flush", segID)
	}

	// Step 2a: Build events.pack from hot files
	t1 := time.Now()

	// Read ledger_offsets.dat to build appData (padded to SegmentLedgerOffsetsSize)
	ledgerOffsPath := filepath.Join(w.hotDir, hotLedgerOffsFile)
	ledgerOffsRaw, err := os.ReadFile(ledgerOffsPath)
	if err != nil {
		return nil, fmt.Errorf("read ledger_offsets.dat: %w", err)
	}

	// Pad to full SegmentLedgerOffsetsSize (40,000 bytes)
	appData := make([]byte, SegmentLedgerOffsetsSize)
	copy(appData, ledgerOffsRaw)

	// Read events from hot events.dat using events.idx, write to cold events.pack
	eventsIdxPath := filepath.Join(w.hotDir, hotEventsIdxFile)
	eventsDatPath := filepath.Join(w.hotDir, hotEventsDatFile)

	idxData, err := os.ReadFile(eventsIdxPath)
	if err != nil {
		return nil, fmt.Errorf("read events.idx: %w", err)
	}
	datData, err := os.ReadFile(eventsDatPath)
	if err != nil {
		return nil, fmt.Errorf("read events.dat: %w", err)
	}

	numEvents := len(idxData) / 8

	// Start the SegmentDataWriter chunk for cold output
	if sdw != nil {
		if err := sdw.StartChunk(segID); err != nil {
			return nil, fmt.Errorf("start cold chunk: %w", err)
		}

		for i := 0; i < numEvents; i++ {
			offset := binary.LittleEndian.Uint64(idxData[i*8 : (i+1)*8])

			// Determine event size: next offset - current offset, or end of file
			var eventEnd uint64
			if i+1 < numEvents {
				eventEnd = binary.LittleEndian.Uint64(idxData[(i+1)*8 : (i+2)*8])
			} else {
				eventEnd = uint64(len(datData))
			}

			eventData := datData[offset:eventEnd]
			if err := sdw.AppendEvent(uint32(i), eventData); err != nil {
				return nil, fmt.Errorf("append event %d to cold pack: %w", i, err)
			}
		}

		if err := sdw.FinalizeChunk(appData); err != nil {
			return nil, fmt.Errorf("finalize cold events.pack: %w", err)
		}
	}

	eventsPackTime := time.Since(t1)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] events.pack (%d events): %v\n", segID, numEvents, eventsPackTime)

	// Step 2b: Build index.hash + index.pack from flushed bitmaps
	t2 := time.Now()
	if err := WriteSegmentDir(coldBasePath, segID, cached.Contracts, cached.Topics); err != nil {
		return nil, fmt.Errorf("write cold index files: %w", err)
	}
	mphfTime := time.Since(t2)
	fmt.Fprintf(os.Stderr, "  [hot→cold %06d] MPHF index build: %v\n", segID, mphfTime)

	// Step 3: Delete hot directory
	t3 := time.Now()
	if err := w.Cleanup(); err != nil {
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

	stats := &progress.FreezeSegmentStats{
		SegmentID:    segID,
		Events:       numEvents,
		FlushMs:      float64(flushTime.Microseconds()) / 1000,
		EventsPackMs: float64(eventsPackTime.Microseconds()) / 1000,
		MphfMs:       float64(mphfTime.Microseconds()) / 1000,
		CleanupMs:    float64(cleanupTime.Microseconds()) / 1000,
		TotalMs:      float64(totalTime.Microseconds()) / 1000,
		HeapFreedMB:  freedMB,
	}

	return stats, nil
}

// Cleanup deletes the hot segment directory and all its files.
func (w *HotSegmentWriter) Cleanup() error {
	// Close files first
	w.closeFiles()
	return os.RemoveAll(w.hotDir)
}

// Close closes all open file handles without deleting files.
func (w *HotSegmentWriter) Close() error {
	w.closeFiles()
	return nil
}

func (w *HotSegmentWriter) closeFiles() {
	for _, f := range []*os.File{w.eventsDat, w.eventsIdx, w.indexDeltas, w.ledgerOffs} {
		if f != nil {
			f.Close()
		}
	}
	w.eventsDat = nil
	w.eventsIdx = nil
	w.indexDeltas = nil
	w.ledgerOffs = nil
}
