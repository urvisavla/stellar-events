package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/tamirms/streamhash"

	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/packfile"
)

// SegmentIndexFlusher is a flusher for segment-file-only mode.
// Encodes ledger offsetss directly from counters (no RocksDB merge).
type SegmentIndexFlusher struct{}

func (f *SegmentIndexFlusher) Flush(segments []bitmapChunk, counters map[uint32]*segmentEventCounter, _ bool) (map[uint32][]byte, error) {
	ledgerOffsetsData := make(map[uint32][]byte, len(counters))
	for segmentID, counter := range counters {
		ledgerOffsetsData[segmentID] = EncodeSegmentLedgerOffsets(segmentID, counter.eventCounts)
	}
	return ledgerOffsetsData, nil
}

func (f *SegmentIndexFlusher) Close() error { return nil }

// FinalizeSegment writes flat file indexes for a completed segment using cached data from Flush().
// Returns nil if no cached data exists (caller should fall back to an alternative path).
// The ledger offsets (segment.offsets) is already written by IndexStore.Flush() — this only writes
// the MPHF bitmap indexes (.hash/.pack) and finalizes the event data chunk.
func FinalizeSegment(indexStore *IndexStore, segmentPath string, segmentID uint32, sdw *SegmentDataWriter) error {
	cached := indexStore.PopSegmentTerms(segmentID)
	if cached == nil {
		return fmt.Errorf("no cached terms for segment %d", segmentID)
	}

	// Write MPHF bitmap indexes (.hash/.pack) for this segment
	if err := WriteSegmentDir(segmentPath, segmentID, cached.Contracts, cached.Topics); err != nil {
		return err
	}

	// Finalize segment data chunk if it matches this segment
	if sdw != nil && sdw.IsActive() && sdw.ChunkID() == segmentID {
		if err := sdw.FinalizeChunk(); err != nil {
			return fmt.Errorf("failed to finalize segment data chunk %d: %w", segmentID, err)
		}
	}

	return nil
}


const (
	EventsFileName = "events.pack"
)

// SegmentDataWriter writes event data to flat files during ingestion.
type SegmentDataWriter struct {
	basePath       string
	chunkID        uint32
	ew             *eventstore.Writer // eventstore writer for current chunk
	active         bool               // true if a chunk is currently open
	compressEvents bool               // enable zstd compression
	blockSize      int                // events per compression block
}

// NewSegmentDataWriter creates a new segment data writer.
// compressEvents enables zstd compression. groupSize sets events per block (0 = default 128).
func NewSegmentDataWriter(basePath string, compressEvents bool, groupSize int) *SegmentDataWriter {
	blockSize := groupSize
	if blockSize <= 0 {
		blockSize = eventstore.DefaultRecordSize
	}
	return &SegmentDataWriter{
		basePath:       basePath,
		compressEvents: compressEvents,
		blockSize:      blockSize,
	}
}

// StartChunk begins a new chunk (segment). Creates the directory if needed
// and opens an eventstore writer.
func (w *SegmentDataWriter) StartChunk(chunkID uint32) error {
	dirName := fmt.Sprintf("%06d", chunkID)
	dirPath := filepath.Join(w.basePath, dirName)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create chunk dir %s: %w", dirPath, err)
	}

	var format packfile.RecordFormat
	if !w.compressEvents {
		format = packfile.Uncompressed
	}

	ew, err := eventstore.Create(filepath.Join(dirPath, EventsFileName), eventstore.WriterOptions{
		RecordSize: w.blockSize,
		Format:     format,
	})
	if err != nil {
		return fmt.Errorf("failed to create eventstore: %w", err)
	}

	w.chunkID = chunkID
	w.ew = ew
	w.active = true

	fmt.Fprintf(os.Stderr, "  [event-file-store] started chunk %06d\n", chunkID)
	return nil
}

// AppendEvent writes an event to the eventstore. Events must be appended
// in dense ID order. The denseID parameter is accepted for API compatibility
// but the position is determined by append order.
func (w *SegmentDataWriter) AppendEvent(denseID uint32, data []byte) error {
	if !w.active {
		return fmt.Errorf("no active chunk")
	}
	return w.ew.Append(data)
}

// FinalizeChunk finalizes the eventstore (flushes, writes index, atomic rename).
func (w *SegmentDataWriter) FinalizeChunk() error {
	if !w.active {
		return nil
	}
	w.active = false

	if w.ew != nil {
		if err := w.ew.Finish(); err != nil {
			return fmt.Errorf("failed to finalize eventstore: %w", err)
		}
		w.ew = nil
	}

	fmt.Fprintf(os.Stderr, "  [event-file-store] finalized chunk %06d\n", w.chunkID)
	return nil
}

// ChunkID returns the current chunk ID being written.
func (w *SegmentDataWriter) ChunkID() uint32 {
	return w.chunkID
}

// IsActive returns true if a chunk is currently open for writing.
func (w *SegmentDataWriter) IsActive() bool {
	return w.active
}

// =============================================================================
// Flat File Segment Index Format — MPHF-backed
// =============================================================================
//
// Directory structure:
//   <base_path>/NNNNNN/
//     contracts.hash     - streamhash MPHF index (32-byte term keys → slots)
//     contracts.pack     - bitmaps ordered by MPHF slot + u64 LE offset trailer
//     topic0.hash        - per-position MPHF index
//     topic0.pack
//     topic1.hash
//     topic1.pack
//     topic2.hash
//     topic2.pack
//     topic3.hash
//     topic3.pack
//     segment.offsets       - 40,000 bytes cumulative count array
//
// .pack layout:
//   [bitmap_0 bytes][bitmap_1 bytes]...[bitmap_{N-1} bytes]
//   [offset_0: u64 LE][offset_1: u64 LE]...[offset_N: u64 LE]   ← (N+1) entries
//
// Lookup:
//   1. hashIdx.Query(termKey) → slot (fingerprint mismatch = not found)
//   2. Read offsets[slot] and offsets[slot+1] from trailer
//   3. pread(pack, offset, length) → bitmap bytes
//   4. roaring.UnmarshalBinary(bytes)

const (
	// LedgerOffsetsFileName is the name of the ledger offsets file.
	LedgerOffsetsFileName = "segment.offsets"
)

// WriteSegmentDir writes MPHF bitmap indexes (.hash/.pack) for a segment.
// The ledger offsets (segment.offsets) is written separately by IndexStore.Flush().
func WriteSegmentDir(basePath string, segmentID uint32, contractTerms []SegmentTermData, topicsByPos [4][]SegmentTermData) error {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(basePath, dirName)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create segment dir %s: %w", dirPath, err)
	}

	// Write contracts.hash + contracts.pack
	if len(contractTerms) > 0 {
		if err := writeMPHFPack(dirPath, "contracts", contractTerms); err != nil {
			return fmt.Errorf("failed to write contracts: %w", err)
		}
	}

	// Write topic0-3.hash + topic0-3.pack
	topicNames := [4]string{"topic0", "topic1", "topic2", "topic3"}
	for pos := 0; pos < 4; pos++ {
		if len(topicsByPos[pos]) > 0 {
			if err := writeMPHFPack(dirPath, topicNames[pos], topicsByPos[pos]); err != nil {
				return fmt.Errorf("failed to write %s: %w", topicNames[pos], err)
			}
		}
	}

	return nil
}

// writeMPHFPack builds a .hash (streamhash MPHF) and .pack (bitmaps + offset trailer)
// file pair for the given terms.
func writeMPHFPack(dirPath, name string, terms []SegmentTermData) error {
	n := uint64(len(terms))
	if n == 0 {
		return nil
	}

	hashPath := filepath.Join(dirPath, name+".hash")
	packPath := filepath.Join(dirPath, name+".pack")
	hashTmpPath := hashPath + ".tmp"
	packTmpPath := packPath + ".tmp"

	// Build .hash using streamhash with unsorted input (safe for any key order)
	builder, err := streamhash.NewBuilder(
		context.Background(),
		hashTmpPath,
		n,
		streamhash.WithFingerprint(2),
		streamhash.WithUnsortedInput(),
	)
	if err != nil {
		return fmt.Errorf("failed to create streamhash builder for %s: %w", name, err)
	}

	for _, t := range terms {
		if err := builder.AddKey(t.TermHash[:], 0); err != nil {
			builder.Close()
			os.Remove(hashTmpPath)
			return fmt.Errorf("failed to add key to streamhash for %s: %w", name, err)
		}
	}

	if err := builder.Finish(); err != nil {
		builder.Close()
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to finish streamhash for %s: %w", name, err)
	}
	builder.Close()

	// Fsync and rename .hash
	if err := fsyncFile(hashTmpPath); err != nil {
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to fsync %s.hash: %w", name, err)
	}

	// Open .hash to get slot assignments
	idx, err := streamhash.Open(hashTmpPath)
	if err != nil {
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to open streamhash for %s: %w", name, err)
	}
	defer idx.Close()

	// Map each term to its MPHF slot
	type slotEntry struct {
		slot uint64
		data []byte
	}
	slotData := make([]slotEntry, n)
	for i, t := range terms {
		slot, err := idx.Query(t.TermHash[:])
		if err != nil {
			os.Remove(hashTmpPath)
			return fmt.Errorf("failed to query streamhash slot for %s term %d: %w", name, i, err)
		}
		slotData[i] = slotEntry{slot: slot, data: t.BitmapData}
	}

	// Sort by slot to write bitmaps in MPHF slot order
	sort.Slice(slotData, func(i, j int) bool {
		return slotData[i].slot < slotData[j].slot
	})

	// Build .pack: concatenate bitmaps in slot order, then append (N+1) u64 LE offset trailer
	packFile, err := os.Create(packTmpPath)
	if err != nil {
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to create %s.pack: %w", name, err)
	}

	offsets := make([]uint64, n+1)
	var currentOffset uint64

	for i, entry := range slotData {
		offsets[i] = currentOffset
		if _, err := packFile.Write(entry.data); err != nil {
			packFile.Close()
			os.Remove(hashTmpPath)
			os.Remove(packTmpPath)
			return fmt.Errorf("failed to write bitmap to %s.pack: %w", name, err)
		}
		currentOffset += uint64(len(entry.data))
	}
	offsets[n] = currentOffset // sentinel: end of last bitmap

	// Write offset trailer
	trailer := make([]byte, (n+1)*8)
	for i, off := range offsets {
		binary.LittleEndian.PutUint64(trailer[i*8:(i+1)*8], off)
	}
	if _, err := packFile.Write(trailer); err != nil {
		packFile.Close()
		os.Remove(hashTmpPath)
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to write trailer to %s.pack: %w", name, err)
	}

	// Fsync and close pack file
	if err := packFile.Sync(); err != nil {
		packFile.Close()
		os.Remove(hashTmpPath)
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to fsync %s.pack: %w", name, err)
	}
	packFile.Close()

	// Atomic rename both files
	if err := os.Rename(hashTmpPath, hashPath); err != nil {
		os.Remove(hashTmpPath)
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to rename %s.hash: %w", name, err)
	}
	if err := os.Rename(packTmpPath, packPath); err != nil {
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to rename %s.pack: %w", name, err)
	}

	return nil
}

// fsyncFile opens a file, fsyncs it, and closes it.
func fsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// writeFileAtomic writes data to a file using a temp file + rename for atomicity.
func writeFileAtomic(path string, data []byte) error {
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
