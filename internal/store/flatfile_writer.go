package store

import (
	"context"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/tamirms/streamhash"
	"github.com/zeebo/xxh3"

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
// Writes MPHF bitmap indexes (.hash/.pack) and finalizes the event data chunk with
// ledger offsets embedded as packfile appData.
func FinalizeSegment(indexStore *IndexStore, segmentPath string, segmentID uint32, sdw *SegmentDataWriter) error {
	cached := indexStore.PopSegmentTerms(segmentID)
	if cached == nil {
		return fmt.Errorf("no cached terms for segment %d", segmentID)
	}

	t0 := time.Now()
	if err := WriteSegmentDir(segmentPath, segmentID, cached.Contracts, cached.Topics); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "    [segment %d] WriteSegmentDir: %v\n", segmentID, time.Since(t0))

	if sdw != nil && sdw.IsActive() && sdw.ChunkID() == segmentID {
		t1 := time.Now()
		if err := sdw.FinalizeChunk(cached.LedgerOffsetsData); err != nil {
			return fmt.Errorf("failed to finalize segment data chunk %d: %w", segmentID, err)
		}
		fmt.Fprintf(os.Stderr, "    [segment %d] FinalizeChunk: %v\n", segmentID, time.Since(t1))
	}

	return nil
}


const (
	EventsFileName = "events.pack"
)

// SegmentDataWriter writes event data to flat files during ingestion.
// Uses packfile.Writer directly to support appData (ledger offsets) in Finish.
type SegmentDataWriter struct {
	basePath       string
	chunkID        uint32
	pw             *packfile.Writer // packfile writer for current chunk
	active         bool             // true if a chunk is currently open
	compressEvents bool             // enable zstd compression
	blockSize      int              // events per compression block
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
// and opens a packfile writer.
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

	pw, err := packfile.Create(filepath.Join(dirPath, EventsFileName), packfile.WriterOptions{
		RecordSize:   w.blockSize,
		Format:       format,
		Concurrency:  8,
		BytesPerSync: 1 << 20,
	})
	if err != nil {
		return fmt.Errorf("failed to create packfile: %w", err)
	}

	w.chunkID = chunkID
	w.pw = pw
	w.active = true

	fmt.Fprintf(os.Stderr, "  [event-file-store] started chunk %06d\n", chunkID)
	return nil
}

// AppendEvent writes an event to the packfile. Events must be appended
// in dense ID order. The denseID parameter is accepted for API compatibility
// but the position is determined by append order.
func (w *SegmentDataWriter) AppendEvent(denseID uint32, data []byte) error {
	if !w.active {
		return fmt.Errorf("no active chunk")
	}
	return w.pw.Append(data)
}

// FinalizeChunk finalizes the packfile with optional appData (ledger offsets).
func (w *SegmentDataWriter) FinalizeChunk(appData []byte) error {
	if !w.active {
		return nil
	}
	w.active = false

	if w.pw != nil {
		if err := w.pw.Finish(appData); err != nil {
			return fmt.Errorf("failed to finalize packfile: %w", err)
		}
		w.pw = nil
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
//     index.hash         - single streamhash MPHF index (32-byte keys → slots)
//     index.pack         - all bitmaps ordered by MPHF slot + u64 LE offset trailer
//     events.pack        - event data (ledger offsets embedded as packfile appData)
//
// Key format: [PreHash(compositeKey):16][termHash:16] = 32 bytes
//   compositeKey = [termHash:16][fieldIndex:1] (17 bytes)
//   PreHash produces uniform 16-byte keys via xxhash128, ensuring both
//   good block distribution and unique (k0, k1) pairs for different fieldIndex values.
//   The second half stores the original termHash for full 32-byte key size.
//
//   fieldIndex 0x00 = contract
//   fieldIndex 0x01 = topic position 0
//   fieldIndex 0x02 = topic position 1
//   fieldIndex 0x03 = topic position 2
//   fieldIndex 0x04 = topic position 3
//
// .pack layout:
//   [fp:4][field:1][bitmap_0 bytes][fp:4][field:1][bitmap_1 bytes]...
//   [offset_0: u64 LE][offset_1: u64 LE]...[offset_N: u64 LE]   ← (N+1) entries
//
// Each record: [fingerprint:4][fieldIndex:1][bitmap bytes]
// Fingerprint: first 4 bytes of xxh3.Hash(streamhashKey).
//
// Lookup:
//   1. hashIdx.Query(queryKey) → slot (fingerprint mismatch = not found)
//   2. Read offsets[slot] and offsets[slot+1] from trailer
//   3. Verify 4-byte fingerprint and 1-byte fieldIndex at record start
//   4. pread(pack, offset+5, length-5) → bitmap bytes
//   5. roaring.UnmarshalBinary(bytes)


// indexPackEntry holds a 32-byte key and its serialized bitmap data for the unified index.
type indexPackEntry struct {
	Key        []byte // [xxh3(composite):16][termHash:16] = 32 bytes
	FieldIndex byte   // fieldIndex (0x00=contract, 0x01-0x04=topic positions)
	BitmapData []byte
}

// makeStreamhashKey builds a 32-byte streamhash key from a termHash and fieldIndex.
// Format: [xxh3(composite):16][termHash:16] where composite = [termHash:16][fieldIndex:1].
// xxh3.Hash128 ensures unique (k0, k1) pairs even when the same termHash appears in multiple fields.
func makeStreamhashKey(termHash [16]byte, fieldIndex byte) []byte {
	// Build 17-byte composite key: [termHash:16][fieldIndex:1]
	var composite [17]byte
	copy(composite[:16], termHash[:])
	composite[16] = fieldIndex

	// Hash composite into first 16 bytes, store original termHash in second half
	h := xxh3.Hash128(composite[:])
	key := make([]byte, 32)
	binary.LittleEndian.PutUint64(key[0:8], h.Lo)
	binary.LittleEndian.PutUint64(key[8:16], h.Hi)
	copy(key[16:32], termHash[:])
	return key
}

// WriteSegmentDir writes a single MPHF bitmap index (index.hash + index.pack) for a segment.
// All fields (contracts + 4 topic positions) are merged into one index using 32-byte keys.
func WriteSegmentDir(basePath string, segmentID uint32, contractTerms []SegmentTermData, topicsByPos [4][]SegmentTermData) error {
	dirName := fmt.Sprintf("%06d", segmentID)
	dirPath := filepath.Join(basePath, dirName)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create segment dir %s: %w", dirPath, err)
	}

	// Collect all terms into a single slice with 32-byte keys
	totalTerms := len(contractTerms)
	for pos := 0; pos < 4; pos++ {
		totalTerms += len(topicsByPos[pos])
	}

	if totalTerms == 0 {
		return nil
	}

	entries := make([]indexPackEntry, 0, totalTerms)

	// Contracts: fieldIndex = 0x00
	for _, t := range contractTerms {
		entries = append(entries, indexPackEntry{
			Key:        makeStreamhashKey(t.TermHash, 0x00),
			FieldIndex: 0x00,
			BitmapData: t.BitmapData,
		})
	}

	// Topics: fieldIndex = 0x01..0x04
	for pos := 0; pos < 4; pos++ {
		for _, t := range topicsByPos[pos] {
			fi := byte(pos + 1)
			entries = append(entries, indexPackEntry{
				Key:        makeStreamhashKey(t.TermHash, fi),
				FieldIndex: fi,
				BitmapData: t.BitmapData,
			})
		}
	}

	// Pre-sort entries by key bytes for streamhash block order.
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	return writeIndexPack(dirPath, entries)
}

// writeIndexPack builds index.hash (streamhash MPHF) and index.pack (bitmaps + offset trailer)
// from a unified set of 32-byte keyed entries.
func writeIndexPack(dirPath string, entries []indexPackEntry) error {
	n := uint64(len(entries))
	if n == 0 {
		return nil
	}

	fmt.Fprintf(os.Stderr, "      [writeIndexPack] %d entries\n", n)

	hashPath := filepath.Join(dirPath, "index.hash")
	packPath := filepath.Join(dirPath, "index.pack")
	hashTmpPath := hashPath + ".tmp"
	packTmpPath := packPath + ".tmp"

	// Build .hash using streamhash
	tNew := time.Now()
	builder, err := streamhash.NewBuilder(
		context.Background(),
		hashTmpPath,
		n,
		streamhash.WithUnsortedInput(),
	)
	if err != nil {
		return fmt.Errorf("failed to create streamhash builder: %w", err)
	}
	fmt.Fprintf(os.Stderr, "      [writeIndexPack] NewBuilder: %v\n", time.Since(tNew))

	tAdd := time.Now()
	for _, e := range entries {
		if err := builder.AddKey(e.Key, 0); err != nil {
			builder.Close()
			os.Remove(hashTmpPath)
			return fmt.Errorf("failed to add key to streamhash: %w", err)
		}
	}
	fmt.Fprintf(os.Stderr, "      [writeIndexPack] AddKey ×%d: %v\n", n, time.Since(tAdd))

	tFinish := time.Now()
	if err := builder.Finish(); err != nil {
		builder.Close()
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to finish streamhash: %w", err)
	}
	builder.Close()
	fmt.Fprintf(os.Stderr, "      [writeIndexPack] Finish: %v\n", time.Since(tFinish))

	// Fsync .hash tmp
	tFsync := time.Now()
	if err := fsyncFile(hashTmpPath); err != nil {
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to fsync index.hash: %w", err)
	}
	fmt.Fprintf(os.Stderr, "      [writeIndexPack] fsync .hash: %v\n", time.Since(tFsync))

	// Open .hash to get slot assignments
	idx, err := streamhash.Open(hashTmpPath)
	if err != nil {
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to open streamhash: %w", err)
	}
	defer idx.Close()

	// Map each entry to its MPHF slot, computing fingerprint from the 32-byte key
	tQuery := time.Now()
	type slotEntry struct {
		slot        uint64
		fingerprint [4]byte
		fieldIndex  byte
		data        []byte
	}
	slotData := make([]slotEntry, n)
	for i, e := range entries {
		slot, err := idx.Query(e.Key)
		if err != nil {
			os.Remove(hashTmpPath)
			return fmt.Errorf("failed to query streamhash slot for entry %d: %w", i, err)
		}
		// Fingerprint: first 4 bytes of xxh3.Hash(queryKey)
		fp := xxh3.Hash(e.Key)
		var fpBytes [4]byte
		binary.LittleEndian.PutUint32(fpBytes[:], uint32(fp))
		slotData[i] = slotEntry{slot: slot, fingerprint: fpBytes, fieldIndex: e.FieldIndex, data: e.BitmapData}
	}
	fmt.Fprintf(os.Stderr, "      [writeIndexPack] slot queries: %v\n", time.Since(tQuery))

	// Sort by slot to write bitmaps in MPHF slot order
	sort.Slice(slotData, func(i, j int) bool {
		return slotData[i].slot < slotData[j].slot
	})

	// Build .pack: [fp:4][field:1][bitmap]... per record, then (N+1) u64 LE offset trailer
	tPack := time.Now()
	packFile, err := os.Create(packTmpPath)
	if err != nil {
		os.Remove(hashTmpPath)
		return fmt.Errorf("failed to create index.pack: %w", err)
	}

	offsets := make([]uint64, n+1)
	var currentOffset uint64

	for i, entry := range slotData {
		offsets[i] = currentOffset
		// Write 4-byte fingerprint
		if _, err := packFile.Write(entry.fingerprint[:]); err != nil {
			packFile.Close()
			os.Remove(hashTmpPath)
			os.Remove(packTmpPath)
			return fmt.Errorf("failed to write fingerprint to index.pack: %w", err)
		}
		// Write 1-byte field index
		if _, err := packFile.Write([]byte{entry.fieldIndex}); err != nil {
			packFile.Close()
			os.Remove(hashTmpPath)
			os.Remove(packTmpPath)
			return fmt.Errorf("failed to write field index to index.pack: %w", err)
		}
		// Write bitmap data
		if _, err := packFile.Write(entry.data); err != nil {
			packFile.Close()
			os.Remove(hashTmpPath)
			os.Remove(packTmpPath)
			return fmt.Errorf("failed to write bitmap to index.pack: %w", err)
		}
		currentOffset += 5 + uint64(len(entry.data))
	}
	offsets[n] = currentOffset // sentinel: end of last record

	// Write offset trailer
	trailer := make([]byte, (n+1)*8)
	for i, off := range offsets {
		binary.LittleEndian.PutUint64(trailer[i*8:(i+1)*8], off)
	}
	if _, err := packFile.Write(trailer); err != nil {
		packFile.Close()
		os.Remove(hashTmpPath)
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to write trailer to index.pack: %w", err)
	}
	fmt.Fprintf(os.Stderr, "      [writeIndexPack] pack write: %v\n", time.Since(tPack))

	// Fsync and close pack file
	tSync := time.Now()
	if err := packFile.Sync(); err != nil {
		packFile.Close()
		os.Remove(hashTmpPath)
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to fsync index.pack: %w", err)
	}
	packFile.Close()
	fmt.Fprintf(os.Stderr, "      [writeIndexPack] fsync+close .pack: %v\n", time.Since(tSync))

	// Atomic rename both files
	if err := os.Rename(hashTmpPath, hashPath); err != nil {
		os.Remove(hashTmpPath)
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to rename index.hash: %w", err)
	}
	if err := os.Rename(packTmpPath, packPath); err != nil {
		os.Remove(packTmpPath)
		return fmt.Errorf("failed to rename index.pack: %w", err)
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
