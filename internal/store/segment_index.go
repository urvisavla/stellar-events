package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/tamirms/streamhash"
)

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
//     segment.meta       - 40,000 bytes cumulative count array
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
	// LedgerMapFileName is the name of the ledger map file.
	LedgerMapFileName = "segment.meta"
)

// SegmentTermData holds a term hash and its serialized bitmap data.
type SegmentTermData struct {
	TermHash   [32]byte
	BitmapData []byte
}

// WriteSegmentDir writes a complete segment directory with MPHF indexes.
// contractTerms and topicsByPos are the pre-collected bitmap data.
func WriteSegmentDir(basePath string, segmentID uint32, contractTerms []SegmentTermData, topicsByPos [4][]SegmentTermData, lm *SegmentLedgerMap) error {
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

	// Write segment.meta
	if lm != nil && len(lm.Data) == SegmentLedgerMapSize {
		if err := writeFileAtomic(filepath.Join(dirPath, LedgerMapFileName), lm.Data); err != nil {
			return fmt.Errorf("failed to write ledger map: %w", err)
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

// =============================================================================
// Mmap Support (used for segment.meta ledger map reads and .pack files)
// =============================================================================

// MmapFile wraps a memory-mapped file region for zero-copy reads.
type MmapFile struct {
	data []byte // mmap'd region (nil for empty files)
}

// OpenMmap memory-maps a file for reading. The returned MmapFile must be
// closed when no longer needed. Empty files return a valid MmapFile with nil data.
func OpenMmap(path string) (*MmapFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(fi.Size())
	if size == 0 {
		return &MmapFile{}, nil
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	return &MmapFile{data: data}, nil
}

// Close unmaps the memory-mapped region.
func (m *MmapFile) Close() error {
	if m.data != nil {
		return syscall.Munmap(m.data)
	}
	return nil
}

// Len returns the size of the mapped region.
func (m *MmapFile) Len() int {
	return len(m.data)
}

// ValidateLedgerMapFile validates a ledgermap.dat file by checking its size.
func ValidateLedgerMapFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat ledger map file: %w", err)
	}
	if info.Size() != int64(SegmentLedgerMapSize) {
		return fmt.Errorf("invalid ledger map size: got %d, expected %d", info.Size(), SegmentLedgerMapSize)
	}
	return nil
}

// CleanupTmpFiles removes any .tmp files from a segment directory (restart recovery).
func CleanupTmpFiles(dirPath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			if err := os.Remove(filepath.Join(dirPath, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// =============================================================================
// Segment Ledger Map
// =============================================================================

// SegmentLedgerMap maps dense sequential local IDs to (ledger, eventSeq) pairs
// within a segment. Stored as a fixed-size cumulative array of event counts.
//
// Entry i = total events in ledgers at offsets 0 through i (inclusive cumulative sum).
// The array has exactly SegmentSize entries (10,000), stored as little-endian uint32s.
//
// Key format in CFDefault: [0xFF][segmentID:4] — 5 bytes
// Value: [cumulative_count:4] × SegmentSize — exactly 40,000 bytes (fixed)
type SegmentLedgerMap struct {
	SegmentID uint32
	Data      []byte // raw 40,000-byte cumulative array (zero-copy from RocksDB)
}

const (
	// SegmentLedgerMapSize is the size of the cumulative array in bytes.
	SegmentLedgerMapSize = int(SegmentSize) * 4 // 40,000 bytes

	// segmentLedgerMapPrefix is the key prefix used to avoid collision with text keys in CFDefault.
	segmentLedgerMapPrefix = 0xFF
)

// SegmentLedgerMapKey returns the CFDefault key for a segment's ledger map.
// Format: [0xFF][segmentID:4] — 5 bytes
func SegmentLedgerMapKey(segmentID uint32) []byte {
	key := make([]byte, 5)
	key[0] = segmentLedgerMapPrefix
	binary.BigEndian.PutUint32(key[1:5], segmentID)
	return key
}

// TotalEvents returns the total number of events in this segment (last cumulative entry).
func (m *SegmentLedgerMap) TotalEvents() uint32 {
	if len(m.Data) < SegmentLedgerMapSize {
		return 0
	}
	return m.getCumulative(int(SegmentSize) - 1)
}

// LedgerRangeToIDRange converts a ledger offset range to a dense ID range. O(1).
// startOff and endOff are ledger offsets within the segment (0-based, inclusive).
// Returns (startID, endID) where endID is inclusive. If the range has no events,
// returns (0, 0) with startID > endID being impossible, so caller should check endID >= startID.
func (m *SegmentLedgerMap) LedgerRangeToIDRange(startOff, endOff uint16) (uint32, uint32) {
	if len(m.Data) < SegmentLedgerMapSize {
		return 0, 0
	}

	var startID uint32
	if startOff > 0 {
		startID = m.getCumulative(int(startOff) - 1)
	}

	endID := m.getCumulative(int(endOff))
	if endID == 0 {
		return 0, 0
	}
	return startID, endID - 1
}

// DenseIDToLedgerAndSeq converts a dense local ID to an absolute ledger sequence
// and event sequence within that ledger. O(log N) via binary search.
func (m *SegmentLedgerMap) DenseIDToLedgerAndSeq(denseID uint32) (ledger uint32, seq uint16) {
	if len(m.Data) < SegmentLedgerMapSize {
		return 0, 0
	}

	segmentStart := m.SegmentID * SegmentSize
	n := int(SegmentSize)

	// Binary search for smallest i where cumulative[i] > denseID
	i := sort.Search(n, func(j int) bool {
		return m.getCumulative(j) > denseID
	})

	if i >= n {
		// denseID is out of range
		return 0, 0
	}

	ledger = segmentStart + uint32(i)
	if i == 0 {
		seq = uint16(denseID)
	} else {
		seq = uint16(denseID - m.getCumulative(i-1))
	}
	return ledger, seq
}

// getCumulative reads the cumulative count at offset i from the raw data.
func (m *SegmentLedgerMap) getCumulative(i int) uint32 {
	off := i * 4
	return binary.LittleEndian.Uint32(m.Data[off : off+4])
}

// EncodeSegmentLedgerMap builds a cumulative array from per-ledger event counts.
// ledgerEventCounts[i] = number of events at ledger offset i.
// Returns the raw 40,000-byte cumulative array.
func EncodeSegmentLedgerMap(segmentID uint32, ledgerEventCounts [SegmentSize]uint32) []byte {
	data := make([]byte, SegmentLedgerMapSize)
	var cumulative uint32
	for i := uint32(0); i < SegmentSize; i++ {
		cumulative += ledgerEventCounts[i]
		off := i * 4
		binary.LittleEndian.PutUint32(data[off:off+4], cumulative)
	}
	return data
}

// MergeSegmentLedgerMapData merges new per-ledger event counts into an existing
// cumulative array. The existing data is read, counts are added at the specified
// offsets, and the cumulative sums are recomputed from the point of first change.
// Returns a new 40,000-byte cumulative array.
func MergeSegmentLedgerMapData(existing []byte, newCounts map[uint16]uint32) []byte {
	if len(existing) < SegmentLedgerMapSize {
		// No existing data — build from scratch
		var counts [SegmentSize]uint32
		for off, count := range newCounts {
			counts[off] = count
		}
		return EncodeSegmentLedgerMap(0, counts)
	}

	// Decode existing cumulative into per-ledger counts
	var counts [SegmentSize]uint32
	prev := uint32(0)
	for i := uint32(0); i < SegmentSize; i++ {
		off := i * 4
		cum := binary.LittleEndian.Uint32(existing[off : off+4])
		counts[i] = cum - prev
		prev = cum
	}

	// Add new counts
	for off, count := range newCounts {
		counts[off] += count
	}

	// Re-encode
	return EncodeSegmentLedgerMap(0, counts)
}

// segmentEventCounter tracks per-ledger event counts during ingestion for a single segment.
type segmentEventCounter struct {
	eventCounts [SegmentSize]uint32 // per-ledger-offset event count
	nextDenseID uint32              // running total = next ID to assign
}

// assignDenseID assigns the next dense local ID for a ledger within this segment.
func (c *segmentEventCounter) assignDenseID(ledgerOffset uint16) uint32 {
	id := c.nextDenseID
	c.nextDenseID++
	c.eventCounts[ledgerOffset]++
	return id
}
