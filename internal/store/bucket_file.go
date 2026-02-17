package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

// =============================================================================
// Flat File Bucket Index Format
// =============================================================================
//
// Directory structure:
//   <base_path>/NNNNNN/
//     contracts.idx      - sorted term index (binary-searchable)
//     contracts.pack     - concatenated roaring bitmap blobs
//     topic0.idx + topic0.pack
//     topic1.idx + topic1.pack
//     topic2.idx + topic2.pack
//     topic3.idx + topic3.pack
//     topics.idx + topics.pack    - combined fallback (if position unknown)
//     ledgermap.dat      - 40,000 bytes cumulative count array
//
// Term Index File (.idx):
//   [Header: 16 bytes][Entry × N: 24 bytes each, sorted by term_hash]
//
// Header (16 bytes):
//   [Magic:4 "BIDX"][Version:2 BE][EntryCount:4 BE][Reserved:6]
//
// Entry (24 bytes):
//   [term_hash:16][pack_offset:4 BE][pack_length:4 BE]
//
// Bitmap Data File (.pack):
//   Concatenated serialized roaring bitmaps, no header.

const (
	// IdxMagic is the magic bytes for .idx files.
	IdxMagic = "BIDX"

	// IdxVersion is the current format version.
	IdxVersion uint16 = 1

	// IdxHeaderSize is the size of the .idx file header.
	IdxHeaderSize = 16

	// IdxEntrySize is the size of each entry in the .idx file.
	IdxEntrySize = 24

	// LedgerMapFileName is the name of the ledger map file.
	LedgerMapFileName = "ledgermap.dat"
)

// BucketTermData holds a term hash and its serialized bitmap data.
type BucketTermData struct {
	TermHash   [16]byte
	BitmapData []byte
}

// WriteBucketDir writes a complete bucket directory with .idx/.pack file pairs.
// contractTerms and topicsByPos are the pre-collected bitmap data.
// combinedTopics is the fallback for topics with unknown position.
func WriteBucketDir(basePath string, bucketID uint32, contractTerms []BucketTermData, topicsByPos [4][]BucketTermData, combinedTopics []BucketTermData, lm *BucketLedgerMap) error {
	dirName := fmt.Sprintf("%06d", bucketID)
	dirPath := filepath.Join(basePath, dirName)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create bucket dir %s: %w", dirPath, err)
	}

	// Write contracts.idx + contracts.pack
	if len(contractTerms) > 0 {
		if err := writeIdxPackPair(dirPath, "contracts", contractTerms); err != nil {
			return fmt.Errorf("failed to write contracts: %w", err)
		}
	}

	// Write topic0-3.idx + topic0-3.pack
	topicNames := [4]string{"topic0", "topic1", "topic2", "topic3"}
	for pos := 0; pos < 4; pos++ {
		if len(topicsByPos[pos]) > 0 {
			if err := writeIdxPackPair(dirPath, topicNames[pos], topicsByPos[pos]); err != nil {
				return fmt.Errorf("failed to write %s: %w", topicNames[pos], err)
			}
		}
	}

	// Write combined topics fallback
	if len(combinedTopics) > 0 {
		if err := writeIdxPackPair(dirPath, "topics", combinedTopics); err != nil {
			return fmt.Errorf("failed to write combined topics: %w", err)
		}
	}

	// Write ledgermap.dat
	if lm != nil && len(lm.Data) == BucketLedgerMapSize {
		if err := writeFileAtomic(filepath.Join(dirPath, LedgerMapFileName), lm.Data); err != nil {
			return fmt.Errorf("failed to write ledger map: %w", err)
		}
	}

	return nil
}

// writeIdxPackPair writes a sorted .idx file and concatenated .pack file.
func writeIdxPackPair(dirPath, name string, terms []BucketTermData) error {
	// Sort terms by term_hash for binary search
	sort.Slice(terms, func(i, j int) bool {
		return bytes.Compare(terms[i].TermHash[:], terms[j].TermHash[:]) < 0
	})

	entryCount := uint32(len(terms))

	// Build .idx header
	idxBuf := make([]byte, IdxHeaderSize+int(entryCount)*IdxEntrySize)
	copy(idxBuf[0:4], IdxMagic)
	binary.BigEndian.PutUint16(idxBuf[4:6], IdxVersion)
	binary.BigEndian.PutUint32(idxBuf[6:10], entryCount)
	// Reserved bytes 10-15 are already zero

	// Build .pack data and .idx entries
	var packBuf []byte
	for i, t := range terms {
		packOffset := uint32(len(packBuf))
		packLength := uint32(len(t.BitmapData))

		entryOffset := IdxHeaderSize + i*IdxEntrySize
		copy(idxBuf[entryOffset:entryOffset+16], t.TermHash[:])
		binary.BigEndian.PutUint32(idxBuf[entryOffset+16:entryOffset+20], packOffset)
		binary.BigEndian.PutUint32(idxBuf[entryOffset+20:entryOffset+24], packLength)

		packBuf = append(packBuf, t.BitmapData...)
	}

	// Atomic writes: .tmp + rename
	idxPath := filepath.Join(dirPath, name+".idx")
	packPath := filepath.Join(dirPath, name+".pack")

	if err := writeFileAtomic(idxPath, idxBuf); err != nil {
		return fmt.Errorf("failed to write %s.idx: %w", name, err)
	}
	if err := writeFileAtomic(packPath, packBuf); err != nil {
		return fmt.Errorf("failed to write %s.pack: %w", name, err)
	}

	return nil
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
// Read Path: Index File + Binary Search
// =============================================================================

// IndexFile represents an opened .idx file for binary searching.
type IndexFile struct {
	data       []byte // raw file contents (could be mmap'd in future)
	entryCount uint32
}

// OpenIndexFile opens and validates a .idx file.
func OpenIndexFile(path string) (*IndexFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file: %w", err)
	}

	if len(data) < IdxHeaderSize {
		return nil, fmt.Errorf("index file too small: %d bytes", len(data))
	}

	// Validate magic
	if string(data[0:4]) != IdxMagic {
		return nil, fmt.Errorf("invalid magic: %x", data[0:4])
	}

	// Validate version
	version := binary.BigEndian.Uint16(data[4:6])
	if version != IdxVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	entryCount := binary.BigEndian.Uint32(data[6:10])
	expectedSize := IdxHeaderSize + int(entryCount)*IdxEntrySize
	if len(data) != expectedSize {
		return nil, fmt.Errorf("size mismatch: got %d, expected %d (%d entries)", len(data), expectedSize, entryCount)
	}

	return &IndexFile{data: data, entryCount: entryCount}, nil
}

// EntryCount returns the number of entries in the index.
func (f *IndexFile) EntryCount() uint32 {
	return f.entryCount
}

// IndexEntry holds the result of looking up a term in an index file.
type IndexEntry struct {
	TermHash   [16]byte
	PackOffset uint32
	PackLength uint32
}

// LookupTerm performs a binary search for a 16-byte term hash.
// Returns the entry if found, or nil if not found.
func (f *IndexFile) LookupTerm(termHash [16]byte) *IndexEntry {
	lo, hi := 0, int(f.entryCount)
	for lo < hi {
		mid := lo + (hi-lo)/2
		entryOffset := IdxHeaderSize + mid*IdxEntrySize
		cmp := bytes.Compare(termHash[:], f.data[entryOffset:entryOffset+16])
		if cmp == 0 {
			return &IndexEntry{
				TermHash:   termHash,
				PackOffset: binary.BigEndian.Uint32(f.data[entryOffset+16 : entryOffset+20]),
				PackLength: binary.BigEndian.Uint32(f.data[entryOffset+20 : entryOffset+24]),
			}
		} else if cmp < 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return nil
}

// GetEntry returns the entry at a given index (0-based).
func (f *IndexFile) GetEntry(idx int) *IndexEntry {
	if idx < 0 || idx >= int(f.entryCount) {
		return nil
	}
	entryOffset := IdxHeaderSize + idx*IdxEntrySize
	var termHash [16]byte
	copy(termHash[:], f.data[entryOffset:entryOffset+16])
	return &IndexEntry{
		TermHash:   termHash,
		PackOffset: binary.BigEndian.Uint32(f.data[entryOffset+16 : entryOffset+20]),
		PackLength: binary.BigEndian.Uint32(f.data[entryOffset+20 : entryOffset+24]),
	}
}

// ReadBitmapFromPack reads a bitmap blob from a .pack file given offset and length.
func ReadBitmapFromPack(packPath string, offset, length uint32) ([]byte, error) {
	f, err := os.Open(packPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open pack file: %w", err)
	}
	defer f.Close()

	data := make([]byte, length)
	n, err := f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("failed to read pack data at offset %d: %w", offset, err)
	}
	if n != int(length) {
		return nil, fmt.Errorf("short read: got %d, expected %d", n, length)
	}

	return data, nil
}

// =============================================================================
// Mmap Support
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

// OpenIndexFileMmap creates an IndexFile from a memory-mapped .idx file.
// The IndexFile.data points directly into the mmap region (zero-copy).
func OpenIndexFileMmap(mm *MmapFile) (*IndexFile, error) {
	data := mm.data
	if len(data) < IdxHeaderSize {
		return nil, fmt.Errorf("index file too small: %d bytes", len(data))
	}

	if string(data[0:4]) != IdxMagic {
		return nil, fmt.Errorf("invalid magic: %x", data[0:4])
	}

	version := binary.BigEndian.Uint16(data[4:6])
	if version != IdxVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	entryCount := binary.BigEndian.Uint32(data[6:10])
	expectedSize := IdxHeaderSize + int(entryCount)*IdxEntrySize
	if len(data) != expectedSize {
		return nil, fmt.Errorf("size mismatch: got %d, expected %d (%d entries)", len(data), expectedSize, entryCount)
	}

	return &IndexFile{data: data, entryCount: entryCount}, nil
}

// ReadBitmapFromPackMmap returns a zero-copy slice from a mmap'd .pack file.
func ReadBitmapFromPackMmap(mm *MmapFile, offset, length uint32) ([]byte, error) {
	end := offset + length
	if int(end) > len(mm.data) {
		return nil, fmt.Errorf("pack read out of bounds: offset=%d length=%d fileSize=%d", offset, length, len(mm.data))
	}
	return mm.data[offset:end], nil
}

// ValidateLedgerMapFile validates a ledgermap.dat file by checking its size.
func ValidateLedgerMapFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat ledger map file: %w", err)
	}
	if info.Size() != int64(BucketLedgerMapSize) {
		return fmt.Errorf("invalid ledger map size: got %d, expected %d", info.Size(), BucketLedgerMapSize)
	}
	return nil
}

// CleanupTmpFiles removes any .tmp files from a bucket directory (restart recovery).
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
