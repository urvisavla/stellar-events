package reader

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	IndexHeaderSize     = 8
	ChunkSize           = 10000
	FirstLedgerSequence = 2
	IndexVersion        = 1
)

// LedgerReader reads ledgers from the file-based storage
type LedgerReader struct {
	dataDir string
	decoder *zstd.Decoder
}

// NewLedgerReader creates a new ledger reader
func NewLedgerReader(dataDir string) (*LedgerReader, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &LedgerReader{
		dataDir: dataDir,
		decoder: decoder,
	}, nil
}

// Close closes the reader
func (r *LedgerReader) Close() {
	r.decoder.Close()
}

// LedgerTiming holds timing breakdown for ledger reads
type LedgerTiming struct {
	DiskRead   time.Duration
	Decompress time.Duration
}

// GetLedger reads a single ledger by sequence number
func (r *LedgerReader) GetLedger(sequence uint32) ([]byte, error) {
	data, _, err := r.GetLedgerWithTiming(sequence)
	return data, err
}

// GetLedgerWithTiming reads a single ledger and returns timing breakdown
func (r *LedgerReader) GetLedgerWithTiming(sequence uint32) ([]byte, *LedgerTiming, error) {
	timing := &LedgerTiming{}

	if sequence < FirstLedgerSequence {
		return nil, timing, fmt.Errorf("invalid sequence: must be >= %d", FirstLedgerSequence)
	}

	// Calculate chunk location
	chunkID := (sequence - FirstLedgerSequence) / ChunkSize
	localIndex := (sequence - FirstLedgerSequence) % ChunkSize

	// Build file paths
	parentDir := chunkID / 1000
	basePath := filepath.Join(r.dataDir, "chunks", fmt.Sprintf("%04d", parentDir), fmt.Sprintf("%06d", chunkID))
	indexPath := basePath + ".index"
	dataPath := basePath + ".data"

	// Start timing disk read
	diskStart := time.Now()

	// Open index file
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return nil, timing, fmt.Errorf("failed to open index file: %w", err)
	}
	defer indexFile.Close()

	// Read and validate index header
	header := make([]byte, IndexHeaderSize)
	if _, err := indexFile.ReadAt(header, 0); err != nil {
		return nil, timing, fmt.Errorf("failed to read index header: %w", err)
	}

	version := header[0]
	offsetSize := header[1]

	if version != IndexVersion {
		return nil, timing, fmt.Errorf("unsupported index version: %d", version)
	}

	if offsetSize != 4 && offsetSize != 8 {
		return nil, timing, fmt.Errorf("invalid offset size: %d", offsetSize)
	}

	// Read two adjacent offsets
	entryPos := int64(IndexHeaderSize) + int64(localIndex)*int64(offsetSize)
	offsetBuf := make([]byte, offsetSize*2)
	if _, err := indexFile.ReadAt(offsetBuf, entryPos); err != nil {
		return nil, timing, fmt.Errorf("failed to read offsets from index: %w", err)
	}

	var startOffset, endOffset uint64
	if offsetSize == 4 {
		startOffset = uint64(binary.LittleEndian.Uint32(offsetBuf[0:4]))
		endOffset = uint64(binary.LittleEndian.Uint32(offsetBuf[4:8]))
	} else {
		startOffset = binary.LittleEndian.Uint64(offsetBuf[0:8])
		endOffset = binary.LittleEndian.Uint64(offsetBuf[8:16])
	}

	recordSize := endOffset - startOffset

	// Open data file and read compressed data
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, timing, fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	compressed := make([]byte, recordSize)
	if _, err := dataFile.ReadAt(compressed, int64(startOffset)); err != nil {
		return nil, timing, fmt.Errorf("failed to read compressed data: %w", err)
	}

	// Record disk read time
	timing.DiskRead = time.Since(diskStart)

	// Start timing decompression
	decompressStart := time.Now()

	// Decompress
	decompressed, err := r.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, timing, fmt.Errorf("failed to decompress ledger: %w", err)
	}

	// Record decompress time
	timing.Decompress = time.Since(decompressStart)

	return decompressed, timing, nil
}

// LedgerIterator efficiently iterates over a range of ledgers by keeping
// chunk files open while reading ledgers from the same chunk
type LedgerIterator struct {
	dataDir    string
	startSeq   uint32
	endSeq     uint32
	currentSeq uint32

	// Current chunk state
	currentChunkID uint32
	indexFile      *os.File
	dataFile       *os.File
	offsets        []uint64
	offsetSize     uint8

	// Decoder (reused)
	decoder *zstd.Decoder
}

// NewLedgerIterator creates a new iterator for the given range
func NewLedgerIterator(dataDir string, startSeq, endSeq uint32) (*LedgerIterator, error) {
	if startSeq < FirstLedgerSequence {
		return nil, fmt.Errorf("invalid start sequence: must be >= %d (Stellar starts at ledger 2)", FirstLedgerSequence)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &LedgerIterator{
		dataDir:        dataDir,
		startSeq:       startSeq,
		endSeq:         endSeq,
		currentSeq:     startSeq,
		currentChunkID: ^uint32(0), // Invalid chunk ID to force initial load
		decoder:        decoder,
	}, nil
}

// Next returns the next ledger in the range
// Returns (ledgerBytes, ledgerSeq, true, nil) for each ledger
// Returns (nil, 0, false, nil) when iteration is complete
// Returns (nil, seq, false, err) on error
func (it *LedgerIterator) Next() ([]byte, uint32, bool, error) {
	if it.currentSeq > it.endSeq {
		return nil, 0, false, nil
	}

	ledgerSeq := it.currentSeq
	chunkID := (ledgerSeq - FirstLedgerSequence) / ChunkSize

	// Load new chunk if needed
	if chunkID != it.currentChunkID {
		if err := it.loadChunk(chunkID); err != nil {
			return nil, ledgerSeq, false, err
		}
	}

	// Get offsets for this ledger
	localIndex := (ledgerSeq - FirstLedgerSequence) % ChunkSize
	startOffset := it.offsets[localIndex]
	endOffset := it.offsets[localIndex+1]
	recordSize := endOffset - startOffset

	// Read compressed data
	compressed := make([]byte, recordSize)
	if _, err := it.dataFile.ReadAt(compressed, int64(startOffset)); err != nil {
		return nil, ledgerSeq, false, fmt.Errorf("failed to read data for ledger %d: %w", ledgerSeq, err)
	}

	// Decompress
	decompressed, err := it.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, ledgerSeq, false, fmt.Errorf("failed to decompress ledger %d: %w", ledgerSeq, err)
	}

	it.currentSeq++
	return decompressed, ledgerSeq, true, nil
}

// loadChunk loads a new chunk's index and opens its data file
func (it *LedgerIterator) loadChunk(chunkID uint32) error {
	// Close previous chunk files if open
	it.closeChunkFiles()

	// Build file paths
	parentDir := chunkID / 1000
	basePath := filepath.Join(it.dataDir, "chunks", fmt.Sprintf("%04d", parentDir), fmt.Sprintf("%06d", chunkID))
	indexPath := basePath + ".index"
	dataPath := basePath + ".data"

	// Open index file
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("failed to open index file for chunk %d: %w", chunkID, err)
	}
	it.indexFile = indexFile

	// Read header
	header := make([]byte, IndexHeaderSize)
	if _, err := indexFile.ReadAt(header, 0); err != nil {
		return fmt.Errorf("failed to read index header for chunk %d: %w", chunkID, err)
	}

	version := header[0]
	it.offsetSize = header[1]

	if version != IndexVersion {
		return fmt.Errorf("unsupported index version %d for chunk %d", version, chunkID)
	}

	if it.offsetSize != 4 && it.offsetSize != 8 {
		return fmt.Errorf("invalid offset size %d for chunk %d", it.offsetSize, chunkID)
	}

	// Get file size to determine number of offsets
	fileInfo, err := indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file for chunk %d: %w", chunkID, err)
	}

	numOffsets := (fileInfo.Size() - IndexHeaderSize) / int64(it.offsetSize)

	// Read all offsets for this chunk
	offsetsData := make([]byte, numOffsets*int64(it.offsetSize))
	if _, err := indexFile.ReadAt(offsetsData, IndexHeaderSize); err != nil {
		return fmt.Errorf("failed to read offsets for chunk %d: %w", chunkID, err)
	}

	// Parse offsets
	it.offsets = make([]uint64, numOffsets)
	for i := int64(0); i < numOffsets; i++ {
		if it.offsetSize == 4 {
			it.offsets[i] = uint64(binary.LittleEndian.Uint32(offsetsData[i*4 : i*4+4]))
		} else {
			it.offsets[i] = binary.LittleEndian.Uint64(offsetsData[i*8 : i*8+8])
		}
	}

	// Open data file
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("failed to open data file for chunk %d: %w", chunkID, err)
	}
	it.dataFile = dataFile

	// Update chunk state
	it.currentChunkID = chunkID

	return nil
}

// closeChunkFiles closes the current chunk's files
func (it *LedgerIterator) closeChunkFiles() {
	if it.indexFile != nil {
		it.indexFile.Close()
		it.indexFile = nil
	}
	if it.dataFile != nil {
		it.dataFile.Close()
		it.dataFile = nil
	}
	it.offsets = nil
}

// Close closes the iterator and releases resources
func (it *LedgerIterator) Close() {
	it.closeChunkFiles()
	if it.decoder != nil {
		it.decoder.Close()
		it.decoder = nil
	}
}

// GetLedgerDataStats scans the chunks directory to find available ledger range
func GetLedgerDataStats(dataDir string) (minLedger, maxLedger uint32, chunkCount int, err error) {
	chunksDir := filepath.Join(dataDir, "chunks")

	var minChunkID, maxChunkID uint32
	firstChunk := true

	err = filepath.Walk(chunksDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".index" {
			return nil
		}

		base := filepath.Base(path)
		var chunkID uint32
		if _, scanErr := fmt.Sscanf(base, "%06d.index", &chunkID); scanErr != nil {
			return nil
		}

		chunkCount++
		if firstChunk {
			minChunkID = chunkID
			maxChunkID = chunkID
			firstChunk = false
		} else {
			if chunkID < minChunkID {
				minChunkID = chunkID
			}
			if chunkID > maxChunkID {
				maxChunkID = chunkID
			}
		}
		return nil
	})

	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to scan chunks directory: %w", err)
	}

	if chunkCount == 0 {
		return 0, 0, 0, fmt.Errorf("no chunk files found in %s", chunksDir)
	}

	minLedger = minChunkID*ChunkSize + FirstLedgerSequence
	maxLedger = (maxChunkID+1)*ChunkSize + FirstLedgerSequence - 1

	return minLedger, maxLedger, chunkCount, nil
}

// ChunkDirInfo holds information about a parent chunk directory
type ChunkDirInfo struct {
	DirName    string // e.g., "0000"
	MinLedger  uint32
	MaxLedger  uint32
	ChunkCount int
	DataSizeMB float64
}

// GetChunkDirectoryInfo scans the chunks directory and returns info for each parent directory
func GetChunkDirectoryInfo(dataDir string) ([]ChunkDirInfo, error) {
	chunksDir := filepath.Join(dataDir, "chunks")

	// First, find all parent directories and their chunks
	dirChunks := make(map[string][]uint32)  // parent dir -> list of chunk IDs
	dirSizes := make(map[string]int64)      // parent dir -> total data size

	err := filepath.Walk(chunksDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}

		// Get parent directory name (e.g., "0000")
		parentDir := filepath.Base(filepath.Dir(path))

		if filepath.Ext(path) == ".index" {
			base := filepath.Base(path)
			var chunkID uint32
			if _, scanErr := fmt.Sscanf(base, "%06d.index", &chunkID); scanErr == nil {
				dirChunks[parentDir] = append(dirChunks[parentDir], chunkID)
			}
		}

		// Count data file sizes
		if filepath.Ext(path) == ".data" {
			dirSizes[parentDir] += info.Size()
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan chunks directory: %w", err)
	}

	if len(dirChunks) == 0 {
		return nil, fmt.Errorf("no chunk files found in %s", chunksDir)
	}

	// Build sorted list of directory info
	var dirNames []string
	for name := range dirChunks {
		dirNames = append(dirNames, name)
	}
	sort.Strings(dirNames)

	var results []ChunkDirInfo
	for _, dirName := range dirNames {
		chunks := dirChunks[dirName]
		if len(chunks) == 0 {
			continue
		}

		// Find min and max chunk IDs in this directory
		minChunk := chunks[0]
		maxChunk := chunks[0]
		for _, chunkID := range chunks {
			if chunkID < minChunk {
				minChunk = chunkID
			}
			if chunkID > maxChunk {
				maxChunk = chunkID
			}
		}

		// Calculate ledger range
		minLedger := minChunk*ChunkSize + FirstLedgerSequence
		maxLedger := (maxChunk+1)*ChunkSize + FirstLedgerSequence - 1

		results = append(results, ChunkDirInfo{
			DirName:    dirName,
			MinLedger:  minLedger,
			MaxLedger:  maxLedger,
			ChunkCount: len(chunks),
			DataSizeMB: float64(dirSizes[dirName]) / (1024 * 1024),
		})
	}

	return results, nil
}
