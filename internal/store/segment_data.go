package store

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/urvisavla/stellar-events/internal/eventstore"
)

// =============================================================================
//
// Segment Data (Flat Files) — eventstore-backed
// =============================================================================
//
// Stores event data (binary v3 blobs) using the eventstore package which
// provides blocked, zstd-compressed storage with parallel I/O.
//
// File layout per segment directory:
//   events.dat  — raw concatenated compressed blocks
//   events.idx  — flat array of int64 offsets into events.dat
//
// The eventstore format internally handles:
//   - Blocking events into groups (default 128)
//   - Per-block zstd compression
//   - FOR-encoded size index within each block
//   - Atomic writes (temp file + rename)

const (
	MaxEventsPerChunk   = 10_000_000
	EventsDataFileName  = "events.dat"
	EventsIndexFileName = "events.idx"
)

// =============================================================================
// Write Path
// =============================================================================

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
		blockSize = eventstore.DefaultBlockSize
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

	ew, err := eventstore.Create(dirPath, eventstore.WriterOptions{
		BlockSize:     w.blockSize,
		NoCompression: !w.compressEvents,
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
