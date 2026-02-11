package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// =============================================================================
// Event Volume Flat Files
// =============================================================================
//
// Stores event data (binary v3 blobs) in flat files alongside bitmap indexes.
// Enables a fully file-based query path: indexes + events from files, no RocksDB.
//
// File layout per bucket directory:
//   event_offsets.dat  — header + variable-length array of uint32 byte offsets
//   events.dat         — concatenated binary v3 event blobs in dense ID order
//
// event_offsets.dat:
//   Header (16 bytes):
//     [Magic:4 "EVOF"][Version:2 LE][EventCount:4 LE][Flags:1][Reserved:5]
//     Flags: bit 0 = zstd compressed (events.dat contains per-event zstd blobs)
//   Offsets ((EventCount+1) × 4 bytes):
//     Entry i = byte offset of event with dense ID i in events.dat.
//     Sentinel entry at position EventCount = total file size of events.dat.
//   Total size: 16 + (EventCount + 1) × 4

const (
	MaxEventsPerChunk      = 10_000_000
	OffsetEntrySize        = 4 // uint32
	EventOffsetsFileName   = "event_offsets.dat"
	EventsFileName         = "events.dat"
	EventOffsetsHeaderSize = 16
	EventOffsetsMagic      = "EVOF"
	EventOffsetsVersion    = uint16(2)

	// Header flags byte (offset 10)
	EventVolumeFlagZstd = 0x01 // events.dat contains per-event zstd compressed blobs
)

// =============================================================================
// Write Path
// =============================================================================

// EventVolumeWriter writes event data to flat files during ingestion.
type EventVolumeWriter struct {
	basePath       string
	chunkID        uint32
	eventsFile     *os.File      // events.dat open for writing
	offsets        []uint32      // byte offset per dense ID
	writeOffset    uint32        // current write position in events.dat
	active         bool          // true if a chunk is currently open
	compressEvents bool          // zstd compress each event blob
	zstdEncoder    *zstd.Encoder // reused across events (nil if compression disabled)
}

// NewEventVolumeWriter creates a new event volume writer.
func NewEventVolumeWriter(basePath string, compressEvents bool) *EventVolumeWriter {
	w := &EventVolumeWriter{
		basePath:       basePath,
		compressEvents: compressEvents,
	}
	if compressEvents {
		// SpeedFastest for ingestion throughput; small blobs don't benefit from higher levels
		w.zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	}
	return w
}

// StartChunk begins a new chunk (bucket). Creates the directory if needed
// and opens events.dat for writing.
func (w *EventVolumeWriter) StartChunk(chunkID uint32) error {
	dirName := fmt.Sprintf("%06d", chunkID)
	dirPath := filepath.Join(w.basePath, dirName)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create chunk dir %s: %w", dirPath, err)
	}

	eventsPath := filepath.Join(dirPath, EventsFileName)
	f, err := os.Create(eventsPath)
	if err != nil {
		return fmt.Errorf("failed to create events.dat: %w", err)
	}

	w.chunkID = chunkID
	w.eventsFile = f
	w.offsets = w.offsets[:0] // reset but keep capacity
	w.writeOffset = 0
	w.active = true

	fmt.Fprintf(os.Stderr, "  [event-volume] started chunk %06d\n", chunkID)
	return nil
}

// AppendEvent records the offset for a dense ID and writes the event data.
func (w *EventVolumeWriter) AppendEvent(denseID uint32, data []byte) error {
	if !w.active {
		return fmt.Errorf("no active chunk")
	}

	// Grow offsets slice if needed (fill gaps with current writeOffset)
	for uint32(len(w.offsets)) <= denseID {
		w.offsets = append(w.offsets, w.writeOffset)
	}
	w.offsets[denseID] = w.writeOffset

	// Optionally compress
	writeData := data
	if w.compressEvents && w.zstdEncoder != nil {
		writeData = w.zstdEncoder.EncodeAll(data, nil)
	}

	// Write event data
	n, err := w.eventsFile.Write(writeData)
	if err != nil {
		return fmt.Errorf("failed to write event data: %w", err)
	}
	w.writeOffset += uint32(n)

	return nil
}

// FinalizeChunk writes the event_offsets.dat file and closes events.dat.
func (w *EventVolumeWriter) FinalizeChunk() error {
	if !w.active {
		return nil
	}
	w.active = false

	// Close events.dat
	if w.eventsFile != nil {
		if err := w.eventsFile.Close(); err != nil {
			return fmt.Errorf("failed to close events.dat: %w", err)
		}
		w.eventsFile = nil
	}

	// Build variable-size offsets: header + (eventCount+1) offset entries
	eventCount := uint32(len(w.offsets))
	offsetsBuf := make([]byte, EventOffsetsHeaderSize+(eventCount+1)*OffsetEntrySize)

	// Write header
	copy(offsetsBuf[0:4], EventOffsetsMagic)
	binary.LittleEndian.PutUint16(offsetsBuf[4:6], EventOffsetsVersion)
	binary.LittleEndian.PutUint32(offsetsBuf[6:10], eventCount)
	// Flags byte at offset 10
	if w.compressEvents {
		offsetsBuf[10] = EventVolumeFlagZstd
	}
	// Bytes 11-15 remain reserved/zero

	// Write offset entries after header
	dataStart := EventOffsetsHeaderSize
	for i, off := range w.offsets {
		binary.LittleEndian.PutUint32(offsetsBuf[dataStart+i*OffsetEntrySize:], off)
	}
	// Sentinel entry: total events.dat size
	binary.LittleEndian.PutUint32(offsetsBuf[dataStart+int(eventCount)*OffsetEntrySize:], w.writeOffset)

	// Write event_offsets.dat atomically (tmp + rename)
	dirName := fmt.Sprintf("%06d", w.chunkID)
	dirPath := filepath.Join(w.basePath, dirName)
	offsetsPath := filepath.Join(dirPath, EventOffsetsFileName)

	if err := writeFileAtomic(offsetsPath, offsetsBuf); err != nil {
		return fmt.Errorf("failed to write event_offsets.dat: %w", err)
	}

	fmt.Fprintf(os.Stderr, "  [event-volume] finalized chunk %06d (%d events, %d bytes)\n", w.chunkID, len(w.offsets), w.writeOffset)
	return nil
}

// ChunkID returns the current chunk ID being written.
func (w *EventVolumeWriter) ChunkID() uint32 {
	return w.chunkID
}

// IsActive returns true if a chunk is currently open for writing.
func (w *EventVolumeWriter) IsActive() bool {
	return w.active
}

// =============================================================================
// Read Path
// =============================================================================

// eventVolumeHeader holds parsed header info for read operations.
type eventVolumeHeader struct {
	dataStart  int64 // byte offset where offset entries begin
	compressed bool  // events are zstd compressed
}

// parseOffsetsHeader reads and parses the event_offsets.dat header.
// Returns format info needed by readers.
func parseOffsetsHeader(offsetsFile *os.File) (*eventVolumeHeader, error) {
	headerBuf := make([]byte, EventOffsetsHeaderSize)
	_, err := offsetsFile.ReadAt(headerBuf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	if string(headerBuf[0:4]) == EventOffsetsMagic {
		return &eventVolumeHeader{
			dataStart:  EventOffsetsHeaderSize,
			compressed: headerBuf[10]&EventVolumeFlagZstd != 0,
		}, nil
	}

	// V1 legacy format (no header)
	return &eventVolumeHeader{dataStart: 0, compressed: false}, nil
}

// package-level zstd decoder, safe for concurrent use
var zstdDecoder *zstd.Decoder

func init() {
	zstdDecoder, _ = zstd.NewReader(nil)
}

// ReadEventFromVolume reads a single event blob from flat files.
// Returns the raw binary event data.
func ReadEventFromVolume(basePath string, chunkID, denseID uint32) ([]byte, error) {
	dirName := fmt.Sprintf("%06d", chunkID)
	dirPath := filepath.Join(basePath, dirName)

	// Read header + two consecutive offsets
	offsetsPath := filepath.Join(dirPath, EventOffsetsFileName)
	offsetsFile, err := os.Open(offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open event_offsets.dat: %w", err)
	}
	defer offsetsFile.Close()

	hdr, err := parseOffsetsHeader(offsetsFile)
	if err != nil {
		return nil, err
	}

	// Read offsets[denseID] and offsets[denseID+1]
	buf := make([]byte, 8) // two uint32s
	_, err = offsetsFile.ReadAt(buf, hdr.dataStart+int64(denseID)*int64(OffsetEntrySize))
	if err != nil {
		return nil, fmt.Errorf("failed to read offsets: %w", err)
	}

	startOff := binary.LittleEndian.Uint32(buf[0:4])
	endOff := binary.LittleEndian.Uint32(buf[4:8])

	if endOff <= startOff {
		return nil, fmt.Errorf("invalid offset range: start=%d end=%d for denseID=%d", startOff, endOff, denseID)
	}

	// Read event data from events.dat
	eventsPath := filepath.Join(dirPath, EventsFileName)
	eventsFile, err := os.Open(eventsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open events.dat: %w", err)
	}
	defer eventsFile.Close()

	eventLen := endOff - startOff
	data := make([]byte, eventLen)
	n, err := eventsFile.ReadAt(data, int64(startOff))
	if err != nil {
		return nil, fmt.Errorf("failed to read event data: %w", err)
	}
	if uint32(n) != eventLen {
		return nil, fmt.Errorf("short read: got %d, expected %d", n, eventLen)
	}

	if hdr.compressed {
		data, err = zstdDecoder.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress event: %w", err)
		}
	}

	return data, nil
}

// ReadEventsFromVolume reads multiple events from flat files in batch.
// Opens files once and reads all requested events. Sorts denseIDs for sequential I/O.
// Returns a map from denseID to raw binary event data.
func ReadEventsFromVolume(basePath string, chunkID uint32, denseIDs []uint32) (map[uint32][]byte, error) {
	if len(denseIDs) == 0 {
		return nil, nil
	}

	dirName := fmt.Sprintf("%06d", chunkID)
	dirPath := filepath.Join(basePath, dirName)

	// Open offsets file
	offsetsPath := filepath.Join(dirPath, EventOffsetsFileName)
	offsetsFile, err := os.Open(offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open event_offsets.dat: %w", err)
	}
	defer offsetsFile.Close()

	hdr, err := parseOffsetsHeader(offsetsFile)
	if err != nil {
		return nil, err
	}

	// Open events file
	eventsPath := filepath.Join(dirPath, EventsFileName)
	eventsFile, err := os.Open(eventsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open events.dat: %w", err)
	}
	defer eventsFile.Close()

	// Sort for sequential I/O
	sorted := make([]uint32, len(denseIDs))
	copy(sorted, denseIDs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	result := make(map[uint32][]byte, len(sorted))
	offsetBuf := make([]byte, 8) // two uint32s

	for _, denseID := range sorted {
		// Read offsets[denseID] and offsets[denseID+1]
		_, err := offsetsFile.ReadAt(offsetBuf, hdr.dataStart+int64(denseID)*int64(OffsetEntrySize))
		if err != nil {
			continue // skip this event
		}

		startOff := binary.LittleEndian.Uint32(offsetBuf[0:4])
		endOff := binary.LittleEndian.Uint32(offsetBuf[4:8])

		if endOff <= startOff {
			continue // invalid or empty
		}

		eventLen := endOff - startOff
		data := make([]byte, eventLen)
		n, err := eventsFile.ReadAt(data, int64(startOff))
		if err != nil || uint32(n) != eventLen {
			continue // skip on error
		}

		if hdr.compressed {
			decompressed, err := zstdDecoder.DecodeAll(data, nil)
			if err != nil {
				continue // skip on decompression error
			}
			data = decompressed
		}

		result[denseID] = data
	}

	return result, nil
}

// EventVolumeExists checks if event volume files exist for a given chunk.
func EventVolumeExists(basePath string, chunkID uint32) bool {
	dirName := fmt.Sprintf("%06d", chunkID)
	offsetsPath := filepath.Join(basePath, dirName, EventOffsetsFileName)
	_, err := os.Stat(offsetsPath)
	return err == nil
}
