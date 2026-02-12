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
	EventVolumeFlagZstd     = 0x01 // events.dat contains per-event zstd compressed blobs
	EventVolumeFlagZstdDict = 0x02 // events.dat uses zstd dictionary compression

	EventsDictFileName = "events.dict"

	defaultDictSampleCount = 16_384    // default events to buffer before building dictionary
	dictMaxSize            = 32 * 1024 // 32KB max dictionary size
	dictMinSamples         = 128       // minimum events for ZDICT_trainFromBuffer (hangs with fewer)
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

	// Zstd dictionary compression fields
	dictCompress    bool     // use zstd dictionary compression
	dictSampleCount int      // events to buffer before training dict
	dictTrained     bool     // dictionary has been trained for this chunk
	dictData        []byte   // trained dictionary bytes
	dictEncoder     *CDict   // CGO dict compressor (nil until trained)
	sampleBuf       [][]byte // buffered event blobs for dict training
	sampleDenseIDs  []uint32 // corresponding dense IDs for buffered events
}

// NewEventVolumeWriter creates a new event volume writer.
func NewEventVolumeWriter(basePath string, compressEvents bool, dictCompress bool, dictSampleCount int) *EventVolumeWriter {
	if dictSampleCount <= 0 {
		dictSampleCount = defaultDictSampleCount
	}
	w := &EventVolumeWriter{
		basePath:        basePath,
		compressEvents:  compressEvents,
		dictCompress:    dictCompress,
		dictSampleCount: dictSampleCount,
	}
	if dictCompress {
		// Dict compression takes priority; plain zstd encoder not needed
	} else if compressEvents {
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

	// Reset dict state for new chunk
	if w.dictCompress {
		w.dictTrained = false
		w.dictData = nil
		if w.dictEncoder != nil {
			w.dictEncoder.Close()
			w.dictEncoder = nil
		}
		w.sampleBuf = nil
		w.sampleDenseIDs = nil
	}

	fmt.Fprintf(os.Stderr, "  [event-volume] started chunk %06d\n", chunkID)
	return nil
}

// AppendEvent records the offset for a dense ID and writes the event data.
func (w *EventVolumeWriter) AppendEvent(denseID uint32, data []byte) error {
	if !w.active {
		return fmt.Errorf("no active chunk")
	}

	// Dict compression: buffer events until we have enough to train
	if w.dictCompress && !w.dictTrained {
		// Make a copy since the caller may reuse the buffer
		buf := make([]byte, len(data))
		copy(buf, data)
		w.sampleBuf = append(w.sampleBuf, buf)
		w.sampleDenseIDs = append(w.sampleDenseIDs, denseID)

		if len(w.sampleBuf) >= w.dictSampleCount {
			if err := w.trainAndFlush(); err != nil {
				return fmt.Errorf("failed to train dict and flush: %w", err)
			}
		}
		return nil
	}

	// Grow offsets slice if needed (fill gaps with current writeOffset)
	for uint32(len(w.offsets)) <= denseID {
		w.offsets = append(w.offsets, w.writeOffset)
	}
	w.offsets[denseID] = w.writeOffset

	// Optionally compress
	writeData := data
	if w.dictCompress && w.dictTrained && w.dictEncoder != nil {
		var err error
		writeData, err = w.dictEncoder.Compress(data)
		if err != nil {
			return fmt.Errorf("failed to compress event with dict: %w", err)
		}
	} else if w.compressEvents && w.zstdEncoder != nil {
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

// trainAndFlush trains a zstd dictionary via CGO from buffered samples and flushes them to disk.
// If there are too few samples for effective training, writes events uncompressed instead.
func (w *EventVolumeWriter) trainAndFlush() error {
	if len(w.sampleBuf) == 0 {
		return nil
	}

	// ZDICT_trainFromBuffer hangs or produces poor dicts with too few samples.
	// Fall back to writing uncompressed when below the minimum.
	if len(w.sampleBuf) < dictMinSamples {
		fmt.Fprintf(os.Stderr, "  [event-volume] too few samples (%d < %d) for dict training, writing uncompressed for chunk %06d\n",
			len(w.sampleBuf), dictMinSamples, w.chunkID)

		w.dictTrained = true
		w.dictData = nil // no dict → header flag stays 0

		for i, denseID := range w.sampleDenseIDs {
			data := w.sampleBuf[i]
			for uint32(len(w.offsets)) <= denseID {
				w.offsets = append(w.offsets, w.writeOffset)
			}
			w.offsets[denseID] = w.writeOffset
			n, err := w.eventsFile.Write(data)
			if err != nil {
				return fmt.Errorf("failed to write buffered event: %w", err)
			}
			w.writeOffset += uint32(n)
		}

		w.sampleBuf = nil
		w.sampleDenseIDs = nil
		return nil
	}

	// Train dictionary using C ZDICT_trainFromBuffer
	dict, err := trainDictCgo(w.sampleBuf, dictMaxSize)
	if err != nil {
		return fmt.Errorf("failed to train dict: %w", err)
	}
	w.dictData = dict

	// Create CGO compressor with trained dictionary (level 1 = fast)
	enc, err := newCDict(w.dictData, 1)
	if err != nil {
		return fmt.Errorf("failed to create CDict: %w", err)
	}
	w.dictEncoder = enc
	w.dictTrained = true

	fmt.Fprintf(os.Stderr, "  [event-volume] trained dict (%d bytes) from %d samples for chunk %06d\n",
		len(w.dictData), len(w.sampleBuf), w.chunkID)

	// Flush all buffered events
	for i, denseID := range w.sampleDenseIDs {
		data := w.sampleBuf[i]

		// Grow offsets slice if needed
		for uint32(len(w.offsets)) <= denseID {
			w.offsets = append(w.offsets, w.writeOffset)
		}
		w.offsets[denseID] = w.writeOffset

		compressed, err := w.dictEncoder.Compress(data)
		if err != nil {
			return fmt.Errorf("failed to compress buffered event: %w", err)
		}
		n, err := w.eventsFile.Write(compressed)
		if err != nil {
			return fmt.Errorf("failed to write buffered event: %w", err)
		}
		w.writeOffset += uint32(n)
	}

	fmt.Fprintf(os.Stderr, "  [event-volume] flushed %d buffered events for chunk %06d\n",
		len(w.sampleDenseIDs), w.chunkID)

	// Free sample buffers
	w.sampleBuf = nil
	w.sampleDenseIDs = nil

	return nil
}

// FinalizeChunk writes the event_offsets.dat file and closes events.dat.
func (w *EventVolumeWriter) FinalizeChunk() error {
	if !w.active {
		return nil
	}
	w.active = false

	// If dict compression is enabled but dict not yet trained (fewer events than sample count),
	// train on whatever we have and flush
	if w.dictCompress && !w.dictTrained && len(w.sampleBuf) > 0 {
		if err := w.trainAndFlush(); err != nil {
			return fmt.Errorf("failed to train dict on final chunk: %w", err)
		}
	}

	// Close events.dat
	if w.eventsFile != nil {
		if err := w.eventsFile.Close(); err != nil {
			return fmt.Errorf("failed to close events.dat: %w", err)
		}
		w.eventsFile = nil
	}

	// Write dictionary file if dict compression was used
	if w.dictCompress && w.dictData != nil {
		dirName := fmt.Sprintf("%06d", w.chunkID)
		dirPath := filepath.Join(w.basePath, dirName)
		dictPath := filepath.Join(dirPath, EventsDictFileName)
		if err := writeFileAtomic(dictPath, w.dictData); err != nil {
			return fmt.Errorf("failed to write events.dict: %w", err)
		}
	}

	// Close dict encoder
	if w.dictEncoder != nil {
		w.dictEncoder.Close()
		w.dictEncoder = nil
	}

	// Build variable-size offsets: header + (eventCount+1) offset entries
	eventCount := uint32(len(w.offsets))
	offsetsBuf := make([]byte, EventOffsetsHeaderSize+(eventCount+1)*OffsetEntrySize)

	// Write header
	copy(offsetsBuf[0:4], EventOffsetsMagic)
	binary.LittleEndian.PutUint16(offsetsBuf[4:6], EventOffsetsVersion)
	binary.LittleEndian.PutUint32(offsetsBuf[6:10], eventCount)
	// Flags byte at offset 10
	if w.dictCompress && w.dictData != nil {
		offsetsBuf[10] = EventVolumeFlagZstdDict
	} else if w.compressEvents {
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
	dataStart      int64 // byte offset where offset entries begin
	compressed     bool  // events are zstd compressed (plain)
	dictCompressed bool  // events are zstd dictionary compressed
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
			dataStart:      EventOffsetsHeaderSize,
			compressed:     headerBuf[10]&EventVolumeFlagZstd != 0,
			dictCompressed: headerBuf[10]&EventVolumeFlagZstdDict != 0,
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

	if hdr.dictCompressed {
		dictPath := filepath.Join(dirPath, EventsDictFileName)
		dictBytes, err := os.ReadFile(dictPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read events.dict: %w", err)
		}
		dec, err := newDDict(dictBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDict: %w", err)
		}
		defer dec.Close()
		data, err = dec.Decompress(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress event with dict: %w", err)
		}
	} else if hdr.compressed {
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

	// Load dictionary if dict-compressed (once for the whole batch)
	var dictDecoder *DDict
	if hdr.dictCompressed {
		dictPath := filepath.Join(dirPath, EventsDictFileName)
		dictBytes, err := os.ReadFile(dictPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read events.dict: %w", err)
		}
		dictDecoder, err = newDDict(dictBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDict: %w", err)
		}
		defer dictDecoder.Close()
	}

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

		if hdr.dictCompressed {
			decompressed, err := dictDecoder.Decompress(data)
			if err != nil {
				continue // skip on decompression error
			}
			data = decompressed
		} else if hdr.compressed {
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
