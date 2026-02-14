package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
)

// VolumeReadTiming holds timing breakdown for event volume read operations.
type VolumeReadTiming struct {
	DiskReadTime   time.Duration
	DecompressTime time.Duration
}

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
	EventVolumeFlagGrouped  = 0x04 // events.dat contains grouped compression blobs

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

	// Grouped compression fields
	groupSize  int      // events per compression group (0 or 1 = per-event)
	groupBuf   [][]byte // buffered event data within current group
	eventCount uint32   // total events written (needed when offsets are per-group)
}

// NewEventVolumeWriter creates a new event volume writer.
func NewEventVolumeWriter(basePath string, compressEvents bool, dictCompress bool, dictSampleCount int, groupSize int) *EventVolumeWriter {
	if dictSampleCount <= 0 {
		dictSampleCount = defaultDictSampleCount
	}
	// Grouped compression only applies when plain zstd is active (not dict mode)
	if dictCompress || !compressEvents {
		groupSize = 0
	}
	w := &EventVolumeWriter{
		basePath:        basePath,
		compressEvents:  compressEvents,
		dictCompress:    dictCompress,
		dictSampleCount: dictSampleCount,
		groupSize:       groupSize,
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
	w.eventCount = 0
	w.groupBuf = w.groupBuf[:0]

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

	// Grouped compression: buffer events, flush when group is full
	if w.groupSize > 1 {
		buf := make([]byte, len(data))
		copy(buf, data)
		w.groupBuf = append(w.groupBuf, buf)
		w.eventCount++

		if len(w.groupBuf) >= w.groupSize {
			if err := w.flushGroup(); err != nil {
				return fmt.Errorf("failed to flush group: %w", err)
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

// flushGroup compresses buffered events as a single group and writes to events.dat.
// Group blob format (offset table):
//   [off0:4 LE][off1:4 LE]...[off_{N-1}:4 LE][event0_data][event1_data]...
// where off_i is the byte offset of event i from the start of the blob.
func (w *EventVolumeWriter) flushGroup() error {
	if len(w.groupBuf) == 0 {
		return nil
	}

	// Build uncompressed group blob: offset table + concatenated event data
	n := len(w.groupBuf)
	headerSize := n * 4 // offset table: one uint32 per event
	totalSize := headerSize
	for _, ev := range w.groupBuf {
		totalSize += len(ev)
	}
	blob := make([]byte, totalSize)

	// Compute and write offset table
	offset := uint32(headerSize)
	for i, ev := range w.groupBuf {
		binary.LittleEndian.PutUint32(blob[i*4:i*4+4], offset)
		offset += uint32(len(ev))
	}

	// Write concatenated event data after the offset table
	pos := headerSize
	for _, ev := range w.groupBuf {
		copy(blob[pos:pos+len(ev)], ev)
		pos += len(ev)
	}

	// Compress the entire group blob
	compressed := w.zstdEncoder.EncodeAll(blob, nil)

	// Record group offset
	w.offsets = append(w.offsets, w.writeOffset)

	// Write compressed group to events.dat
	n, err := w.eventsFile.Write(compressed)
	if err != nil {
		return fmt.Errorf("failed to write group data: %w", err)
	}
	w.writeOffset += uint32(n)

	// Clear group buffer
	w.groupBuf = w.groupBuf[:0]
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

	// Flush partial group if grouped compression is active
	if w.groupSize > 1 && len(w.groupBuf) > 0 {
		if err := w.flushGroup(); err != nil {
			return fmt.Errorf("failed to flush final group: %w", err)
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

	// Determine event count and number of offset entries
	grouped := w.groupSize > 1
	var totalEvents uint32
	var numOffsetEntries uint32
	if grouped {
		totalEvents = w.eventCount
		numOffsetEntries = uint32(len(w.offsets)) // one per group (sentinel added below)
	} else {
		totalEvents = uint32(len(w.offsets))
		numOffsetEntries = totalEvents // one per event (sentinel added below)
	}

	// Build variable-size offsets: header + (numOffsetEntries+1) offset entries
	offsetsBuf := make([]byte, EventOffsetsHeaderSize+(numOffsetEntries+1)*OffsetEntrySize)

	// Write header
	copy(offsetsBuf[0:4], EventOffsetsMagic)
	binary.LittleEndian.PutUint16(offsetsBuf[4:6], EventOffsetsVersion)
	binary.LittleEndian.PutUint32(offsetsBuf[6:10], totalEvents)
	// Flags byte at offset 10
	if w.dictCompress && w.dictData != nil {
		offsetsBuf[10] = EventVolumeFlagZstdDict
	} else if grouped {
		offsetsBuf[10] = EventVolumeFlagGrouped | EventVolumeFlagZstd
		offsetsBuf[11] = byte(w.groupSize) // GroupSize at byte 11
	} else if w.compressEvents {
		offsetsBuf[10] = EventVolumeFlagZstd
	}
	// Bytes 12-15 remain reserved/zero

	// Write offset entries after header
	dataStart := EventOffsetsHeaderSize
	for i, off := range w.offsets {
		binary.LittleEndian.PutUint32(offsetsBuf[dataStart+i*OffsetEntrySize:], off)
	}
	// Sentinel entry: total events.dat size
	binary.LittleEndian.PutUint32(offsetsBuf[dataStart+int(numOffsetEntries)*OffsetEntrySize:], w.writeOffset)

	// Write event_offsets.dat atomically (tmp + rename)
	dirName := fmt.Sprintf("%06d", w.chunkID)
	dirPath := filepath.Join(w.basePath, dirName)
	offsetsPath := filepath.Join(dirPath, EventOffsetsFileName)

	if err := writeFileAtomic(offsetsPath, offsetsBuf); err != nil {
		return fmt.Errorf("failed to write event_offsets.dat: %w", err)
	}

	if grouped {
		fmt.Fprintf(os.Stderr, "  [event-volume] finalized chunk %06d (%d events, %d groups, %d bytes)\n", w.chunkID, totalEvents, len(w.offsets), w.writeOffset)
	} else {
		fmt.Fprintf(os.Stderr, "  [event-volume] finalized chunk %06d (%d events, %d bytes)\n", w.chunkID, totalEvents, w.writeOffset)
	}
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
	dataStart      int64  // byte offset where offset entries begin
	compressed     bool   // events are zstd compressed (plain)
	dictCompressed bool   // events are zstd dictionary compressed
	grouped        bool   // events are grouped compressed
	groupSize      uint32 // events per group (0 or 1 = per-event)
	eventCount     uint32 // total number of events
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
		flags := headerBuf[10]
		grouped := flags&EventVolumeFlagGrouped != 0
		var groupSize uint32
		if grouped {
			groupSize = uint32(headerBuf[11])
			if groupSize == 0 {
				groupSize = 1
			}
		}
		eventCount := binary.LittleEndian.Uint32(headerBuf[6:10])
		return &eventVolumeHeader{
			dataStart:      EventOffsetsHeaderSize,
			compressed:     flags&EventVolumeFlagZstd != 0,
			dictCompressed: flags&EventVolumeFlagZstdDict != 0,
			grouped:        grouped,
			groupSize:      groupSize,
			eventCount:     eventCount,
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

	// Grouped compression: read the group, decompress, extract the event
	if hdr.grouped && hdr.groupSize > 1 {
		groupIdx := denseID / hdr.groupSize
		posInGroup := denseID % hdr.groupSize

		// Read offsets[groupIdx] and offsets[groupIdx+1]
		buf := make([]byte, 8)
		_, err = offsetsFile.ReadAt(buf, hdr.dataStart+int64(groupIdx)*int64(OffsetEntrySize))
		if err != nil {
			return nil, fmt.Errorf("failed to read group offsets: %w", err)
		}
		startOff := binary.LittleEndian.Uint32(buf[0:4])
		endOff := binary.LittleEndian.Uint32(buf[4:8])
		if endOff <= startOff {
			return nil, fmt.Errorf("invalid group offset range: start=%d end=%d for groupIdx=%d", startOff, endOff, groupIdx)
		}

		// Read compressed group blob
		eventsPath := filepath.Join(dirPath, EventsFileName)
		eventsFile, err := os.Open(eventsPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open events.dat: %w", err)
		}
		defer eventsFile.Close()

		groupData := make([]byte, endOff-startOff)
		n, err := eventsFile.ReadAt(groupData, int64(startOff))
		if err != nil {
			return nil, fmt.Errorf("failed to read group data: %w", err)
		}
		if uint32(n) != endOff-startOff {
			return nil, fmt.Errorf("short read: got %d, expected %d", n, endOff-startOff)
		}

		// Decompress group
		decompressed, err := zstdDecoder.DecodeAll(groupData, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress group: %w", err)
		}

		// Compute how many events are in this group (last group may be partial)
		eventsInGroup := hdr.groupSize
		if remaining := hdr.eventCount - groupIdx*hdr.groupSize; remaining < hdr.groupSize {
			eventsInGroup = remaining
		}

		return extractEventFromGroup(decompressed, posInGroup, eventsInGroup)
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

// extractEventFromGroup extracts a single event at the given position from a
// decompressed group blob using the offset table. O(1) random access.
// Group blob format: [off0:4 LE][off1:4 LE]...[off_{N-1}:4 LE][event0_data][event1_data]...
func extractEventFromGroup(groupBlob []byte, posInGroup, eventsInGroup uint32) ([]byte, error) {
	if posInGroup >= eventsInGroup {
		return nil, fmt.Errorf("posInGroup %d >= eventsInGroup %d", posInGroup, eventsInGroup)
	}

	// Read start offset from the offset table
	offPos := posInGroup * 4
	if int(offPos+4) > len(groupBlob) {
		return nil, fmt.Errorf("group blob too short for offset at pos %d (len=%d)", posInGroup, len(groupBlob))
	}
	start := binary.LittleEndian.Uint32(groupBlob[offPos : offPos+4])

	// Read end: next event's offset, or end of blob for the last event
	var end uint32
	if posInGroup+1 < eventsInGroup {
		end = binary.LittleEndian.Uint32(groupBlob[(posInGroup+1)*4 : (posInGroup+1)*4+4])
	} else {
		end = uint32(len(groupBlob))
	}

	if start > end || int(end) > len(groupBlob) {
		return nil, fmt.Errorf("invalid event bounds: start=%d end=%d blobLen=%d", start, end, len(groupBlob))
	}

	result := make([]byte, end-start)
	copy(result, groupBlob[start:end])
	return result, nil
}

// ReadEventsFromVolume reads multiple events from flat files in batch.
// Opens files once and reads all requested events. Sorts denseIDs for sequential I/O.
// Returns a map from denseID to raw binary event data and timing breakdown.
func ReadEventsFromVolume(basePath string, chunkID uint32, denseIDs []uint32) (map[uint32][]byte, *VolumeReadTiming, error) {
	timing := &VolumeReadTiming{}

	if len(denseIDs) == 0 {
		return nil, timing, nil
	}

	dirName := fmt.Sprintf("%06d", chunkID)
	dirPath := filepath.Join(basePath, dirName)

	// Open offsets file
	offsetsPath := filepath.Join(dirPath, EventOffsetsFileName)
	offsetsFile, err := os.Open(offsetsPath)
	if err != nil {
		return nil, timing, fmt.Errorf("failed to open event_offsets.dat: %w", err)
	}
	defer offsetsFile.Close()

	hdr, err := parseOffsetsHeader(offsetsFile)
	if err != nil {
		return nil, timing, err
	}

	// Open events file
	eventsPath := filepath.Join(dirPath, EventsFileName)
	eventsFile, err := os.Open(eventsPath)
	if err != nil {
		return nil, timing, fmt.Errorf("failed to open events.dat: %w", err)
	}
	defer eventsFile.Close()

	// Sort for sequential I/O
	sorted := make([]uint32, len(denseIDs))
	copy(sorted, denseIDs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	result := make(map[uint32][]byte, len(sorted))

	// Grouped compression: batch by group to decompress each group only once
	if hdr.grouped && hdr.groupSize > 1 {
		// Group denseIDs by their group index
		groupedIDs := make(map[uint32][]uint32) // groupIdx -> list of denseIDs
		for _, denseID := range sorted {
			gIdx := denseID / hdr.groupSize
			groupedIDs[gIdx] = append(groupedIDs[gIdx], denseID)
		}

		// Sort group indices for sequential I/O
		groupIndices := make([]uint32, 0, len(groupedIDs))
		for gIdx := range groupedIDs {
			groupIndices = append(groupIndices, gIdx)
		}
		sort.Slice(groupIndices, func(i, j int) bool { return groupIndices[i] < groupIndices[j] })

		offsetBuf := make([]byte, 8)
		for _, gIdx := range groupIndices {
			// Read group offsets
			_, err := offsetsFile.ReadAt(offsetBuf, hdr.dataStart+int64(gIdx)*int64(OffsetEntrySize))
			if err != nil {
				continue
			}
			startOff := binary.LittleEndian.Uint32(offsetBuf[0:4])
			endOff := binary.LittleEndian.Uint32(offsetBuf[4:8])
			if endOff <= startOff {
				continue
			}

			// Read compressed group data (disk I/O)
			groupData := make([]byte, endOff-startOff)
			diskStart := time.Now()
			n, err := eventsFile.ReadAt(groupData, int64(startOff))
			timing.DiskReadTime += time.Since(diskStart)
			if err != nil || uint32(n) != endOff-startOff {
				continue
			}

			// Decompress group
			decompStart := time.Now()
			decompressed, err := zstdDecoder.DecodeAll(groupData, nil)
			timing.DecompressTime += time.Since(decompStart)
			if err != nil {
				continue
			}

			// Compute how many events are in this group (last group may be partial)
			eventsInGroup := hdr.groupSize
			if remaining := hdr.eventCount - gIdx*hdr.groupSize; remaining < hdr.groupSize {
				eventsInGroup = remaining
			}

			// Extract each requested event from the decompressed group
			for _, denseID := range groupedIDs[gIdx] {
				posInGroup := denseID % hdr.groupSize
				eventData, err := extractEventFromGroup(decompressed, posInGroup, eventsInGroup)
				if err != nil {
					continue
				}
				result[denseID] = eventData
			}
		}

		return result, timing, nil
	}

	// Load dictionary if dict-compressed (once for the whole batch)
	var dictDecoder *DDict
	if hdr.dictCompressed {
		dictPath := filepath.Join(dirPath, EventsDictFileName)
		dictBytes, err := os.ReadFile(dictPath)
		if err != nil {
			return nil, timing, fmt.Errorf("failed to read events.dict: %w", err)
		}
		dictDecoder, err = newDDict(dictBytes)
		if err != nil {
			return nil, timing, fmt.Errorf("failed to create DDict: %w", err)
		}
		defer dictDecoder.Close()
	}

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
		diskStart := time.Now()
		n, err := eventsFile.ReadAt(data, int64(startOff))
		timing.DiskReadTime += time.Since(diskStart)
		if err != nil || uint32(n) != eventLen {
			continue // skip on error
		}

		if hdr.dictCompressed {
			decompStart := time.Now()
			decompressed, err := dictDecoder.Decompress(data)
			timing.DecompressTime += time.Since(decompStart)
			if err != nil {
				continue // skip on decompression error
			}
			data = decompressed
		} else if hdr.compressed {
			decompStart := time.Now()
			decompressed, err := zstdDecoder.DecodeAll(data, nil)
			timing.DecompressTime += time.Since(decompStart)
			if err != nil {
				continue // skip on decompression error
			}
			data = decompressed
		}

		result[denseID] = data
	}

	return result, timing, nil
}

// EventVolumeExists checks if event volume files exist for a given chunk.
func EventVolumeExists(basePath string, chunkID uint32) bool {
	dirName := fmt.Sprintf("%06d", chunkID)
	offsetsPath := filepath.Join(basePath, dirName, EventOffsetsFileName)
	_, err := os.Stat(offsetsPath)
	return err == nil
}
