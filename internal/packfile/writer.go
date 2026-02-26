package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
)

// Writer creates a new packfile. Records must be appended in order.
type Writer struct {
	file       *os.File
	path       string // final path
	tmpPath    string // {path}.tmp.{random}
	pos        int64
	offsets    []int64
	opts       WriterOptions
	err        error // sticky — once set, all subsequent ops fail
	closed     bool  // set by Finish or Abort
	fileClosed bool  // set when file.Close() succeeds, prevents double-close in Abort
}

// Create starts writing a new packfile at path.
// The file is not visible at path until Finish is called.
func Create(path string, opts WriterOptions) (*Writer, error) {
	tmpPath := path + ".tmp." + strconv.FormatInt(rand.Int63(), 10)
	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, err
	}
	return &Writer{
		file:    f,
		path:    path,
		tmpPath: tmpPath,
		opts:    opts,
	}, nil
}

// Append writes a single record. Records are opaque byte slices.
func (w *Writer) Append(record []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("packfile: writer is closed")
	}
	w.offsets = append(w.offsets, w.pos)
	n, err := w.file.Write(record)
	w.pos += int64(n)
	if err != nil {
		w.err = err
	}
	return err
}

// Finish writes the index, metadata, and trailer, fsyncs, and
// atomically renames to the final path. Returns an error if the
// writer has already been finished or aborted. On failure, the caller
// should call Abort to clean up the temp file.
func (w *Writer) Finish() (Trailer, error) {
	if w.err != nil {
		return Trailer{}, w.err
	}
	if w.closed {
		return Trailer{}, errors.New("packfile: writer is closed")
	}
	w.offsets = append(w.offsets, w.pos) // end-of-data offset

	// Encode index using FOR-128.
	var indexBuf bytes.Buffer
	recordCount := len(w.offsets) - 1
	if recordCount > math.MaxUint32 {
		w.err = errors.New("packfile: record count exceeds uint32 max")
		return Trailer{}, w.err
	}

	var deltas []uint32
	for g := 0; g*groupSize < recordCount; g++ {
		base := g * groupSize
		end := min(base+groupSize, recordCount)

		deltas = deltas[:0]
		if cap(deltas) < end-base {
			deltas = make([]uint32, 0, end-base)
		}
		for j := base; j < end; j++ {
			d := w.offsets[j+1] - w.offsets[j]
			if d > math.MaxUint32 {
				w.err = errors.New("packfile: record size exceeds 4GB")
				return Trailer{}, w.err
			}
			deltas = append(deltas, uint32(d))
		}

		indexBuf.Write(EncodeGroup(deltas))
	}

	// CRC32C over raw index bytes.
	binary.Write(&indexBuf, binary.LittleEndian, crc32c(indexBuf.Bytes()))

	indexSize := uint32(indexBuf.Len())

	// Write index section.
	if _, err := w.file.Write(indexBuf.Bytes()); err != nil {
		w.err = err
		return Trailer{}, err
	}

	// Write metadata.
	metadataSize := uint32(len(w.opts.Metadata))
	if metadataSize > 0 {
		if _, err := w.file.Write(w.opts.Metadata); err != nil {
			w.err = err
			return Trailer{}, err
		}
	}

	// Build and write trailer (32 bytes).
	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:], magic)
	trailer[4] = version
	trailer[5] = 0 // reserved
	binary.LittleEndian.PutUint32(trailer[6:], uint32(recordCount))
	binary.LittleEndian.PutUint32(trailer[10:], indexSize)
	binary.LittleEndian.PutUint32(trailer[14:], metadataSize)
	binary.LittleEndian.PutUint32(trailer[18:], crc32c(trailer[0:18]))
	// trailer[22:32] reserved, already zero

	if _, err := w.file.Write(trailer[:]); err != nil {
		w.err = err
		return Trailer{}, err
	}

	// Fsync and atomic rename.
	if err := w.file.Sync(); err != nil {
		w.err = err
		return Trailer{}, err
	}
	if err := w.file.Close(); err != nil {
		w.err = err
		return Trailer{}, err
	}
	w.fileClosed = true
	if err := os.Rename(w.tmpPath, w.path); err != nil {
		w.err = err
		return Trailer{}, err
	}

	// Fsync parent directory to ensure the rename is durable.
	if dir, err := os.Open(filepath.Dir(w.path)); err == nil {
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			w.err = err
			return Trailer{}, err
		}
	}

	w.closed = true
	return Trailer{
		Version:         version,
		RecordCount:     uint32(recordCount),
		IndexSize:       indexSize,
		MetadataSize:    metadataSize,
		TrailerChecksum: crc32c(trailer[0:18]),
	}, nil
}

// SetMetadata sets the opaque metadata to be written at Finish time.
// Must be called before Finish.
func (w *Writer) SetMetadata(meta []byte) {
	w.opts.Metadata = meta
}

// Abort discards the in-progress packfile and removes the temp file.
// Safe to call after a failed Finish to clean up.
// No-op only after a successful Finish or a previous Abort.
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var closeErr error
	if !w.fileClosed {
		closeErr = w.file.Close()
	}
	removeErr := os.Remove(w.tmpPath)
	return errors.Join(closeErr, removeErr)
}
