package packfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// ReadAtCloser is the minimal interface needed by Reader to access packfile data.
// *os.File satisfies this interface.
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

const (
	magic       = 0x534C4348 // "SLCH"
	version     = 1
	groupSize   = 128
	trailerSize = 32
)

// Trailer holds the parsed trailer fields.
type Trailer struct {
	Version         uint8
	RecordCount     uint32
	IndexSize       uint32
	MetadataSize    uint32
	TrailerChecksum uint32
}

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
	Metadata []byte // opaque, caller-defined, stored in the file
}

// Errors
var (
	ErrCorrupt    = errors.New("packfile: corrupt file")
	ErrMagic      = fmt.Errorf("%w: invalid magic number", ErrCorrupt)
	ErrVersion    = fmt.Errorf("%w: unsupported version", ErrCorrupt)
	ErrChecksum   = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	ErrSize       = fmt.Errorf("%w: file size inconsistent with trailer", ErrCorrupt)
	ErrIndexRange = errors.New("packfile: record index out of range")
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crc32cTable) }
