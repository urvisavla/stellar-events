package store

// #include <zstd.h>
// #include <zdict.h>
// #include <stdlib.h>
// #include <string.h>
import "C"
import (
	"fmt"
	"unsafe"
)

// trainDictCgo trains a zstd dictionary from samples using the C ZDICT API.
// Returns a properly formatted zstd dictionary with FSE/Huffman tables.
func trainDictCgo(samples [][]byte, maxDictSize int) ([]byte, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples provided")
	}

	// Concatenate all samples into one contiguous buffer
	totalSize := 0
	for _, s := range samples {
		totalSize += len(s)
	}

	samplesBuf := C.malloc(C.size_t(totalSize))
	if samplesBuf == nil {
		return nil, fmt.Errorf("failed to allocate samples buffer (%d bytes)", totalSize)
	}
	defer C.free(samplesBuf)

	// Build sample sizes array and copy data
	sampleSizes := make([]C.size_t, len(samples))
	offset := 0
	for i, s := range samples {
		sampleSizes[i] = C.size_t(len(s))
		if len(s) > 0 {
			C.memcpy(unsafe.Pointer(uintptr(samplesBuf)+uintptr(offset)), unsafe.Pointer(&s[0]), C.size_t(len(s)))
		}
		offset += len(s)
	}

	// Allocate output dictionary buffer
	dictBuf := C.malloc(C.size_t(maxDictSize))
	if dictBuf == nil {
		return nil, fmt.Errorf("failed to allocate dict buffer (%d bytes)", maxDictSize)
	}
	defer C.free(dictBuf)

	// Train the dictionary
	dictSize := C.ZDICT_trainFromBuffer(
		dictBuf, C.size_t(maxDictSize),
		samplesBuf, &sampleSizes[0], C.uint(len(samples)),
	)

	if C.ZDICT_isError(dictSize) != 0 {
		return nil, fmt.Errorf("ZDICT_trainFromBuffer failed: %s", C.GoString(C.ZDICT_getErrorName(dictSize)))
	}

	// Copy result to Go slice
	result := make([]byte, int(dictSize))
	copy(result, unsafe.Slice((*byte)(dictBuf), int(dictSize)))
	return result, nil
}

// CDict wraps a precomputed zstd compression dictionary for fast repeated compression.
type CDict struct {
	cdict *C.ZSTD_CDict
	cctx  *C.ZSTD_CCtx
}

// newCDict creates a CDict from dictionary data at the given compression level.
func newCDict(dictData []byte, level int) (*CDict, error) {
	if len(dictData) == 0 {
		return nil, fmt.Errorf("empty dictionary data")
	}

	cdict := C.ZSTD_createCDict(
		unsafe.Pointer(&dictData[0]), C.size_t(len(dictData)),
		C.int(level),
	)
	if cdict == nil {
		return nil, fmt.Errorf("ZSTD_createCDict failed")
	}

	cctx := C.ZSTD_createCCtx()
	if cctx == nil {
		C.ZSTD_freeCDict(cdict)
		return nil, fmt.Errorf("ZSTD_createCCtx failed")
	}

	return &CDict{cdict: cdict, cctx: cctx}, nil
}

// Compress compresses src using the precomputed dictionary.
func (d *CDict) Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("empty input")
	}

	// Allocate output buffer: ZSTD_compressBound gives worst-case size
	bound := C.ZSTD_compressBound(C.size_t(len(src)))
	dst := make([]byte, int(bound))

	result := C.ZSTD_compress_usingCDict(
		d.cctx,
		unsafe.Pointer(&dst[0]), bound,
		unsafe.Pointer(&src[0]), C.size_t(len(src)),
		d.cdict,
	)

	if C.ZSTD_isError(result) != 0 {
		return nil, fmt.Errorf("ZSTD_compress_usingCDict failed: %s", C.GoString(C.ZSTD_getErrorName(result)))
	}

	return dst[:int(result)], nil
}

// Close frees the C resources.
func (d *CDict) Close() {
	if d.cctx != nil {
		C.ZSTD_freeCCtx(d.cctx)
		d.cctx = nil
	}
	if d.cdict != nil {
		C.ZSTD_freeCDict(d.cdict)
		d.cdict = nil
	}
}

// DDict wraps a precomputed zstd decompression dictionary.
type DDict struct {
	ddict *C.ZSTD_DDict
	dctx  *C.ZSTD_DCtx
}

// newDDict creates a DDict from dictionary data.
func newDDict(dictData []byte) (*DDict, error) {
	if len(dictData) == 0 {
		return nil, fmt.Errorf("empty dictionary data")
	}

	ddict := C.ZSTD_createDDict(
		unsafe.Pointer(&dictData[0]), C.size_t(len(dictData)),
	)
	if ddict == nil {
		return nil, fmt.Errorf("ZSTD_createDDict failed")
	}

	dctx := C.ZSTD_createDCtx()
	if dctx == nil {
		C.ZSTD_freeDDict(ddict)
		return nil, fmt.Errorf("ZSTD_createDCtx failed")
	}

	return &DDict{ddict: ddict, dctx: dctx}, nil
}

// Decompress decompresses src using the precomputed dictionary.
// Uses ZSTD_getFrameContentSize to determine the output size.
func (d *DDict) Decompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("empty input")
	}

	// Get decompressed size from frame header
	frameSize := C.ZSTD_getFrameContentSize(unsafe.Pointer(&src[0]), C.size_t(len(src)))
	if frameSize == C.ZSTD_CONTENTSIZE_UNKNOWN || frameSize == C.ZSTD_CONTENTSIZE_ERROR {
		return nil, fmt.Errorf("cannot determine decompressed size")
	}

	dst := make([]byte, int(frameSize))
	if frameSize == 0 {
		return dst, nil
	}

	result := C.ZSTD_decompress_usingDDict(
		d.dctx,
		unsafe.Pointer(&dst[0]), C.size_t(frameSize),
		unsafe.Pointer(&src[0]), C.size_t(len(src)),
		d.ddict,
	)

	if C.ZSTD_isError(result) != 0 {
		return nil, fmt.Errorf("ZSTD_decompress_usingDDict failed: %s", C.GoString(C.ZSTD_getErrorName(result)))
	}

	return dst[:int(result)], nil
}

// Close frees the C resources.
func (d *DDict) Close() {
	if d.dctx != nil {
		C.ZSTD_freeDCtx(d.dctx)
		d.dctx = nil
	}
	if d.ddict != nil {
		C.ZSTD_freeDDict(d.ddict)
		d.ddict = nil
	}
}
