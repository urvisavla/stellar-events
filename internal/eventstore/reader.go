package eventstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urvisavla/stellar-events/internal/packfile"
	"github.com/urvisavla/stellar-events/internal/zstd"
)

// ReadStats accumulates I/O and decompression timing from read operations.
// Pass a non-nil pointer to ReadEvents or ReadIndices to collect metrics.
type ReadStats struct {
	DiskReadTime   time.Duration // Time spent reading blocks from disk
	DecompressTime time.Duration // Time spent on zstd decompression
	BlocksRead     int          // Number of compressed blocks read
}

var ErrIndexRange = fmt.Errorf("eventstore: index out of range")

// blockBuf holds reusable buffers and a dedicated zstd decompressor for ReadEvent.
// Pooled via sync.Pool to avoid per-call allocations and shared decoder contention.
type blockBuf struct {
	compressed   []byte
	decompressed []byte
	sizes        []uint32
	offsets      []int // prefix sum of sizes: offsets[i] = byte offset of event i
	padded       []byte
	decompressor *zstd.Decompressor
}

var blockBufPool = sync.Pool{
	New: func() any {
		bb := &blockBuf{decompressor: zstd.NewDecompressor()}
		runtime.SetFinalizer(bb, func(b *blockBuf) {
			b.decompressor.Close()
		})
		return bb
	},
}

// decode decompresses a block (unless noCompress) and parses its trailing FOR
// index, populating sizes, offsets, and decompressed data.
func (bb *blockBuf) decode(data []byte, n int, noCompress bool) error {
	if noCompress {
		bb.decompressed = append(bb.decompressed[:0], data...)
	} else {
		decoded, err := bb.decompressor.Decode(bb.decompressed[:0], data)
		if err != nil {
			return err
		}
		bb.decompressed = decoded
	}
	var err error
	bb.sizes, bb.padded, err = decodeBlock(bb.decompressed, n, bb.sizes, bb.padded)
	if err != nil {
		return err
	}
	// Build prefix sum of sizes for offset lookups.
	if cap(bb.offsets) <= n {
		bb.offsets = make([]int, n+1)
	} else {
		bb.offsets = bb.offsets[:n+1]
	}
	bb.offsets[0] = 0
	for i, s := range bb.sizes {
		bb.offsets[i+1] = bb.offsets[i] + int(s)
	}
	return nil
}

// event returns a copy of the event at localIdx within the decoded block.
func (bb *blockBuf) event(localIdx int) []byte {
	offset := bb.offsets[localIdx]
	ev := make([]byte, bb.offsets[localIdx+1]-offset)
	copy(ev, bb.decompressed[offset:])
	return ev
}

type Reader struct {
	datFile     *os.File // events.dat, kept open for ReadAt
	blockOffsets []int64 // blockCount+1 offsets from events.idx
	concurrency int

	nEvents    int
	blockN     int
	blockCount int
	noCompress bool
	dir        string
	openErr    error
	once       sync.Once
}

type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for ReadIndices.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

// Open opens an eventstore for reading from the given directory.
// The directory should contain events.dat and events.idx.
// Returns immediately; actual I/O is deferred to the first method call.
// Close must always be called.
func Open(dir string, opts ...ReaderOption) *Reader {
	r := &Reader{
		dir:         dir,
		concurrency: 8,
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.concurrency < 1 {
		r.concurrency = 1
	}
	return r
}

func (r *Reader) waitOpen() error {
	r.once.Do(func() {
		idxPath := filepath.Join(r.dir, "events.idx")
		idxData, err := os.ReadFile(idxPath)
		if err != nil {
			r.openErr = fmt.Errorf("eventstore: read index: %w", err)
			return
		}
		if len(idxData) < 24 {
			r.openErr = fmt.Errorf("eventstore: index file too small (%d bytes)", len(idxData))
			return
		}

		// Parse header
		magic := binary.LittleEndian.Uint32(idxData[0:])
		if magic != idxMagic {
			r.openErr = fmt.Errorf("eventstore: bad index magic 0x%08X (want 0x%08X)", magic, idxMagic)
			return
		}
		r.nEvents = int(binary.LittleEndian.Uint32(idxData[4:]))
		r.blockN = int(binary.LittleEndian.Uint32(idxData[8:]))
		flags := binary.LittleEndian.Uint32(idxData[12:])
		r.noCompress = flags&1 != 0
		r.blockCount = int(binary.LittleEndian.Uint32(idxData[16:]))

		if r.blockN <= 0 {
			r.openErr = fmt.Errorf("eventstore: invalid blockN %d in index", r.blockN)
			return
		}

		// Validate block count
		expectedBlocks := (r.nEvents + r.blockN - 1) / r.blockN
		if r.nEvents == 0 {
			expectedBlocks = 0
		}
		if expectedBlocks != r.blockCount {
			r.openErr = fmt.Errorf("eventstore: index says %d blocks but %d events / %d blockN = %d blocks",
				r.blockCount, r.nEvents, r.blockN, expectedBlocks)
			return
		}

		// Read offsets: (blockCount+1) int64 values
		offsetBytes := (r.blockCount + 1) * 8
		expectedSize := 24 + offsetBytes
		if len(idxData) < expectedSize {
			r.openErr = fmt.Errorf("eventstore: index file truncated: got %d bytes, want %d", len(idxData), expectedSize)
			return
		}
		r.blockOffsets = make([]int64, r.blockCount+1)
		for i := range r.blockOffsets {
			r.blockOffsets[i] = int64(binary.LittleEndian.Uint64(idxData[24+i*8:]))
		}

		// Open data file
		datPath := filepath.Join(r.dir, "events.dat")
		r.datFile, err = os.Open(datPath)
		if err != nil {
			r.openErr = fmt.Errorf("eventstore: open data file: %w", err)
			return
		}
	})
	return r.openErr
}

// EventCount returns the total number of events.
func (r *Reader) EventCount() (int, error) {
	if err := r.waitOpen(); err != nil {
		return 0, err
	}
	return r.nEvents, nil
}

// Close closes the underlying data file.
func (r *Reader) Close() error {
	if r.datFile != nil {
		return r.datFile.Close()
	}
	return nil
}

func (r *Reader) eventsInBlock(blockIdx int) int {
	if r.nEvents == 0 {
		return 0
	}
	last := r.blockCount - 1
	if blockIdx < last {
		return r.blockN
	}
	rem := r.nEvents % r.blockN
	if rem == 0 {
		return r.blockN
	}
	return rem
}

// readBlock reads the compressed block at blockIdx into buf, returning the
// data slice (possibly reallocated if buf is too small).
func (r *Reader) readBlock(blockIdx int, buf []byte) ([]byte, error) {
	start := r.blockOffsets[blockIdx]
	end := r.blockOffsets[blockIdx+1]
	size := int(end - start)

	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	_, err := r.datFile.ReadAt(buf, start)
	if err != nil {
		return nil, fmt.Errorf("eventstore: read block %d: %w", blockIdx, err)
	}
	return buf, nil
}

// decodeBlock parses the trailing FOR index from a decompressed block.
// Returns event sizes and the reusable padded scratch buffer.
// Callers must reassign both sizes and padded from the return values
// to benefit from buffer reuse.
func decodeBlock(raw []byte, n int, sizes []uint32, padded []byte) ([]uint32, []byte, error) {
	if len(raw) < 6 { // minimum: 1 event byte + 4B min + 1B W
		return sizes, padded, fmt.Errorf("eventstore: block too small (%d bytes)", len(raw))
	}

	w := raw[len(raw)-1] // W is the last byte
	if w > 32 {
		return sizes, padded, fmt.Errorf("eventstore: invalid FOR width %d in block (max 32)", w)
	}
	packSize := (int(w)*n + 7) / 8
	indexSize := 4 + packSize + 1 // min(4) + packed + W(1)

	if indexSize > len(raw) {
		return sizes, padded, fmt.Errorf("eventstore: index size %d exceeds block size %d", indexSize, len(raw))
	}

	indexStart := len(raw) - indexSize

	// Reconstruct standard FOR layout: [W][min][packed] with 7-byte overshoot
	paddedSize := 1 + 4 + packSize + 7
	if cap(padded) < paddedSize {
		padded = make([]byte, paddedSize)
	} else {
		padded = padded[:paddedSize]
		clear(padded)
	}
	padded[0] = w
	copy(padded[1:], raw[indexStart:len(raw)-1]) // min + packed

	sizes, _ = packfile.DecodeGroup(padded, n, sizes)
	dataEnd := indexStart

	// Validate sum(sizes) == dataEnd
	sum := 0
	for _, s := range sizes {
		sum += int(s)
	}
	if sum != dataEnd {
		return sizes, padded, fmt.Errorf("eventstore: size sum %d != data end %d", sum, dataEnd)
	}

	return sizes, padded, nil
}

// ReadEvent reads a single event by global index.
// The caller owns the returned slice.
func (r *Reader) ReadEvent(index int) ([]byte, error) {
	if err := r.waitOpen(); err != nil {
		return nil, err
	}

	if index < 0 || index >= r.nEvents {
		return nil, ErrIndexRange
	}

	blockIdx := index / r.blockN
	localIdx := index % r.blockN

	bb := blockBufPool.Get().(*blockBuf)
	defer blockBufPool.Put(bb)

	var err error
	bb.compressed, err = r.readBlock(blockIdx, bb.compressed)
	if err != nil {
		return nil, err
	}
	if err := bb.decode(bb.compressed, r.eventsInBlock(blockIdx), r.noCompress); err != nil {
		return nil, err
	}
	return bb.event(localIdx), nil
}

// ReadEvents returns an iterator over count contiguous events starting at start.
// Each yielded []byte is valid only until the next iteration.
// If stats is non-nil, it accumulates disk read and decompression timing.
func (r *Reader) ReadEvents(start, count int, stats *ReadStats) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}

		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}

		if start < 0 || count < 0 || start > r.nEvents || count > r.nEvents-start {
			panic(fmt.Sprintf("eventstore: ReadEvents(%d, %d) out of range [0, %d)",
				start, count, r.nEvents))
		}

		firstBlock := start / r.blockN
		lastBlock := (start + count - 1) / r.blockN

		bb := blockBufPool.Get().(*blockBuf)
		defer blockBufPool.Put(bb)

		globalIdx := start

		for blockIdx := firstBlock; blockIdx <= lastBlock; blockIdx++ {
			diskStart := time.Now()
			var err error
			bb.compressed, err = r.readBlock(blockIdx, bb.compressed)
			if stats != nil {
				stats.DiskReadTime += time.Since(diskStart)
			}
			if err != nil {
				yield(nil, err)
				return
			}

			n := r.eventsInBlock(blockIdx)
			decompStart := time.Now()
			if err := bb.decode(bb.compressed, n, r.noCompress); err != nil {
				yield(nil, err)
				return
			}
			if stats != nil {
				stats.DecompressTime += time.Since(decompStart)
				stats.BlocksRead++
			}

			// Compute start/end within this block
			blockStart := blockIdx * r.blockN
			localStart := 0
			if globalIdx > blockStart {
				localStart = globalIdx - blockStart
			}
			localEnd := n
			remaining := start + count - blockStart
			if remaining < localEnd {
				localEnd = remaining
			}

			offset := bb.offsets[localStart]
			for i := localStart; i < localEnd; i++ {
				end := bb.offsets[i+1]
				if !yield(bb.decompressed[offset:end], nil) {
					return
				}
				offset = end
				globalIdx++
			}
		}
	}
}

// indicesWork holds working state for ReadIndices.
type indicesWork struct {
	blocks []blockRange // unique blocks to read
	events [][]byte     // output: one per input index
}

// blockRange maps a block to its slice of requested indices.
// Since indices are sorted, all indices for a given block are contiguous
// in the original indices slice — represented as [inputStart, inputEnd).
type blockRange struct {
	blockIdx   int
	inputStart int // start index into indices[]
	inputEnd   int // end index into indices[]
}

func (w *indicesWork) reset(n int) {
	w.blocks = w.blocks[:0]
	if cap(w.events) < n {
		w.events = make([][]byte, n)
	} else {
		w.events = w.events[:n]
		clear(w.events)
	}
}

// ReadIndices reads events at scattered indices with parallel I/O.
// indices must be sorted in ascending order with no duplicates. Panics if any
// index is out of [0, EventCount) or if indices are not sorted/unique.
// Each yielded []byte is owned by the caller (copied out of pooled buffers).
// If stats is non-nil, it accumulates disk read and decompression timing.
func (r *Reader) ReadIndices(ctx context.Context, indices []int, stats *ReadStats) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if len(indices) == 0 {
			return
		}

		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}

		// Validate bounds and sorted+unique invariant
		for i, idx := range indices {
			if idx < 0 || idx >= r.nEvents {
				panic(fmt.Sprintf("eventstore: ReadIndices index %d out of range [0, %d)",
					idx, r.nEvents))
			}
			if i > 0 && indices[i] <= indices[i-1] {
				panic(fmt.Sprintf("eventstore: ReadIndices indices not sorted/unique at position %d: %d <= %d",
					i, indices[i], indices[i-1]))
			}
		}

		w := &indicesWork{}
		w.reset(len(indices))

		// Phase 1: group indices by block.
		prevBlockIdx := -1
		for i, idx := range indices {
			blkIdx := idx / r.blockN
			if blkIdx != prevBlockIdx {
				if len(w.blocks) > 0 {
					w.blocks[len(w.blocks)-1].inputEnd = i
				}
				w.blocks = append(w.blocks, blockRange{blockIdx: blkIdx, inputStart: i})
				prevBlockIdx = blkIdx
			}
		}
		if len(w.blocks) > 0 {
			w.blocks[len(w.blocks)-1].inputEnd = len(indices)
		}

		// Phase 2: fixed worker pool, each worker steals blocks via atomic counter.
		var errOnce sync.Once
		var firstErr error

		numBlocks := len(w.blocks)
		numWorkers := min(numBlocks, r.concurrency)

		// Atomic accumulators for parallel stats (nanoseconds).
		var diskNs, decompNs atomic.Int64
		var blocksRead atomic.Int64

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		var nextBlock atomic.Int64
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for range numWorkers {
			go func() {
				defer wg.Done()
				bb := blockBufPool.Get().(*blockBuf)
				defer func() {
					if rv := recover(); rv != nil {
						// Don't return bb to pool — CGO decompressor may be in invalid state.
						errOnce.Do(func() { firstErr = fmt.Errorf("eventstore: panic in worker: %v", rv) })
						cancel()
						return
					}
					blockBufPool.Put(bb)
				}()

				for {
					bi := int(nextBlock.Add(1)) - 1
					if bi >= numBlocks || ctx.Err() != nil {
						return
					}
					if err := r.processBlock(w, bi, indices, bb, &diskNs, &decompNs, &blocksRead); err != nil {
						errOnce.Do(func() { firstErr = err })
						cancel()
						return
					}
				}
			}()
		}
		wg.Wait()

		if stats != nil {
			stats.DiskReadTime += time.Duration(diskNs.Load())
			stats.DecompressTime += time.Duration(decompNs.Load())
			stats.BlocksRead += int(blocksRead.Load())
		}

		// Phase 3: yield results in index order.
		if firstErr != nil {
			yield(nil, firstErr)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}
		for _, ev := range w.events {
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// processBlock reads and decodes a single block, populating w.events for
// the requested indices within that block.
func (r *Reader) processBlock(w *indicesWork, bi int, indices []int, bb *blockBuf, diskNs, decompNs, blocksRead *atomic.Int64) error {
	blk := w.blocks[bi]

	diskStart := time.Now()
	var err error
	bb.compressed, err = r.readBlock(blk.blockIdx, bb.compressed)
	diskNs.Add(int64(time.Since(diskStart)))
	if err != nil {
		return err
	}

	decompStart := time.Now()
	if err := bb.decode(bb.compressed, r.eventsInBlock(blk.blockIdx), r.noCompress); err != nil {
		return err
	}
	decompNs.Add(int64(time.Since(decompStart)))
	blocksRead.Add(1)

	for j := blk.inputStart; j < blk.inputEnd; j++ {
		w.events[j] = bb.event(indices[j] % r.blockN)
	}
	return nil
}
