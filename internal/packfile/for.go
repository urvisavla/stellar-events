package packfile

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// EncodeGroup FOR-encodes values into one group: [1B W][4B min LE][packed residuals].
// W = bits.Len32(max - min), clamped to min 1. Pure codec — no CRC, no trailer.
// Panics if len(values) == 0.
func EncodeGroup(values []uint32) []byte {
	minVal := values[0]
	maxVal := values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	width := uint8(bits.Len32(maxVal - minVal))
	if width == 0 {
		width = 1
	}

	packSize := (int(width)*len(values) + 7) / 8
	buf := make([]byte, 5+packSize+7) // header + packed + 7 overshoot for safe writes

	buf[0] = width
	binary.LittleEndian.PutUint32(buf[1:], minVal)

	for j, v := range values {
		residual := uint64(v - minVal)
		bitPos := uint64(j) * uint64(width)
		bytePos := 5 + bitPos/8
		shift := bitPos % 8
		existing := binary.LittleEndian.Uint64(buf[bytePos:])
		binary.LittleEndian.PutUint64(buf[bytePos:], existing|(residual<<shift))
	}

	return buf[:5+packSize]
}

// DecodeGroup FOR-decodes one group of n values from data into dst.
// Returns values (possibly reallocated if dst is too small) and bytes consumed.
// No CRC verification. Panics if n <= 0. data must have 7 bytes of overshoot
// past the encoded payload for safe 8-byte reads.
func DecodeGroup(data []byte, n int, dst []uint32) (values []uint32, size int) {
	if cap(dst) < n {
		dst = make([]uint32, n)
	} else {
		dst = dst[:n]
	}
	return decodeGroupCore(data, n, dst)
}

func decodeGroupCore(data []byte, n int, values []uint32) ([]uint32, int) {
	w := uint64(data[0])
	if w > 32 {
		panic(fmt.Sprintf("packfile: invalid FOR width %d (max 32)", w))
	}
	groupMin := binary.LittleEndian.Uint32(data[1:])

	packSize := (int(w)*n + 7) / 8
	size := 5 + packSize

	if w == 0 {
		for i := range values {
			values[i] = groupMin
		}
		return values, size
	}

	mask := uint64((1 << w) - 1)
	for j := range n {
		bitPos := uint64(j) * w
		bytePos := 5 + bitPos/8
		shift := bitPos % 8
		raw := binary.LittleEndian.Uint64(data[bytePos:])
		residual := uint32((raw >> shift) & mask)
		values[j] = groupMin + residual
	}
	return values, size
}
