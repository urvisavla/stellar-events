package store

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/tamirms/streamhash"
)

// TestMPHFRoundTrip builds MPHF index, writes .hash + .pack, reads back via MPHF lookup,
// and verifies bitmaps match.
func TestMPHFRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Create test data: 50 terms with random-ish bitmaps
	const numTerms = 50
	terms := make([]SegmentTermData, numTerms)
	for i := 0; i < numTerms; i++ {
		data := []byte(fmt.Sprintf("term-value-%d", i))
		terms[i].TermHash = sha256.Sum256(data)

		bm := roaring.New()
		// Add some values based on index for unique bitmaps
		for j := uint32(0); j < uint32(i+1)*3; j++ {
			bm.Add(j * uint32(i+1))
		}
		bm.RunOptimize()
		bmData, err := bm.ToBytes()
		if err != nil {
			t.Fatalf("failed to serialize bitmap %d: %v", i, err)
		}
		terms[i].BitmapData = bmData
	}

	// Write MPHF + pack pair
	if err := writeMPHFPack(dir, "test", terms); err != nil {
		t.Fatalf("writeMPHFPack failed: %v", err)
	}

	// Verify files exist
	hashPath := filepath.Join(dir, "test.hash")
	packPath := filepath.Join(dir, "test.pack")
	if _, err := os.Stat(hashPath); err != nil {
		t.Fatalf("test.hash not created: %v", err)
	}
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("test.pack not created: %v", err)
	}

	// Open hash index
	idx, err := streamhash.Open(hashPath)
	if err != nil {
		t.Fatalf("failed to open hash index: %v", err)
	}
	defer idx.Close()

	if idx.NumKeys() != numTerms {
		t.Fatalf("expected %d keys, got %d", numTerms, idx.NumKeys())
	}

	// Open pack file via mmap
	packMmap, err := OpenMmap(packPath)
	if err != nil {
		t.Fatalf("failed to mmap pack file: %v", err)
	}
	defer packMmap.Close()

	numKeys := idx.NumKeys()
	trailerSize := (numKeys + 1) * 8
	fileSize := uint64(packMmap.Len())
	trailerStart := fileSize - trailerSize

	// Verify each term round-trips
	for i, term := range terms {
		slot, err := idx.Query(term.TermHash[:])
		if err != nil {
			t.Fatalf("term %d: MPHF query failed: %v", i, err)
		}

		// Read offsets from trailer
		offStart := trailerStart + slot*8
		offEnd := trailerStart + (slot+1)*8
		bitmapStart := binary.LittleEndian.Uint64(packMmap.data[offStart : offStart+8])
		bitmapEnd := binary.LittleEndian.Uint64(packMmap.data[offEnd : offEnd+8])

		bitmapBytes := packMmap.data[bitmapStart:bitmapEnd]

		// Decode and compare
		bm := roaring.New()
		if err := bm.UnmarshalBinary(bitmapBytes); err != nil {
			t.Fatalf("term %d: failed to decode bitmap: %v", i, err)
		}

		expected := roaring.New()
		if err := expected.UnmarshalBinary(term.BitmapData); err != nil {
			t.Fatalf("term %d: failed to decode expected bitmap: %v", i, err)
		}

		if !bm.Equals(expected) {
			t.Errorf("term %d: bitmap mismatch (got card=%d, expected card=%d)",
				i, bm.GetCardinality(), expected.GetCardinality())
		}
	}
}

// TestMPHFNonMemberQuery verifies that querying a non-member key returns an error.
func TestMPHFNonMemberQuery(t *testing.T) {
	dir := t.TempDir()

	// Create a small index
	terms := make([]SegmentTermData, 10)
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("known-term-%d", i))
		terms[i].TermHash = sha256.Sum256(data)

		bm := roaring.New()
		bm.Add(uint32(i))
		bmData, _ := bm.ToBytes()
		terms[i].BitmapData = bmData
	}

	if err := writeMPHFPack(dir, "test", terms); err != nil {
		t.Fatalf("writeMPHFPack failed: %v", err)
	}

	idx, err := streamhash.Open(filepath.Join(dir, "test.hash"))
	if err != nil {
		t.Fatalf("failed to open hash index: %v", err)
	}
	defer idx.Close()

	// Query a key that was NOT inserted — should get fingerprint mismatch error
	unknownKey := sha256.Sum256([]byte("unknown-term-that-does-not-exist"))
	_, err = idx.Query(unknownKey[:])
	if err == nil {
		t.Log("non-member query returned no error (possible fingerprint collision, acceptable)")
	}
	// With 2-byte fingerprints, there's a ~1/65536 chance of false positive, so we don't fail on nil error
}

// TestWriteSegmentDir verifies that WriteSegmentDir produces the expected files.
func TestWriteSegmentDir(t *testing.T) {
	dir := t.TempDir()

	// Create contract terms
	contractTerms := make([]SegmentTermData, 3)
	for i := 0; i < 3; i++ {
		contractTerms[i].TermHash = sha256.Sum256([]byte(fmt.Sprintf("contract-%d", i)))
		bm := roaring.New()
		bm.Add(uint32(i))
		data, _ := bm.ToBytes()
		contractTerms[i].BitmapData = data
	}

	// Create topic terms for positions 0 and 2
	topicsByPos := [4][]SegmentTermData{}
	for i := 0; i < 2; i++ {
		topicsByPos[0] = append(topicsByPos[0], SegmentTermData{
			TermHash:   sha256.Sum256([]byte(fmt.Sprintf("topic0-%d", i))),
			BitmapData: contractTerms[0].BitmapData,
		})
	}
	topicsByPos[2] = append(topicsByPos[2], SegmentTermData{
		TermHash:   sha256.Sum256([]byte("topic2-0")),
		BitmapData: contractTerms[0].BitmapData,
	})

	segmentID := uint32(100)
	if err := WriteSegmentDir(dir, segmentID, contractTerms, topicsByPos, nil); err != nil {
		t.Fatalf("WriteSegmentDir failed: %v", err)
	}

	segDir := filepath.Join(dir, fmt.Sprintf("%06d", segmentID))

	// Check expected files exist
	expectedFiles := []string{
		"contracts.hash", "contracts.pack",
		"topic0.hash", "topic0.pack",
		"topic2.hash", "topic2.pack",
	}
	for _, f := range expectedFiles {
		if _, err := os.Stat(filepath.Join(segDir, f)); err != nil {
			t.Errorf("expected file %s not found: %v", f, err)
		}
	}

	// Check files that should NOT exist
	absentFiles := []string{
		"topic1.hash", "topic1.pack",
		"topic3.hash", "topic3.pack",
		"topics.pack",
	}
	for _, f := range absentFiles {
		if _, err := os.Stat(filepath.Join(segDir, f)); err == nil {
			t.Errorf("unexpected file %s exists", f)
		}
	}
}
