package store

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestWriteAndReadIdxPack_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Create some test terms with bitmap data
	terms := make([]BucketTermData, 5)
	for i := 0; i < 5; i++ {
		bm := roaring.New()
		for j := uint32(0); j < uint32(i+1)*10; j++ {
			bm.Add(j)
		}
		data, err := bm.ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		var hash [16]byte
		hash[0] = byte(i + 1) // ensure distinct, non-zero hashes
		hash[15] = byte(i)
		terms[i] = BucketTermData{
			TermHash:   hash,
			BitmapData: data,
		}
	}

	// Write idx+pack pair
	err := writeIdxPackPair(dir, "test", terms)
	if err != nil {
		t.Fatal(err)
	}

	// Open and validate .idx
	idxFile, err := OpenIndexFile(filepath.Join(dir, "test.idx"))
	if err != nil {
		t.Fatal(err)
	}

	if idxFile.EntryCount() != 5 {
		t.Fatalf("expected 5 entries, got %d", idxFile.EntryCount())
	}

	// Verify all terms can be looked up and round-trip correctly
	packPath := filepath.Join(dir, "test.pack")
	for _, term := range terms {
		entry := idxFile.LookupTerm(term.TermHash)
		if entry == nil {
			t.Fatalf("term %x not found in index", term.TermHash)
		}

		// Read bitmap from pack
		data, err := ReadBitmapFromPack(packPath, entry.PackOffset, entry.PackLength)
		if err != nil {
			t.Fatal(err)
		}

		// Verify data matches
		if !bytes.Equal(data, term.BitmapData) {
			t.Fatalf("bitmap data mismatch for term %x", term.TermHash)
		}

		// Verify bitmap is valid
		bm := roaring.New()
		_, err = bm.FromBuffer(data)
		if err != nil {
			t.Fatalf("failed to decode bitmap for term %x: %v", term.TermHash, err)
		}
	}
}

func TestIdxFile_BinarySearch(t *testing.T) {
	dir := t.TempDir()

	// Create 100 terms with sequential hashes
	terms := make([]BucketTermData, 100)
	for i := 0; i < 100; i++ {
		bm := roaring.New()
		bm.Add(uint32(i))
		data, err := bm.ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		var hash [16]byte
		binary.BigEndian.PutUint16(hash[0:2], uint16(i))
		terms[i] = BucketTermData{
			TermHash:   hash,
			BitmapData: data,
		}
	}

	err := writeIdxPackPair(dir, "search", terms)
	if err != nil {
		t.Fatal(err)
	}

	idxFile, err := OpenIndexFile(filepath.Join(dir, "search.idx"))
	if err != nil {
		t.Fatal(err)
	}

	// Search for existing terms
	for i := 0; i < 100; i++ {
		var hash [16]byte
		binary.BigEndian.PutUint16(hash[0:2], uint16(i))
		entry := idxFile.LookupTerm(hash)
		if entry == nil {
			t.Fatalf("term %d not found", i)
		}
	}

	// Search for non-existing term
	var missingHash [16]byte
	missingHash[0] = 0xFF
	missingHash[1] = 0xFF
	entry := idxFile.LookupTerm(missingHash)
	if entry != nil {
		t.Fatal("expected nil for missing term")
	}
}

func TestIdxFile_GetEntry(t *testing.T) {
	dir := t.TempDir()

	terms := make([]BucketTermData, 3)
	for i := 0; i < 3; i++ {
		bm := roaring.New()
		bm.Add(uint32(i * 100))
		data, err := bm.ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		var hash [16]byte
		hash[0] = byte(i + 1)
		terms[i] = BucketTermData{
			TermHash:   hash,
			BitmapData: data,
		}
	}

	err := writeIdxPackPair(dir, "entries", terms)
	if err != nil {
		t.Fatal(err)
	}

	idxFile, err := OpenIndexFile(filepath.Join(dir, "entries.idx"))
	if err != nil {
		t.Fatal(err)
	}

	// Valid entries
	for i := 0; i < 3; i++ {
		entry := idxFile.GetEntry(i)
		if entry == nil {
			t.Fatalf("entry %d is nil", i)
		}
	}

	// Out of bounds
	if idxFile.GetEntry(-1) != nil {
		t.Fatal("expected nil for negative index")
	}
	if idxFile.GetEntry(3) != nil {
		t.Fatal("expected nil for out-of-bounds index")
	}
}

func TestIdxFile_CorruptionDetection(t *testing.T) {
	dir := t.TempDir()

	// Test: file too small
	smallPath := filepath.Join(dir, "small.idx")
	os.WriteFile(smallPath, []byte("BIDX"), 0644)
	_, err := OpenIndexFile(smallPath)
	if err == nil {
		t.Fatal("expected error for too-small file")
	}

	// Test: bad magic
	badMagic := make([]byte, IdxHeaderSize)
	copy(badMagic[0:4], "XXXX")
	badMagicPath := filepath.Join(dir, "badmagic.idx")
	os.WriteFile(badMagicPath, badMagic, 0644)
	_, err = OpenIndexFile(badMagicPath)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}

	// Test: bad version
	badVersion := make([]byte, IdxHeaderSize)
	copy(badVersion[0:4], IdxMagic)
	binary.BigEndian.PutUint16(badVersion[4:6], 99) // unsupported version
	badVersionPath := filepath.Join(dir, "badversion.idx")
	os.WriteFile(badVersionPath, badVersion, 0644)
	_, err = OpenIndexFile(badVersionPath)
	if err == nil {
		t.Fatal("expected error for bad version")
	}

	// Test: size mismatch (header says 1 entry but no entry data)
	sizeMismatch := make([]byte, IdxHeaderSize)
	copy(sizeMismatch[0:4], IdxMagic)
	binary.BigEndian.PutUint16(sizeMismatch[4:6], IdxVersion)
	binary.BigEndian.PutUint32(sizeMismatch[6:10], 1) // 1 entry claimed
	sizeMismatchPath := filepath.Join(dir, "sizemismatch.idx")
	os.WriteFile(sizeMismatchPath, sizeMismatch, 0644)
	_, err = OpenIndexFile(sizeMismatchPath)
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}

	// Test: valid empty file
	emptyIdx := make([]byte, IdxHeaderSize)
	copy(emptyIdx[0:4], IdxMagic)
	binary.BigEndian.PutUint16(emptyIdx[4:6], IdxVersion)
	binary.BigEndian.PutUint32(emptyIdx[6:10], 0) // 0 entries
	emptyPath := filepath.Join(dir, "empty.idx")
	os.WriteFile(emptyPath, emptyIdx, 0644)
	f, err := OpenIndexFile(emptyPath)
	if err != nil {
		t.Fatalf("expected success for empty valid file: %v", err)
	}
	if f.EntryCount() != 0 {
		t.Fatalf("expected 0 entries, got %d", f.EntryCount())
	}
}

func TestWriteBucketDir_Full(t *testing.T) {
	dir := t.TempDir()

	// Create contract terms
	contractTerms := []BucketTermData{
		{TermHash: [16]byte{1}, BitmapData: makeBitmapData(t, 0, 10, 20)},
		{TermHash: [16]byte{2}, BitmapData: makeBitmapData(t, 5, 15, 25)},
	}

	// Create topic terms per position
	var topicsByPos [4][]BucketTermData
	topicsByPos[0] = []BucketTermData{
		{TermHash: [16]byte{10}, BitmapData: makeBitmapData(t, 1, 2, 3)},
	}
	topicsByPos[1] = []BucketTermData{
		{TermHash: [16]byte{20}, BitmapData: makeBitmapData(t, 4, 5, 6)},
	}

	// Create ledger map
	lm := &BucketLedgerMap{
		BucketID: 5,
		Data:     make([]byte, BucketLedgerMapSize),
	}

	err := WriteBucketDir(dir, 5, contractTerms, topicsByPos, nil, lm)
	if err != nil {
		t.Fatal(err)
	}

	bucketDir := filepath.Join(dir, "000005")

	// Verify contracts.idx exists and is valid
	idxFile, err := OpenIndexFile(filepath.Join(bucketDir, "contracts.idx"))
	if err != nil {
		t.Fatal(err)
	}
	if idxFile.EntryCount() != 2 {
		t.Fatalf("expected 2 contract entries, got %d", idxFile.EntryCount())
	}

	// Verify topic0.idx exists
	topic0Idx, err := OpenIndexFile(filepath.Join(bucketDir, "topic0.idx"))
	if err != nil {
		t.Fatal(err)
	}
	if topic0Idx.EntryCount() != 1 {
		t.Fatalf("expected 1 topic0 entry, got %d", topic0Idx.EntryCount())
	}

	// Verify topic1.idx exists
	topic1Idx, err := OpenIndexFile(filepath.Join(bucketDir, "topic1.idx"))
	if err != nil {
		t.Fatal(err)
	}
	if topic1Idx.EntryCount() != 1 {
		t.Fatalf("expected 1 topic1 entry, got %d", topic1Idx.EntryCount())
	}

	// Verify topic2.idx and topic3.idx do NOT exist (no data)
	if _, err := os.Stat(filepath.Join(bucketDir, "topic2.idx")); !os.IsNotExist(err) {
		t.Fatal("topic2.idx should not exist")
	}
	if _, err := os.Stat(filepath.Join(bucketDir, "topic3.idx")); !os.IsNotExist(err) {
		t.Fatal("topic3.idx should not exist")
	}

	// Verify ledgermap.dat
	err = ValidateLedgerMapFile(filepath.Join(bucketDir, LedgerMapFileName))
	if err != nil {
		t.Fatal(err)
	}
}

func TestCleanupTmpFiles(t *testing.T) {
	dir := t.TempDir()

	// Create some tmp files and regular files
	os.WriteFile(filepath.Join(dir, "contracts.idx.tmp"), []byte("tmp"), 0644)
	os.WriteFile(filepath.Join(dir, "contracts.pack.tmp"), []byte("tmp"), 0644)
	os.WriteFile(filepath.Join(dir, "contracts.idx"), []byte("real"), 0644)

	err := CleanupTmpFiles(dir)
	if err != nil {
		t.Fatal(err)
	}

	// tmp files should be gone
	if _, err := os.Stat(filepath.Join(dir, "contracts.idx.tmp")); !os.IsNotExist(err) {
		t.Fatal("tmp file should have been cleaned up")
	}
	if _, err := os.Stat(filepath.Join(dir, "contracts.pack.tmp")); !os.IsNotExist(err) {
		t.Fatal("tmp file should have been cleaned up")
	}

	// Regular file should still exist
	if _, err := os.Stat(filepath.Join(dir, "contracts.idx")); err != nil {
		t.Fatal("regular file should still exist")
	}
}

func TestCleanupTmpFiles_NonexistentDir(t *testing.T) {
	err := CleanupTmpFiles("/nonexistent/path")
	if err != nil {
		t.Fatalf("expected nil error for nonexistent dir, got: %v", err)
	}
}

func TestTermKeySize(t *testing.T) {
	// Verify the constants are consistent
	if TermKeySize != 16 {
		t.Fatalf("expected TermKeySize=16, got %d", TermKeySize)
	}
	if IndexKeySize != 20 {
		t.Fatalf("expected IndexKeySize=20, got %d", IndexKeySize)
	}
}

func TestContractTermKey_Truncated(t *testing.T) {
	contractID := []byte("CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC")
	key := ContractTermKey(contractID)

	// Should be 16 bytes
	if len(key) != 16 {
		t.Fatalf("expected 16-byte key, got %d", len(key))
	}

	// Should be deterministic
	key2 := ContractTermKey(contractID)
	if key != key2 {
		t.Fatal("ContractTermKey is not deterministic")
	}

	// Different input should produce different key
	key3 := ContractTermKey([]byte("other"))
	if key == key3 {
		t.Fatal("different inputs produced same key")
	}
}

func TestTopicTermKey_Truncated(t *testing.T) {
	topicXDR := []byte{0x00, 0x01, 0x02, 0x03}

	key0 := TopicTermKey(0, topicXDR)
	key1 := TopicTermKey(1, topicXDR)

	// Should be 16 bytes
	if len(key0) != 16 {
		t.Fatalf("expected 16-byte key, got %d", len(key0))
	}

	// Different positions should produce different keys
	if key0 == key1 {
		t.Fatal("different positions produced same key")
	}
}

func TestEncodeIndexKey_20Bytes(t *testing.T) {
	var termKey [16]byte
	termKey[0] = 0xAB
	termKey[15] = 0xCD

	key := EncodeIndexKey(termKey, 100)

	if len(key) != 20 {
		t.Fatalf("expected 20-byte key, got %d", len(key))
	}

	// Verify term hash portion
	if key[0] != 0xAB || key[15] != 0xCD {
		t.Fatal("term hash not correctly encoded")
	}

	// Verify bucket ID
	bucketID := binary.BigEndian.Uint32(key[16:20])
	expectedBucketID := BucketID(100)
	if bucketID != expectedBucketID {
		t.Fatalf("expected bucket ID %d, got %d", expectedBucketID, bucketID)
	}
}

func TestEncodeIndexKeyWithBucket_20Bytes(t *testing.T) {
	var termKey [16]byte
	termKey[0] = 0xFF

	key := EncodeIndexKeyWithBucket(termKey, 42)

	if len(key) != 20 {
		t.Fatalf("expected 20-byte key, got %d", len(key))
	}

	// Verify bucket ID
	bucketID := binary.BigEndian.Uint32(key[16:20])
	if bucketID != 42 {
		t.Fatalf("expected bucket ID 42, got %d", bucketID)
	}
}

// makeBitmapData creates a roaring bitmap with the given values and serializes it.
func makeBitmapData(t *testing.T, values ...uint32) []byte {
	t.Helper()
	bm := roaring.New()
	for _, v := range values {
		bm.Add(v)
	}
	data, err := bm.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	return data
}
