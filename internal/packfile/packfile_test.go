package packfile

import (
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func writeTestPackfile(t *testing.T, records [][]byte, opts WriterOptions) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.pack")
	w, err := Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return path
}

func makeRecords(n, size int) [][]byte {
	records := make([][]byte, n)
	for i := range records {
		rec := make([]byte, size)
		rand.Read(rec)
		records[i] = rec
	}
	return records
}

func TestRoundTrip(t *testing.T) {
	records := makeRecords(500, 1024)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != len(records) {
		t.Fatalf("RecordCount = %d, want %d", rc, len(records))
	}

	for i, want := range records {
		got, err := r.ReadRecordInto(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}
}

func TestEmptyFile(t *testing.T) {
	path := writeTestPackfile(t, nil, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Fatalf("RecordCount = %d, want 0", rc)
	}

	_, err = r.ReadRecordInto(0, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(0) on empty: got %v, want ErrIndexRange", err)
	}
}

func TestSingleRecord(t *testing.T) {
	records := makeRecords(1, 256)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RecordCount = %d, want 1", rc)
	}

	got, err := r.ReadRecordInto(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, records[0]) {
		t.Fatal("data mismatch")
	}
}

func TestPartialLastGroup(t *testing.T) {
	// 200 records = 1 full group (128) + 72 in partial group.
	records := makeRecords(200, 512)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 200 {
		t.Fatalf("RecordCount = %d, want 200", rc)
	}

	// Verify all records.
	for i, want := range records {
		got, err := r.ReadRecordInto(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}
}

func TestLargeRecords(t *testing.T) {
	// Records > 1MB to exceed ReadRecords batch buffer.
	records := [][]byte{
		make([]byte, 2*1024*1024), // 2MB
		make([]byte, 500),         // small
		make([]byte, 1500*1024),   // 1.5MB
	}
	for _, r := range records {
		rand.Read(r)
	}

	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	// Point reads.
	for i, want := range records {
		got, err := r.ReadRecordInto(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}

	// ReadRecords with large records.
	j := 0
	for raw, err := range r.ReadRecords(0, 3) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, records[j]) {
			t.Fatalf("ReadRecords[%d]: data mismatch", j)
		}
		j++
	}
	if j != 3 {
		t.Fatalf("ReadRecords yielded %d records, want 3", j)
	}
}

func TestIndexIntegrity(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	// Read file, corrupt a byte in the index section, write back.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Index section starts after records, ends before metadata+trailer.
	// For 10 records of 100 bytes each, records end at byte 1000.
	// Flip a bit in the first byte after the records.
	indexStart := 10 * 100
	if indexStart < len(data)-trailerSize {
		data[indexStart] ^= 0xFF
	}

	corruptPath := filepath.Join(t.TempDir(), "corrupt.pack")
	if err := os.WriteFile(corruptPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath)
	defer r.Close()
	_, err = r.RecordCount()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open corrupt index: got %v, want ErrChecksum", err)
	}
}

func TestTrailerIntegrity(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the magic bytes in the trailer.
	trailerStart := len(data) - trailerSize
	data[trailerStart] ^= 0xFF

	corruptPath := filepath.Join(t.TempDir(), "corrupt_trailer.pack")
	if err := os.WriteFile(corruptPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath)
	defer r.Close()
	_, err = r.RecordCount()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open corrupt trailer: got %v, want ErrCorrupt", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	records := makeRecords(100, 512)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := range records {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, err := r.ReadRecordInto(idx, nil)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, records[idx]) {
				errs <- errors.New("data mismatch")
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
}

func TestReadRecordsIterator(t *testing.T) {
	records := makeRecords(50, 2048)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	// Full range.
	j := 0
	for raw, err := range r.ReadRecords(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, records[j]) {
			t.Fatalf("ReadRecords[%d]: data mismatch", j)
		}
		j++
	}
	if j != 50 {
		t.Fatalf("ReadRecords yielded %d records, want 50", j)
	}

	// Partial range.
	j = 0
	for raw, err := range r.ReadRecords(10, 5) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, records[10+j]) {
			t.Fatalf("ReadRecords partial [%d]: data mismatch", j)
		}
		j++
	}
	if j != 5 {
		t.Fatalf("ReadRecords partial yielded %d, want 5", j)
	}

	// Early break.
	j = 0
	for _, err := range r.ReadRecords(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		j++
		if j == 3 {
			break
		}
	}
	if j != 3 {
		t.Fatalf("Early break: got %d iterations, want 3", j)
	}

	// Empty range.
	j = 0
	for _, err := range r.ReadRecords(0, 0) {
		if err != nil {
			t.Fatal(err)
		}
		j++
	}
	if j != 0 {
		t.Fatalf("Empty ReadRecords yielded %d, want 0", j)
	}
}

func TestAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "atomic.pack")

	w, err := Create(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// File should not exist at final path yet.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file exists at final path before Finish")
	}

	if err := w.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	// Now file should exist.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not at final path after Finish: %v", err)
	}
}

func TestAbortCleansUp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "abort.pack")

	w, err := Create(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}

	tmpPath := w.tmpPath
	if err := w.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	// Tmp file should exist.
	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("tmp file not found: %v", err)
	}

	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}

	// Tmp file should be gone.
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatal("tmp file still exists after Abort")
	}

	// Final path should not exist.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file exists at final path after Abort")
	}
}

func TestMetadataRoundTrip(t *testing.T) {
	meta := []byte("chunk-meta:version=1,first_ledger=420000")
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{Metadata: meta})

	r := Open(path)
	defer r.Close()

	got, err := r.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, meta) {
		t.Fatalf("Metadata mismatch: got %q, want %q", got, meta)
	}
}

func TestVariableSizeRecords(t *testing.T) {
	// Records with varying sizes to exercise FOR compression.
	records := make([][]byte, 300)
	for i := range records {
		size := 5000 + (i % 200) // 5000-5199 bytes
		rec := make([]byte, size)
		rand.Read(rec)
		records[i] = rec
	}

	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	for i, want := range records {
		got, err := r.ReadRecordInto(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch (len got=%d, want=%d)", i, len(got), len(want))
		}
	}
}

func TestUniformSizeRecords(t *testing.T) {
	// All records same size — exercises width=0→1 path.
	records := makeRecords(256, 1000)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	for i, want := range records {
		got, err := r.ReadRecordInto(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}
}

func TestReadRecordOutOfRange(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	_, err := r.ReadRecordInto(-1, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(-1): got %v, want ErrIndexRange", err)
	}

	_, err = r.ReadRecordInto(5, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(5): got %v, want ErrIndexRange", err)
	}

	_, err = r.ReadRecordInto(100, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(100): got %v, want ErrIndexRange", err)
	}
}

func TestReadRecordsPanic(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	assertPanics := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		f()
	}

	assertPanics("negative index", func() { for range r.ReadRecords(-1, 1) {} })
	assertPanics("negative count", func() { for range r.ReadRecords(0, -1) {} })
	assertPanics("out of range", func() { for range r.ReadRecords(3, 5) {} })
}

func TestFORRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []uint32
	}{
		{"uniform", []uint32{100, 100, 100, 100}},
		{"ascending", []uint32{10, 20, 30, 40, 50}},
		{"single", []uint32{42}},
		{"wide_range", []uint32{0, 1, 1000000}},
		{"max_group", func() []uint32 {
			v := make([]uint32, 128)
			for i := range v {
				v[i] = uint32(i * 7)
			}
			return v
		}()},
	}

	var dst []uint32
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeGroup(tt.values)
			// Pad for safe 8-byte overshoot reads.
			padded := make([]byte, len(encoded)+7)
			copy(padded, encoded)

			var consumed int
			dst, consumed = DecodeGroup(padded, len(tt.values), dst)
			if consumed != len(encoded) {
				t.Fatalf("consumed %d bytes, want %d", consumed, len(encoded))
			}
			if len(dst) != len(tt.values) {
				t.Fatalf("decoded %d values, want %d", len(dst), len(tt.values))
			}
			for i, v := range dst {
				if v != tt.values[i] {
					t.Fatalf("value[%d] = %d, want %d", i, v, tt.values[i])
				}
			}
		})
	}
}

func TestOpenBadPath(t *testing.T) {
	r := Open("/nonexistent/path/to/file.pack")
	defer r.Close()

	_, err := r.ReadRecordInto(0, nil)
	if err == nil {
		t.Fatal("expected error for bad path")
	}
}

func TestCloseBeforeRead(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	err1 := r.Close()
	err2 := r.Close()
	if err1 != err2 {
		t.Fatalf("double Close: first=%v, second=%v", err1, err2)
	}
}
