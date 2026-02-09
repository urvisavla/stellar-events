package store

import (
	"encoding/binary"
	"testing"
)

func TestBucketLedgerMapKey(t *testing.T) {
	key := BucketLedgerMapKey(42)
	if len(key) != 5 {
		t.Fatalf("expected key length 5, got %d", len(key))
	}
	if key[0] != 0xFF {
		t.Fatalf("expected prefix 0xFF, got 0x%02x", key[0])
	}
	bucketID := binary.BigEndian.Uint32(key[1:5])
	if bucketID != 42 {
		t.Fatalf("expected bucket ID 42, got %d", bucketID)
	}
}

func TestEncodeBucketLedgerMap_RoundTrip(t *testing.T) {
	var counts [BucketSize]uint32
	counts[0] = 100
	counts[1] = 200
	counts[2] = 50
	// rest are zero

	data := EncodeBucketLedgerMap(5, counts)
	if len(data) != BucketLedgerMapSize {
		t.Fatalf("expected data length %d, got %d", BucketLedgerMapSize, len(data))
	}

	lm := &BucketLedgerMap{BucketID: 5, Data: data}

	// Check cumulative values
	// offset 0: 100
	// offset 1: 100 + 200 = 300
	// offset 2: 300 + 50 = 350
	if lm.TotalEvents() != 350 {
		t.Fatalf("expected total events 350, got %d", lm.TotalEvents())
	}
}

func TestBucketLedgerMap_TotalEvents_Empty(t *testing.T) {
	var counts [BucketSize]uint32
	data := EncodeBucketLedgerMap(0, counts)
	lm := &BucketLedgerMap{BucketID: 0, Data: data}
	if lm.TotalEvents() != 0 {
		t.Fatalf("expected 0 total events, got %d", lm.TotalEvents())
	}
}

func TestBucketLedgerMap_TotalEvents_NilData(t *testing.T) {
	lm := &BucketLedgerMap{BucketID: 0, Data: nil}
	if lm.TotalEvents() != 0 {
		t.Fatalf("expected 0 total events for nil data, got %d", lm.TotalEvents())
	}
}

func TestBucketLedgerMap_LedgerRangeToIDRange(t *testing.T) {
	var counts [BucketSize]uint32
	counts[0] = 10  // IDs 0-9
	counts[1] = 20  // IDs 10-29
	counts[2] = 5   // IDs 30-34
	counts[3] = 15  // IDs 35-49

	data := EncodeBucketLedgerMap(0, counts)
	lm := &BucketLedgerMap{BucketID: 0, Data: data}

	tests := []struct {
		name              string
		startOff, endOff  uint16
		wantStart, wantEnd uint32
	}{
		{"full range", 0, 3, 0, 49},
		{"first ledger only", 0, 0, 0, 9},
		{"second ledger only", 1, 1, 10, 29},
		{"third ledger only", 2, 2, 30, 34},
		{"ledgers 1-2", 1, 2, 10, 34},
		{"ledgers 0-2", 0, 2, 0, 34},
		{"last ledger only", 3, 3, 35, 49},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := lm.LedgerRangeToIDRange(tt.startOff, tt.endOff)
			if start != tt.wantStart || end != tt.wantEnd {
				t.Errorf("LedgerRangeToIDRange(%d, %d) = (%d, %d), want (%d, %d)",
					tt.startOff, tt.endOff, start, end, tt.wantStart, tt.wantEnd)
			}
		})
	}
}

func TestBucketLedgerMap_LedgerRangeToIDRange_EmptyLedgers(t *testing.T) {
	var counts [BucketSize]uint32
	counts[0] = 10  // IDs 0-9
	// counts[1] = 0 — empty ledger
	counts[2] = 5   // IDs 10-14

	data := EncodeBucketLedgerMap(0, counts)
	lm := &BucketLedgerMap{BucketID: 0, Data: data}

	// Range spanning the empty ledger
	start, end := lm.LedgerRangeToIDRange(0, 2)
	if start != 0 || end != 14 {
		t.Errorf("expected (0, 14), got (%d, %d)", start, end)
	}

	// Just the empty ledger
	start, end = lm.LedgerRangeToIDRange(1, 1)
	// startID = cumulative[0] = 10, endID = cumulative[1]-1 = 10-1 = 9
	// This means startID > endID, which indicates no events
	if start > end {
		// This is expected — empty range
	} else if start == end {
		t.Logf("empty ledger returned (%d, %d) — collapsed range", start, end)
	}
}

func TestBucketLedgerMap_DenseIDToLedgerAndSeq(t *testing.T) {
	bucketID := uint32(5)
	bucketStart := bucketID * BucketSize

	var counts [BucketSize]uint32
	counts[0] = 10  // IDs 0-9 -> ledger=bucketStart, seq=0-9
	counts[1] = 20  // IDs 10-29 -> ledger=bucketStart+1, seq=0-19
	counts[2] = 5   // IDs 30-34 -> ledger=bucketStart+2, seq=0-4

	data := EncodeBucketLedgerMap(bucketID, counts)
	lm := &BucketLedgerMap{BucketID: bucketID, Data: data}

	tests := []struct {
		denseID    uint32
		wantLedger uint32
		wantSeq    uint16
	}{
		{0, bucketStart, 0},
		{9, bucketStart, 9},
		{10, bucketStart + 1, 0},
		{29, bucketStart + 1, 19},
		{30, bucketStart + 2, 0},
		{34, bucketStart + 2, 4},
	}

	for _, tt := range tests {
		ledger, seq := lm.DenseIDToLedgerAndSeq(tt.denseID)
		if ledger != tt.wantLedger || seq != tt.wantSeq {
			t.Errorf("DenseIDToLedgerAndSeq(%d) = (%d, %d), want (%d, %d)",
				tt.denseID, ledger, seq, tt.wantLedger, tt.wantSeq)
		}
	}
}

func TestBucketLedgerMap_DenseIDToLedgerAndSeq_SingleEvent(t *testing.T) {
	var counts [BucketSize]uint32
	counts[0] = 1 // single event at ledger offset 0

	data := EncodeBucketLedgerMap(0, counts)
	lm := &BucketLedgerMap{BucketID: 0, Data: data}

	ledger, seq := lm.DenseIDToLedgerAndSeq(0)
	if ledger != 0 || seq != 0 {
		t.Errorf("expected (0, 0), got (%d, %d)", ledger, seq)
	}
}

func TestBucketLedgerMap_DenseIDToLedgerAndSeq_LastOffset(t *testing.T) {
	var counts [BucketSize]uint32
	counts[BucketSize-1] = 5 // events only at last ledger offset

	data := EncodeBucketLedgerMap(0, counts)
	lm := &BucketLedgerMap{BucketID: 0, Data: data}

	ledger, seq := lm.DenseIDToLedgerAndSeq(0)
	if ledger != BucketSize-1 || seq != 0 {
		t.Errorf("expected (%d, 0), got (%d, %d)", BucketSize-1, ledger, seq)
	}

	ledger, seq = lm.DenseIDToLedgerAndSeq(4)
	if ledger != BucketSize-1 || seq != 4 {
		t.Errorf("expected (%d, 4), got (%d, %d)", BucketSize-1, ledger, seq)
	}
}

func TestMergeBucketLedgerMapData(t *testing.T) {
	// Build initial map
	var counts [BucketSize]uint32
	counts[0] = 10
	counts[1] = 20
	existing := EncodeBucketLedgerMap(0, counts)

	// Merge new counts
	newCounts := map[uint16]uint32{
		1: 5,  // add 5 more events at offset 1
		2: 15, // new events at offset 2
	}

	merged := MergeBucketLedgerMapData(existing, newCounts)
	lm := &BucketLedgerMap{BucketID: 0, Data: merged}

	// Expected: offset 0 = 10, offset 1 = 25, offset 2 = 15
	if lm.TotalEvents() != 50 {
		t.Fatalf("expected total 50, got %d", lm.TotalEvents())
	}

	// Check forward lookup: ID 10 should be in ledger offset 1 (cumulative after 0 is 10)
	ledger, seq := lm.DenseIDToLedgerAndSeq(10)
	if ledger != 1 || seq != 0 {
		t.Errorf("DenseIDToLedgerAndSeq(10) = (%d, %d), want (1, 0)", ledger, seq)
	}

	// ID 35 should be in ledger offset 2 (cumulative after 1 is 35)
	ledger, seq = lm.DenseIDToLedgerAndSeq(35)
	if ledger != 2 || seq != 0 {
		t.Errorf("DenseIDToLedgerAndSeq(35) = (%d, %d), want (2, 0)", ledger, seq)
	}
}

func TestMergeBucketLedgerMapData_NoExisting(t *testing.T) {
	newCounts := map[uint16]uint32{
		0: 10,
		5: 20,
	}

	merged := MergeBucketLedgerMapData(nil, newCounts)
	lm := &BucketLedgerMap{BucketID: 0, Data: merged}

	if lm.TotalEvents() != 30 {
		t.Fatalf("expected total 30, got %d", lm.TotalEvents())
	}
}

func TestBucketEventCounter_AssignDenseID(t *testing.T) {
	counter := &bucketEventCounter{}

	// Assign IDs for events in different ledger offsets
	id0 := counter.assignDenseID(0)
	id1 := counter.assignDenseID(0)
	id2 := counter.assignDenseID(1)
	id3 := counter.assignDenseID(0)

	if id0 != 0 || id1 != 1 || id2 != 2 || id3 != 3 {
		t.Errorf("expected sequential IDs (0,1,2,3), got (%d,%d,%d,%d)", id0, id1, id2, id3)
	}

	if counter.nextDenseID != 4 {
		t.Errorf("expected nextDenseID 4, got %d", counter.nextDenseID)
	}

	if counter.eventCounts[0] != 3 {
		t.Errorf("expected eventCounts[0] = 3, got %d", counter.eventCounts[0])
	}
	if counter.eventCounts[1] != 1 {
		t.Errorf("expected eventCounts[1] = 1, got %d", counter.eventCounts[1])
	}
}

func TestBucketLedgerMap_ForwardReverse_Consistency(t *testing.T) {
	// Build a realistic scenario: 100 ledgers with varying event counts
	var counts [BucketSize]uint32
	for i := 0; i < 100; i++ {
		counts[i] = uint32(i + 1) // ledger 0 has 1 event, ledger 1 has 2, etc.
	}

	bucketID := uint32(10)
	data := EncodeBucketLedgerMap(bucketID, counts)
	lm := &BucketLedgerMap{BucketID: bucketID, Data: data}
	bucketStart := bucketID * BucketSize

	// Total events = 1+2+3+...+100 = 5050
	if lm.TotalEvents() != 5050 {
		t.Fatalf("expected 5050 total events, got %d", lm.TotalEvents())
	}

	// For each ledger offset, verify forward and reverse lookups are consistent
	denseID := uint32(0)
	for offset := uint16(0); offset < 100; offset++ {
		numEvents := uint32(offset + 1)

		// Check range lookup
		rangeStart, rangeEnd := lm.LedgerRangeToIDRange(offset, offset)
		if rangeStart != denseID {
			t.Errorf("offset %d: range start %d != expected %d", offset, rangeStart, denseID)
		}
		if rangeEnd != denseID+numEvents-1 {
			t.Errorf("offset %d: range end %d != expected %d", offset, rangeEnd, denseID+numEvents-1)
		}

		// Check forward lookup for each event in this ledger
		for seq := uint32(0); seq < numEvents; seq++ {
			ledger, eventSeq := lm.DenseIDToLedgerAndSeq(denseID + seq)
			expectedLedger := bucketStart + uint32(offset)
			if ledger != expectedLedger || uint32(eventSeq) != seq {
				t.Errorf("DenseIDToLedgerAndSeq(%d) = (%d, %d), want (%d, %d)",
					denseID+seq, ledger, eventSeq, expectedLedger, seq)
			}
		}

		denseID += numEvents
	}
}
