package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

func corruptByteAt(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, offset); err != nil {
		t.Fatalf("read byte to corrupt: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, offset); err != nil {
		t.Fatalf("write corrupted byte: %v", err)
	}
}

func mustAppendSync(t *testing.T, w *WAL, rec Record) {
	t.Helper()
	if err := w.Append(rec); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func TestAppendAndReplaySingleRecord(t *testing.T) {
	w, _ := newTestWAL(t)
	mustAppendSync(t, w, Record{Type: OpPut, Key: []byte("a"), Value: []byte("value1")})

	var got []Record
	if err := w.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("Replay: got %d records, want 1", len(got))
	}
	if got[0].Type != OpPut || !bytes.Equal(got[0].Key, []byte("a")) || !bytes.Equal(got[0].Value, []byte("value1")) {
		t.Fatalf("Replay: got %+v", got[0])
	}
}

func TestReplayOrderingAndOverwrite(t *testing.T) {
	w, path := newTestWAL(t)

	records := []Record{
		{Type: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Type: OpPut, Key: []byte("a"), Value: []byte("2")},
		{Type: OpDelete, Key: []byte("a")},
		{Type: OpPut, Key: []byte("b"), Value: []byte("3")},
	}
	for _, r := range records {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	defer w2.Close()

	state := map[string]string{}
	var order []OpType
	err = w2.Replay(func(r Record) error {
		order = append(order, r.Type)
		switch r.Type {
		case OpPut:
			state[string(r.Key)] = string(r.Value)
		case OpDelete:
			delete(state, string(r.Key))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(order) != 4 {
		t.Fatalf("Replay: got %d records, want 4", len(order))
	}
	if _, ok := state["a"]; ok {
		t.Fatalf("state[a]: expected absent after delete, got %q", state["a"])
	}
	if state["b"] != "3" {
		t.Fatalf("state[b]: got %q, want %q", state["b"], "3")
	}
}

func TestReplayEmptyValuePreserved(t *testing.T) {
	w, _ := newTestWAL(t)
	mustAppendSync(t, w, Record{Type: OpPut, Key: []byte("a"), Value: []byte{}})

	var got []Record
	if err := w.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if len(got[0].Value) != 0 {
		t.Fatalf("Value: got %q, want empty", got[0].Value)
	}
}

func TestAppendRejectsEmptyKey(t *testing.T) {
	w, _ := newTestWAL(t)
	if err := w.Append(Record{Type: OpPut, Key: nil, Value: []byte("v")}); err == nil {
		t.Fatalf("Append with empty key: got nil error, want error")
	}
}

func TestBinaryKeyAndValueRoundTrip(t *testing.T) {
	w, path := newTestWAL(t)

	key := []byte{0x00, 0xFF, '\n', '\t', 0x01}
	value := []byte{0xFF, 0x00, 0x00, '\n', 0xFE}
	mustAppendSync(t, w, Record{Type: OpPut, Key: key, Value: value})
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	defer w2.Close()

	var got Record
	if err := w2.Replay(func(r Record) error {
		got = r
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !bytes.Equal(got.Key, key) {
		t.Fatalf("Key: got %x, want %x", got.Key, key)
	}
	if !bytes.Equal(got.Value, value) {
		t.Fatalf("Value: got %x, want %x", got.Value, value)
	}
}

func TestReplayDetectsChecksumCorruption(t *testing.T) {
	w, path := newTestWAL(t)
	rec := Record{Type: OpPut, Key: []byte("a"), Value: []byte("value1")}
	mustAppendSync(t, w, rec)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte inside the value, so checksum validation -- not header
	// validation -- is what catches this.
	corruptByteAt(t, path, int64(headerSize+len(rec.Key))+1)

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	err = w2.Replay(func(Record) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay: got %v, want ErrCorrupt", err)
	}
}

func TestReplayTruncatesPartialFinalRecordPayload(t *testing.T) {
	w, path := newTestWAL(t)

	rec1 := Record{Type: OpPut, Key: []byte("a"), Value: []byte("one")}
	rec2 := Record{Type: OpPut, Key: []byte("b"), Value: []byte("two")}

	enc1, err := encodeRecord(rec1)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	if err := w.Append(rec1); err != nil {
		t.Fatalf("Append rec1: %v", err)
	}
	if err := w.Append(rec2); err != nil {
		t.Fatalf("Append rec2: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write of rec2: only its header and one byte of
	// payload survive.
	tornSize := int64(len(enc1)) + int64(headerSize) + 1
	if err := os.Truncate(path, tornSize); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	var got []Record
	if err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: got error %v, want nil (torn tail must be silently truncated)", err)
	}
	if len(got) != 1 {
		t.Fatalf("Replay: got %d records, want 1 (only rec1)", len(got))
	}
	if !bytes.Equal(got[0].Key, rec1.Key) {
		t.Fatalf("Replay: got key %q, want %q", got[0].Key, rec1.Key)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != int64(len(enc1)) {
		t.Fatalf("file size after torn-tail truncation: got %d, want %d", info.Size(), len(enc1))
	}

	// The WAL must still be usable for further appends after recovering
	// from a torn tail.
	if err := w2.Append(Record{Type: OpPut, Key: []byte("c"), Value: []byte("three")}); err != nil {
		t.Fatalf("Append after torn-tail recovery: %v", err)
	}
	if err := w2.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w3, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w3.Close()
	got = nil
	if err := w3.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Replay after append: got %d records, want 2", len(got))
	}
	if !bytes.Equal(got[1].Key, []byte("c")) {
		t.Fatalf("Replay after append: got second key %q, want %q", got[1].Key, "c")
	}
}

func TestReplayTruncatesPartialHeader(t *testing.T) {
	w, path := newTestWAL(t)

	rec1 := Record{Type: OpPut, Key: []byte("a"), Value: []byte("one")}
	enc1, err := encodeRecord(rec1)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	mustAppendSync(t, w, rec1)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append raw bytes that look like the start of a second record's
	// header but stop well short of headerSize bytes.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for raw append: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatalf("raw append: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	var got []Record
	if err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: got error %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != int64(len(enc1)) {
		t.Fatalf("size after truncation: got %d, want %d", info.Size(), len(enc1))
	}
}

func TestReplayDoesNotIgnoreCorruptionInMiddle(t *testing.T) {
	w, path := newTestWAL(t)

	rec1 := Record{Type: OpPut, Key: []byte("a"), Value: []byte("one")}
	rec2 := Record{Type: OpPut, Key: []byte("b"), Value: []byte("two")}
	rec3 := Record{Type: OpPut, Key: []byte("c"), Value: []byte("three")}

	enc1, err := encodeRecord(rec1)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}

	for _, r := range []Record{rec1, rec2, rec3} {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Corrupt a byte inside rec2's value. rec3, a complete and valid
	// record, still follows it on disk -- this is not a torn tail.
	corruptOffset := int64(len(enc1)) + int64(headerSize+len(rec2.Key)) + 1
	corruptByteAt(t, path, corruptOffset)

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	var applied []Record
	err = w2.Replay(func(r Record) error {
		applied = append(applied, r)
		return nil
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay: got %v, want ErrCorrupt", err)
	}
	if len(applied) != 1 {
		t.Fatalf("Replay: applied %d records before error, want 1 (only rec1)", len(applied))
	}
}

func TestReplayRejectsOversizedLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// Hand-craft a header declaring an absurd key length. encodeRecord
	// would refuse to ever produce this; it can only arise from
	// corruption.
	hdrBytes := make([]byte, headerSize)
	hdrBytes[4] = byte(OpPut)
	binary.LittleEndian.PutUint32(hdrBytes[5:9], 0xFFFFFFFF) // keyLen
	binary.LittleEndian.PutUint32(hdrBytes[9:13], 0)         // valLen
	if err := os.WriteFile(path, hdrBytes, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	err = w.Replay(func(Record) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay: got %v, want ErrCorrupt", err)
	}
}
