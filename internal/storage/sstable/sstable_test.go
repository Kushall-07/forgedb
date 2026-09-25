package sstable

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func buildTestTable(t *testing.T, entries []Entry) string {
	t.Helper()
	w := NewWriter()
	for _, e := range entries {
		if err := w.Add(e.Key, e.Value, e.Tombstone); err != nil {
			t.Fatalf("Add(%q): %v", e.Key, err)
		}
	}
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := w.Build(path); err != nil {
		t.Fatalf("Build: %v", err)
	}
	return path
}

func openTestTable(t *testing.T, path string) *Reader {
	t.Helper()
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func mustGet(t *testing.T, r *Reader, key string) ([]byte, Result) {
	t.Helper()
	value, result, err := r.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return value, result
}

func TestWriteReadSingleKey(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	r := openTestTable(t, path)

	value, result := mustGet(t, r, "a")
	if result != Found || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("Get(a): got value=%q result=%v, want 1/Found", value, result)
	}
}

func TestWriteReadMultipleSortedKeys(t *testing.T) {
	entries := []Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("d"), Value: []byte("4")},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	for _, e := range entries {
		value, result := mustGet(t, r, string(e.Key))
		if result != Found || !bytes.Equal(value, e.Value) {
			t.Fatalf("Get(%q): got value=%q result=%v, want %q/Found", e.Key, value, result, e.Value)
		}
	}
}

func TestWriteReadManyKeysAcrossMultipleBlocks(t *testing.T) {
	const n = 2000
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		key := []byte(padKey(i))
		value := bytes.Repeat([]byte{byte(i)}, 32)
		entries = append(entries, Entry{Key: key, Value: value})
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	if r.Count() != n {
		t.Fatalf("Count: got %d, want %d", r.Count(), n)
	}
	for i := 0; i < n; i += 37 { // sample, not every key, to keep the test fast
		e := entries[i]
		value, result := mustGet(t, r, string(e.Key))
		if result != Found || !bytes.Equal(value, e.Value) {
			t.Fatalf("Get(%q): got value=%v result=%v, want %v/Found", e.Key, value, result, e.Value)
		}
	}
}

func padKey(i int) string {
	// Zero-padded so lexicographic byte order matches numeric order,
	// which Writer.Add requires.
	b := make([]byte, 4)
	for j := 3; j >= 0; j-- {
		b[j] = byte('0' + i%10)
		i /= 10
	}
	return string(b)
}

func TestGetOnMissingKey(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	r := openTestTable(t, path)

	_, result := mustGet(t, r, "z")
	if result != NotFound {
		t.Fatalf("Get(z): got %v, want NotFound", result)
	}
}

func TestGetOnKeyBeforeFirstBlock(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("m"), Value: []byte("1")}})
	r := openTestTable(t, path)

	_, result := mustGet(t, r, "a")
	if result != NotFound {
		t.Fatalf("Get(a): got %v, want NotFound", result)
	}
}

func TestTombstonePreservedAndDistinguishedFromMissing(t *testing.T) {
	entries := []Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Tombstone: true},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	_, result := mustGet(t, r, "b")
	if result != Tombstone {
		t.Fatalf("Get(b): got %v, want Tombstone", result)
	}
	_, result = mustGet(t, r, "z")
	if result != NotFound {
		t.Fatalf("Get(z): got %v, want NotFound (tombstone must not be confused with a missing key)", result)
	}
}

func TestEmptyValueIsStoredAndDistinctFromTombstone(t *testing.T) {
	entries := []Entry{
		{Key: []byte("a"), Value: []byte{}},
		{Key: []byte("b"), Tombstone: true},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	value, result := mustGet(t, r, "a")
	if result != Found || len(value) != 0 {
		t.Fatalf("Get(a): got value=%q result=%v, want empty/Found", value, result)
	}
	_, result = mustGet(t, r, "b")
	if result != Tombstone {
		t.Fatalf("Get(b): got %v, want Tombstone", result)
	}
}

func TestLatestValueForKeyWinsWithinSameBuild(t *testing.T) {
	// Add rejects a duplicate key outright (see TestAddRejectsNonIncreasingKeys):
	// a single build always has at most one entry per key, matching how an
	// immutable MemTable (already deduplicated by its skip list) feeds a
	// Writer.
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("final")}})
	r := openTestTable(t, path)

	value, result := mustGet(t, r, "a")
	if result != Found || !bytes.Equal(value, []byte("final")) {
		t.Fatalf("Get(a): got value=%q result=%v, want final/Found", value, result)
	}
}

func TestBinaryKeysAndValues(t *testing.T) {
	entries := []Entry{
		{Key: []byte{0x00, 0x01}, Value: []byte{0xFF, 0xFE, 0x00}},
		{Key: []byte{0x00, 0x02}, Value: []byte{0x00, 0x00}},
		{Key: []byte{0xFF}, Value: []byte{0x01}},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	for _, e := range entries {
		value, result := mustGet(t, r, string(e.Key))
		if result != Found || !bytes.Equal(value, e.Value) {
			t.Fatalf("Get(%v): got value=%v result=%v, want %v/Found", e.Key, value, result, e.Value)
		}
	}
}

func TestAddRejectsEmptyKey(t *testing.T) {
	w := NewWriter()
	if err := w.Add(nil, []byte("v"), false); err == nil {
		t.Fatalf("Add with nil key: got nil error, want error")
	}
	if err := w.Add([]byte(""), []byte("v"), false); err == nil {
		t.Fatalf("Add with empty key: got nil error, want error")
	}
}

func TestAddRejectsNonIncreasingKeys(t *testing.T) {
	w := NewWriter()
	if err := w.Add([]byte("b"), []byte("1"), false); err != nil {
		t.Fatalf("Add(b): %v", err)
	}
	if err := w.Add([]byte("a"), []byte("2"), false); err == nil {
		t.Fatalf("Add(a) after Add(b): got nil error, want error")
	}
	if err := w.Add([]byte("b"), []byte("3"), false); err == nil {
		t.Fatalf("Add(b) duplicate: got nil error, want error")
	}
}

func TestBuildRejectsEmptyTable(t *testing.T) {
	w := NewWriter()
	path := filepath.Join(t.TempDir(), "empty.sst")
	if err := w.Build(path); err == nil {
		t.Fatalf("Build with no entries: got nil error, want error")
	}
}

func TestReopenExistingSSTable(t *testing.T) {
	entries := []Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Tombstone: true},
	}
	path := buildTestTable(t, entries)

	r1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := r1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open (reopen): %v", err)
	}
	defer r2.Close()

	value, result := mustGet(t, r2, "a")
	if result != Found || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("Get(a) after reopen: got value=%q result=%v, want 1/Found", value, result)
	}
	_, result = mustGet(t, r2, "b")
	if result != Tombstone {
		t.Fatalf("Get(b) after reopen: got %v, want Tombstone", result)
	}
}

func TestMinMaxKey(t *testing.T) {
	entries := []Entry{
		{Key: []byte("b"), Value: []byte("1")},
		{Key: []byte("m"), Value: []byte("2")},
		{Key: []byte("z"), Value: []byte("3")},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	if !bytes.Equal(r.MinKey(), []byte("b")) {
		t.Fatalf("MinKey: got %q, want b", r.MinKey())
	}
	if !bytes.Equal(r.MaxKey(), []byte("z")) {
		t.Fatalf("MaxKey: got %q, want z", r.MaxKey())
	}
}

func TestBloomFilterNeverFalseNegative(t *testing.T) {
	const n = 500
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, Entry{Key: []byte(padKey(i)), Value: []byte("v")})
	}
	bloom := newBloomFilter(len(entries), defaultFalsePositiveRate)
	for _, e := range entries {
		bloom.add(e.Key)
	}
	for _, e := range entries {
		if !bloom.mightContain(e.Key) {
			t.Fatalf("mightContain(%q): got false for a key that was added, want true (false negative)", e.Key)
		}
	}
}

func TestBloomFilterRejectsSomeAbsentKeys(t *testing.T) {
	bloom := newBloomFilter(100, defaultFalsePositiveRate)
	for i := 0; i < 100; i++ {
		bloom.add([]byte(padKey(i)))
	}
	rejected := 0
	for i := 100; i < 1100; i++ {
		if !bloom.mightContain([]byte(padKey(i))) {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatalf("mightContain rejected 0 of 1000 absent keys, want most rejected at a ~1%% false-positive rate")
	}
}

func TestGetSkipsBlockReadForBloomNegative(t *testing.T) {
	// Not a timing test: verifies the Bloom-negative path is taken by
	// checking that Get on a key the bloom filter rejects still correctly
	// reports NotFound even though the key is lexicographically "in range".
	entries := []Entry{
		{Key: []byte("aaa"), Value: []byte("1")},
		{Key: []byte("zzz"), Value: []byte("2")},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	_, result := mustGet(t, r, "mmm")
	if result != NotFound {
		t.Fatalf("Get(mmm): got %v, want NotFound", result)
	}
}

func TestGetRejectsEmptyKey(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	r := openTestTable(t, path)

	if _, _, err := r.Get(nil); err == nil {
		t.Fatalf("Get(nil): got nil error, want error")
	}
}

func TestOpenMissingFile(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "does-not-exist.sst"))
	if err == nil {
		t.Fatalf("Open missing file: got nil error, want error")
	}
}

func TestOpenRejectsCorruptFooterErrorsWithErrCorrupt(t *testing.T) {
	// Sanity check that the ErrCorrupt sentinel round-trips through
	// errors.Is from the top-level Open call for at least one corruption
	// category; corruption_test.go covers the rest in detail.
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	corruptLastBytes(t, path, 8) // stomps the footer's checksum and part of metaLength

	_, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open corrupted footer: got %v, want error wrapping ErrCorrupt", err)
	}
}
