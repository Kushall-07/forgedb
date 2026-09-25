package sstable

import (
	"bytes"
	"errors"
	"testing"
)

func TestIteratorYieldsAllEntriesInOrder(t *testing.T) {
	entries := []Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	it := r.NewIterator()
	var got []Entry
	for it.Next() {
		e := it.Entry()
		got = append(got, Entry{Key: append([]byte(nil), e.Key...), Value: append([]byte(nil), e.Value...), Tombstone: e.Tombstone})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}
	for i, e := range entries {
		if !bytes.Equal(got[i].Key, e.Key) || !bytes.Equal(got[i].Value, e.Value) {
			t.Fatalf("entry %d: got %+v, want %+v", i, got[i], e)
		}
	}
}

func TestIteratorSpansMultipleBlocks(t *testing.T) {
	const n = 2000
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, Entry{Key: []byte(padKey(i)), Value: bytes.Repeat([]byte{byte(i)}, 32)})
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	it := r.NewIterator()
	count := 0
	for it.Next() {
		e := it.Entry()
		want := entries[count]
		if !bytes.Equal(e.Key, want.Key) || !bytes.Equal(e.Value, want.Value) {
			t.Fatalf("entry %d: got key=%q, want %q", count, e.Key, want.Key)
		}
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if count != n {
		t.Fatalf("got %d entries, want %d", count, n)
	}
}

func TestIteratorIncludesTombstones(t *testing.T) {
	entries := []Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Tombstone: true},
	}
	path := buildTestTable(t, entries)
	r := openTestTable(t, path)

	it := r.NewIterator()
	var got []Entry
	for it.Next() {
		got = append(got, it.Entry())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(got) != 2 || !got[1].Tombstone {
		t.Fatalf("got %+v, want second entry to be a tombstone", got)
	}
}

func TestIteratorPropagatesBlockCorruption(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	data := readFile(t, path)
	data[3] ^= 0xFF // inside the single data block, which starts at offset 0
	writeFile(t, path, data)

	r := openTestTable(t, path) // footer/index/bloom/meta are untouched, so Open still succeeds
	it := r.NewIterator()
	if it.Next() {
		t.Fatalf("Next on corrupted block: got true, want false")
	}
	if !errors.Is(it.Err(), ErrCorrupt) {
		t.Fatalf("Err on corrupted block: got %v, want error wrapping ErrCorrupt", it.Err())
	}
}

func TestIteratorOnSingleEntryTableStopsCleanly(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	r := openTestTable(t, path)

	it := r.NewIterator()
	if !it.Next() {
		t.Fatalf("Next: got false, want true for first entry")
	}
	if it.Next() {
		t.Fatalf("Next: got true, want false after the only entry")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
}
