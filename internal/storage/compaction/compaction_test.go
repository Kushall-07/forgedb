package compaction

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Kushall-07/forgedb/internal/storage/manifest"
	"github.com/Kushall-07/forgedb/internal/storage/sstable"
)

func mustOpenManifest(t *testing.T, dir string) *manifest.VersionSet {
	t.Helper()
	vs, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("manifest.Open: %v", err)
	}
	return vs
}

// addTable builds an SSTable from entries under name in dir and registers
// it with vs, returning its assigned Manifest ID.
func addTable(t *testing.T, vs *manifest.VersionSet, dir, name string, entries []sstable.Entry) uint64 {
	t.Helper()
	w := sstable.NewWriter()
	for _, e := range entries {
		if err := w.Add(e.Key, e.Value, e.Tombstone); err != nil {
			t.Fatalf("Add(%q): %v", e.Key, err)
		}
	}
	path := filepath.Join(dir, name)
	if err := w.Build(path); err != nil {
		t.Fatalf("Build(%s): %v", name, err)
	}
	id, err := vs.AddTable(name)
	if err != nil {
		t.Fatalf("AddTable(%s): %v", name, err)
	}
	return id
}

func readAll(t *testing.T, path string) []sstable.Entry {
	t.Helper()
	r, err := sstable.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	defer r.Close()

	var out []sstable.Entry
	it := r.NewIterator()
	for it.Next() {
		e := it.Entry()
		out = append(out, sstable.Entry{
			Key:       append([]byte(nil), e.Key...),
			Value:     append([]byte(nil), e.Value...),
			Tombstone: e.Tombstone,
		})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterate %s: %v", path, err)
	}
	return out
}

func findEntry(entries []sstable.Entry, key string) (sstable.Entry, bool) {
	for _, e := range entries {
		if string(e.Key) == key {
			return e, true
		}
	}
	return sstable.Entry{}, false
}

// --- 1. Basic merge ---

func TestCompactBasicMerge(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "a.sst", []sstable.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	})
	id2 := addTable(t, vs, dir, "b.sst", []sstable.Entry{
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("d"), Value: []byte("4")},
	})

	result, err := Compact(vs, dir, []uint64{id1, id2}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.OutputFileName == "" {
		t.Fatalf("Compact: got empty OutputFileName, want a new table")
	}

	entries := readAll(t, filepath.Join(dir, result.OutputFileName))
	want := []string{"a", "b", "c", "d"}
	if len(entries) != len(want) {
		t.Fatalf("output entries: got %d, want %d (%+v)", len(entries), len(want), entries)
	}
	for i, k := range want {
		if string(entries[i].Key) != k {
			t.Fatalf("output entry %d: got key %q, want %q", i, entries[i].Key, k)
		}
	}
}

// --- 2. Newer value wins ---

func TestCompactNewerValueWins(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	oldID := addTable(t, vs, dir, "old.sst", []sstable.Entry{{Key: []byte("key"), Value: []byte("old")}})
	newID := addTable(t, vs, dir, "new.sst", []sstable.Entry{{Key: []byte("key"), Value: []byte("new")}})

	// Pass inputs in reverse order to confirm the winner is chosen by
	// table ID, not by input-slice position.
	result, err := Compact(vs, dir, []uint64{newID, oldID}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	entries := readAll(t, filepath.Join(dir, result.OutputFileName))
	if len(entries) != 1 || string(entries[0].Value) != "new" {
		t.Fatalf("got %+v, want single entry key=new", entries)
	}
}

// --- 3. Multiple updates ---

func TestCompactMultipleUpdatesOnlyNewestSurvives(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "v1.sst", []sstable.Entry{{Key: []byte("key"), Value: []byte("v1")}})
	id2 := addTable(t, vs, dir, "v2.sst", []sstable.Entry{{Key: []byte("key"), Value: []byte("v2")}})
	id3 := addTable(t, vs, dir, "v3.sst", []sstable.Entry{{Key: []byte("key"), Value: []byte("v3")}})

	result, err := Compact(vs, dir, []uint64{id1, id2, id3}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	entries := readAll(t, filepath.Join(dir, result.OutputFileName))
	if len(entries) != 1 || string(entries[0].Value) != "v3" {
		t.Fatalf("got %+v, want single entry key=v3", entries)
	}
}

// --- 4. Delete/tombstone ---

func TestCompactFullCompactionDropsSafeTombstone(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "v.sst", []sstable.Entry{{Key: []byte("key"), Value: []byte("v")}})
	id2 := addTable(t, vs, dir, "del.sst", []sstable.Entry{{Key: []byte("key"), Tombstone: true}})

	// This compaction covers every active table, so there is nothing
	// older left that the dropped tombstone could resurrect: the
	// database ends up with zero live entries and no output table.
	result, err := Compact(vs, dir, []uint64{id1, id2}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.OutputFileName != "" || result.OutputID != 0 {
		t.Fatalf("got %+v, want no output table (all content was a droppable tombstone)", result)
	}
	if len(vs.Current().Tables) != 0 {
		t.Fatalf("Current after compaction: got %+v, want no active tables", vs.Current().Tables)
	}
}

// --- 5. Tombstone safety ---

func TestCompactTombstoneKeptWhenOlderTableOutsideInputSet(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	oldID := addTable(t, vs, dir, "old.sst", []sstable.Entry{{Key: []byte("key"), Value: []byte("old-value")}})
	midID := addTable(t, vs, dir, "mid.sst", []sstable.Entry{{Key: []byte("key"), Tombstone: true}})

	// Compact only midID. oldID (smaller ID, so older) is left active but
	// outside the compaction, so dropping the tombstone here would let a
	// later read fall through to oldID's stale value -- it must survive.
	result, err := Compact(vs, dir, []uint64{midID}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.OutputFileName == "" {
		t.Fatalf("expected the tombstone to be preserved in an output table, got no output")
	}
	entries := readAll(t, filepath.Join(dir, result.OutputFileName))
	e, found := findEntry(entries, "key")
	if !found || !e.Tombstone {
		t.Fatalf("got %+v, want a preserved tombstone for key", entries)
	}

	stillActive := false
	for _, tm := range vs.Current().Tables {
		if tm.ID == oldID {
			stillActive = true
		}
	}
	if !stillActive {
		t.Fatalf("oldID should remain active: it was not part of the compaction input")
	}
}

// --- 6. Sorted output ---

func TestCompactProducesSortedOutputAcrossInterleavedTables(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{
		{Key: []byte("b"), Value: []byte("1")},
		{Key: []byte("d"), Value: []byte("2")},
	})
	id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{
		{Key: []byte("a"), Value: []byte("3")},
		{Key: []byte("c"), Value: []byte("4")},
	})

	result, err := Compact(vs, dir, []uint64{id1, id2}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	entries := readAll(t, filepath.Join(dir, result.OutputFileName))
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Fatalf("output not strictly sorted at index %d: %q then %q", i, entries[i-1].Key, entries[i].Key)
		}
	}
	if len(entries) != 4 {
		t.Fatalf("got %d entries, want 4", len(entries))
	}
}

// --- 7. Manifest replacement ---

func TestCompactManifestReplacement(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
	id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("b"), Value: []byte("2")}})

	result, err := Compact(vs, dir, []uint64{id1, id2}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	v := vs.Current()
	if len(v.Tables) != 1 {
		t.Fatalf("Current after compact: got %d tables, want 1", len(v.Tables))
	}
	if v.Tables[0].ID != result.OutputID || v.Tables[0].FileName != result.OutputFileName {
		t.Fatalf("Current after compact: got %+v, want output %+v", v.Tables[0], result)
	}
}

// --- 8. Restart/reopen ---

func TestCompactRestartRecoversCorrectActiveSet(t *testing.T) {
	dir := t.TempDir()
	vs1 := mustOpenManifest(t, dir)

	id1 := addTable(t, vs1, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
	id2 := addTable(t, vs1, dir, "t2.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("2")}})

	result, err := Compact(vs1, dir, []uint64{id1, id2}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	vs2, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("manifest.Open (reopen): %v", err)
	}
	v := vs2.Current()
	if len(v.Tables) != 1 || v.Tables[0].ID != result.OutputID || v.Tables[0].FileName != result.OutputFileName {
		t.Fatalf("recovered version after restart: got %+v, want only %+v", v.Tables, result)
	}

	entries := readAll(t, filepath.Join(dir, v.Tables[0].FileName))
	if len(entries) != 1 || string(entries[0].Value) != "2" {
		t.Fatalf("recovered output content: got %+v, want value 2", entries)
	}
}

// --- 9. Failure safety ---

func TestCompactFailureUnderReadOnlyDirectoryLeavesActiveStateValid(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
	id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("b"), Value: []byte("2")}})

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Skipf("cannot make directory read-only on this platform: %v", err)
	}
	defer os.Chmod(dir, 0o700)

	_, compactErr := Compact(vs, dir, []uint64{id1, id2}, nil)

	os.Chmod(dir, 0o700) // restore so the test can read files back

	if compactErr == nil {
		t.Skip("Compact unexpectedly succeeded under a read-only directory on this platform")
	}

	v := vs.Current()
	if len(v.Tables) != 2 {
		t.Fatalf("Current after failed compaction: got %d tables, want 2 (unchanged)", len(v.Tables))
	}
	entries := readAll(t, filepath.Join(dir, "t1.sst"))
	if len(entries) != 1 || string(entries[0].Value) != "1" {
		t.Fatalf("t1.sst after failed compaction: got %+v, want intact original content", entries)
	}
}

// --- 10. Reader safety ---

func TestCompactReaderSafetyWithTracker(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
	id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("b"), Value: []byte("2")}})

	tr := NewTracker()
	oldPath1 := filepath.Join(dir, "t1.sst")
	release := tr.Acquire(oldPath1) // simulate an in-flight reader still using t1.sst

	result, err := Compact(vs, dir, []uint64{id1, id2}, tr)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if len(vs.Current().Tables) != 1 || vs.Current().Tables[0].ID != result.OutputID {
		t.Fatalf("Current after compact: got %+v, want only the output table", vs.Current().Tables)
	}
	// The still-held input file must not be deleted while a reader holds it.
	if _, err := os.Stat(oldPath1); err != nil {
		t.Fatalf("input file deleted while still held by a reader: stat err=%v", err)
	}
	// The other (never-acquired) input file should already be reclaimed.
	if _, err := os.Stat(filepath.Join(dir, "t2.sst")); !os.IsNotExist(err) {
		t.Fatalf("unheld input file was not deleted after compaction: stat err=%v", err)
	}

	release()
	if _, err := os.Stat(oldPath1); !os.IsNotExist(err) {
		t.Fatalf("input file still present after its reader released: stat err=%v", err)
	}
}

// --- 11. Corruption ---

func TestCompactCorruptInputReturnsError(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
	id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("b"), Value: []byte("2")}})

	path := filepath.Join(dir, "t1.sst")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xFF // corrupt the footer checksum
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Compact(vs, dir, []uint64{id1, id2}, nil)
	if !errors.Is(err, sstable.ErrCorrupt) {
		t.Fatalf("Compact with corrupt input: got %v, want error wrapping sstable.ErrCorrupt", err)
	}
	if len(vs.Current().Tables) != 2 {
		t.Fatalf("Current after failed compaction: got %d tables, want 2 (unchanged)", len(vs.Current().Tables))
	}
}

// --- 12. Determinism ---

func TestCompactDeterministicAcrossEquivalentInputPartitions(t *testing.T) {
	run := func(t *testing.T, build func(vs *manifest.VersionSet, dir string) []uint64) []sstable.Entry {
		dir := t.TempDir()
		vs := mustOpenManifest(t, dir)
		ids := build(vs, dir)
		result, err := Compact(vs, dir, ids, nil)
		if err != nil {
			t.Fatalf("Compact: %v", err)
		}
		return readAll(t, filepath.Join(dir, result.OutputFileName))
	}

	entriesA := run(t, func(vs *manifest.VersionSet, dir string) []uint64 {
		id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: []byte("2")}})
		id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("c"), Value: []byte("3")}})
		return []uint64{id1, id2}
	})
	entriesB := run(t, func(vs *manifest.VersionSet, dir string) []uint64 {
		id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
		id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("b"), Value: []byte("2")}, {Key: []byte("c"), Value: []byte("3")}})
		return []uint64{id1, id2}
	})

	if len(entriesA) != len(entriesB) {
		t.Fatalf("got %d entries vs %d entries, want equal logical content", len(entriesA), len(entriesB))
	}
	for i := range entriesA {
		if string(entriesA[i].Key) != string(entriesB[i].Key) ||
			!bytes.Equal(entriesA[i].Value, entriesB[i].Value) ||
			entriesA[i].Tombstone != entriesB[i].Tombstone {
			t.Fatalf("entry %d differs: %+v vs %+v", i, entriesA[i], entriesB[i])
		}
	}
}

func TestCompactOutputFileNameIsDeterministicForEquivalentInputIDs(t *testing.T) {
	build := func(t *testing.T) (dir string, ids []uint64, vs *manifest.VersionSet) {
		dir = t.TempDir()
		vs = mustOpenManifest(t, dir)
		id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
		id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("b"), Value: []byte("2")}})
		return dir, []uint64{id1, id2}, vs
	}
	dir1, ids1, vs1 := build(t)
	dir2, ids2, vs2 := build(t)

	r1, err := Compact(vs1, dir1, ids1, nil)
	if err != nil {
		t.Fatalf("Compact (1): %v", err)
	}
	r2, err := Compact(vs2, dir2, ids2, nil)
	if err != nil {
		t.Fatalf("Compact (2): %v", err)
	}
	if r1.OutputFileName != r2.OutputFileName {
		t.Fatalf("output file names differ for equivalent input ID sequences: %q vs %q", r1.OutputFileName, r2.OutputFileName)
	}
}

// --- Additional edge cases ---

func TestCompactSingleTableCompactionDropsSafeTombstoneAndKeepsValue(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Tombstone: true},
	})

	result, err := Compact(vs, dir, []uint64{id1}, nil)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	entries := readAll(t, filepath.Join(dir, result.OutputFileName))
	if _, found := findEntry(entries, "b"); found {
		t.Fatalf("single-table full compaction: tombstone for b should be dropped, got %+v", entries)
	}
	e, found := findEntry(entries, "a")
	if !found || string(e.Value) != "1" {
		t.Fatalf("got %+v, want a=1 preserved", entries)
	}
}

func TestCompactEmptyInputListReturnsError(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)

	_, err := Compact(vs, dir, nil, nil)
	if !errors.Is(err, ErrNoInputTables) {
		t.Fatalf("Compact with no inputs: got %v, want ErrNoInputTables", err)
	}
}

func TestCompactUnknownTableIDReturnsError(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)
	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})

	_, err := Compact(vs, dir, []uint64{id1, 9999}, nil)
	if err == nil {
		t.Fatalf("Compact with an unknown table id: got nil error, want error")
	}
	if len(vs.Current().Tables) != 1 {
		t.Fatalf("Current after failed compaction: got %d tables, want 1 (unchanged)", len(vs.Current().Tables))
	}
}

func TestCompactDuplicateInputIDReturnsError(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)
	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})

	_, err := Compact(vs, dir, []uint64{id1, id1}, nil)
	if err == nil {
		t.Fatalf("Compact with a duplicate input id: got nil error, want error")
	}
}

func TestCompactMissingInputFileReturnsError(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpenManifest(t, dir)
	id1 := addTable(t, vs, dir, "t1.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
	id2 := addTable(t, vs, dir, "t2.sst", []sstable.Entry{{Key: []byte("b"), Value: []byte("2")}})

	if err := os.Remove(filepath.Join(dir, "t1.sst")); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	_, err := Compact(vs, dir, []uint64{id1, id2}, nil)
	if err == nil {
		t.Fatalf("Compact with a missing input file: got nil error, want error")
	}
	if len(vs.Current().Tables) != 2 {
		t.Fatalf("Current after failed compaction: got %d tables, want 2 (unchanged)", len(vs.Current().Tables))
	}
}
