package manifest_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/Kushall-07/forgedb/internal/storage/manifest"
	"github.com/Kushall-07/forgedb/internal/storage/sstable"
)

// This test exercises the full Phase 3 recovery path end to end, gluing
// the sstable and manifest packages together the way a caller eventually
// will: create SSTable(s), record them in the Manifest, close everything,
// reopen the Manifest, discover the active SSTables it names, and open
// each of them -- without either package needing to know about the
// other's types.

func TestReopenRecoversActiveSSTablesFromManifest(t *testing.T) {
	dir := t.TempDir()

	table1 := buildSSTable(t, dir, "000001.sst", []sstable.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	})
	table2 := buildSSTable(t, dir, "000002.sst", []sstable.Entry{
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("d"), Tombstone: true},
	})

	vs1, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("manifest.Open: %v", err)
	}
	id1, err := vs1.AddTable(table1)
	if err != nil {
		t.Fatalf("AddTable(%s): %v", table1, err)
	}
	id2, err := vs1.AddTable(table2)
	if err != nil {
		t.Fatalf("AddTable(%s): %v", table2, err)
	}

	// Simulate a restart: nothing keeps vs1 or any open Reader alive
	// across this point.
	vs2, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("manifest.Open (reopen): %v", err)
	}

	version := vs2.Current()
	if len(version.Tables) != 2 {
		t.Fatalf("recovered version: got %d tables, want 2", len(version.Tables))
	}

	byID := map[uint64]string{}
	for _, tm := range version.Tables {
		byID[tm.ID] = tm.FileName
	}
	if byID[id1] != table1 {
		t.Fatalf("recovered table %d: got %q, want %q", id1, byID[id1], table1)
	}
	if byID[id2] != table2 {
		t.Fatalf("recovered table %d: got %q, want %q", id2, byID[id2], table2)
	}

	// Open every recovered SSTable and verify its content is intact.
	for _, tm := range version.Tables {
		r, err := sstable.Open(filepath.Join(dir, tm.FileName))
		if err != nil {
			t.Fatalf("sstable.Open(%s): %v", tm.FileName, err)
		}
		defer r.Close()

		switch tm.FileName {
		case table1:
			assertFound(t, r, "a", "1")
			assertFound(t, r, "b", "2")
		case table2:
			assertFound(t, r, "c", "3")
			assertTombstone(t, r, "d")
		}
	}
}

func TestReopenAfterReplaceRecoversOnlyReplacementTable(t *testing.T) {
	dir := t.TempDir()

	oldTable := buildSSTable(t, dir, "old.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("1")}})
	newTable := buildSSTable(t, dir, "new.sst", []sstable.Entry{{Key: []byte("a"), Value: []byte("2")}})

	vs1, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("manifest.Open: %v", err)
	}
	oldID, err := vs1.AddTable(oldTable)
	if err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	if _, err := vs1.ReplaceTable(oldID, newTable); err != nil {
		t.Fatalf("ReplaceTable: %v", err)
	}

	vs2, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("manifest.Open (reopen): %v", err)
	}
	version := vs2.Current()
	if len(version.Tables) != 1 || version.Tables[0].FileName != newTable {
		t.Fatalf("recovered version after replace: got %+v, want only %q", version.Tables, newTable)
	}
}

func buildSSTable(t *testing.T, dir, name string, entries []sstable.Entry) string {
	t.Helper()
	w := sstable.NewWriter()
	for _, e := range entries {
		if err := w.Add(e.Key, e.Value, e.Tombstone); err != nil {
			t.Fatalf("Add(%q): %v", e.Key, err)
		}
	}
	if err := w.Build(filepath.Join(dir, name)); err != nil {
		t.Fatalf("Build(%s): %v", name, err)
	}
	return name
}

func assertFound(t *testing.T, r *sstable.Reader, key, want string) {
	t.Helper()
	value, result, err := r.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if result != sstable.Found || !bytes.Equal(value, []byte(want)) {
		t.Fatalf("Get(%q): got value=%q result=%v, want %q/Found", key, value, result, want)
	}
}

func assertTombstone(t *testing.T, r *sstable.Reader, key string) {
	t.Helper()
	_, result, err := r.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if result != sstable.Tombstone {
		t.Fatalf("Get(%q): got %v, want Tombstone", key, result)
	}
}
