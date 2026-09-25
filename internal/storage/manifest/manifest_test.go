package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenCreatesInitialEmptyManifest(t *testing.T) {
	dir := t.TempDir()

	vs, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	v := vs.Current()
	if len(v.Tables) != 0 {
		t.Fatalf("Current: got %d tables, want 0", len(v.Tables))
	}
	if _, err := os.Stat(filepath.Join(dir, manifestFileName)); err != nil {
		t.Fatalf("expected an initial MANIFEST file to be created: %v", err)
	}
}

func TestAddTableAssignsIncreasingIDs(t *testing.T) {
	vs := mustOpen(t, t.TempDir())

	id1, err := vs.AddTable("000001.sst")
	if err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	id2, err := vs.AddTable("000002.sst")
	if err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	if id2 <= id1 {
		t.Fatalf("AddTable IDs: got id1=%d id2=%d, want id2 > id1", id1, id2)
	}

	v := vs.Current()
	if len(v.Tables) != 2 {
		t.Fatalf("Current: got %d tables, want 2", len(v.Tables))
	}
	if v.Tables[0].ID != id1 || v.Tables[0].FileName != "000001.sst" {
		t.Fatalf("Current[0]: got %+v, want ID=%d FileName=000001.sst", v.Tables[0], id1)
	}
	if v.Tables[1].ID != id2 || v.Tables[1].FileName != "000002.sst" {
		t.Fatalf("Current[1]: got %+v, want ID=%d FileName=000002.sst", v.Tables[1], id2)
	}
}

func TestRemoveTable(t *testing.T) {
	vs := mustOpen(t, t.TempDir())

	id1, _ := vs.AddTable("000001.sst")
	id2, _ := vs.AddTable("000002.sst")

	if err := vs.RemoveTable(id1); err != nil {
		t.Fatalf("RemoveTable: %v", err)
	}

	v := vs.Current()
	if len(v.Tables) != 1 || v.Tables[0].ID != id2 {
		t.Fatalf("Current after remove: got %+v, want only id2=%d", v.Tables, id2)
	}
}

func TestRemoveMissingTableIsNotAnError(t *testing.T) {
	vs := mustOpen(t, t.TempDir())

	if err := vs.RemoveTable(9999); err != nil {
		t.Fatalf("RemoveTable on missing ID: got %v, want nil", err)
	}
}

func TestReplaceTable(t *testing.T) {
	vs := mustOpen(t, t.TempDir())

	oldID, _ := vs.AddTable("old.sst")
	newID, err := vs.ReplaceTable(oldID, "new.sst")
	if err != nil {
		t.Fatalf("ReplaceTable: %v", err)
	}
	if newID == oldID {
		t.Fatalf("ReplaceTable: new ID equals old ID (%d)", oldID)
	}

	v := vs.Current()
	if len(v.Tables) != 1 {
		t.Fatalf("Current after replace: got %d tables, want 1", len(v.Tables))
	}
	if v.Tables[0].ID != newID || v.Tables[0].FileName != "new.sst" {
		t.Fatalf("Current after replace: got %+v, want ID=%d FileName=new.sst", v.Tables[0], newID)
	}
}

func TestRemoveTablesAtomicallyRemovesAllGivenIDs(t *testing.T) {
	vs := mustOpen(t, t.TempDir())

	id1, _ := vs.AddTable("000001.sst")
	id2, _ := vs.AddTable("000002.sst")
	id3, _ := vs.AddTable("000003.sst")

	if err := vs.RemoveTables([]uint64{id1, id2}); err != nil {
		t.Fatalf("RemoveTables: %v", err)
	}

	v := vs.Current()
	if len(v.Tables) != 1 || v.Tables[0].ID != id3 {
		t.Fatalf("Current after RemoveTables: got %+v, want only id3=%d", v.Tables, id3)
	}
}

func TestRemoveTablesWithMissingIDIsNotAnError(t *testing.T) {
	vs := mustOpen(t, t.TempDir())
	id1, _ := vs.AddTable("000001.sst")

	if err := vs.RemoveTables([]uint64{id1, 9999}); err != nil {
		t.Fatalf("RemoveTables with one missing ID: got %v, want nil", err)
	}
	if len(vs.Current().Tables) != 0 {
		t.Fatalf("Current after RemoveTables: got %+v, want empty", vs.Current().Tables)
	}
}

func TestReplaceTablesAtomicallyReplacesMultipleInputs(t *testing.T) {
	vs := mustOpen(t, t.TempDir())

	id1, _ := vs.AddTable("a.sst")
	id2, _ := vs.AddTable("b.sst")
	id3, _ := vs.AddTable("c.sst")

	newID, err := vs.ReplaceTables([]uint64{id1, id2}, "merged.sst")
	if err != nil {
		t.Fatalf("ReplaceTables: %v", err)
	}
	if newID == id1 || newID == id2 || newID == id3 {
		t.Fatalf("ReplaceTables: new ID %d collides with an existing ID", newID)
	}

	v := vs.Current()
	byID := map[uint64]string{}
	for _, tm := range v.Tables {
		byID[tm.ID] = tm.FileName
	}
	if len(v.Tables) != 2 {
		t.Fatalf("Current after ReplaceTables: got %d tables, want 2", len(v.Tables))
	}
	if byID[id3] != "c.sst" {
		t.Fatalf("Current after ReplaceTables: untouched table id3=%d got %+v, want c.sst", id3, v.Tables)
	}
	if byID[newID] != "merged.sst" {
		t.Fatalf("Current after ReplaceTables: new table got %+v, want merged.sst at id %d", v.Tables, newID)
	}
}

func TestReopenReconstructsVersionSet(t *testing.T) {
	dir := t.TempDir()

	vs1 := mustOpen(t, dir)
	id1, err := vs1.AddTable("000001.sst")
	if err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	id2, err := vs1.AddTable("000002.sst")
	if err != nil {
		t.Fatalf("AddTable: %v", err)
	}

	vs2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}

	v := vs2.Current()
	if len(v.Tables) != 2 {
		t.Fatalf("Current after reopen: got %d tables, want 2", len(v.Tables))
	}
	if v.Tables[0].ID != id1 || v.Tables[0].FileName != "000001.sst" {
		t.Fatalf("Current[0] after reopen: got %+v", v.Tables[0])
	}
	if v.Tables[1].ID != id2 || v.Tables[1].FileName != "000002.sst" {
		t.Fatalf("Current[1] after reopen: got %+v", v.Tables[1])
	}

	// A table added after reopening must get an ID that keeps increasing
	// from where the persisted nextID left off, not restart from 1 and
	// risk colliding with an existing table's ID.
	id3, err := vs2.AddTable("000003.sst")
	if err != nil {
		t.Fatalf("AddTable after reopen: %v", err)
	}
	if id3 <= id2 {
		t.Fatalf("AddTable after reopen: got id3=%d, want > id2=%d", id3, id2)
	}
}

func TestReopenAfterRemoveReflectsRemoval(t *testing.T) {
	dir := t.TempDir()

	vs1 := mustOpen(t, dir)
	id1, _ := vs1.AddTable("000001.sst")
	_, _ = vs1.AddTable("000002.sst")
	if err := vs1.RemoveTable(id1); err != nil {
		t.Fatalf("RemoveTable: %v", err)
	}

	vs2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	v := vs2.Current()
	if len(v.Tables) != 1 || v.Tables[0].FileName != "000002.sst" {
		t.Fatalf("Current after reopen: got %+v, want only 000002.sst", v.Tables)
	}
}

func TestOpenRejectsCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	mustOpen(t, dir) // create a valid initial manifest first

	path := filepath.Join(dir, manifestFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[0] ^= 0xFF // corrupt the magic
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Open(dir)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with corrupted manifest: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestOpenRejectsManifestChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpen(t, dir)
	if _, err := vs.AddTable("000001.sst"); err != nil {
		t.Fatalf("AddTable: %v", err)
	}

	path := filepath.Join(dir, manifestFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xFF // corrupt the trailing checksum
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Open(dir)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with checksum mismatch: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeManifestRejectsOversizedTableCount(t *testing.T) {
	mf := manifestFile{nextID: 1, tables: []tableMeta{{id: 1, fileName: "a.sst"}}}
	buf := encodeManifest(mf)
	// numTables field is bytes [20:24].
	buf[20], buf[21], buf[22], buf[23] = 0xFF, 0xFF, 0xFF, 0xFF

	_, err := decodeManifest(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeManifest with oversized table count: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeManifestRejectsInvalidFileNameLength(t *testing.T) {
	mf := manifestFile{nextID: 1, tables: []tableMeta{{id: 1, fileName: "a.sst"}}}
	buf := encodeManifest(mf)
	// The first (only) table entry's fileNameLen field is at
	// manifestHeaderSize+8 .. +12.
	off := manifestHeaderSize + 8
	buf[off], buf[off+1], buf[off+2], buf[off+3] = 0xFF, 0xFF, 0xFF, 0xFF

	_, err := decodeManifest(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeManifest with invalid file name length: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeManifestRejectsTruncatedFile(t *testing.T) {
	mf := manifestFile{nextID: 1, tables: []tableMeta{{id: 1, fileName: "a.sst"}}}
	buf := encodeManifest(mf)

	_, err := decodeManifest(buf[:manifestHeaderSize])
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeManifest truncated before any table entry: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeManifestRejectsUnsupportedVersion(t *testing.T) {
	mf := manifestFile{nextID: 1}
	buf := encodeManifest(mf)
	buf[8] = 99 // formatVersion field
	// Leave the checksum stale on purpose: an unsupported version must be
	// rejected on its own, and a real corrupted-version file would not
	// have a matching checksum either.

	_, err := decodeManifest(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeManifest with unsupported version: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestPersistFailureLeavesPreviousManifestIntact(t *testing.T) {
	dir := t.TempDir()
	vs := mustOpen(t, dir)
	if _, err := vs.AddTable("000001.sst"); err != nil {
		t.Fatalf("AddTable: %v", err)
	}

	path := filepath.Join(dir, manifestFileName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Make the manifest's directory read-only so the next persist's
	// rename (or temp-file creation) fails, simulating an update that
	// cannot complete.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Skipf("cannot make directory read-only on this platform: %v", err)
	}
	defer os.Chmod(dir, 0o700)

	_, addErr := vs.AddTable("000002.sst")

	os.Chmod(dir, 0o700) // restore so the test can read the file back

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after failed update: %v", err)
	}

	if addErr == nil {
		// The platform did not actually block the write (e.g. running as
		// an account that ignores directory permissions); nothing to
		// assert about failure behavior in that case.
		t.Skip("AddTable unexpectedly succeeded under a read-only directory on this platform")
	}
	if string(before) != string(after) {
		t.Fatalf("manifest file changed after a failed update: before=%q after=%q", before, after)
	}

	// The VersionSet's in-memory state must also have rolled back, so a
	// subsequent successful update is still consistent with disk.
	if len(vs.Current().Tables) != 1 {
		t.Fatalf("VersionSet in-memory state after failed update: got %d tables, want 1 (rolled back)", len(vs.Current().Tables))
	}
}

func mustOpen(t *testing.T, dir string) *VersionSet {
	t.Helper()
	vs, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return vs
}
