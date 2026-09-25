package manifest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/Kushall-07/forgedb/internal/storage/atomicfile"
)

// manifestFileName is the Manifest's file name within a VersionSet's
// directory. Phase 3 uses a single, whole-state Manifest file; there is
// no rotation or edit-log compaction.
const manifestFileName = "MANIFEST"

// TableMeta describes one SSTable that is part of the active database
// version: its file name (relative to the VersionSet's directory) and
// its ID. IDs are assigned in increasing order as tables are added, so a
// larger ID is a newer table -- this is what a later phase's LSM merge
// read will use to let a newer table's entry (including a tombstone)
// shadow an older table's entry for the same key, exactly as a later
// MemTable Put already shadows an earlier one.
type TableMeta struct {
	ID       uint64
	FileName string
}

// Version is an immutable snapshot of the active SSTable set at a point
// in time, ordered by ID (oldest first). Mutating a Version obtained from
// VersionSet.Current does not affect the VersionSet.
type Version struct {
	Tables []TableMeta
}

// VersionSet tracks the active Version and persists every change to a
// Manifest file, so the active SSTable set can be reconstructed after a
// restart (see Open). It is safe for concurrent use.
//
// VersionSet deliberately does not know how to open or read an SSTable
// file -- it only tracks file names and IDs. A caller reconstructing
// state after a restart combines VersionSet.Open (to learn which files
// are active) with sstable.Open (to actually open them); see
// docs/storage/phase3-sstables-manifest.md for a worked example.
type VersionSet struct {
	mu           sync.RWMutex
	manifestPath string
	nextID       uint64
	tables       map[uint64]string
}

// Open loads dir's Manifest file and returns a VersionSet reconstructing
// the active SSTable set it describes. If dir has no Manifest yet, Open
// creates an initial, empty one (nextID starting at 1, no active tables)
// and returns a VersionSet for it. Open returns an error, and no usable
// VersionSet, if an existing Manifest file fails structural validation --
// a corrupt Manifest is a hard failure here, not something this package
// attempts to auto-repair.
func Open(dir string) (*VersionSet, error) {
	path := filepath.Join(dir, manifestFileName)

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		vs := &VersionSet{manifestPath: path, nextID: 1, tables: map[uint64]string{}}
		if err := vs.persist(); err != nil {
			return nil, fmt.Errorf("manifest: create initial manifest at %s: %w", path, err)
		}
		return vs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("manifest: read %s: %w", path, err)
	}

	mf, err := decodeManifest(data)
	if err != nil {
		return nil, fmt.Errorf("manifest: decode %s: %w", path, err)
	}

	tables := make(map[uint64]string, len(mf.tables))
	for _, t := range mf.tables {
		tables[t.id] = t.fileName
	}
	return &VersionSet{manifestPath: path, nextID: mf.nextID, tables: tables}, nil
}

// Current returns a snapshot of the active version.
func (vs *VersionSet) Current() Version {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.currentLocked()
}

func (vs *VersionSet) currentLocked() Version {
	tables := make([]TableMeta, 0, len(vs.tables))
	for id, name := range vs.tables {
		tables = append(tables, TableMeta{ID: id, FileName: name})
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].ID < tables[j].ID })
	return Version{Tables: tables}
}

// AddTable registers fileName as a new active SSTable, assigns it the
// next sequence ID, persists the updated Manifest atomically, and
// returns the assigned ID. If persisting fails, AddTable returns the
// error and leaves the VersionSet's in-memory and on-disk state exactly
// as it was before the call -- the new table is not considered active.
func (vs *VersionSet) AddTable(fileName string) (uint64, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	prevTables, prevNextID := vs.snapshotLocked()

	id := vs.nextID
	vs.tables[id] = fileName
	vs.nextID++

	if err := vs.persist(); err != nil {
		vs.restoreLocked(prevTables, prevNextID)
		return 0, fmt.Errorf("manifest: persist after add: %w", err)
	}
	return id, nil
}

// RemoveTable removes id from the active version and persists the
// update. Removing an ID that is not currently active is not an error.
// If persisting fails, RemoveTable returns the error and leaves state
// unchanged, exactly as AddTable does.
func (vs *VersionSet) RemoveTable(id uint64) error {
	return vs.RemoveTables([]uint64{id})
}

// RemoveTables atomically removes every ID in ids from the active
// version in a single Manifest update. Removing an ID that is not
// currently active is not an error, exactly like RemoveTable. This is
// the building block a compaction that discards all of its input tables'
// content (every entry was a tombstone that could be safely dropped, so
// there is no replacement table to add) uses to retire its inputs
// without any intermediate Manifest state that describes only some of
// them removed.
func (vs *VersionSet) RemoveTables(ids []uint64) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	prevTables, prevNextID := vs.snapshotLocked()
	for _, id := range ids {
		delete(vs.tables, id)
	}

	if err := vs.persist(); err != nil {
		vs.restoreLocked(prevTables, prevNextID)
		return fmt.Errorf("manifest: persist after remove: %w", err)
	}
	return nil
}

// ReplaceTable atomically removes oldID and adds newFileName as a new
// table in a single Manifest update, returning the new table's ID.
func (vs *VersionSet) ReplaceTable(oldID uint64, newFileName string) (uint64, error) {
	return vs.ReplaceTables([]uint64{oldID}, newFileName)
}

// ReplaceTables atomically removes every ID in oldIDs and adds
// newFileName as a single new table, in one Manifest update, returning
// the new table's ID. This generalizes ReplaceTable to a compaction that
// merges more than one input table: the Manifest is never observed
// describing neither the old tables nor the new one, and never
// describing only some of the old tables removed.
func (vs *VersionSet) ReplaceTables(oldIDs []uint64, newFileName string) (uint64, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	prevTables, prevNextID := vs.snapshotLocked()

	for _, id := range oldIDs {
		delete(vs.tables, id)
	}
	newID := vs.nextID
	vs.tables[newID] = newFileName
	vs.nextID++

	if err := vs.persist(); err != nil {
		vs.restoreLocked(prevTables, prevNextID)
		return 0, fmt.Errorf("manifest: persist after replace: %w", err)
	}
	return newID, nil
}

func (vs *VersionSet) snapshotLocked() (map[uint64]string, uint64) {
	tables := make(map[uint64]string, len(vs.tables))
	for k, v := range vs.tables {
		tables[k] = v
	}
	return tables, vs.nextID
}

func (vs *VersionSet) restoreLocked(tables map[uint64]string, nextID uint64) {
	vs.tables = tables
	vs.nextID = nextID
}

// persist serializes the current state and atomically replaces the
// Manifest file with it (write temp file, fsync, atomic rename, fsync
// directory -- see atomicfile.Write). Callers must hold vs.mu.
//
// Because atomicfile.Write never modifies the destination path except by
// a single atomic rename at the very end, a failure at any step before
// that rename leaves the previous Manifest file on disk completely
// untouched: a failed update never replaces a valid Manifest with a
// truncated or partially written one. Combined with the in-memory
// rollback in AddTable/RemoveTable/ReplaceTable, a failed update leaves
// both the in-memory VersionSet and the on-disk Manifest exactly as they
// were before the call.
func (vs *VersionSet) persist() error {
	mf := manifestFile{nextID: vs.nextID}
	for id, name := range vs.tables {
		mf.tables = append(mf.tables, tableMeta{id: id, fileName: name})
	}
	sort.Slice(mf.tables, func(i, j int) bool { return mf.tables[i].id < mf.tables[j].id })
	return atomicfile.Write(vs.manifestPath, encodeManifest(mf))
}
