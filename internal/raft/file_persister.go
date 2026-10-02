package raft

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/Kushall-07/forgedb/internal/storage/atomicfile"
)

// FilePersister is a Persister backed by a single file on disk. It is
// Phase 6's production implementation: SaveState writes the complete new
// state and atomically replaces the previous file via
// internal/storage/atomicfile.Write (create temp file -> write -> fsync ->
// rename -> best-effort directory fsync), the same protocol Phase 3 uses
// for the Manifest. A reader can therefore never observe a half-written
// file: the path either holds the previous complete state or the new
// complete state, never something in between.
//
// FilePersister deliberately does not attempt to be a general-purpose
// storage engine, and it is not related to internal/storage or the KV
// WAL: it exists solely to make a Node's own currentTerm, votedFor, and
// log durable. See docs/raft/phase6-raft-persistence.md.
//
// FilePersister is safe for concurrent use: SaveState calls are
// serialized by an internal mutex, so two concurrent saves can never
// interleave or be applied out of order on disk (see the package doc
// comment on ordering). In practice a Node only ever calls SaveState
// while holding its own lock, which already serializes every save; the
// mutex here is a second, independent guarantee that holds even if a
// FilePersister were ever shared or driven directly by a test.
type FilePersister struct {
	mu           sync.Mutex
	path         string
	snapshotPath string
}

// snapshotFileSuffix names the snapshot file FilePersister derives from
// its state file's own path (path + snapshotFileSuffix), so that
// NewFilePersister's existing single-argument signature (and therefore
// every pre-Phase-9 call site) never needed to change.
const snapshotFileSuffix = ".snapshot"

// NewFilePersister returns a FilePersister that reads and writes state at
// path, and reads and writes its snapshot (see Snapshot) at a second file
// derived from path (path+".snapshot"). It performs no I/O itself; both
// files (and their directory) are created on the first SaveState/
// SaveSnapshot call, following atomicfile.Write's normal
// directory-creation behavior.
func NewFilePersister(path string) *FilePersister {
	return &FilePersister{path: path, snapshotPath: path + snapshotFileSuffix}
}

// SaveState implements Persister.
func (p *FilePersister) SaveState(state PersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeState(state)
	if err != nil {
		return fmt.Errorf("raft: encode persisted state: %w", err)
	}
	if err := atomicfile.Write(p.path, data); err != nil {
		return fmt.Errorf("raft: write persisted state: %w", err)
	}
	return nil
}

// LoadState implements Persister. It returns ErrNoState if path does not
// exist (a brand new node), or a wrapped ErrCorrupt if the file exists
// but fails to decode -- see decodeState.
func (p *FilePersister) LoadState() (PersistentState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, ErrNoState
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("raft: read persisted state %s: %w", p.path, err)
	}
	return decodeState(data)
}

// SaveSnapshot implements Persister. It writes snap via the same
// write-temp/fsync/atomic-rename protocol SaveState uses, to a file
// distinct from the state file (see snapshotFileSuffix) -- a crash can
// therefore never corrupt one by interrupting a write to the other.
func (p *FilePersister) SaveSnapshot(snap Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeSnapshot(snap)
	if err != nil {
		return fmt.Errorf("raft: encode persisted snapshot: %w", err)
	}
	if err := atomicfile.Write(p.snapshotPath, data); err != nil {
		return fmt.Errorf("raft: write persisted snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot implements Persister. It returns ErrNoSnapshot if the
// snapshot file does not exist (a node that has never compacted its log),
// or a wrapped ErrCorrupt if it exists but fails to decode.
func (p *FilePersister) LoadSnapshot() (Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.snapshotPath)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, ErrNoSnapshot
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("raft: read persisted snapshot %s: %w", p.snapshotPath, err)
	}
	return decodeSnapshot(data)
}
