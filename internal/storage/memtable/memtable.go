// Package memtable implements ForgeDB's in-memory, ordered key/value
// structure: the MemTable. It is the Phase 1 building block of the
// eventual LSM storage engine (MemTable -> immutable MemTable -> SSTables).
//
// MemTable knows nothing about Raft, replication, networking, or disk
// persistence. It only implements ordered key/value storage semantics,
// including tombstone-based deletes, so that later phases can layer a
// WAL and SSTables underneath it without changing this contract.
package memtable

import (
	"errors"
	"sync"
)

// ErrKeyNotFound is returned when a key has no live (non-deleted) entry.
var ErrKeyNotFound = errors.New("memtable: key not found")

// ErrEmptyKey is returned when an operation is given a nil or zero-length
// key. Empty keys are never valid; empty values are valid and distinct
// from a missing or deleted key.
var ErrEmptyKey = errors.New("memtable: key must not be empty")

// MemTable is a concurrency-safe, ordered key/value store backed by a
// skip list. Deletes are recorded as tombstones rather than removing the
// entry outright, matching how deletes must behave once the MemTable is
// merged with immutable MemTables and SSTables in later phases.
type MemTable struct {
	mu   sync.RWMutex
	list *skipList
}

// New creates an empty MemTable.
func New() *MemTable {
	return &MemTable{list: newSkipList()}
}

// Put inserts key, or updates it if already present, to value. Both key
// and value are copied, so the caller may freely reuse or mutate the
// slices it passed in without affecting stored state.
func (m *MemTable) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	keyCopy := append([]byte(nil), key...)
	var valueCopy []byte
	if len(value) > 0 {
		valueCopy = append([]byte(nil), value...)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.upsert(keyCopy, valueCopy, false)
	return nil
}

// Get returns a copy of the value stored for key. found is false if the
// key was never written, or if the most recent write was a Delete. The
// returned slice is a copy, so the caller cannot mutate internal state
// through it.
func (m *MemTable) Get(key []byte) (value []byte, found bool) {
	if len(key) == 0 {
		return nil, false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.list.search(key)
	if !ok || node.deleted {
		return nil, false
	}
	return append([]byte(nil), node.value...), true
}

// Delete marks key as logically absent by writing a tombstone. Deleting a
// key that does not currently exist, or was already deleted, is not an
// error: it simply records the tombstone.
func (m *MemTable) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	keyCopy := append([]byte(nil), key...)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.upsert(keyCopy, nil, true)
	return nil
}
