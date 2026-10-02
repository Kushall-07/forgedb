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

	// liveCount and liveBytes are maintained incrementally by Put/Delete
	// (via skipList.upsert's existed/wasDeleted/oldValueLen return values)
	// so Len/Bytes never need to walk the list -- see Len's doc comment
	// and docs/observability/phase12-observability.md's rule against
	// scanning the whole database on every metrics scrape.
	liveCount int
	liveBytes int64
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
	existed, wasDeleted, oldValueLen := m.list.upsert(keyCopy, valueCopy, false)
	switch {
	case !existed, wasDeleted:
		m.liveCount++
		m.liveBytes += int64(len(keyCopy)) + int64(len(valueCopy))
	default: // existed and was already live: value replaced in place
		m.liveBytes += int64(len(valueCopy)) - int64(oldValueLen)
	}
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
	existed, wasDeleted, oldValueLen := m.list.upsert(keyCopy, nil, true)
	if existed && !wasDeleted {
		m.liveCount--
		m.liveBytes -= int64(len(keyCopy)) + int64(oldValueLen)
	}
	return nil
}

// Len returns the number of currently live (non-tombstone) entries. It
// is O(1): liveCount is maintained incrementally by Put/Delete, not
// computed by scanning the list, so it is cheap to call on every
// metrics scrape (see docs/observability/phase12-observability.md).
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.liveCount
}

// Bytes returns the approximate total size, in bytes, of every
// currently live entry's key plus value. Like Len, it is O(1).
func (m *MemTable) Bytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.liveBytes
}

// Entry is a single live key/value pair, as returned by All.
type Entry struct {
	Key   []byte
	Value []byte
}

// All returns every currently live (non-tombstone) entry, in ascending
// key order, each a fresh copy safe for the caller to retain. It exists
// for Phase 9 state-machine snapshotting (see internal/storage.Store.Snapshot);
// tombstones are deliberately omitted, since a deleted key's absence is
// already fully captured by the key simply not appearing.
func (m *MemTable) All() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []Entry
	m.list.forEach(func(n *skipListNode) {
		if n.deleted {
			return
		}
		out = append(out, Entry{
			Key:   append([]byte(nil), n.key...),
			Value: append([]byte(nil), n.value...),
		})
	})
	return out
}
