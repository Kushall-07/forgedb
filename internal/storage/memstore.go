package storage

import "github.com/Kushall-07/forgedb/internal/storage/memtable"

// MemStore is the Phase 1 Store implementation: a single in-memory
// MemTable. It holds no on-disk state and survives only for the lifetime
// of the process. Phase 2 is expected to add a WAL-backed Store that
// recovers this same key/value contract from disk; nothing in this type
// depends on that yet to exist.
type MemStore struct {
	mt *memtable.MemTable
}

// NewMemStore creates an empty, in-memory Store.
func NewMemStore() *MemStore {
	return &MemStore{mt: memtable.New()}
}

var _ Store = (*MemStore)(nil)

func (s *MemStore) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	return s.mt.Put(key, value)
}

func (s *MemStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	value, found := s.mt.Get(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

func (s *MemStore) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	return s.mt.Delete(key)
}

// Close is a no-op: MemStore holds no file handles or other resources to
// release. It exists to satisfy Store so future disk-backed
// implementations can be swapped in without changing callers.
func (s *MemStore) Close() error {
	return nil
}
