package storage

import (
	"fmt"
	"path/filepath"

	"github.com/Kushall-07/forgedb/internal/storage/memtable"
	"github.com/Kushall-07/forgedb/internal/storage/wal"
)

// walFileName is the WAL's file name within a MemStore's data directory.
// Phase 2 uses a single, unsegmented WAL file; rotation and segmentation
// are not implemented yet.
const walFileName = "wal.log"

// MemStore is the Phase 2 Store implementation: an in-memory MemTable
// made durable by a write-ahead log. Every Put and Delete is appended to
// the WAL and fsynced before the MemTable is mutated (see Put/Delete), so
// any MemTable state visible to a caller is always reconstructible from
// the WAL after a crash. NewMemStore replays any existing WAL in its data
// directory to rebuild the MemTable before returning.
type MemStore struct {
	mt  *memtable.MemTable
	log *wal.WAL
}

// NewMemStore opens (or creates) a WAL-backed Store rooted at dataDir,
// creating the directory if it does not already exist, and replays any
// existing WAL contents to reconstruct the MemTable before returning.
func NewMemStore(dataDir string) (*MemStore, error) {
	log, err := wal.Open(filepath.Join(dataDir, walFileName))
	if err != nil {
		return nil, fmt.Errorf("storage: open wal: %w", err)
	}

	mt := memtable.New()
	err = log.Replay(func(rec wal.Record) error {
		switch rec.Type {
		case wal.OpPut:
			return mt.Put(rec.Key, rec.Value)
		case wal.OpDelete:
			return mt.Delete(rec.Key)
		default:
			return fmt.Errorf("storage: unknown wal record type %d", rec.Type)
		}
	})
	if err != nil {
		log.Close()
		return nil, fmt.Errorf("storage: recover from wal: %w", err)
	}

	return &MemStore{mt: mt, log: log}, nil
}

var _ Store = (*MemStore)(nil)

// Put appends the mutation to the WAL and syncs it -- the durability
// boundary -- before applying it to the MemTable. If the WAL append or
// sync fails, the MemTable is left unmutated and the error is returned.
func (s *MemStore) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if err := s.log.Append(wal.Record{Type: wal.OpPut, Key: key, Value: value}); err != nil {
		return fmt.Errorf("storage: wal append: %w", err)
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("storage: wal sync: %w", err)
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

// Delete appends a tombstone to the WAL and syncs it before applying it
// to the MemTable, following the same durability-before-memory ordering
// as Put.
func (s *MemStore) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if err := s.log.Append(wal.Record{Type: wal.OpDelete, Key: key}); err != nil {
		return fmt.Errorf("storage: wal append: %w", err)
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("storage: wal sync: %w", err)
	}
	return s.mt.Delete(key)
}

// Close closes the underlying WAL file.
func (s *MemStore) Close() error {
	return s.log.Close()
}
