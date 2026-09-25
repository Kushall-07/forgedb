// Package storage defines ForgeDB's storage-engine boundary and its
// Phase 1 implementation.
//
// This package is intentionally ignorant of Raft: it knows nothing about
// terms, log indices, leaders, quorums, or how a command arrived here. It
// only implements key/value storage semantics. A future State Machine
// package will sit between Raft and Store, translating committed Raft log
// entries into calls against this interface. That boundary lets later
// phases add a WAL and SSTables underneath Store without changing this
// contract or making Raft aware of storage internals.
package storage

import "errors"

// ErrKeyNotFound is returned by Get when the key has no live value: it was
// never written, or was written and then deleted.
var ErrKeyNotFound = errors.New("storage: key not found")

// ErrEmptyKey is returned when an operation is given a nil or zero-length
// key. Empty keys are never valid. Empty values are valid and are treated
// as ordinary stored values, distinct from a missing or deleted key.
var ErrEmptyKey = errors.New("storage: key must not be empty")

// Store is the storage engine's public contract. Implementations must
// distinguish a missing key from a stored value (via ErrKeyNotFound) and
// must not let callers mutate stored state through slices passed to Put
// or returned from Get.
type Store interface {
	// Put inserts key, or updates it if already present, to value.
	Put(key, value []byte) error

	// Get returns the value stored for key, or ErrKeyNotFound if the key
	// is missing or has been deleted.
	Get(key []byte) ([]byte, error)

	// Delete marks key as logically absent. Deleting a key that does not
	// exist is not an error.
	Delete(key []byte) error

	// Close releases any resources held by the store.
	Close() error
}
