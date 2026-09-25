// Package sstable implements ForgeDB's immutable, block-based SSTable
// format: the on-disk representation that an immutable MemTable is
// eventually flushed into, once a MemTable is full (see
// docs/storage/phase3-sstables-manifest.md for the full format and the
// current integration boundary).
//
// An SSTable is built once, by a Writer, and never modified afterward.
// Every entry a Reader returns is either the live value for a key, an
// explicit tombstone recording that the key was deleted, or a report that
// the key is not present in this table at all -- these are three distinct
// outcomes (see Result), which matters once a later phase merges multiple
// SSTables and MemTables together: a tombstone in a newer table must be
// able to shadow a live value in an older one.
//
// This package knows nothing about the Manifest/Version Set (which
// SSTables currently make up the database), MemTable, WAL, Raft, or
// networking. It only implements reading and writing a single SSTable
// file.
package sstable

import (
	"errors"
	"hash/crc32"
)

// ErrCorrupt indicates an SSTable file (or one of its sections) fails
// structural validation: a bad magic number or checksum, an offset or
// length that falls outside the file, or a header declaring an impossible
// value. It is always returned rather than silently worked around --
// Open and Get never guess at the intended content of a malformed file.
var ErrCorrupt = errors.New("sstable: corrupt file")

// crcTable is the CRC-32C (Castagnoli) polynomial table, the same one the
// wal package uses, for the same reason: better error detection than the
// default IEEE polynomial on short, structured records.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

const (
	// maxKeySize and maxValueSize bound how large a single key or value
	// may be, matching the wal package's limits. Every length read from
	// an SSTable section is validated against these bounds before any
	// buffer is allocated from it, so a corrupted length field can never
	// trigger an unbounded allocation.
	maxKeySize   = 1 << 20 // 1 MiB
	maxValueSize = 1 << 26 // 64 MiB
)

// Entry is a single key/value pair, or tombstone, as stored in or read
// from an SSTable.
type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

// Result describes the outcome of a Reader.Get lookup.
type Result int

const (
	// NotFound means the key does not appear in this table at all.
	NotFound Result = iota
	// Found means the key has a live value in this table.
	Found
	// Tombstone means the key was explicitly deleted in this table. This
	// is distinct from NotFound: a tombstone must be able to shadow a
	// value for the same key in an older SSTable once later phases merge
	// tables together, so it is preserved rather than treated the same
	// as an absent key.
	Tombstone
)
