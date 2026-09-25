// Package manifest implements ForgeDB's Manifest: the on-disk record of
// which SSTables currently make up the active database state, and the
// VersionSet: a clean in-memory representation of that active set.
//
// This package knows nothing about the SSTable reader/writer, MemTable,
// WAL, Raft, or networking. It only tracks SSTable file names and the
// IDs used to order them; opening the SSTables it names, and deciding
// when to create new ones, are the caller's responsibility (see
// docs/storage/phase3-sstables-manifest.md for where this sits in the
// architecture and how a caller is expected to glue the two together).
package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// ErrCorrupt indicates a Manifest file fails structural validation: a bad
// magic number or checksum, an unsupported format version, or a header
// declaring an impossible table count or file name length. It is always
// returned rather than silently worked around.
var ErrCorrupt = errors.New("manifest: corrupt file")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var magic = [8]byte{'F', 'o', 'r', 'g', 'e', 'M', 'F', '1'}

const formatVersion1 = 1

// maxTables and maxFileNameLen bound how large a table count or file name
// length declared in a Manifest file may be. Every such value is
// validated against these limits before it is used to size a slice or
// string, so a corrupted count or length can never trigger an unbounded
// allocation.
const (
	maxTables      = 1 << 20
	maxFileNameLen = 4096
)

// tableMeta is one active SSTable's Manifest entry: its assigned ID and
// its file name (relative to the VersionSet's directory).
type tableMeta struct {
	id       uint64
	fileName string
}

// manifestFile is the full decoded content of a Manifest file: the next
// ID to assign to a new table, and the currently active tables.
type manifestFile struct {
	nextID uint64
	tables []tableMeta
}

// The Manifest file format is a full snapshot of the current state, not
// an append-only edit log: every update writes out the complete new
// state and atomically replaces the previous file (see
// atomicfile.Write and VersionSet.persist). This keeps recovery trivial
// -- there is exactly one file to read, with no edit history to replay --
// at the cost of rewriting the whole (typically small) table list on
// every change, which is an acceptable trade for Phase 3's scale.
//
//	magic (8 bytes) | formatVersion (u32 LE) | nextID (u64 LE) | numTables (u32 LE) |
//	  [ id (u64 LE) | fileNameLen (u32 LE) | fileName ] * numTables |
//	checksum (u32 LE, CRC-32C over every preceding byte)
const manifestHeaderSize = 8 + 4 + 8 + 4

func encodeManifest(m manifestFile) []byte {
	size := manifestHeaderSize
	for _, t := range m.tables {
		size += 8 + 4 + len(t.fileName)
	}
	buf := make([]byte, size+4)

	copy(buf[0:8], magic[:])
	binary.LittleEndian.PutUint32(buf[8:12], formatVersion1)
	binary.LittleEndian.PutUint64(buf[12:20], m.nextID)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(len(m.tables)))

	off := manifestHeaderSize
	for _, t := range m.tables {
		binary.LittleEndian.PutUint64(buf[off:off+8], t.id)
		binary.LittleEndian.PutUint32(buf[off+8:off+12], uint32(len(t.fileName)))
		off += 12
		copy(buf[off:], t.fileName)
		off += len(t.fileName)
	}

	checksum := crc32.Checksum(buf[:off], crcTable)
	binary.LittleEndian.PutUint32(buf[off:], checksum)
	return buf
}

func decodeManifest(data []byte) (manifestFile, error) {
	if len(data) < manifestHeaderSize+4 {
		return manifestFile{}, fmt.Errorf("manifest: file too small (%d bytes): %w", len(data), ErrCorrupt)
	}
	if !bytes.Equal(data[0:8], magic[:]) {
		return manifestFile{}, fmt.Errorf("manifest: bad magic: %w", ErrCorrupt)
	}
	version := binary.LittleEndian.Uint32(data[8:12])
	if version != formatVersion1 {
		return manifestFile{}, fmt.Errorf("manifest: unsupported format version %d: %w", version, ErrCorrupt)
	}

	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	body := data[:len(data)-4]
	if crc32.Checksum(body, crcTable) != checksum {
		return manifestFile{}, fmt.Errorf("manifest: checksum mismatch: %w", ErrCorrupt)
	}

	nextID := binary.LittleEndian.Uint64(data[12:20])
	numTables := binary.LittleEndian.Uint32(data[20:24])
	if numTables > maxTables {
		return manifestFile{}, fmt.Errorf("manifest: table count %d exceeds maximum of %d: %w", numTables, maxTables, ErrCorrupt)
	}

	tables := make([]tableMeta, 0, numTables)
	off := manifestHeaderSize
	for i := uint32(0); i < numTables; i++ {
		if len(body)-off < 8+4 {
			return manifestFile{}, fmt.Errorf("manifest: truncated table entry %d: %w", i, ErrCorrupt)
		}
		id := binary.LittleEndian.Uint64(body[off : off+8])
		nameLen := binary.LittleEndian.Uint32(body[off+8 : off+12])
		off += 12
		if nameLen == 0 || nameLen > maxFileNameLen {
			return manifestFile{}, fmt.Errorf("manifest: invalid file name length %d at entry %d: %w", nameLen, i, ErrCorrupt)
		}
		if len(body)-off < int(nameLen) {
			return manifestFile{}, fmt.Errorf("manifest: truncated file name at entry %d: %w", i, ErrCorrupt)
		}
		name := string(body[off : off+int(nameLen)])
		off += int(nameLen)
		tables = append(tables, tableMeta{id: id, fileName: name})
	}
	if off != len(body) {
		return manifestFile{}, fmt.Errorf("manifest: trailing bytes after table entries: %w", ErrCorrupt)
	}

	return manifestFile{nextID: nextID, tables: tables}, nil
}
