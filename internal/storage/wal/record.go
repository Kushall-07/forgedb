package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// OpType identifies the logical operation a Record represents.
type OpType uint8

const (
	// OpPut represents a Put(key, value) mutation.
	OpPut OpType = 1
	// OpDelete represents a Delete(key) mutation (a tombstone). Value is
	// always empty for a Delete record.
	OpDelete OpType = 2
)

// Record is one logical WAL entry: a single Put or Delete operation.
type Record struct {
	Type  OpType
	Key   []byte
	Value []byte
}

const (
	// headerSize is the fixed size, in bytes, of every record's header: a
	// 4-byte CRC-32C checksum, a 1-byte operation type, and two 4-byte
	// (uint32, little-endian) length fields for the key and value.
	headerSize = 4 + 1 + 4 + 4

	// maxKeySize and maxValueSize bound how large a single key or value
	// may be. Every length read from a WAL record is validated against
	// these limits before any buffer sized from it is allocated, so a
	// corrupted length field can never trigger an unbounded allocation.
	// They are generous for Phase 2's needs and are not user-configurable
	// yet.
	maxKeySize   = 1 << 20 // 1 MiB
	maxValueSize = 1 << 26 // 64 MiB
)

// crcTable is the CRC-32C (Castagnoli) polynomial table, the same
// checksum LevelDB and RocksDB use for their log records. It has better
// error-detection properties than the default IEEE polynomial for short,
// structured records like these.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// header is the decoded, validated form of a record's fixed-size prefix.
type header struct {
	checksum uint32
	opType   OpType
	keyLen   uint32
	valLen   uint32
}

// encodeRecord serializes rec into a single, self-contained, checksummed
// byte slice ready to be appended to the WAL file. The wire format is:
//
//	bytes 0..4    CRC-32C checksum (little-endian uint32) covering every
//	              byte from offset 4 onward (type, lengths, key, value)
//	byte  4       operation type (OpPut or OpDelete)
//	bytes 5..9    key length (little-endian uint32)
//	bytes 9..13   value length (little-endian uint32)
//	bytes 13..    key bytes, immediately followed by value bytes
//
// The format uses explicit little-endian integers rather than Go's
// in-memory struct layout, so encoding is deterministic and stable across
// platforms, architectures, and Go versions.
func encodeRecord(rec Record) ([]byte, error) {
	if rec.Type != OpPut && rec.Type != OpDelete {
		return nil, fmt.Errorf("wal: unknown record type %d", rec.Type)
	}
	if len(rec.Key) == 0 {
		return nil, fmt.Errorf("wal: record key must not be empty")
	}
	if len(rec.Key) > maxKeySize {
		return nil, fmt.Errorf("wal: key of %d bytes exceeds maximum of %d", len(rec.Key), maxKeySize)
	}
	if len(rec.Value) > maxValueSize {
		return nil, fmt.Errorf("wal: value of %d bytes exceeds maximum of %d", len(rec.Value), maxValueSize)
	}

	buf := make([]byte, headerSize+len(rec.Key)+len(rec.Value))
	buf[4] = byte(rec.Type)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(rec.Key)))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(rec.Value)))
	copy(buf[headerSize:], rec.Key)
	copy(buf[headerSize+len(rec.Key):], rec.Value)

	checksum := crc32.Checksum(buf[4:], crcTable)
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return buf, nil
}

// decodeHeader parses and validates a record's fixed-size header, which
// must be exactly headerSize bytes. It rejects an unknown operation type
// or a length outside the bounds encodeRecord enforces, so a corrupted
// header can never lead a caller to allocate an unbounded buffer for the
// record's payload.
func decodeHeader(buf []byte) (header, error) {
	if len(buf) != headerSize {
		return header{}, fmt.Errorf("wal: invalid header size %d", len(buf))
	}
	h := header{
		checksum: binary.LittleEndian.Uint32(buf[0:4]),
		opType:   OpType(buf[4]),
		keyLen:   binary.LittleEndian.Uint32(buf[5:9]),
		valLen:   binary.LittleEndian.Uint32(buf[9:13]),
	}
	if h.opType != OpPut && h.opType != OpDelete {
		return header{}, fmt.Errorf("wal: unknown record type %d", h.opType)
	}
	if h.keyLen == 0 {
		return header{}, fmt.Errorf("wal: record key length must not be zero")
	}
	if h.keyLen > maxKeySize {
		return header{}, fmt.Errorf("wal: key length %d exceeds maximum of %d", h.keyLen, maxKeySize)
	}
	if h.valLen > maxValueSize {
		return header{}, fmt.Errorf("wal: value length %d exceeds maximum of %d", h.valLen, maxValueSize)
	}
	return h, nil
}
