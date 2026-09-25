package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// The metadata block records table-level summary information used by the
// Reader (and, in a later phase, by compaction) without having to scan
// any data block:
//
//	numEntries (u64 LE) | minKeyLen (u32 LE) | minKey | maxKeyLen (u32 LE) | maxKey | checksum (u32 LE)
type sstableMeta struct {
	numEntries uint64
	minKey     []byte
	maxKey     []byte
}

func encodeMeta(m sstableMeta) []byte {
	size := 8 + 4 + len(m.minKey) + 4 + len(m.maxKey)
	buf := make([]byte, size+4)

	binary.LittleEndian.PutUint64(buf[0:8], m.numEntries)
	off := 8
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(m.minKey)))
	off += 4
	copy(buf[off:], m.minKey)
	off += len(m.minKey)
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(m.maxKey)))
	off += 4
	copy(buf[off:], m.maxKey)
	off += len(m.maxKey)

	checksum := crc32.Checksum(buf[:off], crcTable)
	binary.LittleEndian.PutUint32(buf[off:], checksum)
	return buf
}

func decodeMeta(data []byte) (sstableMeta, error) {
	off := 0
	need := func(n int) error {
		if len(data)-off < n {
			return fmt.Errorf("sstable: truncated metadata block: %w", ErrCorrupt)
		}
		return nil
	}

	if err := need(8); err != nil {
		return sstableMeta{}, err
	}
	numEntries := binary.LittleEndian.Uint64(data[off : off+8])
	off += 8

	if err := need(4); err != nil {
		return sstableMeta{}, err
	}
	minKeyLen := binary.LittleEndian.Uint32(data[off : off+4])
	off += 4
	if minKeyLen == 0 || minKeyLen > maxKeySize {
		return sstableMeta{}, fmt.Errorf("sstable: invalid min key length %d: %w", minKeyLen, ErrCorrupt)
	}
	if err := need(int(minKeyLen)); err != nil {
		return sstableMeta{}, err
	}
	minKey := append([]byte(nil), data[off:off+int(minKeyLen)]...)
	off += int(minKeyLen)

	if err := need(4); err != nil {
		return sstableMeta{}, err
	}
	maxKeyLen := binary.LittleEndian.Uint32(data[off : off+4])
	off += 4
	if maxKeyLen == 0 || maxKeyLen > maxKeySize {
		return sstableMeta{}, fmt.Errorf("sstable: invalid max key length %d: %w", maxKeyLen, ErrCorrupt)
	}
	if err := need(int(maxKeyLen)); err != nil {
		return sstableMeta{}, err
	}
	maxKey := append([]byte(nil), data[off:off+int(maxKeyLen)]...)
	off += int(maxKeyLen)

	if err := need(4); err != nil {
		return sstableMeta{}, err
	}
	if off+4 != len(data) {
		return sstableMeta{}, fmt.Errorf("sstable: trailing bytes in metadata block: %w", ErrCorrupt)
	}
	checksum := binary.LittleEndian.Uint32(data[off:])
	if crc32.Checksum(data[:off], crcTable) != checksum {
		return sstableMeta{}, fmt.Errorf("sstable: metadata checksum mismatch: %w", ErrCorrupt)
	}

	return sstableMeta{numEntries: numEntries, minKey: minKey, maxKey: maxKey}, nil
}
