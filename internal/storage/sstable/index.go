package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// The index is a sparse, sorted list of (first key of block, block
// location) entries, one per data block, followed by a trailing CRC-32C
// checksum:
//
//	entry 0 | entry 1 | ... | entry N-1 | checksum (u32, little-endian)
//
// Each entry is:
//
//	keyLen (u32 LE) | blockOffset (u64 LE) | blockLength (u64 LE) | key
//
// Because data blocks are non-overlapping and written in increasing key
// order, the index entry whose key is the largest one <= a search key
// identifies the only block that could contain it.
const indexEntryHeaderSize = 4 + 8 + 8 // keyLen + blockOffset + blockLength

type indexEntry struct {
	firstKey    []byte
	blockOffset uint64
	blockLength uint64
}

func encodeIndex(entries []indexEntry) ([]byte, error) {
	size := 0
	for _, e := range entries {
		if len(e.firstKey) == 0 || len(e.firstKey) > maxKeySize {
			return nil, fmt.Errorf("sstable: invalid index key length %d", len(e.firstKey))
		}
		size += indexEntryHeaderSize + len(e.firstKey)
	}

	buf := make([]byte, size+4)
	off := 0
	for _, e := range entries {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(e.firstKey)))
		binary.LittleEndian.PutUint64(buf[off+4:off+12], e.blockOffset)
		binary.LittleEndian.PutUint64(buf[off+12:off+20], e.blockLength)
		off += indexEntryHeaderSize
		copy(buf[off:], e.firstKey)
		off += len(e.firstKey)
	}

	checksum := crc32.Checksum(buf[:size], crcTable)
	binary.LittleEndian.PutUint32(buf[size:], checksum)
	return buf, nil
}

func decodeIndex(data []byte) ([]indexEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("sstable: index block too small (%d bytes): %w", len(data), ErrCorrupt)
	}
	payload := data[:len(data)-4]
	wantChecksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(payload, crcTable) != wantChecksum {
		return nil, fmt.Errorf("sstable: index checksum mismatch: %w", ErrCorrupt)
	}

	var entries []indexEntry
	off := 0
	for off < len(payload) {
		if len(payload)-off < indexEntryHeaderSize {
			return nil, fmt.Errorf("sstable: truncated index entry at offset %d: %w", off, ErrCorrupt)
		}
		keyLen := binary.LittleEndian.Uint32(payload[off : off+4])
		blockOffset := binary.LittleEndian.Uint64(payload[off+4 : off+12])
		blockLength := binary.LittleEndian.Uint64(payload[off+12 : off+20])
		if keyLen == 0 || keyLen > maxKeySize {
			return nil, fmt.Errorf("sstable: invalid index key length %d at offset %d: %w", keyLen, off, ErrCorrupt)
		}
		off += indexEntryHeaderSize

		if len(payload)-off < int(keyLen) {
			return nil, fmt.Errorf("sstable: truncated index key at offset %d: %w", off, ErrCorrupt)
		}
		key := append([]byte(nil), payload[off:off+int(keyLen)]...)
		off += int(keyLen)

		entries = append(entries, indexEntry{firstKey: key, blockOffset: blockOffset, blockLength: blockLength})
	}
	return entries, nil
}
