package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// A data block is a sequence of entries, sorted by key, followed by a
// trailing CRC-32C checksum over those entries:
//
//	entry 0 | entry 1 | ... | entry N-1 | checksum (u32, little-endian)
//
// Each entry is:
//
//	flags (u8) | keyLen (u32 LE) | valLen (u32 LE) | key | value
//
// flags bit 0 is the tombstone bit. When set, valLen is always 0 and no
// value bytes follow -- the tombstone itself is the payload; there is no
// value to preserve alongside it. This mirrors the wal package's record
// format (explicit little-endian lengths, no reliance on Go's in-memory
// struct layout) and keeps decoding a simple, bounded loop: every length
// is validated against maxKeySize/maxValueSize, and against the bytes
// actually remaining in the block, before it is used to size a slice.
const (
	entryHeaderSize        = 1 + 4 + 4 // flags + keyLen + valLen
	blockChecksumSize      = 4
	tombstoneFlag     byte = 1 << 0
)

// encodeBlock serializes entries (already sorted and belonging to a
// single block) into its on-disk byte representation.
func encodeBlock(entries []Entry) ([]byte, error) {
	size := 0
	for _, e := range entries {
		if len(e.Key) == 0 {
			return nil, fmt.Errorf("sstable: entry key must not be empty")
		}
		if len(e.Key) > maxKeySize {
			return nil, fmt.Errorf("sstable: key of %d bytes exceeds maximum of %d", len(e.Key), maxKeySize)
		}
		if len(e.Value) > maxValueSize {
			return nil, fmt.Errorf("sstable: value of %d bytes exceeds maximum of %d", len(e.Value), maxValueSize)
		}
		valLen := len(e.Value)
		if e.Tombstone {
			valLen = 0
		}
		size += entryHeaderSize + len(e.Key) + valLen
	}

	buf := make([]byte, size+blockChecksumSize)
	off := 0
	for _, e := range entries {
		flags := byte(0)
		value := e.Value
		if e.Tombstone {
			flags = tombstoneFlag
			value = nil
		}
		buf[off] = flags
		binary.LittleEndian.PutUint32(buf[off+1:off+5], uint32(len(e.Key)))
		binary.LittleEndian.PutUint32(buf[off+5:off+9], uint32(len(value)))
		off += entryHeaderSize
		copy(buf[off:], e.Key)
		off += len(e.Key)
		copy(buf[off:], value)
		off += len(value)
	}

	checksum := crc32.Checksum(buf[:size], crcTable)
	binary.LittleEndian.PutUint32(buf[size:], checksum)
	return buf, nil
}

// decodeBlock parses a block's on-disk bytes (as produced by encodeBlock)
// back into its entries, validating the checksum and every length before
// it is used to size or index a slice.
func decodeBlock(data []byte) ([]Entry, error) {
	if len(data) < blockChecksumSize {
		return nil, fmt.Errorf("sstable: block too small (%d bytes): %w", len(data), ErrCorrupt)
	}
	payload := data[:len(data)-blockChecksumSize]
	wantChecksum := binary.LittleEndian.Uint32(data[len(data)-blockChecksumSize:])
	if crc32.Checksum(payload, crcTable) != wantChecksum {
		return nil, fmt.Errorf("sstable: block checksum mismatch: %w", ErrCorrupt)
	}

	var entries []Entry
	off := 0
	for off < len(payload) {
		if len(payload)-off < entryHeaderSize {
			return nil, fmt.Errorf("sstable: truncated entry header at block offset %d: %w", off, ErrCorrupt)
		}
		flags := payload[off]
		keyLen := binary.LittleEndian.Uint32(payload[off+1 : off+5])
		valLen := binary.LittleEndian.Uint32(payload[off+5 : off+9])
		if keyLen == 0 || keyLen > maxKeySize {
			return nil, fmt.Errorf("sstable: invalid key length %d at block offset %d: %w", keyLen, off, ErrCorrupt)
		}
		if valLen > maxValueSize {
			return nil, fmt.Errorf("sstable: invalid value length %d at block offset %d: %w", valLen, off, ErrCorrupt)
		}
		off += entryHeaderSize

		if len(payload)-off < int(keyLen) {
			return nil, fmt.Errorf("sstable: truncated entry key at block offset %d: %w", off, ErrCorrupt)
		}
		key := append([]byte(nil), payload[off:off+int(keyLen)]...)
		off += int(keyLen)

		tombstone := flags&tombstoneFlag != 0
		if tombstone && valLen != 0 {
			return nil, fmt.Errorf("sstable: tombstone entry at block offset %d has non-zero value length: %w", off, ErrCorrupt)
		}

		if len(payload)-off < int(valLen) {
			return nil, fmt.Errorf("sstable: truncated entry value at block offset %d: %w", off, ErrCorrupt)
		}
		var value []byte
		if valLen > 0 {
			value = append([]byte(nil), payload[off:off+int(valLen)]...)
		}
		off += int(valLen)

		entries = append(entries, Entry{Key: key, Value: value, Tombstone: tombstone})
	}
	return entries, nil
}

// searchBlockEntries binary-searches entries (sorted by key, as every
// decoded block's entries are) for key.
func searchBlockEntries(entries []Entry, key []byte) (Entry, bool) {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := (lo + hi) / 2
		switch c := bytes.Compare(entries[mid].Key, key); {
		case c == 0:
			return entries[mid], true
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return Entry{}, false
}
