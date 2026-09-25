package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// magic identifies a file as a ForgeDB SSTable and lets Open reject an
// arbitrary or truncated file immediately, before trusting any of its
// declared offsets.
var magic = [8]byte{'F', 'o', 'r', 'g', 'e', 'S', 'S', 'T'}

const formatVersion1 = 1

// The footer is a fixed-size (footerSize bytes) structure at the very end
// of the file. Its fixed size and fixed position -- the last footerSize
// bytes of the file, always -- is what lets Open discover every other
// section without any hard-coded offset into the rest of the file: the
// footer's own offset is derived purely from the file's total size, and
// everything else is found by following the offsets the footer records.
//
//	magic (8 bytes) | formatVersion (u32 LE) |
//	indexOffset (u64 LE) | indexLength (u64 LE) |
//	bloomOffset (u64 LE) | bloomLength (u64 LE) |
//	metaOffset  (u64 LE) | metaLength  (u64 LE) |
//	checksum (u32 LE, CRC-32C over every preceding footer byte)
const footerSize = 8 + 4 + 8*6 + 4 // 64 bytes

type footer struct {
	indexOffset, indexLength uint64
	bloomOffset, bloomLength uint64
	metaOffset, metaLength   uint64
}

func encodeFooter(f footer) []byte {
	buf := make([]byte, footerSize)
	copy(buf[0:8], magic[:])
	binary.LittleEndian.PutUint32(buf[8:12], formatVersion1)
	binary.LittleEndian.PutUint64(buf[12:20], f.indexOffset)
	binary.LittleEndian.PutUint64(buf[20:28], f.indexLength)
	binary.LittleEndian.PutUint64(buf[28:36], f.bloomOffset)
	binary.LittleEndian.PutUint64(buf[36:44], f.bloomLength)
	binary.LittleEndian.PutUint64(buf[44:52], f.metaOffset)
	binary.LittleEndian.PutUint64(buf[52:60], f.metaLength)
	checksum := crc32.Checksum(buf[:footerSize-4], crcTable)
	binary.LittleEndian.PutUint32(buf[footerSize-4:], checksum)
	return buf
}

// decodeFooter parses and validates buf (which must be exactly footerSize
// bytes, read from the last footerSize bytes of the file) and checks
// every section's offset/length against fileBodySize -- the size of the
// file excluding the footer itself -- so a corrupted or malicious offset
// can never send a later read outside the file, or overlapping the
// footer.
func decodeFooter(buf []byte, fileBodySize uint64) (footer, error) {
	if len(buf) != footerSize {
		return footer{}, fmt.Errorf("sstable: invalid footer size %d", len(buf))
	}
	if !bytes.Equal(buf[0:8], magic[:]) {
		return footer{}, fmt.Errorf("sstable: bad magic: %w", ErrCorrupt)
	}
	version := binary.LittleEndian.Uint32(buf[8:12])
	if version != formatVersion1 {
		return footer{}, fmt.Errorf("sstable: unsupported format version %d: %w", version, ErrCorrupt)
	}
	checksum := binary.LittleEndian.Uint32(buf[footerSize-4:])
	if crc32.Checksum(buf[:footerSize-4], crcTable) != checksum {
		return footer{}, fmt.Errorf("sstable: footer checksum mismatch: %w", ErrCorrupt)
	}

	f := footer{
		indexOffset: binary.LittleEndian.Uint64(buf[12:20]),
		indexLength: binary.LittleEndian.Uint64(buf[20:28]),
		bloomOffset: binary.LittleEndian.Uint64(buf[28:36]),
		bloomLength: binary.LittleEndian.Uint64(buf[36:44]),
		metaOffset:  binary.LittleEndian.Uint64(buf[44:52]),
		metaLength:  binary.LittleEndian.Uint64(buf[52:60]),
	}
	if err := validateSection("index", f.indexOffset, f.indexLength, fileBodySize); err != nil {
		return footer{}, err
	}
	if err := validateSection("bloom", f.bloomOffset, f.bloomLength, fileBodySize); err != nil {
		return footer{}, err
	}
	if err := validateSection("meta", f.metaOffset, f.metaLength, fileBodySize); err != nil {
		return footer{}, err
	}
	return f, nil
}

// validateSection checks that [offset, offset+length) lies entirely
// within [0, bodySize), without ever computing offset+length directly --
// that addition could overflow uint64 for a maliciously large length, so
// the comparison is done as bodySize-offset instead, which cannot
// overflow once offset <= bodySize has already been established.
func validateSection(name string, offset, length, bodySize uint64) error {
	if length == 0 {
		return fmt.Errorf("sstable: %s section has zero length: %w", name, ErrCorrupt)
	}
	if offset > bodySize {
		return fmt.Errorf("sstable: %s section offset %d exceeds file size %d: %w", name, offset, bodySize, ErrCorrupt)
	}
	if length > bodySize-offset {
		return fmt.Errorf("sstable: %s section length %d at offset %d exceeds file size %d: %w", name, length, offset, bodySize, ErrCorrupt)
	}
	return nil
}
