package sstable

import (
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// These tests exercise malformed SSTable files: hand-crafted section
// buffers for precise, deterministic corruption of one field at a time,
// and real on-disk files for the byte-flip/truncation scenarios that
// plausibly reflect actual file damage. None of it should ever be
// silently accepted -- every case here must produce an error wrapping
// ErrCorrupt (or some other explicit error), never a wrong answer from
// Get.

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	return data
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func corruptLastBytes(t *testing.T, path string, n int) {
	t.Helper()
	data := readFile(t, path)
	for i := len(data) - n; i < len(data); i++ {
		data[i] ^= 0xFF
	}
	writeFile(t, path, data)
}

// --- Footer-level corruption (via Open, on a real file) ---

func TestOpenRejectsBadMagic(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	data := readFile(t, path)
	data[len(data)-footerSize] ^= 0xFF // first byte of the magic
	writeFile(t, path, data)

	_, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with bad magic: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestOpenRejectsFooterChecksumMismatch(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	data := readFile(t, path)
	data[len(data)-1] ^= 0xFF // last byte of the footer checksum
	writeFile(t, path, data)

	_, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with bad footer checksum: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestOpenRejectsTruncatedFile(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	data := readFile(t, path)
	writeFile(t, path, data[:footerSize-1]) // shorter than a single footer

	_, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open truncated to less than a footer: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestOpenRejectsFileTruncatedMidSection(t *testing.T) {
	// Build a table large enough to have a real index/bloom/meta region,
	// then cut the file well before the end. The footer bytes read back
	// will not be the real footer (they are now whatever the file's last
	// footerSize bytes happen to be at the new length), so this is
	// expected to fail magic or checksum validation, not succeed.
	entries := make([]Entry, 0, 200)
	for i := 0; i < 200; i++ {
		entries = append(entries, Entry{Key: []byte(padKey(i)), Value: []byte("value")})
	}
	path := buildTestTable(t, entries)
	data := readFile(t, path)
	writeFile(t, path, data[:len(data)/2])

	_, err := Open(path)
	if err == nil {
		t.Fatalf("Open file truncated mid-section: got nil error, want error")
	}
}

func TestOpenRejectsFooterOffsetBeyondFile(t *testing.T) {
	// Hand-build a footer whose index section claims to start beyond a
	// tiny declared body size, with a checksum that is internally
	// consistent (as real corruption -- or a bug -- might coincidentally
	// produce), to isolate decodeFooter's own bounds check from checksum
	// validation.
	ft := footer{
		indexOffset: 1000, indexLength: 10,
		bloomOffset: 0, bloomLength: 1,
		metaOffset: 0, metaLength: 1,
	}
	buf := encodeFooter(ft)

	_, err := decodeFooter(buf, 100 /* body far smaller than indexOffset */)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeFooter with out-of-range offset: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestOpenRejectsFooterLengthExceedingFile(t *testing.T) {
	ft := footer{
		indexOffset: 0, indexLength: 5000, // far larger than the declared body
		bloomOffset: 0, bloomLength: 1,
		metaOffset: 0, metaLength: 1,
	}
	buf := encodeFooter(ft)

	_, err := decodeFooter(buf, 100)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeFooter with oversized length: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestOpenRejectsUnsupportedFooterVersion(t *testing.T) {
	buf := encodeFooter(footer{indexOffset: 0, indexLength: 1, bloomOffset: 0, bloomLength: 1, metaOffset: 0, metaLength: 1})
	// formatVersion field is bytes [8:12].
	buf[8] = 99
	// Recompute checksum so this is a version mismatch, not a checksum
	// mismatch, isolating exactly what decodeFooter is supposed to catch.
	fixFooterChecksum(buf)

	_, err := decodeFooter(buf, 100)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeFooter with unsupported version: got %v, want error wrapping ErrCorrupt", err)
	}
}

func fixFooterChecksum(buf []byte) {
	checksum := crc32Checksum(buf[:footerSize-4])
	buf[footerSize-4] = byte(checksum)
	buf[footerSize-3] = byte(checksum >> 8)
	buf[footerSize-2] = byte(checksum >> 16)
	buf[footerSize-1] = byte(checksum >> 24)
}

func crc32Checksum(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}

// --- Data block corruption (via Get, on a real file) ---

func TestGetRejectsCorruptedDataBlock(t *testing.T) {
	path := buildTestTable(t, []Entry{{Key: []byte("a"), Value: []byte("1")}})
	data := readFile(t, path)
	data[3] ^= 0xFF // inside the single data block, which starts at offset 0
	writeFile(t, path, data)

	r := openTestTable(t, path) // footer/index/bloom/meta are untouched, so Open still succeeds
	_, _, err := r.Get([]byte("a"))
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get on corrupted data block: got %v, want error wrapping ErrCorrupt", err)
	}
}

// --- Index corruption (function-level, hand-crafted buffers) ---

func TestDecodeIndexRejectsChecksumMismatch(t *testing.T) {
	buf, err := encodeIndex([]indexEntry{{firstKey: []byte("a"), blockOffset: 0, blockLength: 10}})
	if err != nil {
		t.Fatalf("encodeIndex: %v", err)
	}
	buf[len(buf)-1] ^= 0xFF

	_, err = decodeIndex(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeIndex with bad checksum: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeIndexRejectsTruncatedEntry(t *testing.T) {
	buf, err := encodeIndex([]indexEntry{{firstKey: []byte("a"), blockOffset: 0, blockLength: 10}})
	if err != nil {
		t.Fatalf("encodeIndex: %v", err)
	}
	truncated := buf[:indexEntryHeaderSize-1] // cuts off before even the header is complete
	// A checksum computed over the truncated payload will not match
	// what's left in the buffer after slicing, so re-derive one to
	// isolate the truncation check rather than the checksum check.
	buf2 := append([]byte(nil), truncated...)
	buf2 = append(buf2, encodeIndexChecksum(truncated)...)

	_, err = decodeIndex(buf2)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeIndex with truncated entry: got %v, want error wrapping ErrCorrupt", err)
	}
}

func encodeIndexChecksum(payload []byte) []byte {
	c := crc32Checksum(payload)
	return []byte{byte(c), byte(c >> 8), byte(c >> 16), byte(c >> 24)}
}

func TestDecodeIndexRejectsOversizedKeyLength(t *testing.T) {
	// A well-formed single-entry index, then the key-length field is
	// hand-set to an impossible value and the checksum recomputed, so
	// the truncation/bounds check -- not the checksum check -- is what's
	// under test.
	buf, err := encodeIndex([]indexEntry{{firstKey: []byte("a"), blockOffset: 0, blockLength: 10}})
	if err != nil {
		t.Fatalf("encodeIndex: %v", err)
	}
	payload := append([]byte(nil), buf[:len(buf)-4]...)
	payload[0] = 0xFF
	payload[1] = 0xFF
	payload[2] = 0xFF
	payload[3] = 0xFF // keyLen = 0xFFFFFFFF
	fixed := append(payload, encodeIndexChecksum(payload)...)

	_, err = decodeIndex(fixed)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeIndex with oversized key length: got %v, want error wrapping ErrCorrupt", err)
	}
}

// --- Bloom filter corruption (function-level, hand-crafted buffers) ---

func TestDecodeBloomRejectsChecksumMismatch(t *testing.T) {
	b := newBloomFilter(10, defaultFalsePositiveRate)
	b.add([]byte("a"))
	buf := encodeBloom(b)
	buf[len(buf)-1] ^= 0xFF

	_, err := decodeBloom(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeBloom with bad checksum: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeBloomRejectsBitsLengthMismatch(t *testing.T) {
	b := newBloomFilter(10, defaultFalsePositiveRate)
	b.add([]byte("a"))
	buf := encodeBloom(b)
	// bitsLen field is bytes [9:13]; corrupt it without touching numBits,
	// so wantBitsLen (derived from numBits) no longer matches.
	buf[9] ^= 0xFF

	_, err := decodeBloom(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeBloom with mismatched bits length: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeBloomRejectsZeroHashCount(t *testing.T) {
	b := newBloomFilter(10, defaultFalsePositiveRate)
	b.add([]byte("a"))
	buf := encodeBloom(b)
	buf[0] = 0 // numHashes
	fixBloomChecksum(buf)

	_, err := decodeBloom(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeBloom with zero hash count: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeBloomRejectsExcessiveBitCount(t *testing.T) {
	b := newBloomFilter(10, defaultFalsePositiveRate)
	b.add([]byte("a"))
	buf := encodeBloom(b)
	// numBits field is bytes [1:9].
	for i := 1; i < 9; i++ {
		buf[i] = 0xFF
	}
	fixBloomChecksumAfterBitsFieldOnly(buf)

	_, err := decodeBloom(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeBloom with excessive bit count: got %v, want error wrapping ErrCorrupt", err)
	}
}

func fixBloomChecksum(buf []byte) {
	const headerSize = 1 + 8 + 4
	bitsLen := int(le32(buf[9:13]))
	c := crc32Checksum(buf[:headerSize+bitsLen])
	putLE32(buf[headerSize+bitsLen:], c)
}

// fixBloomChecksumAfterBitsFieldOnly recomputes the checksum using the
// original bits length recorded in the buffer -- used when numBits itself
// has been corrupted to an enormous value, since bitsLen is left as-is
// and the mismatch between the two is exactly what the test wants
// decodeBloom to catch first.
func fixBloomChecksumAfterBitsFieldOnly(buf []byte) {
	const headerSize = 1 + 8 + 4
	bitsLen := int(le32(buf[9:13]))
	c := crc32Checksum(buf[:headerSize+bitsLen])
	putLE32(buf[headerSize+bitsLen:], c)
}

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func putLE32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

// --- Metadata block corruption (function-level, hand-crafted buffers) ---

func TestDecodeMetaRejectsChecksumMismatch(t *testing.T) {
	buf := encodeMeta(sstableMeta{numEntries: 1, minKey: []byte("a"), maxKey: []byte("z")})
	buf[len(buf)-1] ^= 0xFF

	_, err := decodeMeta(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeMeta with bad checksum: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeMetaRejectsTruncatedBuffer(t *testing.T) {
	buf := encodeMeta(sstableMeta{numEntries: 1, minKey: []byte("a"), maxKey: []byte("z")})

	_, err := decodeMeta(buf[:5]) // cuts off before even numEntries is complete
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeMeta with truncated buffer: got %v, want error wrapping ErrCorrupt", err)
	}
}

func TestDecodeMetaRejectsInvalidMinKeyLength(t *testing.T) {
	buf := encodeMeta(sstableMeta{numEntries: 1, minKey: []byte("a"), maxKey: []byte("z")})
	// minKeyLen field is bytes [8:12].
	putLE32(buf[8:12], 0xFFFFFFFF)

	_, err := decodeMeta(buf)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decodeMeta with invalid min key length: got %v, want error wrapping ErrCorrupt", err)
	}
}

// --- Sanity: valid encode/decode round-trips do NOT report corruption ---

func TestDecodeFunctionsAcceptTheirOwnEncodeOutput(t *testing.T) {
	if _, err := decodeIndex(mustEncodeIndex(t, []indexEntry{{firstKey: []byte("a"), blockOffset: 0, blockLength: 10}})); err != nil {
		t.Fatalf("decodeIndex on valid input: %v", err)
	}
	b := newBloomFilter(10, defaultFalsePositiveRate)
	b.add([]byte("a"))
	if _, err := decodeBloom(encodeBloom(b)); err != nil {
		t.Fatalf("decodeBloom on valid input: %v", err)
	}
	if _, err := decodeMeta(encodeMeta(sstableMeta{numEntries: 1, minKey: []byte("a"), maxKey: []byte("z")})); err != nil {
		t.Fatalf("decodeMeta on valid input: %v", err)
	}
}

func mustEncodeIndex(t *testing.T, entries []indexEntry) []byte {
	t.Helper()
	buf, err := encodeIndex(entries)
	if err != nil {
		t.Fatalf("encodeIndex: %v", err)
	}
	return buf
}

func TestOpenRejectsFileWithNoTables(t *testing.T) {
	// Not an SSTable at all -- an arbitrary short file. Open must reject
	// it as too small/malformed rather than panicking or misreading it.
	path := filepath.Join(t.TempDir(), "garbage.sst")
	writeFile(t, path, []byte("not an sstable"))

	_, err := Open(path)
	if err == nil {
		t.Fatalf("Open on an arbitrary non-sstable file: got nil error, want error")
	}
}
