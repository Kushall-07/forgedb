package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math"
)

// bloomFilter is a standard bit-array Bloom filter using the
// Kirsch-Mitzenmacher double-hashing technique: two independent base
// hashes of a key (computed with the standard library's FNV-1a and FNV-1,
// hash/fnv -- no third-party dependency, and both deterministic across
// runs and platforms) are combined to derive numHashes probe positions:
//
//	probe_i = (h1 + i*h2) mod numBits,  for i in [0, numHashes)
//
// A key is added by setting the bit at every one of its probe positions.
// mightContain reports "possibly present" only if every one of those bits
// is set, and "definitely absent" as soon as any one of them is not --
// which is why the filter can produce false positives (a bit was set by
// some other key's probe landing on the same position) but can never
// produce a false negative for a key that was actually added: every bit
// add() sets for a key is checked by mightContain for that same key.
//
// numBits and numHashes are sized from the expected number of entries and
// a target false-positive rate using the standard formulas (see
// newBloomFilter), then fixed for the lifetime of the filter -- once
// built, a Bloom filter's parameters are never adjusted based on how full
// it turns out to be, which keeps encoding/decoding simple and the
// on-disk format self-describing.
type bloomFilter struct {
	numHashes uint8
	numBits   uint64
	bits      []byte
}

// defaultFalsePositiveRate is the target false-positive rate used to size
// a new Bloom filter's bit array and probe count.
const defaultFalsePositiveRate = 0.01

// maxBloomBits bounds numBits when decoding an existing filter, purely as
// a corruption guard: no legitimate SSTable built by this package needs a
// filter anywhere near this large, so a header claiming more than this is
// treated as corrupt rather than trusted into an allocation.
const maxBloomBits = 1 << 34 // 2 GiB of bits

// newBloomFilter sizes a Bloom filter for expectedEntries keys at
// falsePositiveRate, using the standard formulas:
//
//	m = ceil(-n * ln(p) / (ln 2)^2)      (bits needed)
//	k = round((m / n) * ln 2)             (probes per key)
func newBloomFilter(expectedEntries int, falsePositiveRate float64) *bloomFilter {
	n := float64(expectedEntries)
	if n < 1 {
		n = 1
	}
	m := math.Ceil(-n * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	if m < 64 {
		m = 64
	}
	k := math.Round((m / n) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	numBits := uint64(m)
	return &bloomFilter{
		numHashes: uint8(k),
		numBits:   numBits,
		bits:      make([]byte, (numBits+7)/8),
	}
}

func (b *bloomFilter) probeHashes(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(key)
	h2 := fnv.New64()
	h2.Write(key)
	return h1.Sum64(), h2.Sum64()
}

// add records key in the filter. After add returns, mightContain(key)
// always reports true -- see the type doc comment for why this can never
// regress into a false negative.
func (b *bloomFilter) add(key []byte) {
	h1, h2 := b.probeHashes(key)
	for i := uint64(0); i < uint64(b.numHashes); i++ {
		idx := (h1 + i*h2) % b.numBits
		b.bits[idx/8] |= 1 << (idx % 8)
	}
}

// mightContain reports whether key may have been added to the filter.
// false means key was definitely never added; true means it possibly was
// (it may be a false positive).
func (b *bloomFilter) mightContain(key []byte) bool {
	h1, h2 := b.probeHashes(key)
	for i := uint64(0); i < uint64(b.numHashes); i++ {
		idx := (h1 + i*h2) % b.numBits
		if b.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// The Bloom filter block is:
//
//	numHashes (u8) | numBits (u64 LE) | bitsLen (u32 LE) | bits | checksum (u32 LE)
//
// bitsLen is redundant with numBits (it is always ceil(numBits/8)), but is
// stored and checked explicitly so decodeBloom can validate the declared
// bit-array length against the buffer's actual length before slicing,
// rather than trusting numBits alone.
func encodeBloom(b *bloomFilter) []byte {
	const headerSize = 1 + 8 + 4
	buf := make([]byte, headerSize+len(b.bits)+4)
	buf[0] = b.numHashes
	binary.LittleEndian.PutUint64(buf[1:9], b.numBits)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(b.bits)))
	copy(buf[headerSize:], b.bits)
	checksum := crc32.Checksum(buf[:headerSize+len(b.bits)], crcTable)
	binary.LittleEndian.PutUint32(buf[headerSize+len(b.bits):], checksum)
	return buf
}

func decodeBloom(data []byte) (*bloomFilter, error) {
	const headerSize = 1 + 8 + 4
	if len(data) < headerSize+4 {
		return nil, fmt.Errorf("sstable: bloom block too small (%d bytes): %w", len(data), ErrCorrupt)
	}
	numHashes := data[0]
	numBits := binary.LittleEndian.Uint64(data[1:9])
	bitsLen := binary.LittleEndian.Uint32(data[9:13])

	if numHashes == 0 || numHashes > 64 {
		return nil, fmt.Errorf("sstable: invalid bloom hash count %d: %w", numHashes, ErrCorrupt)
	}
	if numBits == 0 || numBits > maxBloomBits {
		return nil, fmt.Errorf("sstable: invalid bloom bit count %d: %w", numBits, ErrCorrupt)
	}
	wantBitsLen := (numBits + 7) / 8
	if uint64(bitsLen) != wantBitsLen {
		return nil, fmt.Errorf("sstable: bloom bit array length %d does not match bit count %d: %w", bitsLen, numBits, ErrCorrupt)
	}
	if uint64(len(data)) != uint64(headerSize)+uint64(bitsLen)+4 {
		return nil, fmt.Errorf("sstable: bloom block size mismatch: %w", ErrCorrupt)
	}

	payload := data[:headerSize+int(bitsLen)]
	checksum := binary.LittleEndian.Uint32(data[headerSize+int(bitsLen):])
	if crc32.Checksum(payload, crcTable) != checksum {
		return nil, fmt.Errorf("sstable: bloom checksum mismatch: %w", ErrCorrupt)
	}

	bits := append([]byte(nil), data[headerSize:headerSize+int(bitsLen)]...)
	return &bloomFilter{numHashes: numHashes, numBits: numBits, bits: bits}, nil
}
