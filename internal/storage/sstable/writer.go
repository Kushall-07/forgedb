package sstable

import (
	"bytes"
	"fmt"

	"github.com/Kushall-07/forgedb/internal/storage/atomicfile"
)

// defaultBlockSize is the soft target size, in bytes, for one data
// block's entries (excluding the trailing checksum). Once accumulated
// entries reach this size, the block is closed and a new one is started.
// It is a soft target, not a hard limit: a single entry larger than this
// on its own still gets its own block, so no entry is ever split across
// blocks.
const defaultBlockSize = 4096

// Writer builds a single immutable SSTable file from a stream of entries.
// Entries must be added in strictly increasing key order -- the same
// order an immutable MemTable's skip list already produces -- since the
// resulting data blocks and sparse index both depend on that ordering to
// be non-overlapping and searchable.
//
// A Writer holds its entries in memory until Build is called; it does not
// stream partial output to disk as entries are added. This is deliberate:
// the intended source of entries is an already-fully-materialized
// immutable MemTable, so there is no benefit to streaming, and buffering
// keeps the block-size accounting, the Bloom filter sizing (which needs
// to know the entry count up front), and the min/max key metadata all
// simple to compute correctly in one pass over the finished set.
type Writer struct {
	entries []Entry
	lastKey []byte
	hasLast bool
}

// NewWriter returns an empty Writer.
func NewWriter() *Writer {
	return &Writer{}
}

// Add appends one entry. key and value are copied, so the caller may
// reuse the slices it passed in afterward. Add returns an error if key is
// empty, or if key is not strictly greater than the key of the
// previously added entry -- Add does not sort its input; it only
// validates that the caller already gave it sorted input.
func (w *Writer) Add(key, value []byte, tombstone bool) error {
	if len(key) == 0 {
		return fmt.Errorf("sstable: entry key must not be empty")
	}
	if w.hasLast && bytes.Compare(key, w.lastKey) <= 0 {
		return fmt.Errorf("sstable: keys must be added in strictly increasing order (got %q after %q)", key, w.lastKey)
	}

	keyCopy := append([]byte(nil), key...)
	var valueCopy []byte
	if !tombstone && len(value) > 0 {
		valueCopy = append([]byte(nil), value...)
	}

	w.entries = append(w.entries, Entry{Key: keyCopy, Value: valueCopy, Tombstone: tombstone})
	w.lastKey = keyCopy
	w.hasLast = true
	return nil
}

// Build finalizes the SSTable -- packing entries into data blocks,
// building the sparse index, the Bloom filter, and the metadata block,
// and appending the footer -- and writes the result to path via
// atomicfile.Write, so a crash mid-build can never leave a partially
// written file at path.
//
// Build returns an error, and writes nothing, if no entries were added:
// an SSTable with no entries has no minimum/maximum key and describes no
// useful state, so it is rejected outright rather than represented as a
// degenerate empty file.
func (w *Writer) Build(path string) error {
	if len(w.entries) == 0 {
		return fmt.Errorf("sstable: cannot build an sstable with no entries")
	}

	var body bytes.Buffer
	var index []indexEntry
	var block []Entry
	blockSize := 0

	flushBlock := func() error {
		if len(block) == 0 {
			return nil
		}
		encoded, err := encodeBlock(block)
		if err != nil {
			return err
		}
		index = append(index, indexEntry{
			firstKey:    block[0].Key,
			blockOffset: uint64(body.Len()),
			blockLength: uint64(len(encoded)),
		})
		body.Write(encoded)
		block = block[:0]
		blockSize = 0
		return nil
	}

	for _, e := range w.entries {
		block = append(block, e)
		blockSize += entryHeaderSize + len(e.Key) + len(e.Value)
		if blockSize >= defaultBlockSize {
			if err := flushBlock(); err != nil {
				return fmt.Errorf("sstable: encode data block: %w", err)
			}
		}
	}
	if err := flushBlock(); err != nil {
		return fmt.Errorf("sstable: encode data block: %w", err)
	}

	indexOffset := uint64(body.Len())
	indexBytes, err := encodeIndex(index)
	if err != nil {
		return fmt.Errorf("sstable: encode index: %w", err)
	}
	body.Write(indexBytes)

	bloom := newBloomFilter(len(w.entries), defaultFalsePositiveRate)
	for _, e := range w.entries {
		bloom.add(e.Key)
	}
	bloomOffset := uint64(body.Len())
	bloomBytes := encodeBloom(bloom)
	body.Write(bloomBytes)

	metaOffset := uint64(body.Len())
	metaBytes := encodeMeta(sstableMeta{
		numEntries: uint64(len(w.entries)),
		minKey:     w.entries[0].Key,
		maxKey:     w.entries[len(w.entries)-1].Key,
	})
	body.Write(metaBytes)

	body.Write(encodeFooter(footer{
		indexOffset: indexOffset, indexLength: uint64(len(indexBytes)),
		bloomOffset: bloomOffset, bloomLength: uint64(len(bloomBytes)),
		metaOffset: metaOffset, metaLength: uint64(len(metaBytes)),
	}))

	if err := atomicfile.Write(path, body.Bytes()); err != nil {
		return fmt.Errorf("sstable: write %s: %w", path, err)
	}
	return nil
}
