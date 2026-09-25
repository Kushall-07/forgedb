package sstable

import (
	"bytes"
	"fmt"
	"os"
)

// Reader provides read-only access to an existing, immutable SSTable
// file. Open validates the file's structure -- footer, section bounds,
// and every section's own checksum -- and loads the index and Bloom
// filter into memory; a malformed file is rejected with an error rather
// than opened into a Reader that might return wrong results. Get reads
// and validates one data block per lookup, on demand.
type Reader struct {
	f     *os.File
	index []indexEntry
	bloom *bloomFilter
	meta  sstableMeta
}

// Open opens the SSTable at path.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}

	r, err := open(path, f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func open(path string, f *os.File) (*Reader, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	if info.Size() < footerSize {
		return nil, fmt.Errorf("sstable: %s is too small (%d bytes) to contain a footer: %w", path, info.Size(), ErrCorrupt)
	}

	footerBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footerBuf, info.Size()-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: read footer of %s: %w", path, err)
	}
	bodySize := uint64(info.Size()) - footerSize
	ft, err := decodeFooter(footerBuf, bodySize)
	if err != nil {
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	index, err := readSection(f, path, "index", ft.indexOffset, ft.indexLength, decodeIndex)
	if err != nil {
		return nil, err
	}
	bloom, err := readSection(f, path, "bloom", ft.bloomOffset, ft.bloomLength, decodeBloom)
	if err != nil {
		return nil, err
	}
	meta, err := readSection(f, path, "meta", ft.metaOffset, ft.metaLength, decodeMeta)
	if err != nil {
		return nil, err
	}

	// Every block the index points to must lie entirely within the data
	// region (everything before the index itself begins). This catches
	// an index whose own checksum is intact but whose entries have been
	// tampered with to reference bytes outside the data section.
	for _, e := range index {
		if e.blockOffset > ft.indexOffset || e.blockLength > ft.indexOffset-e.blockOffset {
			return nil, fmt.Errorf("sstable: %s: index entry references block outside data region: %w", path, ErrCorrupt)
		}
	}

	return &Reader{f: f, index: index, bloom: bloom, meta: meta}, nil
}

// readSection reads the length bytes at offset (already validated by
// decodeFooter to lie within the file body) and decodes them with decode.
func readSection[T any](f *os.File, path, name string, offset, length uint64, decode func([]byte) (T, error)) (T, error) {
	var zero T
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, int64(offset)); err != nil {
		return zero, fmt.Errorf("sstable: read %s section of %s: %w", name, path, err)
	}
	v, err := decode(buf)
	if err != nil {
		return zero, fmt.Errorf("sstable: %s: %w", path, err)
	}
	return v, nil
}

// Get looks up key. It first consults the Bloom filter to avoid reading a
// data block for a key that was definitely never written to this table,
// then binary-searches the index for the one block that could contain
// key, reads that block, and searches it.
func (r *Reader) Get(key []byte) ([]byte, Result, error) {
	if len(key) == 0 {
		return nil, NotFound, fmt.Errorf("sstable: key must not be empty")
	}
	if !r.bloom.mightContain(key) {
		return nil, NotFound, nil
	}

	i := r.candidateBlock(key)
	if i < 0 {
		return nil, NotFound, nil
	}
	loc := r.index[i]

	buf := make([]byte, loc.blockLength)
	if _, err := r.f.ReadAt(buf, int64(loc.blockOffset)); err != nil {
		return nil, NotFound, fmt.Errorf("sstable: read block at offset %d: %w", loc.blockOffset, err)
	}
	entries, err := decodeBlock(buf)
	if err != nil {
		return nil, NotFound, fmt.Errorf("sstable: decode block at offset %d: %w", loc.blockOffset, err)
	}

	entry, ok := searchBlockEntries(entries, key)
	if !ok {
		return nil, NotFound, nil
	}
	if entry.Tombstone {
		return nil, Tombstone, nil
	}
	return entry.Value, Found, nil
}

// candidateBlock returns the index of the last index entry whose first
// key is <= key, or -1 if key precedes every block's first key (meaning
// key cannot be present in this table).
func (r *Reader) candidateBlock(key []byte) int {
	lo, hi := 0, len(r.index)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(r.index[mid].firstKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

// Count returns the number of entries (including tombstones) in this
// table, as recorded in its metadata block.
func (r *Reader) Count() uint64 { return r.meta.numEntries }

// MinKey returns a copy of the smallest key in this table.
func (r *Reader) MinKey() []byte { return append([]byte(nil), r.meta.minKey...) }

// MaxKey returns a copy of the largest key in this table.
func (r *Reader) MaxKey() []byte { return append([]byte(nil), r.meta.maxKey...) }

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.f.Close()
}
