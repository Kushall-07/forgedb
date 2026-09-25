package sstable

import "fmt"

// Iterator provides forward, in-order iteration over every entry in an
// SSTable -- live values and tombstones alike -- reading one data block
// at a time rather than loading the whole table into memory. Get never
// needed this: a lookup only ever reads the one block its key could be
// in. A later phase's merge does need it, to walk a table's entries in
// sorted key order without re-deriving the block layout Reader already
// knows, so this is the minimal addition Phase 4's compaction requires
// rather than a new, separate abstraction.
//
// The zero value is not usable; obtain an Iterator via Reader.NewIterator.
type Iterator struct {
	r         *Reader
	nextBlock int
	entries   []Entry
	pos       int
	err       error
}

// NewIterator returns an Iterator over every entry in r, positioned
// before the first one. Call Next to advance to each entry in turn.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{r: r, pos: -1}
}

// Next advances the iterator to the next entry, in key order, and
// reports whether one was found. It must be called before the first call
// to Entry, and again before every subsequent one. Once Next returns
// false, iteration is over: check Err to distinguish "reached the end
// cleanly" (nil) from "stopped because a block failed to read or
// decode" (the reason); either way, the iterator must not be used
// further.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		if it.pos+1 < len(it.entries) {
			it.pos++
			return true
		}
		if it.nextBlock >= len(it.r.index) {
			return false
		}
		loc := it.r.index[it.nextBlock]
		it.nextBlock++

		buf := make([]byte, loc.blockLength)
		if _, err := it.r.f.ReadAt(buf, int64(loc.blockOffset)); err != nil {
			it.err = fmt.Errorf("sstable: iterator: read block at offset %d: %w", loc.blockOffset, err)
			return false
		}
		entries, err := decodeBlock(buf)
		if err != nil {
			it.err = fmt.Errorf("sstable: iterator: decode block at offset %d: %w", loc.blockOffset, err)
			return false
		}
		it.entries = entries
		it.pos = -1
	}
}

// Entry returns the entry the most recent call to Next advanced to. It
// must not be called before a call to Next that returned true.
func (it *Iterator) Entry() Entry {
	return it.entries[it.pos]
}

// Err returns the first error encountered while reading or decoding a
// block, or nil if iteration has not failed (whether or not it has
// finished).
func (it *Iterator) Err() error {
	return it.err
}
