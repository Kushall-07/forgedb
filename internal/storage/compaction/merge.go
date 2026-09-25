package compaction

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/Kushall-07/forgedb/internal/storage/sstable"
)

// heapItem is one input table's current front entry, waiting to be
// merged. tableID is the table's Manifest ID -- needed to resolve which
// of several tables' entries for the same key wins, using the existing
// Manifest ordering rule that a larger ID is a newer table (see
// manifest.TableMeta).
type heapItem struct {
	tableID uint64
	entry   sstable.Entry
}

// entryHeap is a container/heap.Interface ordering heapItems by key, so
// the item with the smallest key is always at the root.
type entryHeap []heapItem

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return bytes.Compare(h[i].entry.Key, h[j].entry.Key) < 0 }
func (h entryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)        { *h = append(*h, x.(heapItem)) }
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// mergeTables performs a streaming k-way merge of sortedIDs' tables --
// each read one data block at a time via sstable.Iterator, never loading
// a whole input table into memory at once -- producing entries in
// strictly increasing key order with duplicates resolved.
//
// When the same key appears in more than one input table, the winner is
// the entry from the table with the largest ID: this is the same
// "larger ID is newer" rule manifest.TableMeta already documents for
// reads, applied here instead of inventing a separate ordering model.
// Every other input table's entry for that key is discarded entirely --
// it is already shadowed and contributes nothing to the merged state.
//
// dropTombstones, when true, additionally omits a winning tombstone from
// the result rather than carrying it into the output. The caller (see
// Compact) is responsible for only passing true when no active table
// outside the input set is old enough that omitting the tombstone could
// resurrect a stale value for that key.
func mergeTables(sortedIDs []uint64, readers map[uint64]*sstable.Reader, dropTombstones bool) ([]sstable.Entry, error) {
	iters := make(map[uint64]*sstable.Iterator, len(sortedIDs))
	for _, id := range sortedIDs {
		iters[id] = readers[id].NewIterator()
	}

	h := &entryHeap{}
	advance := func(id uint64) error {
		it := iters[id]
		if it.Next() {
			heap.Push(h, heapItem{tableID: id, entry: it.Entry()})
			return nil
		}
		return it.Err()
	}
	for _, id := range sortedIDs {
		if err := advance(id); err != nil {
			return nil, fmt.Errorf("read table %d: %w", id, err)
		}
	}

	var out []sstable.Entry
	for h.Len() > 0 {
		key := (*h)[0].entry.Key

		var best heapItem
		haveBest := false
		for h.Len() > 0 && bytes.Equal((*h)[0].entry.Key, key) {
			item := heap.Pop(h).(heapItem)
			if !haveBest || item.tableID > best.tableID {
				best = item
				haveBest = true
			}
			if err := advance(item.tableID); err != nil {
				return nil, fmt.Errorf("read table %d: %w", item.tableID, err)
			}
		}

		if best.entry.Tombstone && dropTombstones {
			continue
		}
		out = append(out, best.entry)
	}
	return out, nil
}
