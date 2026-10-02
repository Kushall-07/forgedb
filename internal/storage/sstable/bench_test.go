// Phase 13 benchmarks for the standalone SSTable format.
//
// IMPORTANT: as of this phase, internal/storage.MemStore (the live Store
// implementation every dbnode.Node actually uses) never flushes to an
// SSTable, never opens one, and never invokes this package at all -- see
// benchmark/README.md's "what the live write path actually exercises"
// section and internal/metrics/catalog.go's comment to the same effect.
// These benchmarks exist purely to characterize this package's own
// performance in isolation, exactly as docs/storage/phase3-sstables-manifest.md
// describes it, and must never be read as representing DBNode or Store
// end-to-end numbers.
package sstable

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// buildBenchTable builds and opens an SSTable of n sorted, sequential
// keys, each holding a size-byte value, returning the Reader. Building is
// excluded from the caller's measured section unless the caller is
// BenchmarkSSTableWrite itself.
func buildBenchTable(b *testing.B, n, size int) (path string, entries []Entry) {
	b.Helper()
	r := rand.New(rand.NewSource(1))
	val := make([]byte, size)
	r.Read(val)

	w := NewWriter()
	entries = make([]Entry, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%010d", i))
		entries[i] = Entry{Key: key, Value: val}
		if err := w.Add(key, val, false); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
	path = filepath.Join(b.TempDir(), "bench.sst")
	if err := w.Build(path); err != nil {
		b.Fatalf("Build: %v", err)
	}
	return path, entries
}

// --- Write ---------------------------------------------------------------

// BenchmarkSSTableWrite measures building (Add + Build) an SSTable of n
// sorted entries from scratch.
func BenchmarkSSTableWrite(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			val := make([]byte, 256)
			rand.New(rand.NewSource(1)).Read(val)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				w := NewWriter()
				path := filepath.Join(b.TempDir(), fmt.Sprintf("bench-%d.sst", i))
				b.StartTimer()

				for k := 0; k < n; k++ {
					if err := w.Add([]byte(fmt.Sprintf("key-%010d", k)), val, false); err != nil {
						b.Fatalf("Add: %v", err)
					}
				}
				if err := w.Build(path); err != nil {
					b.Fatalf("Build: %v", err)
				}
			}
		})
	}
}

// --- Open ------------------------------------------------------------------

// BenchmarkSSTableOpen measures opening (reading the footer, index,
// bloom filter, and meta sections) an already-built table of n entries.
func BenchmarkSSTableOpen(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			path, _ := buildBenchTable(b, n, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := Open(path)
				if err != nil {
					b.Fatalf("Open: %v", err)
				}
				r.Close()
			}
		})
	}
}

// --- Point lookups -----------------------------------------------------------

// BenchmarkSSTablePointLookupHit measures Get for a key present in the
// table.
func BenchmarkSSTablePointLookupHit(b *testing.B) {
	path, entries := buildBenchTable(b, 10_000, 256)
	r, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer r.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := entries[i%len(entries)].Key
		if _, result, err := r.Get(key); err != nil || result != Found {
			b.Fatalf("Get(%q) = result %v, err %v, want Found", key, result, err)
		}
	}
}

// BenchmarkSSTablePointLookupMiss measures Get for a key absent from the
// table but within its min/max key range, so the lookup cannot be
// short-circuited by range alone -- it must consult the bloom filter (see
// BenchmarkSSTableBloomNegativeLookup for isolating that filter's own
// cost) and, on a bloom false positive, a data block.
func BenchmarkSSTablePointLookupMiss(b *testing.B) {
	path, _ := buildBenchTable(b, 10_000, 256)
	r, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer r.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%010d-missing", i))
		if _, result, err := r.Get(key); err != nil || result != NotFound {
			b.Fatalf("Get(%q) = result %v, err %v, want NotFound", key, result, err)
		}
	}
}

// BenchmarkSSTableBloomNegativeLookup measures Get for keys guaranteed
// never to have been added (a disjoint key range entirely above the
// table's max key), isolating the common case where the bloom filter
// alone resolves the lookup without any data block read -- see
// reader.go's Get and bloom.go's mightContain.
func BenchmarkSSTableBloomNegativeLookup(b *testing.B) {
	path, _ := buildBenchTable(b, 10_000, 256)
	r, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer r.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("zzz-key-%010d", i))
		if _, result, err := r.Get(key); err != nil || result != NotFound {
			b.Fatalf("Get(%q) = result %v, err %v, want NotFound", key, result, err)
		}
	}
}

// --- Iteration ---------------------------------------------------------------

// BenchmarkSSTableIteratorScan measures a full sequential scan of every
// entry in the table via Iterator.
func BenchmarkSSTableIteratorScan(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			path, _ := buildBenchTable(b, n, 256)
			r, err := Open(path)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer r.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				it := r.NewIterator()
				count := 0
				for it.Next() {
					_ = it.Entry()
					count++
				}
				if err := it.Err(); err != nil {
					b.Fatalf("iterate: %v", err)
				}
				if count != n {
					b.Fatalf("scanned %d entries, want %d", count, n)
				}
			}
		})
	}
}
