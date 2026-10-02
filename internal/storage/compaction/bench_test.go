// Phase 13 benchmarks for the standalone compaction package: merging
// multiple SSTables into one via Compact (see compaction.go).
//
// IMPORTANT: like internal/storage/sstable and internal/storage/manifest,
// this package is never invoked by the live MemStore write path (see
// internal/storage/sstable/bench_test.go's package doc) -- these
// benchmarks characterize Compact itself, not anything DBNode or Store
// currently does in production. Compact's own tombstone-dropping rules
// are exercised as-is (see TestCompactFullCompactionDropsSafeTombstone in
// compaction_test.go); nothing here weakens or bypasses them to make
// compaction look cheaper than it is.
package compaction

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/Kushall-07/forgedb/internal/storage/manifest"
	"github.com/Kushall-07/forgedb/internal/storage/sstable"
)

func openBenchManifest(b *testing.B, dir string) *manifest.VersionSet {
	b.Helper()
	vs, err := manifest.Open(dir)
	if err != nil {
		b.Fatalf("manifest.Open: %v", err)
	}
	return vs
}

// addBenchTable builds an SSTable from entries (which must already be in
// ascending key order, per sstable.Writer.Add's contract) and registers
// it with vs.
func addBenchTable(b *testing.B, vs *manifest.VersionSet, dir, name string, entries []sstable.Entry) uint64 {
	b.Helper()
	w := sstable.NewWriter()
	for _, e := range entries {
		if err := w.Add(e.Key, e.Value, e.Tombstone); err != nil {
			b.Fatalf("Add(%q): %v", e.Key, err)
		}
	}
	path := filepath.Join(dir, name)
	if err := w.Build(path); err != nil {
		b.Fatalf("Build(%s): %v", name, err)
	}
	id, err := vs.AddTable(name)
	if err != nil {
		b.Fatalf("AddTable(%s): %v", name, err)
	}
	return id
}

// workload describes one compaction input shape: numTables SSTables of
// keysPerTable entries each, either disjoint (non-overlapping) or sharing
// the identical key range (overlapping/duplicate keys, newest table
// wins), optionally scattering tombstones through every 10th key.
type workload struct {
	numTables    int
	keysPerTable int
	overlapping  bool
	tombstones   bool
}

// setupCompactionInputs builds workload w's tables under a fresh
// manifest in its own directory and returns everything Compact needs.
// Called inside the benchmark loop with the timer stopped, since building
// the inputs is setup, not the thing being measured (see
// benchmark/README.md's "setup vs. measurement" rule) -- Compact
// destructively retires its inputs via the Manifest, so each b.N
// iteration needs its own fresh set.
func setupCompactionInputs(b *testing.B, w workload) (vs *manifest.VersionSet, dir string, ids []uint64) {
	b.Helper()
	dir = b.TempDir()
	vs = openBenchManifest(b, dir)
	ids = make([]uint64, w.numTables)
	for t := 0; t < w.numTables; t++ {
		entries := make([]sstable.Entry, w.keysPerTable)
		for k := 0; k < w.keysPerTable; k++ {
			keyIndex := k
			if !w.overlapping {
				keyIndex = t*w.keysPerTable + k
			}
			tombstone := w.tombstones && k%10 == 0
			entries[k] = sstable.Entry{
				Key:       []byte(fmt.Sprintf("key-%010d", keyIndex)),
				Value:     []byte(fmt.Sprintf("value-table%d", t)),
				Tombstone: tombstone,
			}
		}
		ids[t] = addBenchTable(b, vs, dir, fmt.Sprintf("table-%03d.sst", t), entries)
	}
	return vs, dir, ids
}

// runCompactionBenchmark drives Compact b.N times, rebuilding workload
// w's input tables fresh before each call (Compact retires its inputs,
// so they cannot be reused across iterations).
func runCompactionBenchmark(b *testing.B, w workload) {
	b.ReportMetric(float64(w.numTables*w.keysPerTable), "input-records/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		vs, dir, ids := setupCompactionInputs(b, w)
		b.StartTimer()

		if _, err := Compact(vs, dir, ids, nil); err != nil {
			b.Fatalf("Compact: %v", err)
		}
	}
}

// BenchmarkCompactNonOverlapping merges tables whose key ranges never
// intersect -- the cheapest shape, since the merge is effectively a
// concatenation with no value-shadowing decisions to make.
func BenchmarkCompactNonOverlapping(b *testing.B) {
	for _, n := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("tables=%d", n), func(b *testing.B) {
			runCompactionBenchmark(b, workload{numTables: n, keysPerTable: 500, overlapping: false})
		})
	}
}

// BenchmarkCompactOverlapping merges tables that all share the identical
// key range (every key duplicated across every input table): the merge
// must resolve, for every key, which table's value is newest -- see
// merge.go's heap-based merge and compaction.go's "newer table wins"
// rule.
func BenchmarkCompactOverlapping(b *testing.B) {
	for _, n := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("tables=%d", n), func(b *testing.B) {
			runCompactionBenchmark(b, workload{numTables: n, keysPerTable: 500, overlapping: true})
		})
	}
}

// BenchmarkCompactWithTombstones merges overlapping tables where every
// 10th key is a tombstone in its owning table, exercising Compact's
// tombstone-retention/drop logic (a tombstone is dropped only when no
// older table outside the input set could still hold a shadowed value --
// see TestCompactFullCompactionDropsSafeTombstone) rather than measuring
// a workload with no deletes at all.
func BenchmarkCompactWithTombstones(b *testing.B) {
	for _, n := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("tables=%d", n), func(b *testing.B) {
			runCompactionBenchmark(b, workload{numTables: n, keysPerTable: 500, overlapping: true, tombstones: true})
		})
	}
}
