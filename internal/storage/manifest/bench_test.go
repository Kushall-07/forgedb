// Phase 13 benchmarks for the Manifest/VersionSet: metadata bookkeeping
// for which SSTables currently make up the database (see version.go).
// Like internal/storage/sstable, this package is not reachable from the
// live MemStore write path (see internal/storage/sstable/bench_test.go's
// package doc for the full explanation) -- these benchmarks characterize
// this package alone. Kept deliberately small, per benchmark/README.md:
// Manifest operations are cheap metadata writes, not a primary
// performance concern for this phase.
package manifest

import (
	"fmt"
	"testing"
)

func openBenchManifest(b *testing.B) *VersionSet {
	b.Helper()
	vs, err := Open(b.TempDir())
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	return vs
}

// BenchmarkManifestAddTable measures AddTable's cost (which durably
// persists the whole manifest file on every call -- see version.go's
// persist) as the table count already on record grows.
func BenchmarkManifestAddTable(b *testing.B) {
	for _, preexisting := range []int{10, 100, 1_000} {
		b.Run(fmt.Sprintf("existingTables=%d", preexisting), func(b *testing.B) {
			vs := openBenchManifest(b)
			for i := 0; i < preexisting; i++ {
				if _, err := vs.AddTable(fmt.Sprintf("%06d.sst", i)); err != nil {
					b.Fatalf("AddTable (preload): %v", err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := vs.AddTable(fmt.Sprintf("new-%010d.sst", i)); err != nil {
					b.Fatalf("AddTable: %v", err)
				}
			}
		})
	}
}

// BenchmarkManifestReopen measures Open's cost (replaying the manifest
// file to reconstruct the current Version) as a function of how many
// tables it already records.
func BenchmarkManifestReopen(b *testing.B) {
	for _, n := range []int{10, 100, 1_000} {
		b.Run(fmt.Sprintf("tables=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			vs, err := Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			for i := 0; i < n; i++ {
				if _, err := vs.AddTable(fmt.Sprintf("%06d.sst", i)); err != nil {
					b.Fatalf("AddTable: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reopened, err := Open(dir)
				if err != nil {
					b.Fatalf("Open (reopen): %v", err)
				}
				if got := len(reopened.Current().Tables); got != n {
					b.Fatalf("reopened with %d tables, want %d", got, n)
				}
			}
		})
	}
}
