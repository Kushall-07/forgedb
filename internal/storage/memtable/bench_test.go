// Phase 13 benchmarks for the MemTable: the in-memory, ordered skip-list
// structure at the bottom of ForgeDB's storage stack (see memtable.go's
// package doc). These are pure in-memory microbenchmarks -- no WAL, no
// disk, no Raft -- so they isolate the cost of the skip list itself from
// every durability or replication cost layered on top of it in
// internal/storage and above. See benchmark/README.md for how these
// numbers relate to the WAL/Storage/DBNode benchmarks that do include
// those costs, and docs/benchmarking/phase13-benchmarking.md for recorded
// results and interpretation.
package memtable

import (
	"fmt"
	"math/rand"
	"testing"
)

// benchKeyspace bounds every steady-state benchmark below to a fixed
// number of distinct keys, regardless of how large the Go benchmark
// framework grows b.N: without this, a Put-heavy benchmark run long
// enough to get a stable ns/op would grow the MemTable (and this
// process's memory) without bound. A fixed keyspace that every iteration
// indexes into with i%benchKeyspace instead models a steady-state
// workload against an already-warm dataset -- see benchmark/README.md's
// "steady-state vs. growth" section for why both shapes matter and are
// measured separately (growth is exercised by BenchmarkMemTablePutSequentialGrowth
// below).
const benchKeyspace = 100_000

// benchValueSizes is the value-size matrix every Put/Mixed benchmark
// below runs across, chosen to span ForgeDB's expected small-to-large
// value range: a tiny config-style value (64B) up to a large blob (64KiB),
// each a power-of-4 step apart. This mirrors the matrix used by the WAL,
// Storage, and DBNode benchmarks so results are directly comparable
// layer-to-layer.
var benchValueSizes = []struct {
	name string
	n    int
}{
	{"64B", 64},
	{"256B", 256},
	{"1KiB", 1024},
	{"4KiB", 4096},
	{"16KiB", 16384},
	{"64KiB", 65536},
}

// benchKey formats a deterministic, fixed-width sequential key so every
// benchmark run compares identical key bytes regardless of i's magnitude
// (a variable-width key, e.g. from fmt.Sprintf("%d", i), would otherwise
// change average key size as the keyspace grows).
func benchKey(i int) []byte {
	return []byte(fmt.Sprintf("bench-key-%010d", i))
}

// benchValue deterministically fills a size-byte value from a fixed seed,
// so repeated benchmark runs (and runs on different machines) operate on
// byte-identical data -- see docs' rule against uncontrolled randomness in
// a benchmark (benchmark/README.md's "Determinism" section).
func benchValue(seed int64, size int) []byte {
	r := rand.New(rand.NewSource(seed))
	v := make([]byte, size)
	r.Read(v)
	return v
}

// preloadedMemTable returns a MemTable with benchKeyspace keys already
// present, each holding a size-byte value -- used by every Get/Delete/Mixed
// benchmark so the measured section never includes the cost of
// originally populating the table (see Setup vs. Measurement in
// benchmark/README.md).
func preloadedMemTable(size int) *MemTable {
	m := New()
	val := benchValue(1, size)
	for i := 0; i < benchKeyspace; i++ {
		m.Put(benchKey(i), val)
	}
	return m
}

// --- Put ---------------------------------------------------------------

// BenchmarkMemTablePut measures steady-state Put cost (insert-or-overwrite
// against a fixed benchKeyspace) across the value-size matrix.
func BenchmarkMemTablePut(b *testing.B) {
	for _, vs := range benchValueSizes {
		b.Run(vs.name, func(b *testing.B) {
			m := New()
			val := benchValue(1, vs.n)
			b.ReportAllocs()
			b.SetBytes(int64(vs.n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := m.Put(benchKey(i%benchKeyspace), val); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

// BenchmarkMemTablePutSequentialGrowth measures Put cost while the
// MemTable grows without bound (a strictly increasing sequential key per
// call, never overwriting). This is the "growth" counterpart to the
// steady-state BenchmarkMemTablePut above -- see benchmark/README.md's
// "storage growth" section -- and is expected to show skip-list insertion
// cost growing (slowly, since skip lists are O(log n)) as the table gets
// larger. Run with a bounded -benchtime (e.g. -benchtime=200000x) to keep
// memory use practical; this benchmark intentionally does not cap its own
// keyspace.
func BenchmarkMemTablePutSequentialGrowth(b *testing.B) {
	m := New()
	val := benchValue(1, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.Put(benchKey(i), val); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

// --- Get -----------------------------------------------------------------

// BenchmarkMemTableGetHit measures Get cost for a key that is always
// present, across the value-size matrix, against a preloaded table.
func BenchmarkMemTableGetHit(b *testing.B) {
	for _, vs := range benchValueSizes {
		b.Run(vs.name, func(b *testing.B) {
			m := preloadedMemTable(vs.n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := m.Get(benchKey(i % benchKeyspace)); !ok {
					b.Fatalf("Get(%d): not found", i%benchKeyspace)
				}
			}
		})
	}
}

// BenchmarkMemTableGetMiss measures Get cost for a key that is never
// present -- the skip list must walk to where the key would be and find
// nothing, which can be cheaper or more expensive than a hit depending on
// where in the list the miss falls; comparing the two is the point.
func BenchmarkMemTableGetMiss(b *testing.B) {
	m := preloadedMemTable(256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := m.Get(benchKey(benchKeyspace + i)); ok {
			b.Fatalf("Get(missing): unexpectedly found")
		}
	}
}

// --- Delete ----------------------------------------------------------------

// BenchmarkMemTableDelete measures tombstone-write cost against a fixed
// keyspace (every iteration re-deletes an already-deleted or live key in
// rotation, so the table's live/tombstone mix stabilizes rather than
// growing).
func BenchmarkMemTableDelete(b *testing.B) {
	m := preloadedMemTable(256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.Delete(benchKey(i % benchKeyspace)); err != nil {
			b.Fatalf("Delete: %v", err)
		}
	}
}

// --- Mixed read/write workloads ---------------------------------------------

// runMixedWorkload drives writePercent% Puts and (100-writePercent)% Gets
// against a fixed keyspace, using a deterministic per-call decision so the
// exact sequence of operations is reproducible. keys are uniformly
// distributed across benchKeyspace -- see benchmark/README.md's key
// distribution section for why uniform is the default and hot-set/
// sequential are called out separately where used.
func runMixedWorkload(b *testing.B, writePercent int) {
	m := preloadedMemTable(256)
	val := benchValue(2, 256)
	r := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(r.Intn(benchKeyspace))
		if r.Intn(100) < writePercent {
			m.Put(key, val)
		} else {
			m.Get(key)
		}
	}
}

func BenchmarkMemTableMixed_ReadHeavy_90_10(b *testing.B)  { runMixedWorkload(b, 10) }
func BenchmarkMemTableMixed_Balanced_50_50(b *testing.B)   { runMixedWorkload(b, 50) }
func BenchmarkMemTableMixed_WriteHeavy_10_90(b *testing.B) { runMixedWorkload(b, 90) }

// --- Concurrency / contention ------------------------------------------------

// BenchmarkMemTableConcurrentMixed measures how a 50/50 read/write mix
// scales across increasing concurrency levels, since MemTable's
// sync.RWMutex (see memtable.go) is a single lock shared by every caller:
// this is where that lock's contention, if any, should become visible.
// Go's b.RunParallel spreads iterations across GOMAXPROCS goroutines by
// default; b.SetParallelism scales that multiplier to approximate the
// requested concurrency levels.
func BenchmarkMemTableConcurrentMixed(b *testing.B) {
	for _, concurrency := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			m := preloadedMemTable(256)
			val := benchValue(2, 256)
			b.SetParallelism(concurrency)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewSource(rand.Int63()))
				for pb.Next() {
					key := benchKey(r.Intn(benchKeyspace))
					if r.Intn(100) < 50 {
						m.Put(key, val)
					} else {
						m.Get(key)
					}
				}
			})
		})
	}
}
