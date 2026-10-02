// Phase 13 benchmarks for storage.MemStore: the real, public Store
// implementation every ForgeDB node uses (WAL + MemTable, see
// memstore.go). Unlike the memtable and wal packages' own benchmarks,
// these go through the Store interface exactly as the state machine does
// (internal/statemachine's KVStateMachine.execute), so every number here
// includes the WAL Append+Sync durability cost on every Put/Delete -- see
// benchmark/README.md for how these compare to the memtable-only and
// wal-only numbers.
package storage

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Kushall-07/forgedb/internal/logging"
	"github.com/Kushall-07/forgedb/internal/storage/wal"
)

func init() { logging.SetLevel(slog.LevelError) }

// benchKeyspace is deliberately much smaller than memtable's own
// benchKeyspace: every MemStore write here performs a real fsync (see
// memstore.go's Put/Delete), so a keyspace sized for an in-memory-only
// benchmark would make every preload step here take minutes instead of
// seconds. 2,000 keys is still large enough to exercise realistic
// skip-list depth while keeping fsync-bound setup practical.
const benchKeyspace = 2_000

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

func benchValue(size int) []byte {
	r := rand.New(rand.NewSource(1))
	v := make([]byte, size)
	r.Read(v)
	return v
}

func benchKey(i int) []byte {
	return []byte(fmt.Sprintf("bench-key-%010d", i))
}

func openBenchStore(b *testing.B) *MemStore {
	b.Helper()
	s, err := NewMemStore(b.TempDir())
	if err != nil {
		b.Fatalf("NewMemStore: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// preloadedBenchStore returns a MemStore with benchKeyspace keys already
// durably written, each holding a size-byte value, for read-path
// benchmarks that must not include write setup in their measured section.
func preloadedBenchStore(b *testing.B, size int) *MemStore {
	b.Helper()
	s := openBenchStore(b)
	val := benchValue(size)
	for i := 0; i < benchKeyspace; i++ {
		if err := s.Put(benchKey(i), val); err != nil {
			b.Fatalf("preload Put: %v", err)
		}
	}
	return s
}

// --- Write-only (durable) ----------------------------------------------------

// BenchmarkStorePut is the real production write path: WAL Append, WAL
// Sync (fsync), then MemTable upsert -- see memstore.go's Put. This is
// the number docs/benchmarking/phase13-benchmarking.md calls "the durable
// single-node write cost" that every higher layer (state machine, Raft,
// DBNode) builds on.
func BenchmarkStorePut(b *testing.B) {
	for _, vs := range benchValueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := openBenchStore(b)
			val := benchValue(vs.n)
			b.ReportAllocs()
			b.SetBytes(int64(vs.n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Put(benchKey(i%benchKeyspace), val); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

// --- Read-only ---------------------------------------------------------------

// BenchmarkStoreGet measures Get against a preloaded store -- a pure
// MemTable read (see memstore.go's Get: it never touches the WAL), so
// this isolates read cost from the write path's durability cost entirely.
func BenchmarkStoreGet(b *testing.B) {
	for _, vs := range benchValueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := preloadedBenchStore(b, vs.n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.Get(benchKey(i % benchKeyspace)); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		})
	}
}

// --- Read-after-write ---------------------------------------------------------

// BenchmarkStoreReadAfterWrite alternates a durable Put with an immediate
// Get of the same key, approximating a client that writes and then
// confirms its own write.
func BenchmarkStoreReadAfterWrite(b *testing.B) {
	s := openBenchStore(b)
	val := benchValue(256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(i % benchKeyspace)
		if err := s.Put(key, val); err != nil {
			b.Fatalf("Put: %v", err)
		}
		if _, err := s.Get(key); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// --- Delete --------------------------------------------------------------------

// BenchmarkStoreDelete measures the durable delete path (WAL tombstone
// Append+Sync, then MemTable tombstone) against a fixed keyspace.
func BenchmarkStoreDelete(b *testing.B) {
	s := preloadedBenchStore(b, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Delete(benchKey(i % benchKeyspace)); err != nil {
			b.Fatalf("Delete: %v", err)
		}
	}
}

// --- Mixed read/write workloads ------------------------------------------------

// runMixedStoreWorkload drives writePercent% durable Puts and
// (100-writePercent)% Gets against a fixed keyspace with a deterministic
// per-call decision, uniformly distributed across benchKeyspace.
func runMixedStoreWorkload(b *testing.B, writePercent int) {
	s := preloadedBenchStore(b, 256)
	val := benchValue(256)
	r := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := benchKey(r.Intn(benchKeyspace))
		if r.Intn(100) < writePercent {
			if err := s.Put(key, val); err != nil {
				b.Fatalf("Put: %v", err)
			}
		} else {
			if _, err := s.Get(key); err != nil && err != ErrKeyNotFound {
				b.Fatalf("Get: %v", err)
			}
		}
	}
}

func BenchmarkStoreMixed_ReadOnly_100_0(b *testing.B) {
	s := preloadedBenchStore(b, 256)
	r := rand.New(rand.NewSource(42))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get(benchKey(r.Intn(benchKeyspace))); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}
func BenchmarkStoreMixed_ReadHeavy_90_10(b *testing.B)  { runMixedStoreWorkload(b, 10) }
func BenchmarkStoreMixed_Balanced_50_50(b *testing.B)   { runMixedStoreWorkload(b, 50) }
func BenchmarkStoreMixed_WriteHeavy_10_90(b *testing.B) { runMixedStoreWorkload(b, 90) }
func BenchmarkStoreMixed_WriteOnly_0_100(b *testing.B) {
	s := openBenchStore(b)
	val := benchValue(256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(benchKey(i%benchKeyspace), val); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

// --- Concurrency ----------------------------------------------------------------

// BenchmarkStoreConcurrentPut measures concurrent durable Put throughput:
// every call still goes through the single underlying WAL's mutex-guarded
// Append+Sync (see wal.go), so this is where WAL-level write contention,
// not just MemTable contention, should appear.
func BenchmarkStoreConcurrentPut(b *testing.B) {
	for _, concurrency := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			s := openBenchStore(b)
			val := benchValue(256)
			b.SetParallelism(concurrency)
			b.ReportAllocs()
			b.ResetTimer()
			var counter int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := counter
					counter++
					if err := s.Put(benchKey(int(i)%benchKeyspace), val); err != nil {
						b.Fatalf("Put: %v", err)
					}
				}
			})
		})
	}
}

// --- Reopen / recovery ------------------------------------------------------------

// BenchmarkMemStoreReopen measures startup/reopen time (WAL replay into a
// fresh MemTable, per NewMemStore) as a function of how many records the
// data directory's WAL already contains -- a separate question from
// steady-state Put/Get latency. For each record count, a store is
// populated once outside the timed loop, closed, and then reopened b.N
// times.
func BenchmarkMemStoreReopen(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			dir := filepath.Join(b.TempDir(), "store")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				b.Fatalf("MkdirAll: %v", err)
			}
			// Populate the WAL directly (one Sync at the end, not one per
			// record) rather than through MemStore.Put: Put's per-call
			// fsync (see memstore.go) is the exact cost BenchmarkStorePut
			// already measures, and paying it n times here would make this
			// setup step alone take minutes at n=100,000 for no benefit --
			// a durably-closed WAL of n records is all NewMemStore's
			// replay actually needs to exist beforehand.
			val := benchValue(256)
			w, err := wal.Open(filepath.Join(dir, walFileName))
			if err != nil {
				b.Fatalf("wal.Open: %v", err)
			}
			for i := 0; i < n; i++ {
				if err := w.Append(wal.Record{Type: wal.OpPut, Key: benchKey(i), Value: val}); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}
			if err := w.Sync(); err != nil {
				b.Fatalf("Sync: %v", err)
			}
			if err := w.Close(); err != nil {
				b.Fatalf("Close: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				b.StartTimer()
				reopened, err := NewMemStore(dir)
				if err != nil {
					b.Fatalf("NewMemStore (reopen): %v", err)
				}
				b.StopTimer()
				stats := reopened.Stats()
				if stats.MemTableEntries != n {
					b.Fatalf("recovered %d entries, want %d", stats.MemTableEntries, n)
				}
				reopened.Close()
				b.StartTimer()
			}
		})
	}
}
