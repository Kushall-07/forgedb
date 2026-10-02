// Phase 13 benchmarks for the write-ahead log: the durability boundary
// every MemStore.Put/Delete call crosses (see wal.go and
// ../memstore.go). Append and Sync are benchmarked separately because
// Sync (fsync) is the actual cost a durable write pays -- Append alone
// only measures in-process buffering and checksum computation. See
// benchmark/README.md for why "Append+Sync" rather than "Append" is the
// number that matters for interpreting Storage/DBNode write latency.
package wal

import (
	"fmt"
	"log/slog"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/Kushall-07/forgedb/internal/logging"
)

// Benchmarks exercise the real WAL, including its Info-level recovery log
// line on every Replay -- silence anything below Error so logging I/O
// never skews measured latency (see benchmark/README.md's "warm-up and
// noise" section).
func init() { logging.SetLevel(slog.LevelError) }

// benchValueSizes mirrors memtable's matrix, bounded by this WAL
// implementation's own maxValueSize (64 MiB, see record.go) -- every size
// here is far below that limit.
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

func openBenchWAL(b *testing.B) *WAL {
	b.Helper()
	w, err := Open(filepath.Join(b.TempDir(), "wal.log"))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	return w
}

// --- Append only (no durability) --------------------------------------------

// BenchmarkWALAppendNoSync measures buffered Append cost alone, with no
// fsync -- this is deliberately NOT what a production write path does
// (see memstore.go: every Put/Delete calls Sync immediately after
// Append), and exists only so BenchmarkWALAppendSync's fsync overhead can
// be isolated by comparison. Labeled clearly per benchmark/README.md's
// "what is and isn't durable" rule.
func BenchmarkWALAppendNoSync(b *testing.B) {
	for _, vs := range benchValueSizes {
		b.Run(vs.name, func(b *testing.B) {
			w := openBenchWAL(b)
			val := benchValue(vs.n)
			b.ReportAllocs()
			b.SetBytes(int64(vs.n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Append(Record{Type: OpPut, Key: benchKey(i), Value: val}); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}
		})
	}
}

// --- Append + Sync (the real durability boundary) ---------------------------

// BenchmarkWALAppendSync is the most important benchmark in this file: it
// measures the actual cost MemStore.Put/Delete pays on every call --
// Append followed immediately by an fsync -- across the value-size
// matrix. See docs/benchmarking/phase13-benchmarking.md for how this
// compares to BenchmarkWALAppendNoSync and what fraction of write latency
// the durability boundary accounts for.
func BenchmarkWALAppendSync(b *testing.B) {
	for _, vs := range benchValueSizes {
		b.Run(vs.name, func(b *testing.B) {
			w := openBenchWAL(b)
			val := benchValue(vs.n)
			b.ReportAllocs()
			b.SetBytes(int64(vs.n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Append(Record{Type: OpPut, Key: benchKey(i), Value: val}); err != nil {
					b.Fatalf("Append: %v", err)
				}
				if err := w.Sync(); err != nil {
					b.Fatalf("Sync: %v", err)
				}
			}
		})
	}
}

// BenchmarkWALAppendSyncSequentialRecords measures throughput of several
// small sequential Append+Sync records back to back (as opposed to the
// single-record-per-op shape above), approximating a batch of individual
// client writes landing one after another under a single active WAL file.
func BenchmarkWALAppendSyncSequentialRecords(b *testing.B) {
	w := openBenchWAL(b)
	val := benchValue(256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			if err := w.Append(Record{Type: OpPut, Key: benchKey(i*10 + j), Value: val}); err != nil {
				b.Fatalf("Append: %v", err)
			}
			if err := w.Sync(); err != nil {
				b.Fatalf("Sync: %v", err)
			}
		}
	}
}

// --- Recovery / replay --------------------------------------------------------

// BenchmarkWALReplay measures recovery (replay) time as a function of how
// many records the WAL being opened already contains -- a separate
// question from steady-state Append+Sync latency (see
// benchmark/README.md's "recovery is not steady-state" rule). For each
// record count, a WAL is populated once outside the timed loop, then
// replayed b.N times.
func BenchmarkWALReplay(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "wal.log")
			w, err := Open(path)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			val := benchValue(256)
			for i := 0; i < n; i++ {
				if err := w.Append(Record{Type: OpPut, Key: benchKey(i), Value: val}); err != nil {
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
				r, err := Open(path)
				if err != nil {
					b.Fatalf("Open: %v", err)
				}
				b.StartTimer()

				var replayed int
				if err := r.Replay(func(Record) error { replayed++; return nil }); err != nil {
					b.Fatalf("Replay: %v", err)
				}

				b.StopTimer()
				if replayed != n {
					b.Fatalf("replayed %d records, want %d", replayed, n)
				}
				r.Close()
				b.StartTimer()
			}
		})
	}
}
