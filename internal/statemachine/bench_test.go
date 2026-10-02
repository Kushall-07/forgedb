// Phase 13 benchmarks for the state machine's Apply boundary: where a
// committed Raft command becomes a storage mutation (or a deduplicated
// no-op) -- see statemachine.go. Every benchmark here runs against a
// real storage.NewMemStore, so these numbers already include the WAL
// Append+Sync durability cost storage/bench_test.go measures directly;
// what's added here is dedup-table bookkeeping and command dispatch. See
// benchmark/README.md for how to separate the two.
package statemachine

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/Kushall-07/forgedb/internal/logging"
	"github.com/Kushall-07/forgedb/internal/storage"
)

func init() { logging.SetLevel(slog.LevelError) }

func openBenchStateMachine(b *testing.B) *KVStateMachine {
	b.Helper()
	store, err := storage.NewMemStore(b.TempDir())
	if err != nil {
		b.Fatalf("NewMemStore: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	return NewKVStateMachine(store)
}

func benchKey(i int) []byte { return []byte(fmt.Sprintf("bench-key-%010d", i)) }

// --- New requests --------------------------------------------------------

// BenchmarkApplyPutNew measures Apply for a PUT that is always a new
// (ClientID, RequestID) -- the common case, where the dedup table check
// is a miss and a real storage.Put executes.
func BenchmarkApplyPutNew(b *testing.B) {
	sm := openBenchStateMachine(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := NewPutCommand(fmt.Sprintf("client-%d", i), 1, benchKey(i), []byte("value"))
		res, err := sm.Apply(cmd)
		if err != nil {
			b.Fatalf("Apply: %v", err)
		}
		if res.Err != nil {
			b.Fatalf("Apply result error: %v", res.Err)
		}
	}
}

// BenchmarkApplyDeleteNew measures Apply for a DELETE that is always a
// new (ClientID, RequestID), against keys preloaded by prior PUTs so the
// delete is a real tombstone write, not a no-op on an absent key.
func BenchmarkApplyDeleteNew(b *testing.B) {
	sm := openBenchStateMachine(b)
	const keyspace = 2_000
	for i := 0; i < keyspace; i++ {
		if _, err := sm.Apply(NewPutCommand("preload", uint64(i+1), benchKey(i), []byte("value"))); err != nil {
			b.Fatalf("preload Apply: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := NewDeleteCommand(fmt.Sprintf("client-%d", i), 1, benchKey(i%keyspace))
		if _, err := sm.Apply(cmd); err != nil {
			b.Fatalf("Apply: %v", err)
		}
	}
}

// --- Deduplication ---------------------------------------------------------

// BenchmarkApplyDuplicateRequest measures Apply's dedup-hit path: the
// identical (ClientID, RequestID, Op, Key, Value) applied repeatedly,
// which must be resolved from the in-memory dedup table alone, without a
// second storage mutation -- see statemachine.go's Apply, the
// cmd.RequestID == prev.requestID branch.
func BenchmarkApplyDuplicateRequest(b *testing.B) {
	sm := openBenchStateMachine(b)
	cmd := NewPutCommand("client-dup", 1, []byte("key"), []byte("value"))
	if _, err := sm.Apply(cmd); err != nil {
		b.Fatalf("initial Apply: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := sm.Apply(cmd)
		if err != nil {
			b.Fatalf("Apply: %v", err)
		}
		if !res.Replayed {
			b.Fatalf("Apply: Result.Replayed = false, want true (dedup hit)")
		}
	}
}

// BenchmarkApplyNewVsDuplicate runs the two cases back to back under one
// -bench filter so a single invocation's results sit next to each other
// in output, making the new-vs-replayed cost difference easy to read off
// (see docs/benchmarking/phase13-benchmarking.md's dedup-cost analysis).
func BenchmarkApplyNewVsDuplicate(b *testing.B) {
	b.Run("New", func(b *testing.B) {
		sm := openBenchStateMachine(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmd := NewPutCommand(fmt.Sprintf("client-%d", i), 1, benchKey(i), []byte("value"))
			if _, err := sm.Apply(cmd); err != nil {
				b.Fatalf("Apply: %v", err)
			}
		}
	})
	b.Run("Duplicate", func(b *testing.B) {
		sm := openBenchStateMachine(b)
		cmd := NewPutCommand("client-dup", 1, []byte("key"), []byte("value"))
		if _, err := sm.Apply(cmd); err != nil {
			b.Fatalf("initial Apply: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := sm.Apply(cmd); err != nil {
				b.Fatalf("Apply: %v", err)
			}
		}
	})
}
