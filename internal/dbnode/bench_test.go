// Phase 13 benchmarks for dbnode.Node: the full production composition
// (Raft, with real raft.FilePersister, + Applier/KVStateMachine + real
// storage.MemStore -- see node.go's package doc). Every write benchmark
// here goes through the exact path a real client uses (Propose -> commit
// -> Applier.ApplyAvailable -> storage.Store), so these numbers are the
// fully durable, fully replicated cost: WAL fsync, Raft log fsync
// (FilePersister), and quorum replication all included. Compare against
// internal/raft/bench_test.go (no persistence, no storage) and
// internal/storage/bench_test.go (storage alone, no Raft) to see how much
// each layer adds -- see benchmark/README.md.
//
// As in internal/raft/bench_test.go, there is no single-node committed-
// write benchmark: a literal one-node (zero-peer) Raft cluster never
// advances its own CommitIndex through Propose alone in this codebase
// (see internal/raft/apply_test.go's commitUpTo comment and this
// package's own node_test.go section A, which documents the identical
// choice for its integration tests). Every committed-write/ConsistentGet
// benchmark below therefore uses a 3- or 5-node cluster, matching
// benchmark/README.md's cluster-size matrix.
package dbnode

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/Kushall-07/forgedb/internal/logging"
	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

func init() { logging.SetLevel(slog.LevelError) }

// benchNewCluster is newCluster's *testing.B analogue (see
// cluster_test.go): n real Nodes, each under its own b.TempDir()
// subdirectories, wired through a shared InMemoryTransport.
func benchNewCluster(b *testing.B, n int) (*raft.InMemoryTransport, []*Node) {
	b.Helper()
	tr := raft.NewInMemoryTransport()
	ids := clusterIDs(n)
	nodes := make([]*Node, n)
	for i, id := range ids {
		base := b.TempDir()
		dirs := nodeDirs{raftDir: filepath.Join(base, "raft"), kvDir: filepath.Join(base, "kv")}
		node, err := Open(newNodeConfig(id, peersOf(ids, id), tr, dirs))
		if err != nil {
			b.Fatalf("Open(%s): %v", id, err)
		}
		nodes[i] = node
		b.Cleanup(func() { node.Close() })
	}
	return tr, nodes
}

func benchElectLeader(b *testing.B, node *Node) {
	b.Helper()
	for i := 0; i < testElectionTick+1; i++ {
		node.Tick()
	}
	node.Drain()
	if !node.IsLeader() {
		b.Fatalf("node %s did not become leader", node.ID())
	}
}

func benchApplyAllAvailable(b *testing.B, nodes []*Node) {
	b.Helper()
	for _, n := range nodes {
		if _, err := n.ApplyAvailable(); err != nil {
			b.Fatalf("node %s ApplyAvailable: %v", n.ID(), err)
		}
	}
}

func benchKey(i int) []byte { return []byte(fmt.Sprintf("bench-key-%010d", i)) }

// --- Fully durable, fully replicated Put/Delete, by cluster size -----------

// runCommittedPutBenchmark proposes b.N PUTs through the leader of an
// n-node cluster, driving each one through commit (settleCommit) and
// apply (ApplyAvailable) before measuring the next -- a sequential
// client's full write latency, not concurrent throughput.
//
// IMPORTANT: raft.FilePersister.SaveState rewrites and fsyncs this
// node's *entire* Raft log on every single Propose (see file_persister.go's
// package doc: "SaveState writes the complete new state ... atomically
// replaces the previous file"), so each call's cost grows with however
// large the log has already become. Across b.N growing iterations,
// reported ns/op is therefore an *average* over a workload whose true
// per-call cost is increasing, not a flat steady-state number -- this is
// a genuine, measured property of the real production persistence layer,
// not a benchmark artifact, and is called out in
// docs/benchmarking/phase13-benchmarking.md's bottleneck analysis. Run
// with a bounded -benchtime (an explicit Nx count, e.g. -benchtime=200x)
// rather than a time budget, so results stay comparable across machines
// regardless of how many iterations a time-based run happened to fit.
func runCommittedPutBenchmark(b *testing.B, clusterSize int) {
	_, nodes := benchNewCluster(b, clusterSize)
	leader := nodes[0]
	benchElectLeader(b, leader)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := statemachine.NewPutCommand(fmt.Sprintf("client-%d", i), 1, benchKey(i), []byte("value"))
		if _, _, err := leader.Propose(cmd); err != nil {
			b.Fatalf("Propose: %v", err)
		}
		leader.Drain()
		settleCommit(nodes)
		benchApplyAllAvailable(b, nodes)
	}
}

func BenchmarkDBNodePutThreeNode(b *testing.B) { runCommittedPutBenchmark(b, 3) }
func BenchmarkDBNodePutFiveNode(b *testing.B)  { runCommittedPutBenchmark(b, 5) }

// runCommittedDeleteBenchmark mirrors runCommittedPutBenchmark for
// DELETE, against keys preloaded (and committed/applied) before the timed
// section.
func runCommittedDeleteBenchmark(b *testing.B, clusterSize int) {
	_, nodes := benchNewCluster(b, clusterSize)
	leader := nodes[0]
	benchElectLeader(b, leader)

	// Kept small: raft.FilePersister rewrites the entire Raft log on every
	// Propose (see runCommittedPutBenchmark's doc comment above), so
	// preloading a large keyspace here would make setup itself the
	// dominant cost rather than the thing being measured.
	const keyspace = 150
	for i := 0; i < keyspace; i++ {
		cmd := statemachine.NewPutCommand("preload", uint64(i+1), benchKey(i), []byte("value"))
		if _, _, err := leader.Propose(cmd); err != nil {
			b.Fatalf("preload Propose: %v", err)
		}
	}
	leader.Drain()
	settleCommit(nodes)
	benchApplyAllAvailable(b, nodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := statemachine.NewDeleteCommand(fmt.Sprintf("client-%d", i), 1, benchKey(i%keyspace))
		if _, _, err := leader.Propose(cmd); err != nil {
			b.Fatalf("Propose: %v", err)
		}
		leader.Drain()
		settleCommit(nodes)
		benchApplyAllAvailable(b, nodes)
	}
}

func BenchmarkDBNodeDeleteThreeNode(b *testing.B) { runCommittedDeleteBenchmark(b, 3) }

// --- Local (non-linearizable) read ------------------------------------------

// BenchmarkDBNodeGetLocal measures Store().Get on the leader of a 3-node
// cluster against preloaded, committed, applied data: a direct local
// storage read with no Raft involvement at all, the non-linearizable
// counterpart to BenchmarkDBNodeConsistentGetThreeNode below. See node.go's
// ConsistentGet doc comment for exactly what guarantee this read lacks by
// comparison.
func BenchmarkDBNodeGetLocal(b *testing.B) {
	_, nodes := benchNewCluster(b, 3)
	leader := nodes[0]
	benchElectLeader(b, leader)

	// Kept small for the same reason as runCommittedDeleteBenchmark's
	// keyspace above: preload cost is dominated by FilePersister's
	// full-log rewrite per Propose, not by the Get path this benchmark
	// actually measures.
	const keyspace = 150
	for i := 0; i < keyspace; i++ {
		cmd := statemachine.NewPutCommand("preload", uint64(i+1), benchKey(i), []byte("value"))
		if _, _, err := leader.Propose(cmd); err != nil {
			b.Fatalf("preload Propose: %v", err)
		}
	}
	leader.Drain()
	settleCommit(nodes)
	benchApplyAllAvailable(b, nodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := leader.Store().Get(benchKey(i % keyspace)); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// --- Linearizable (ConsistentGet) read, by cluster size ---------------------

// runConsistentGetBenchmark measures ConsistentGet against preloaded,
// committed, applied data on the leader of an n-node cluster. ctx is
// context.Background(): since this benchmark drives commit/apply
// deterministically via settleCommit/ApplyAvailable before ever calling
// ConsistentGet, the internal wait for the state machine to catch up to
// the read barrier never actually blocks (see node.go's WaitApplied).
func runConsistentGetBenchmark(b *testing.B, clusterSize int) {
	_, nodes := benchNewCluster(b, clusterSize)
	leader := nodes[0]
	benchElectLeader(b, leader)

	const keyspace = 150
	for i := 0; i < keyspace; i++ {
		cmd := statemachine.NewPutCommand("preload", uint64(i+1), benchKey(i), []byte("value"))
		if _, _, err := leader.Propose(cmd); err != nil {
			b.Fatalf("preload Propose: %v", err)
		}
	}
	leader.Drain()
	settleCommit(nodes)
	benchApplyAllAvailable(b, nodes)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := leader.ConsistentGet(ctx, benchKey(i%keyspace)); err != nil {
			b.Fatalf("ConsistentGet: %v", err)
		}
	}
}

func BenchmarkDBNodeConsistentGetThreeNode(b *testing.B) { runConsistentGetBenchmark(b, 3) }
func BenchmarkDBNodeConsistentGetFiveNode(b *testing.B)  { runConsistentGetBenchmark(b, 5) }
