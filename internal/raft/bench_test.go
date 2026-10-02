// Phase 13 benchmarks for internal/raft in isolation: consensus algorithm
// cost with no state machine, no storage, and no real persistence layered
// on top (Persister defaults to the in-memory no-op discardPersister --
// see raft.go's Options doc). Two limitations apply to every benchmark in
// this file, documented in full in benchmark/README.md:
//
//  1. Transport is raft.InMemoryTransport: these numbers measure the Raft
//     algorithm plus an in-process function-call transport, never real
//     network latency (TCP/gRPC). See docs/raft/phase5-raft-consensus.md
//     and benchmark/README.md's "InMemoryTransport is not a network"
//     section.
//  2. No Persister means these numbers exclude the fsync cost a
//     real FilePersister-backed node pays on every Propose (see
//     persist.go and internal/dbnode's own benchmarks, which use the real
//     FilePersister and so include that cost). This file isolates pure
//     consensus overhead; internal/dbnode/bench_test.go isolates the
//     fully-durable end-to-end cost.
package raft

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/Kushall-07/forgedb/internal/logging"
)

func init() { logging.SetLevel(slog.LevelError) }

// benchElectionTick/benchHeartbeatTick configure every cluster this file
// builds for fast, deterministic elections -- equal Min/Max ticks, like
// internal/dbnode/cluster_test.go's testElectionTick, since every
// benchmark here elects one specific, chosen node rather than relying on
// a natural, timing-driven election.
const (
	benchElectionTick  = 5
	benchHeartbeatTick = 1
)

// benchNewCluster is newTestCluster's *testing.B analogue (see
// cluster_test.go): n Nodes "node0".."node{n-1}", wired through one
// shared InMemoryTransport, using fast deterministic election timing.
func benchNewCluster(b *testing.B, n int) (*InMemoryTransport, []*Node) {
	b.Helper()
	tr := NewInMemoryTransport()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}
	nodes := make([]*Node, n)
	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node, err := NewNode(Options{
			ID:              id,
			Peers:           peers,
			Transport:       tr,
			ElectionTickMin: benchElectionTick,
			ElectionTickMax: benchElectionTick,
			HeartbeatTick:   benchHeartbeatTick,
		})
		if err != nil {
			b.Fatalf("NewNode(%s): %v", id, err)
		}
		nodes[i] = node
		tr.Register(id, node)
	}
	return tr, nodes
}

// benchElectLeader is electLeader's *testing.B analogue.
func benchElectLeader(b *testing.B, node *Node) {
	b.Helper()
	for i := 0; i < benchElectionTick+1; i++ {
		node.Tick()
	}
	node.Drain()
	if !node.IsLeader() {
		b.Fatalf("node %s did not become leader", node.ID())
	}
}

// --- Propose / commit latency, by cluster size ------------------------------

// runProposeBenchmark proposes b.N commands through nodes[0] (elected
// leader), draining after each one so its replication RPCs complete
// before the next Propose -- a single Propose+Drain round is sufficient
// for the leader's own CommitIndex to advance once a majority
// acknowledges (see maybeAdvanceCommitIndexLocked, called synchronously
// while processing each AppendEntries reply). This measures one
// sequential client's commit latency, not concurrent throughput -- see
// BenchmarkRaftProposeConcurrent for the latter.
//
// There is deliberately no single-node ("clusterSize=1") variant: as
// apply_test.go's commitUpTo helper documents, maybeAdvanceCommitIndexLocked
// is only ever invoked from a peer's AppendEntries reply, so a one-node
// cluster's own CommitIndex never advances through Propose alone in this
// implementation -- there is no "commit" to time. See
// docs/benchmarking/phase13-benchmarking.md's known-limitations section
// for this finding and BenchmarkRaftProposeSingleNodeUnconfirmed below for
// what a single-node Propose call does cost.
func runProposeBenchmark(b *testing.B, clusterSize int) {
	_, nodes := benchNewCluster(b, clusterSize)
	leader := nodes[0]
	benchElectLeader(b, leader)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index, _, err := leader.Propose(Command(fmt.Sprintf("cmd-%d", i)))
		if err != nil {
			b.Fatalf("Propose: %v", err)
		}
		leader.Drain()
		if leader.CommitIndex() < index {
			b.Fatalf("CommitIndex = %d after Drain, want >= %d", leader.CommitIndex(), index)
		}
	}
}

func BenchmarkRaftProposeThreeNode(b *testing.B) { runProposeBenchmark(b, 3) }
func BenchmarkRaftProposeFiveNode(b *testing.B)  { runProposeBenchmark(b, 5) }

// BenchmarkRaftProposeSingleNodeUnconfirmed measures Propose's own call
// cost (append to local log + persist + a no-op broadcast over zero
// peers) on a single-node cluster. Unlike every other Propose benchmark
// in this file, this does NOT wait for or confirm a commit -- see
// runProposeBenchmark's doc comment above for why a one-node cluster
// never commits through Propose alone in this implementation. This number
// must never be compared directly against BenchmarkRaftProposeThreeNode/
// FiveNode, which both measure a confirmed commit.
func BenchmarkRaftProposeSingleNodeUnconfirmed(b *testing.B) {
	_, nodes := benchNewCluster(b, 1)
	leader := nodes[0]
	benchElectLeader(b, leader)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := leader.Propose(Command(fmt.Sprintf("cmd-%d", i))); err != nil {
			b.Fatalf("Propose: %v", err)
		}
	}
}

// --- Read-index (linearizable read barrier) cost, by cluster size ----------

// runReadIndexBenchmark measures ReadIndex's quorum-confirmation round
// trip -- the mechanism dbnode.Node.ConsistentGet builds on (see read.go)
// -- in isolation from the storage-apply wait ConsistentGet also performs.
func runReadIndexBenchmark(b *testing.B, clusterSize int) {
	_, nodes := benchNewCluster(b, clusterSize)
	leader := nodes[0]
	benchElectLeader(b, leader)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := leader.ReadIndex(); err != nil {
			b.Fatalf("ReadIndex: %v", err)
		}
	}
}

func BenchmarkRaftReadIndexSingleNode(b *testing.B) { runReadIndexBenchmark(b, 1) }
func BenchmarkRaftReadIndexThreeNode(b *testing.B)  { runReadIndexBenchmark(b, 3) }
func BenchmarkRaftReadIndexFiveNode(b *testing.B)   { runReadIndexBenchmark(b, 5) }

// --- Election convergence ---------------------------------------------------

// BenchmarkRaftElectionConvergence measures how long a fresh cluster
// takes to elect its first leader, by cluster size: cluster construction
// is excluded from the timed section (see benchmark/README.md's
// setup-vs-measurement rule), leaving only the Tick/Drain rounds
// benchElectLeader performs.
func BenchmarkRaftElectionConvergence(b *testing.B) {
	for _, n := range []int{1, 3, 5} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				_, nodes := benchNewCluster(b, n)
				b.StartTimer()

				benchElectLeader(b, nodes[0])

				b.StopTimer()
				for _, node := range nodes {
					node.Stop()
				}
				b.StartTimer()
			}
		})
	}
}

// --- Concurrent proposal throughput -----------------------------------------

// BenchmarkRaftProposeConcurrent measures Propose throughput under
// concurrent callers against a single leader -- all Propose calls are
// serialized through the leader's own mutex (see raft.go's Node doc
// comment), so this is where that lock's contention, not replication
// itself, should dominate as concurrency increases.
func BenchmarkRaftProposeConcurrent(b *testing.B) {
	for _, concurrency := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
			_, nodes := benchNewCluster(b, 3)
			leader := nodes[0]
			benchElectLeader(b, leader)

			b.SetParallelism(concurrency)
			b.ReportAllocs()
			b.ResetTimer()
			var counter int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := atomic.AddInt64(&counter, 1)
					if _, _, err := leader.Propose(Command(fmt.Sprintf("cmd-%d", i))); err != nil {
						b.Fatalf("Propose: %v", err)
					}
				}
			})
			leader.Drain()
		})
	}
}
