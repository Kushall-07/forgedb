package dbnode

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/Kushall-07/forgedb/internal/raft"
)

// --- Cluster construction helpers -------------------------------------------
//
// These mirror internal/raft's own newTestCluster/electLeader/drainAll
// helpers (see internal/raft/cluster_test.go) and
// internal/statemachine's equivalent (see internal/statemachine/applier_test.go),
// adapted to build Nodes -- real, file-backed Raft persistence and KV
// storage under a fresh t.TempDir() per node -- instead of bare raft.Nodes
// or bare KVStateMachines. Fast, deterministic election ticks
// (ElectionTickMin == ElectionTickMax) are used throughout so tests never
// depend on real time.

const (
	testElectionTick  = 5
	testHeartbeatTick = 1
)

// nodeDirs is the pair of independent directories one cluster member is
// opened against. Kept around (rather than discarded after newCluster)
// so a test can Close a Node and later re-Open it against the exact same
// directories to simulate a restart.
type nodeDirs struct {
	raftDir string
	kvDir   string
}

func newNodeConfig(id string, peers []string, tr raft.Transport, dirs nodeDirs) Config {
	return Config{
		ID:              id,
		Peers:           peers,
		Transport:       tr,
		RaftDir:         dirs.raftDir,
		KVDir:           dirs.kvDir,
		ElectionTickMin: testElectionTick,
		ElectionTickMax: testElectionTick,
		HeartbeatTick:   testHeartbeatTick,
	}
}

func mustOpen(t *testing.T, cfg Config) *Node {
	t.Helper()
	n, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open(%+v): %v", cfg, err)
	}
	return n
}

// newCluster builds n Nodes ("node0".."node{n-1}") wired through a shared
// InMemoryTransport, each rooted at its own subdirectory of a fresh
// t.TempDir() (so RaftDir/KVDir are always distinct across the whole
// cluster -- see TestIndependentNodeDirectories for this checked
// explicitly). Every Node is registered for t.Cleanup(Close). Returns the
// transport, the Nodes, and each Node's directories (for restart tests).
func newCluster(t *testing.T, n int) (*raft.InMemoryTransport, []*Node, []nodeDirs) {
	t.Helper()
	tr := raft.NewInMemoryTransport()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}

	nodes := make([]*Node, n)
	dirs := make([]nodeDirs, n)
	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		base := t.TempDir()
		d := nodeDirs{raftDir: filepath.Join(base, "raft"), kvDir: filepath.Join(base, "kv")}
		dirs[i] = d

		node := mustOpen(t, newNodeConfig(id, peers, tr, d))
		nodes[i] = node
		t.Cleanup(func() { node.Close() })
	}
	return tr, nodes, dirs
}

func tickAll(nodes []*Node) {
	for _, n := range nodes {
		n.Tick()
	}
}

func drainAll(nodes []*Node) {
	for _, n := range nodes {
		n.Drain()
	}
}

// electLeader ticks node enough times to guarantee its election timeout
// has elapsed and fails the test if it did not become leader.
func electLeader(t *testing.T, node *Node) {
	t.Helper()
	for i := 0; i < testElectionTick+1; i++ {
		node.Tick()
	}
	node.Drain()
	if !node.IsLeader() {
		t.Fatalf("node %s did not become leader", node.ID())
	}
}

// applyAllAvailable calls ApplyAvailable on every node once, failing the
// test if any of them return an error.
func applyAllAvailable(t *testing.T, nodes []*Node) {
	t.Helper()
	for _, n := range nodes {
		if _, err := n.ApplyAvailable(); err != nil {
			t.Fatalf("node %s ApplyAvailable: %v", n.ID(), err)
		}
	}
}

// settleCommit ticks the whole cluster a couple of extra rounds, draining
// fully in between each one, so that every node's own CommitIndex (not
// just the leader's) has caught up to the leader's. A follower only
// learns of a new commit index on the *next* AppendEntries round after the
// entry that advanced it (see the leader's own commit-then-broadcast
// ordering in internal/raft/replication.go), so the first round alone is
// never enough. The intermediate drainAll is what makes the second round
// deterministic: without it, the second round's heartbeat could be
// constructed (broadcastAppendEntriesLocked reads the leader's
// CommitIndex synchronously, before any reply to the first round's
// heartbeat has necessarily been processed) before the first round's
// majority replication has actually advanced that CommitIndex, silently
// carrying a stale LeaderCommit forward and requiring a third round to
// notice.
func settleCommit(nodes []*Node) {
	tickAll(nodes)
	drainAll(nodes)
	tickAll(nodes)
	drainAll(nodes)
}

// clusterIDs returns the "node0".."node{n-1}" ID list newCluster uses.
func clusterIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}
	return ids
}

// peersOf returns every ID in ids except self, preserving order -- the
// same Peers computation newCluster performs for each node, exposed here
// so a restart test can reconstruct one node's Config without rebuilding
// the whole cluster.
func peersOf(ids []string, self string) []string {
	peers := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}
