package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// newTestCluster builds n Nodes wired together through a shared
// InMemoryTransport, with IDs "node0".."node{n-1}" and every node's Peers
// set to every other node's ID. configure, if non-nil, is called with
// each node's Options before construction so a test can override timing
// or randomness; it must not touch ID, Peers, or Transport.
//
// Each node gets its own deterministically-seeded *rand.Rand (seeded from
// its index) purely so that election timeouts differ across nodes the
// same way on every test run -- a real cluster relies on this spread to
// avoid repeated split votes, and a fixed-per-node seed keeps that
// property while keeping the test itself deterministic.
func newTestCluster(t *testing.T, n int, configure func(id string, opts *Options)) (*InMemoryTransport, []*Node) {
	t.Helper()
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
		opts := Options{
			ID:        id,
			Peers:     peers,
			Transport: tr,
			Rand:      rand.New(rand.NewSource(int64(i) + 1)),
		}
		if configure != nil {
			configure(id, &opts)
		}
		node := NewNode(opts)
		nodes[i] = node
		tr.Register(id, node)
	}
	return tr, nodes
}

// drainAll calls Drain on every node. Each Node's Drain only waits on RPC
// goroutines that node itself spawned (directly, or transitively by
// processing a reply -- see trackRPC); since RPCHandler implementations
// never spawn goroutines of their own in response to an incoming call,
// draining every node once is enough to settle an entire round of
// cluster activity triggered by a batch of Tick/Propose calls.
func drainAll(nodes []*Node) {
	for _, n := range nodes {
		n.Drain()
	}
}

// tickAll calls Tick on every node once.
func tickAll(nodes []*Node) {
	for _, n := range nodes {
		n.Tick()
	}
}

// electLeader drives node's election timeout deterministically (by
// ticking it enough times to guarantee its randomized timeout has
// elapsed, per its configured ElectionTickMax) and drains it, then fails
// the test if node did not become leader.
func electLeader(t *testing.T, node *Node, maxElectionTicks int) {
	t.Helper()
	for i := 0; i < maxElectionTicks; i++ {
		node.Tick()
	}
	node.Drain()
	if !node.IsLeader() {
		term, role, _ := node.State()
		t.Fatalf("node %s did not become leader: term=%d role=%s", node.ID(), term, role)
	}
}

// countLeaders returns how many nodes in the cluster currently believe
// themselves to be leader, and the term(s) at which they do.
func countLeaders(nodes []*Node) (count int, terms []uint64) {
	for _, n := range nodes {
		term, role, _ := n.State()
		if role == Leader {
			count++
			terms = append(terms, term)
		}
	}
	return count, terms
}
