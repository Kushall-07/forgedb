package raft

import (
	"errors"
	"sync"
)

// ErrPeerUnreachable is returned by a Transport send when the target node
// is unknown to the transport, or currently unreachable (see
// InMemoryTransport's partition support). A Node treats it exactly like a
// dropped packet or a timed-out real RPC: the send is simply abandoned,
// with no effect on term, vote, or replication state.
var ErrPeerUnreachable = errors.New("raft: peer unreachable")

// Transport is the abstraction a Node uses to deliver RPCs to other
// nodes. It exists so Raft's consensus logic never depends on a concrete
// network implementation: Phase 5 provides only InMemoryTransport, a
// deterministic, in-process transport suitable for tests; a later phase
// can add a real network transport (gRPC or otherwise) behind the same
// interface without changing anything in this package.
type Transport interface {
	// SendRequestVote delivers a RequestVote RPC to target and returns
	// its reply. An error means the RPC could not be delivered at all
	// (the equivalent of a dropped packet or a network timeout); it does
	// not mean the vote was denied.
	SendRequestVote(target string, args RequestVoteArgs) (RequestVoteReply, error)

	// SendAppendEntries delivers an AppendEntries RPC to target and
	// returns its reply. An error means the RPC could not be delivered
	// at all; it does not mean the follower rejected it.
	SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error)

	// SendInstallSnapshot delivers an InstallSnapshot RPC (Phase 9) to
	// target and returns its reply. An error means the RPC could not be
	// delivered at all; it does not mean the follower rejected it.
	SendInstallSnapshot(target string, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}

// RPCHandler is the receiving side of Transport: whatever is registered
// under a node ID must be able to process the two Raft RPCs and produce a
// reply. *Node implements this interface; InMemoryTransport is written
// against the interface, not *Node directly, so a test can register a
// fake handler in place of a real Node if it ever needs to.
type RPCHandler interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
	HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply
}

// InMemoryTransport is a deterministic, in-process Transport: sending an
// RPC to a registered node ID calls straight into that node's handler in
// the caller's own goroutine (there is no real network, no serialization,
// and no background delivery goroutine of its own), and returns its reply
// immediately. This makes cluster tests fully deterministic -- a send
// either completes synchronously or fails immediately with
// ErrPeerUnreachable, with no timing to race against.
//
// InMemoryTransport also supports simulating network partitions: Partition
// marks a node ID as unreachable (every send to or from it fails), and
// Heal reverses that. This is enough to simulate dropped messages and
// split-brain scenarios for tests without needing per-message drop rates
// or any source of randomness.
//
// The zero value is not usable; use NewInMemoryTransport. InMemoryTransport
// is safe for concurrent use.
type InMemoryTransport struct {
	mu       sync.RWMutex
	handlers map[string]RPCHandler
	isolated map[string]bool

	// groupOf implements PartitionGroups/HealPartitions: when non-empty, it
	// maps a node ID to the index of the group it currently belongs to, and
	// a send between two nodes in different groups fails exactly like a
	// Partition'd node would. It is nil/empty whenever PartitionGroups has
	// never been called or HealPartitions has most recently been called,
	// which is also NewInMemoryTransport's zero value -- so every caller
	// that only ever used Partition/Heal (every pre-Phase-10 test) is
	// completely unaffected by this field's existence. A node never
	// mentioned in any group passed to PartitionGroups is left out of
	// groupOf entirely and therefore remains reachable from (and to)
	// everyone, partitioned or not -- see PartitionGroups.
	groupOf map[string]int
}

// NewInMemoryTransport returns an empty InMemoryTransport with no nodes
// registered.
func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{
		handlers: make(map[string]RPCHandler),
		isolated: make(map[string]bool),
	}
}

// Register associates id with h, so that sends targeting id (and, for the
// purposes of Partition/Heal, sends originating from id) are delivered to
// h. Registering the same id again replaces the previous handler.
func (t *InMemoryTransport) Register(id string, h RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = h
}

// Partition marks id as unreachable: every subsequent send to id, and
// every subsequent send originating from id (identified by the
// CandidateID/LeaderID on the RPC), fails with ErrPeerUnreachable until
// Heal(id) is called. This simulates id being cut off from the rest of
// the cluster by a network partition, in both directions.
func (t *InMemoryTransport) Partition(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolated[id] = true
}

// Heal reverses a prior Partition(id), restoring normal delivery to and
// from id. Healing a node that was never partitioned is a no-op.
func (t *InMemoryTransport) Heal(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.isolated, id)
}

// PartitionGroups divides the cluster into disjoint groups for the
// purposes of message delivery: a send between two nodes placed in
// different groups fails with ErrPeerUnreachable in both directions,
// exactly like Partition, while sends between two nodes in the same group
// (or involving a node left out of every group) continue to be governed
// only by Partition/Heal as before. It replaces whatever grouping a prior
// PartitionGroups call established; HealPartitions removes it entirely.
//
// This is what lets a chaos scenario model a true network split such as
// "A B | C" (equivalent to Partition("C")) or, for a larger cluster, "A B
// C | D E" -- a split Partition alone cannot express, since Partition(id)
// always isolates id from literally everyone, including any other
// partitioned node on the "same side" of the intended split.
func (t *InMemoryTransport) PartitionGroups(groups ...[]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	g := make(map[string]int)
	for i, group := range groups {
		for _, id := range group {
			g[id] = i
		}
	}
	t.groupOf = g
}

// HealPartitions reverses a prior PartitionGroups call, restoring full
// connectivity between every node (subject only to any still-active
// single-node Partition/Heal state). Calling it when no grouping is
// active is a no-op.
func (t *InMemoryTransport) HealPartitions() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.groupOf = nil
}

func (t *InMemoryTransport) resolve(from, to string) (RPCHandler, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.isolated[from] || t.isolated[to] {
		return nil, false
	}
	if len(t.groupOf) > 0 {
		gf, okf := t.groupOf[from]
		gt, okt := t.groupOf[to]
		if okf && okt && gf != gt {
			return nil, false
		}
	}
	h, ok := t.handlers[to]
	return h, ok
}

// SendRequestVote implements Transport.
func (t *InMemoryTransport) SendRequestVote(target string, args RequestVoteArgs) (RequestVoteReply, error) {
	h, ok := t.resolve(args.CandidateID, target)
	if !ok {
		return RequestVoteReply{}, ErrPeerUnreachable
	}
	return h.HandleRequestVote(args), nil
}

// SendAppendEntries implements Transport.
func (t *InMemoryTransport) SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	h, ok := t.resolve(args.LeaderID, target)
	if !ok {
		return AppendEntriesReply{}, ErrPeerUnreachable
	}
	return h.HandleAppendEntries(args), nil
}

// SendInstallSnapshot implements Transport.
func (t *InMemoryTransport) SendInstallSnapshot(target string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	h, ok := t.resolve(args.LeaderID, target)
	if !ok {
		return InstallSnapshotReply{}, ErrPeerUnreachable
	}
	return h.HandleInstallSnapshot(args), nil
}
