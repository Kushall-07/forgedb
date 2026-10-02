package raft

import (
	"sync"
	"testing"
	"time"
)

// --- ReadIndex: successful confirmation -------------------------------

func TestNode_ReadIndex_SucceedsWithHealthyMajority(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	idx, _, err := leader.Propose(Command("cmd"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	if got := leader.CommitIndex(); got != idx {
		t.Fatalf("CommitIndex = %d, want %d (majority replicated)", got, idx)
	}

	readIndex, err := leader.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if readIndex != idx {
		t.Fatalf("ReadIndex = %d, want %d", readIndex, idx)
	}
}

// --- ReadIndex: not leader ----------------------------------------------

func TestNode_ReadIndex_NotLeader(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	follower := nodes[1]
	if _, err := follower.ReadIndex(); err != ErrNotLeader {
		t.Fatalf("follower ReadIndex err = %v, want ErrNotLeader", err)
	}
}

// --- ReadIndex: isolated leader cannot confirm quorum --------------------
//
// Mirrors TestNode_PartitionedLeader_CannotFalselyCommit: a leader cut off
// from every peer still believes role == Leader locally, but ReadIndex
// must not trust that belief alone -- it must actually fail to confirm a
// majority and report ErrReadBarrierUnavailable, never a stale index.

func TestNode_ReadIndex_FailsWithoutMajority(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	tr.Partition(leader.ID())

	if _, err := leader.ReadIndex(); err != ErrReadBarrierUnavailable {
		t.Fatalf("isolated leader ReadIndex err = %v, want ErrReadBarrierUnavailable", err)
	}
	// The local role is deliberately left alone by a failed ReadIndex --
	// only an actual higher-term discovery (see the step-down test below)
	// causes a role change. ReadIndex's job is to refuse to vouch for a
	// read, not to force a premature step-down on its own.
	if !leader.IsLeader() {
		t.Fatalf("ReadIndex failure must not itself change local role")
	}
}

// Five-node variant: the leader can reach a strict minority (one of four
// peers), which is still short of the cluster's majority of three.
func TestNode_ReadIndex_FailsWithoutMajority_FiveNodes(t *testing.T) {
	tr, nodes := newTestCluster(t, 5, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	// Partition every peer except one: leader + nodes[1] = 2 of 5,
	// short of the 3-node majority.
	for _, n := range nodes[2:] {
		tr.Partition(n.ID())
	}

	if _, err := leader.ReadIndex(); err != ErrReadBarrierUnavailable {
		t.Fatalf("ReadIndex err = %v, want ErrReadBarrierUnavailable", err)
	}
}

// --- ReadIndex: higher-term discovery forces step-down -------------------
//
// Adapts TestNode_OldLeaderStepsDown_AfterPartitionHeals: nodeA is
// partitioned away, nodeB and nodeC elect a new leader at a higher term
// among themselves, the partition heals, and nodeA's own ReadIndex call
// (not a heartbeat) is what discovers the higher term and forces the
// step-down -- proving Property D (leader changes are handled safely)
// and that ReadIndex is never fooled by a locally-cached belief about its
// own leadership.

func TestNode_ReadIndex_StepsDownOnHigherTermDuringConfirmation(t *testing.T) {
	tr, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
		o.HeartbeatTick = 1
	})
	nodeA, nodeB := nodes[0], nodes[1]

	electLeader(t, nodeA, 5)
	termA, _, _ := nodeA.State()

	tr.Partition(nodeA.ID())

	// nodeB and nodeC are still a majority (2 of 3) and elect a new
	// leader at a higher term even with nodeA cut off.
	electLeader(t, nodeB, 5)
	termB, _, _ := nodeB.State()
	if termB <= termA {
		t.Fatalf("new leader's term %d did not exceed old leader's term %d", termB, termA)
	}

	tr.Heal(nodeA.ID())

	// nodeA still locally believes it is leader at termA -- nothing has
	// told it otherwise yet. Its own ReadIndex call is what must
	// discover nodeB's higher term via the confirmation round's replies.
	if !nodeA.IsLeader() {
		t.Fatalf("nodeA should still locally believe it is leader before ReadIndex")
	}

	if _, err := nodeA.ReadIndex(); err != ErrReadBarrierUnavailable {
		t.Fatalf("nodeA ReadIndex err = %v, want ErrReadBarrierUnavailable", err)
	}

	term, role, leaderID := nodeA.State()
	if role != Follower {
		t.Fatalf("nodeA role after ReadIndex discovered higher term = %s, want Follower", role)
	}
	if term != termB {
		t.Fatalf("nodeA term after stepping down = %d, want %d", term, termB)
	}
	// becomeFollowerLocked clears leaderID to "" on a higher-term
	// discovery from a reply (as opposed to an incoming AppendEntries
	// request, which does carry LeaderID) -- a reply alone never tells
	// this node who the new leader actually is, only that it is stale.
	// nodeA will learn nodeB's identity the next time nodeB's own
	// heartbeat reaches it, exactly as HandleAppendEntries already does.
	if leaderID != "" {
		t.Fatalf("nodeA leaderID after stepping down via a reply = %q, want \"\"", leaderID)
	}
}

// --- ReadIndex: never holds n.mu during the RPC round --------------------
//
// blockingTransport wraps a real InMemoryTransport but blocks every
// SendAppendEntries call to a chosen target until released, letting the
// test observe whether the node's own mutex is still free (via a
// concurrent State() call, which itself needs n.mu) while that RPC is
// outstanding -- directly exercising the "no Raft mutex held during
// network RPC" invariant from internal/raft's own Phase 5/6 design, which
// ReadIndex must preserve.
type blockingTransport struct {
	*InMemoryTransport
	blockTarget string
	release     chan struct{}
}

func (b *blockingTransport) SendAppendEntries(target string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if target == b.blockTarget {
		<-b.release
	}
	return b.InMemoryTransport.SendAppendEntries(target, args)
}

func TestNode_ReadIndex_DoesNotHoldMutexDuringRPC(t *testing.T) {
	inner := NewInMemoryTransport()
	// blockTarget starts empty so electLeader's own initial heartbeat
	// broadcast (becomeLeaderLocked) is never blocked; it is set only
	// after the leader is fully elected and drained, immediately before
	// starting the one ReadIndex call this test actually blocks.
	bt := &blockingTransport{InMemoryTransport: inner, release: make(chan struct{})}

	ids := []string{"node0", "node1", "node2"}
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		n := mustNewNode(t, Options{
			ID: id, Peers: peers, Transport: bt,
			ElectionTickMin: 5, ElectionTickMax: 5,
		})
		nodes[i] = n
		inner.Register(id, n)
	}
	leader := nodes[0]
	electLeader(t, leader, 5)

	bt.blockTarget = "node1"

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		leader.ReadIndex() // blocked on node1 until release is closed below
	}()

	// While ReadIndex's RPC to node1 is blocked, the leader's own mutex
	// must still be free: State() must return promptly, not hang behind
	// a held lock.
	done := make(chan struct{})
	go func() {
		leader.State()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("State() did not return while ReadIndex's RPC to node1 was blocked -- n.mu held during network call")
	}

	close(bt.release)
	wg.Wait()
}

// --- ReadIndex: safe under concurrent calls -------------------------------

func TestNode_ReadIndex_ConcurrentCalls(t *testing.T) {
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	idx, _, err := leader.Propose(Command("cmd"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	if got := leader.CommitIndex(); got != idx {
		t.Fatalf("CommitIndex = %d, want %d", got, idx)
	}

	const n = 8
	results := make([]uint64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = leader.ReadIndex()
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: ReadIndex err = %v", i, errs[i])
		}
		if results[i] != idx {
			t.Fatalf("goroutine %d: ReadIndex = %d, want %d", i, results[i], idx)
		}
	}
}

// --- ReadIndex: single-node cluster needs no RPC round --------------------

func TestNode_ReadIndex_SingleNodeCluster(t *testing.T) {
	_, nodes := newTestCluster(t, 1, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 3, 3
	})
	leader := nodes[0]
	electLeader(t, leader, 3)

	readIndex, err := leader.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if readIndex != leader.CommitIndex() {
		t.Fatalf("ReadIndex = %d, want CommitIndex %d", readIndex, leader.CommitIndex())
	}
}
