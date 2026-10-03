package raft

import "testing"

func TestLastApplied_StartsAtZero(t *testing.T) {
	node := mustNewNode(t, Options{ID: "n", Transport: NewInMemoryTransport()})
	if got := node.LastApplied(); got != 0 {
		t.Fatalf("LastApplied on a fresh node = %d, want 0", got)
	}
}

// commitUpTo elects nodes[0] leader in a fresh 3-node test cluster and
// proposes n commands through it, returning once they have all committed
// (verified via CommitIndex), purely so MarkApplied has a real,
// majority-replicated commitIndex to work against. A 3-node cluster is
// used so these tests also exercise the ordinary multi-peer commit path
// (see TestLastApplied_AdvancesOnSingleNodeCluster below for the
// single-node case, where the leader's own log entry is already a
// majority of one).
func commitUpTo(t *testing.T, n int) *Node {
	t.Helper()
	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)

	for i := 0; i < n; i++ {
		if _, _, err := leader.Propose(Command("x")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()
	if got := leader.CommitIndex(); got != uint64(n) {
		t.Fatalf("leader CommitIndex after %d proposals = %d, want %d", n, got, n)
	}
	return leader
}

func TestMarkApplied_AdvancesMonotonically(t *testing.T) {
	leader := commitUpTo(t, 3)

	if err := leader.MarkApplied(1); err != nil {
		t.Fatalf("MarkApplied(1): %v", err)
	}
	if got := leader.LastApplied(); got != 1 {
		t.Fatalf("LastApplied = %d, want 1", got)
	}
	if err := leader.MarkApplied(2); err != nil {
		t.Fatalf("MarkApplied(2): %v", err)
	}
	if err := leader.MarkApplied(3); err != nil {
		t.Fatalf("MarkApplied(3): %v", err)
	}
	if got := leader.LastApplied(); got != 3 {
		t.Fatalf("LastApplied = %d, want 3", got)
	}
}

func TestMarkApplied_RejectsNonMonotonic(t *testing.T) {
	leader := commitUpTo(t, 3)

	if err := leader.MarkApplied(2); err != nil {
		t.Fatalf("MarkApplied(2): %v", err)
	}
	if err := leader.MarkApplied(2); err == nil {
		t.Fatalf("MarkApplied(2) a second time: want error, got nil")
	}
	if err := leader.MarkApplied(1); err == nil {
		t.Fatalf("MarkApplied(1) after lastApplied=2: want error, got nil")
	}
	if got := leader.LastApplied(); got != 2 {
		t.Fatalf("LastApplied after rejected calls = %d, want unchanged 2", got)
	}
}

// TestLastApplied_AdvancesOnSingleNodeCluster covers the single-node
// regression directly: a zero-peer leader's CommitIndex must advance on
// its own (see TestNode_SingleNodeCluster_CommitsImmediately in
// replication_test.go), and MarkApplied/LastApplied must be able to
// advance against that commitIndex exactly as they do for a
// majority-replicated multi-node commit.
func TestLastApplied_AdvancesOnSingleNodeCluster(t *testing.T) {
	tr := NewInMemoryTransport()
	node := mustNewNode(t, Options{ID: "solo", Transport: tr, ElectionTickMin: 2, ElectionTickMax: 2})
	tr.Register("solo", node)

	node.Tick()
	node.Tick()
	node.Drain()
	if !node.IsLeader() {
		t.Fatalf("single-node cluster did not elect itself leader")
	}

	idx, _, err := node.Propose(Command("cmd"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if got := node.CommitIndex(); got != idx {
		t.Fatalf("CommitIndex = %d, want %d", got, idx)
	}

	if err := node.MarkApplied(idx); err != nil {
		t.Fatalf("MarkApplied(%d): %v", idx, err)
	}
	if got := node.LastApplied(); got != idx {
		t.Fatalf("LastApplied = %d, want %d", got, idx)
	}
}

func TestMarkApplied_RejectsBeyondCommitIndex(t *testing.T) {
	leader := commitUpTo(t, 2)

	if err := leader.MarkApplied(3); err == nil {
		t.Fatalf("MarkApplied(3) with commitIndex=2: want error, got nil")
	}
	if got := leader.LastApplied(); got != 0 {
		t.Fatalf("LastApplied after rejected MarkApplied = %d, want unchanged 0", got)
	}

	if err := leader.MarkApplied(2); err != nil {
		t.Fatalf("MarkApplied(2): %v", err)
	}
}
