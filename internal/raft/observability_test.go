package raft

import (
	"testing"

	"github.com/Kushall-07/forgedb/internal/metrics"
)

// --- Status / PeerStatuses ------------------------------------------

func TestStatus_ReflectsCurrentState(t *testing.T) {
	leader := commitUpTo(t, 3)
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}

	st := leader.Status()
	if st.NodeID != leader.ID() {
		t.Errorf("NodeID = %q, want %q", st.NodeID, leader.ID())
	}
	if st.Role != "Leader" {
		t.Errorf("Role = %q, want Leader", st.Role)
	}
	if st.CommitIndex != 3 {
		t.Errorf("CommitIndex = %d, want 3", st.CommitIndex)
	}
	if st.LastApplied != 3 {
		t.Errorf("LastApplied = %d, want 3", st.LastApplied)
	}
	if st.LastLogIndex != 3 {
		t.Errorf("LastLogIndex = %d, want 3", st.LastLogIndex)
	}
	if len(st.Peers) != 2 {
		t.Errorf("len(Peers) = %d, want 2", len(st.Peers))
	}
}

func TestPeerStatuses_LeaderOnly(t *testing.T) {
	leader := commitUpTo(t, 4)
	leader.Drain()

	peers := leader.PeerStatuses()
	if len(peers) != 2 {
		t.Fatalf("len(PeerStatuses()) = %d, want 2", len(peers))
	}
	for _, p := range peers {
		if p.MatchIndex != 4 {
			t.Errorf("peer %s MatchIndex = %d, want 4 (fully replicated)", p.PeerID, p.MatchIndex)
		}
		if p.Lag != 0 {
			t.Errorf("peer %s Lag = %d, want 0", p.PeerID, p.Lag)
		}
	}
}

func TestPeerStatuses_NonLeaderReturnsNil(t *testing.T) {
	_, nodes := newTestCluster(t, 3, nil)
	if got := nodes[0].PeerStatuses(); got != nil {
		t.Fatalf("PeerStatuses() on a non-leader = %v, want nil", got)
	}
}

// --- Election / leadership metrics -----------------------------------

func TestMetrics_ElectionAndLeaderChange(t *testing.T) {
	startedBefore := metrics.RaftElectionsStartedTotal.Value()
	wonBefore := metrics.RaftElectionsWonTotal.Value()
	changesBefore := metrics.RaftLeaderChangesTotal.Value()

	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	electLeader(t, nodes[0], 5)

	if got := metrics.RaftElectionsStartedTotal.Value(); got <= startedBefore {
		t.Errorf("RaftElectionsStartedTotal did not increase: before=%d after=%d", startedBefore, got)
	}
	if got := metrics.RaftElectionsWonTotal.Value(); got != wonBefore+1 {
		t.Errorf("RaftElectionsWonTotal = %d, want %d", got, wonBefore+1)
	}
	if got := metrics.RaftLeaderChangesTotal.Value(); got <= changesBefore {
		t.Errorf("RaftLeaderChangesTotal did not increase: before=%d after=%d", changesBefore, got)
	}
}

func TestMetrics_HigherTermStepDown(t *testing.T) {
	before := metrics.RaftHigherTermStepsDownTotal.Value()

	tr := NewInMemoryTransport()
	n := mustNewNode(t, Options{ID: "n", Peers: []string{"other"}, Transport: tr})
	tr.Register("n", n)

	if err := n.becomeFollowerLocked(5); err != nil {
		t.Fatalf("becomeFollowerLocked: %v", err)
	}
	if got := metrics.RaftHigherTermStepsDownTotal.Value(); got != before+1 {
		t.Errorf("RaftHigherTermStepsDownTotal = %d, want %d", got, before+1)
	}
}

// --- Replication metrics ----------------------------------------------

func TestMetrics_ReplicationCounters(t *testing.T) {
	sentBefore := metrics.RaftAppendEntriesSentTotal.Value()
	successBefore := metrics.RaftAppendEntriesSuccessTotal.Value()
	committedBefore := metrics.RaftEntriesCommittedTotal.Value()

	leader := commitUpTo(t, 3)
	leader.Drain()

	if got := metrics.RaftAppendEntriesSentTotal.Value(); got <= sentBefore {
		t.Errorf("RaftAppendEntriesSentTotal did not increase: before=%d after=%d", sentBefore, got)
	}
	if got := metrics.RaftAppendEntriesSuccessTotal.Value(); got <= successBefore {
		t.Errorf("RaftAppendEntriesSuccessTotal did not increase: before=%d after=%d", successBefore, got)
	}
	if got := metrics.RaftEntriesCommittedTotal.Value(); got < committedBefore+3 {
		t.Errorf("RaftEntriesCommittedTotal = %d, want at least %d", got, committedBefore+3)
	}
}

// --- Snapshot metrics ---------------------------------------------------

func TestMetrics_SnapshotCreated(t *testing.T) {
	createdBefore := metrics.RaftSnapshotsCreatedTotal.Value()
	bytesBefore := metrics.RaftSnapshotBytesTotal.Value()

	leader := commitUpTo(t, 3)
	for i := uint64(1); i <= 3; i++ {
		if err := leader.MarkApplied(i); err != nil {
			t.Fatalf("MarkApplied(%d): %v", i, err)
		}
	}
	data := []byte("snapshot-payload")
	if err := leader.CreateSnapshot(3, data); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if got := metrics.RaftSnapshotsCreatedTotal.Value(); got != createdBefore+1 {
		t.Errorf("RaftSnapshotsCreatedTotal = %d, want %d", got, createdBefore+1)
	}
	if got := metrics.RaftSnapshotBytesTotal.Value(); got != bytesBefore+uint64(len(data)) {
		t.Errorf("RaftSnapshotBytesTotal = %d, want %d", got, bytesBefore+uint64(len(data)))
	}
}

// --- ReadIndex metrics ---------------------------------------------------

func TestMetrics_ReadIndex_SuccessAndFailure(t *testing.T) {
	totalBefore := metrics.RaftReadIndexTotal.Value()
	successBefore := metrics.RaftReadIndexSuccessTotal.Value()
	failureBefore := metrics.RaftReadIndexFailureTotal.Value()

	_, nodes := newTestCluster(t, 3, func(id string, o *Options) {
		o.ElectionTickMin, o.ElectionTickMax = 5, 5
	})
	leader := nodes[0]
	electLeader(t, leader, 5)
	drainAll(nodes)

	if _, err := leader.ReadIndex(); err != nil {
		t.Fatalf("ReadIndex on a healthy leader: %v", err)
	}
	if _, err := nodes[1].ReadIndex(); err != ErrNotLeader {
		t.Fatalf("ReadIndex on a follower = %v, want ErrNotLeader", err)
	}

	if got := metrics.RaftReadIndexTotal.Value(); got < totalBefore+2 {
		t.Errorf("RaftReadIndexTotal = %d, want at least %d", got, totalBefore+2)
	}
	if got := metrics.RaftReadIndexSuccessTotal.Value(); got != successBefore+1 {
		t.Errorf("RaftReadIndexSuccessTotal = %d, want %d", got, successBefore+1)
	}
	if got := metrics.RaftReadIndexFailureTotal.Value(); got != failureBefore+1 {
		t.Errorf("RaftReadIndexFailureTotal = %d, want %d", got, failureBefore+1)
	}
}
