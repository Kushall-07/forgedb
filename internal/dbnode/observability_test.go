package dbnode

import (
	"context"
	"strings"
	"testing"

	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

func TestStatus_ReflectsLeaderAndStorage(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	if _, _, err := leader.Propose(statemachine.NewPutCommand("c1", 1, []byte("k"), []byte("v"))); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}

	st := leader.Status()
	if st.NodeID != leader.ID() {
		t.Errorf("NodeID = %q, want %q", st.NodeID, leader.ID())
	}
	if st.Raft.Role != "Leader" {
		t.Errorf("Raft.Role = %q, want Leader", st.Raft.Role)
	}
	if st.Storage.MemTableEntries != 1 {
		t.Errorf("Storage.MemTableEntries = %d, want 1", st.Storage.MemTableEntries)
	}
	if len(st.Peers) != 2 {
		t.Errorf("len(Peers) = %d, want 2", len(st.Peers))
	}
}

// TestMetrics_PullGauges_ReflectCurrentNode exercises the pull-based
// Raft gauges wired up in registerMetricsOnce. It deliberately uses a
// single-node cluster: the global metrics registry's gauges always
// describe whichever Node most recently called Open (see currentNode's
// doc comment in metrics.go), so a multi-node test would only be able to
// assert on whichever node happened to be opened last, not on "the
// leader" -- a single-node cluster sidesteps that ambiguity, since there
// is only ever one Node to be current.
func TestMetrics_PullGauges_ReflectCurrentNode(t *testing.T) {
	_, nodes, _ := newCluster(t, 1)
	leader := nodes[0]
	electLeader(t, leader)

	out := metrics.Default.Render()
	if !strings.Contains(out, `forgedb_raft_role{role="leader"} 1`) {
		t.Fatalf("Render() missing leader role=1, got:\n%s", out)
	}
	if !strings.Contains(out, "forgedb_node_info{node_id=\""+leader.ID()+"\"} 1") {
		t.Fatalf("Render() missing node_info for %s, got:\n%s", leader.ID(), out)
	}
}

// TestCurrentNode_FollowsMostRecentOpen documents the actual,
// deliberate behavior of the shared pull-gauge pointer (see metrics.go)
// when more than one Node is open in the same process: it always
// reflects whichever Node most recently called Open, not any particular
// node's role. This is a real limitation of a single global metrics
// registry shared by a multi-node-per-process test binary -- it is not
// a limitation in production, where exactly one Node is ever open per
// process (see docs/observability/phase12-observability.md's known
// limitations).
func TestCurrentNode_FollowsMostRecentOpen(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	lastOpened := nodes[len(nodes)-1]

	out := metrics.Default.Render()
	if !strings.Contains(out, "forgedb_node_info{node_id=\""+lastOpened.ID()+"\"} 1") {
		t.Fatalf("Render() missing node_info for the most recently opened node %s, got:\n%s", lastOpened.ID(), out)
	}
}

func TestMetrics_ConsistentGet_SuccessAndFailure(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	follower := nodes[1]
	electLeader(t, leader)

	if _, _, err := leader.Propose(statemachine.NewPutCommand("c1", 1, []byte("k"), []byte("v"))); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}

	successBefore := metrics.ConsistentGetSuccessTotal.Value()
	failureBefore := metrics.ConsistentGetFailureTotal.Value()

	if _, err := leader.ConsistentGet(context.Background(), []byte("k")); err != nil {
		t.Fatalf("ConsistentGet on leader: %v", err)
	}
	if _, err := follower.ConsistentGet(context.Background(), []byte("k")); err == nil {
		t.Fatalf("ConsistentGet on a follower succeeded, want ErrNotLeader")
	}

	if got := metrics.ConsistentGetSuccessTotal.Value(); got != successBefore+1 {
		t.Errorf("ConsistentGetSuccessTotal = %d, want %d", got, successBefore+1)
	}
	if got := metrics.ConsistentGetFailureTotal.Value(); got != failureBefore+1 {
		t.Errorf("ConsistentGetFailureTotal = %d, want %d", got, failureBefore+1)
	}
}
