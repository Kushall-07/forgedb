package chaos

import (
	"errors"
	"strconv"
	"testing"

	"github.com/Kushall-07/forgedb/internal/dbnode"
)

// --- Scenario B: minority isolation ---------------------------------------
//
//	3-node cluster -> partition one node -> majority continues writes ->
//	minority attempts write/read -> heal -> verify convergence
//
// This exercises Raft's fundamental quorum property (invariant 8 in
// docs/chaos/phase10-chaos-testing.md: a partitioned minority must never
// advance its own committed history on its own).

func TestScenarioB_MinorityIsolation(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	c.Partition("node2")

	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()
	if n := c.Node("node0"); n.Raft().CommitIndex() != 1 {
		t.Fatalf("majority leader CommitIndex = %d, want 1\n\n%s", n.Raft().CommitIndex(), c.Dump())
	}
	assertGet(t, c, "node0", "x", "10")
	assertGet(t, c, "node1", "x", "10")

	// The isolated minority node can still append to its own log if it
	// somehow received the entry (it did not, here -- it's cut off from
	// the leader entirely), but it must never advance CommitIndex on its
	// own, and must never observe the write that only the majority saw.
	if n := c.Node("node2"); n.Raft().CommitIndex() != 0 {
		t.Fatalf("minority node2 CommitIndex = %d, want unchanged 0\n\n%s", n.Raft().CommitIndex(), c.Dump())
	}
	assertMissing(t, c, "node2", "x")

	// A minority of one can never elect itself leader either: it needs 2
	// of 3 votes and has nobody to ask.
	for i := 0; i < DefaultElectionTickMax+1; i++ {
		c.Node("node2").Tick()
	}
	c.Node("node2").Drain()
	c.Observe("TickIsolatedMinority(node2)")
	if c.Node("node2").IsLeader() {
		t.Fatalf("isolated minority node2 elected itself leader with no quorum")
	}

	c.Heal("node2")
	c.Settle()
	mustConverge(t, c, 10)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
	}

	requireNoInvariantViolations(t, c)
}

// --- Scenario E: stale leader ----------------------------------------------
//
//	elect A -> partition A from B/C -> B/C elect new leader ->
//	A attempts PUT -> A attempts ConsistentGet -> heal ->
//	verify A steps down and converges
//
// This directly exercises Phase 8.5's read-consistency guarantee
// (internal/raft.ReadIndex / dbnode.ConsistentGet): a leader that is
// actually cut off from the majority still believes itself to be leader
// (role is never updated by a partition it cannot detect on its own) and
// must therefore be prevented from doing anything unsafe by the
// mechanism itself, not by the test asking it nicely.

func TestScenarioE_StaleLeader(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()

	c.Partition("node0")

	newLeader, err := c.WaitForLeaderAmong(40, "node1", "node2")
	if err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}

	// The stale leader still locally believes it is leader: that belief
	// alone must never be enough to let it commit or read safely.
	if !c.Node("node0").IsLeader() {
		t.Fatalf("node0 should still locally believe itself leader (it cannot detect the partition on its own)")
	}

	if _, _, err := c.Propose("node0", put("c1", 2, "y", "20")); err != nil {
		t.Fatalf("Propose on stale leader (appending locally is allowed): %v", err)
	}
	c.Node("node0").Drain()
	c.Observe("ProposeOnStaleLeader")
	if n := c.Node("node0"); n.Raft().CommitIndex() != 1 {
		t.Fatalf("stale leader node0 CommitIndex = %d, want unchanged 1 (the new entry must never commit without a majority)", n.Raft().CommitIndex())
	}

	if _, err := c.ConsistentGet("node0", []byte("x")); !errors.Is(err, dbnode.ErrReadUnavailable) {
		t.Fatalf("ConsistentGet on stale leader: err = %v, want ErrReadUnavailable\n\n%s", err, c.Dump())
	}

	mustPropose(t, c, newLeader, put("c1", 3, "z", "30"))
	c.Settle()

	c.Heal("node0")
	c.Settle()
	// The stale leader must discover the higher term and step down.
	for i := 0; i < 10 && c.Node("node0").IsLeader(); i++ {
		c.Advance(1)
	}
	if c.Node("node0").IsLeader() {
		t.Fatalf("node0 never stepped down after healing\n\n%s", c.Dump())
	}

	mustConverge(t, c, 20)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
		assertGet(t, c, id, "z", "30")
		assertMissing(t, c, id, "y") // never committed; must not have leaked into any node's state
	}

	requireNoInvariantViolations(t, c)
}

// --- Scenario H: partition/heal cycle --------------------------------------
//
//	healthy -> partition -> heal -> partition -> heal -> partition -> heal
//	-> verify no hidden corruption accumulated
//
// Each cycle isolates a different node and lets the majority keep
// writing, so healing must repeatedly reconcile real divergent history,
// not just reconnect two already-identical replicas.

func TestScenarioH_PartitionHealCycle(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	targets := []string{"node1", "node2", "node1"}
	for i, target := range targets {
		c.Partition(target)
		mustPropose(t, c, "node0", put("c1", uint64(i+1), "k", strconv.Itoa(i+1)))
		c.Settle()
		c.Heal(target)
		c.Settle()
		mustConverge(t, c, 20)
	}

	for _, id := range c.IDs() {
		assertGet(t, c, id, "k", strconv.Itoa(len(targets)))
	}
	requireNoInvariantViolations(t, c)
}
