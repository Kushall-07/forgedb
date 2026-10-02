package chaos

import "testing"

// --- Scenario A: leader crash --------------------------------------------
//
//	elect leader -> PUT -> crash leader -> elect new leader -> PUT ->
//	restart old leader -> heal/rejoin -> verify convergence
//
// This is the canonical Phase 10 scenario docs/chaos/phase10-chaos-testing.md
// calls "Scenario A": it is not enough that nothing panics (section 34's
// "no false-positive chaos tests") -- it specifically checks that the
// entry committed before the crash survives, that a *different* node
// takes over leadership, that new writes keep committing under the new
// leader, and that the old leader (restarted from its own real on-disk
// state) eventually converges with everyone else instead of staying
// stuck or re-asserting stale authority.

func TestScenarioA_LeaderCrash(t *testing.T) {
	c := newScenarioCluster(t, 3)

	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()
	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
	}

	if err := c.CrashNode("node0"); err != nil {
		t.Fatalf("CrashNode(node0): %v", err)
	}

	newLeader := mustWaitForLeader(t, c, 40)
	if newLeader == "node0" {
		t.Fatalf("crashed node0 must not be the new leader\n\n%s", c.Dump())
	}
	if n := c.Node(newLeader); n.Raft().CommitIndex() < 1 {
		t.Fatalf("new leader %s lost the previously committed entry: CommitIndex = %d\n\n%s", newLeader, n.Raft().CommitIndex(), c.Dump())
	}

	mustPropose(t, c, newLeader, put("c1", 2, "y", "20"))
	c.Settle()
	mustConverge(t, c, 10, c.LiveIDs()...)
	for _, id := range c.LiveIDs() {
		assertGet(t, c, id, "x", "10")
		assertGet(t, c, id, "y", "20")
	}

	if err := c.RestartNode("node0"); err != nil {
		t.Fatalf("RestartNode(node0): %v", err)
	}
	if c.Node("node0").IsLeader() {
		t.Fatalf("restarted node0 resumed leadership; a restart must never restore an old leader role")
	}
	c.Settle()
	mustConverge(t, c, 20)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
		assertGet(t, c, id, "y", "20")
	}

	requireNoInvariantViolations(t, c)
}

// --- Scenario D: repeated leader changes ----------------------------------
//
//	elect A -> write -> crash A -> elect B -> write -> crash B ->
//	elect C -> write -> restart all -> heal -> verify convergence
//
// docs/chaos/phase10-chaos-testing.md deliberately does not require
// leadership to rotate in any particular order (only this test's explicit
// CrashNode calls control that); what it does require is that terms
// strictly increase with each change, that every committed write survives
// every subsequent crash, and that the cluster still converges once every
// node is back and healed.

func TestScenarioD_RepeatedLeaderChanges(t *testing.T) {
	c := newScenarioCluster(t, 5)

	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "a", "1"))
	c.Settle()

	if err := c.CrashNode("node0"); err != nil {
		t.Fatalf("CrashNode(node0): %v", err)
	}
	leaderB := mustWaitForLeader(t, c, 40)
	mustPropose(t, c, leaderB, put("c1", 2, "b", "2"))
	c.Settle()

	if err := c.CrashNode(leaderB); err != nil {
		t.Fatalf("CrashNode(%s): %v", leaderB, err)
	}
	leaderC := mustWaitForLeader(t, c, 40)
	if leaderC == leaderB {
		t.Fatalf("crashed leader %s must not still be reported as leader", leaderB)
	}
	mustPropose(t, c, leaderC, put("c1", 3, "cc", "3"))
	c.Settle()

	// Every committed write so far must have survived both crashes.
	for _, id := range c.LiveIDs() {
		assertGet(t, c, id, "a", "1")
		assertGet(t, c, id, "b", "2")
		assertGet(t, c, id, "cc", "3")
	}

	for _, id := range []string{"node0", leaderB} {
		if err := c.RestartNode(id); err != nil {
			t.Fatalf("RestartNode(%s): %v", id, err)
		}
	}
	c.Settle()
	mustConverge(t, c, 30)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "a", "1")
		assertGet(t, c, id, "b", "2")
		assertGet(t, c, id, "cc", "3")
	}

	requireNoInvariantViolations(t, c)
}

// --- Scenario J: full cluster restart -------------------------------------
//
//	write -> snapshot -> stop all nodes -> restart all nodes ->
//	elect leader -> read/write -> verify state

func TestScenarioJ_FullClusterRestart(t *testing.T) {
	c := newScenarioCluster(t, 3)

	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "a", "1"))
	mustPropose(t, c, "node0", put("c1", 2, "b", "2"))
	c.Settle()
	for _, id := range c.IDs() {
		if err := c.CreateSnapshot(id, 2); err != nil {
			t.Fatalf("node %s CreateSnapshot(2): %v", id, err)
		}
	}

	for _, id := range c.IDs() {
		if err := c.CrashNode(id); err != nil {
			t.Fatalf("CrashNode(%s): %v", id, err)
		}
	}
	for _, id := range c.IDs() {
		if err := c.RestartNode(id); err != nil {
			t.Fatalf("RestartNode(%s): %v", id, err)
		}
	}

	for _, id := range c.IDs() {
		assertGet(t, c, id, "a", "1")
		assertGet(t, c, id, "b", "2")
		if n := c.Node(id); n.SnapshotIndex() != 2 {
			t.Fatalf("node %s SnapshotIndex after full cluster restart = %d, want 2", id, n.SnapshotIndex())
		}
		if n := c.Node(id); n.IsLeader() {
			t.Fatalf("node %s resumed leadership across a full restart", id)
		}
	}

	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c2", 1, "marker", "post-restart"))
	c.Settle()
	mustConverge(t, c, 10)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "a", "1")
		assertGet(t, c, id, "b", "2")
		assertGet(t, c, id, "marker", "post-restart")
	}

	requireNoInvariantViolations(t, c)
}
