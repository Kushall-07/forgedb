package chaos

import (
	"fmt"
	"testing"
)

// --- Scenario C: follower lag + snapshot catch-up --------------------------
//
//	3-node cluster -> isolate follower -> majority performs many writes ->
//	snapshot leader/majority -> heal -> follower installs snapshot ->
//	follower catches up -> verify state
//
// docs/chaos/phase10-chaos-testing.md calls this one of the most
// important Phase 10 tests: it validates the Phase 9 snapshot/log-
// compaction system under an actual failure sequence, not just the
// direct unit tests in internal/raft/snapshot_test.go and
// internal/dbnode/snapshot_test.go. The lagging follower must fall far
// enough behind that ordinary AppendEntries replication is no longer
// sufficient (the leader has already compacted past what the follower's
// nextIndex would need), forcing InstallSnapshot, and the test checks
// actual application-level state (every key, via LogicalState) rather
// than only Raft indexes -- per section 14's explicit instruction.

func TestScenarioC_FollowerLagThenSnapshotCatchUp(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	c.Partition("node2")

	const numWrites = 8
	for i := 1; i <= numWrites; i++ {
		mustPropose(t, c, "node0", put("c1", uint64(i), fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}
	c.Settle()
	if n := c.Node("node0"); n.Raft().CommitIndex() != numWrites {
		t.Fatalf("leader CommitIndex = %d, want %d\n\n%s", n.Raft().CommitIndex(), numWrites, c.Dump())
	}

	if err := c.CreateSnapshot("node0", numWrites); err != nil {
		t.Fatalf("CreateSnapshot(node0, %d): %v", numWrites, err)
	}
	if err := c.CreateSnapshot("node1", numWrites); err != nil {
		t.Fatalf("CreateSnapshot(node1, %d): %v", numWrites, err)
	}

	c.Heal("node2")
	// Bounded, not a fixed couple of Settle rounds: the leader notices
	// node2's nextIndex is behind the now-compacted log and sends
	// InstallSnapshot instead of AppendEntries, but a Partition'd (as
	// opposed to crashed) node keeps ticking the whole time it is
	// isolated and can time out its own election before it is healed --
	// see WaitForSnapshotIndex's doc comment for why that can cost a few
	// extra rounds of disruptive re-election before a real leader
	// actually sends the InstallSnapshot this is waiting for.
	if err := c.WaitForSnapshotIndex("node2", numWrites, 40); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
	mustConverge(t, c, 20)
	for i := 1; i <= numWrites; i++ {
		for _, id := range c.IDs() {
			assertGet(t, c, id, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		}
	}

	// The cluster must continue committing new entries normally
	// afterward, with the previously-lagging follower fully participating.
	mustPropose(t, c, "node0", put("c1", numWrites+1, "after", "snapshot-catchup"))
	c.Settle()
	mustConverge(t, c, 10)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "after", "snapshot-catchup")
	}

	requireNoInvariantViolations(t, c)
}

// --- Scenario F: snapshot + restart ----------------------------------------
//
//	write -> apply -> snapshot -> write suffix -> crash -> restart ->
//	verify snapshot + retained-suffix state reconstruct correctly
//
// Run once each for the leader and for a follower, since
// docs/raft/phase9-snapshots.md's restart-recovery path (restore
// snapshot, then replay the retained log suffix on top of it) is
// identical code for either role, but section 29 explicitly asks for
// both to be exercised under chaos, not just the leader.

func TestScenarioF_SnapshotThenCrashRestart(t *testing.T) {
	for _, target := range []string{"node0", "node1"} {
		t.Run(target, func(t *testing.T) {
			c := newScenarioCluster(t, 3)
			mustElectLeader(t, c, "node0")

			mustPropose(t, c, "node0", put("c1", 1, "a", "1"))
			mustPropose(t, c, "node0", put("c1", 2, "b", "2"))
			mustPropose(t, c, "node0", put("c1", 3, "c", "3"))
			c.Settle()

			if err := c.CreateSnapshot(target, 3); err != nil {
				t.Fatalf("CreateSnapshot(%s, 3): %v", target, err)
			}

			mustPropose(t, c, "node0", put("c1", 4, "d", "4"))
			mustPropose(t, c, "node0", put("c1", 5, "e", "5"))
			c.Settle()

			if err := c.CrashNode(target); err != nil {
				t.Fatalf("CrashNode(%s): %v", target, err)
			}
			if err := c.RestartNode(target); err != nil {
				t.Fatalf("RestartNode(%s): %v", target, err)
			}

			if got := c.Node(target).SnapshotIndex(); got != 3 {
				t.Fatalf("node %s SnapshotIndex after restart = %d, want 3 (the snapshot boundary)\n\n%s", target, got, c.Dump())
			}
			assertGet(t, c, target, "a", "1")
			assertGet(t, c, target, "b", "2")
			assertGet(t, c, target, "c", "3")
			assertGet(t, c, target, "d", "4")
			assertGet(t, c, target, "e", "5")

			c.Settle()
			mustConverge(t, c, 20)
			requireNoInvariantViolations(t, c)
		})
	}
}
