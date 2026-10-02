package chaos

import "testing"

// --- Scenario G: deduplication across failure ------------------------------
//
//	submit request -> fail leader (or restart a replica, or snapshot) ->
//	retry the identical (ClientID, RequestID) -> verify exactly-once
//	logical behavior
//
// Phase 7's deduplication table is what makes a client-level retry safe
// even though Raft itself never deduplicates (see
// docs/raft/phase7-state-machine.md); Phase 9 made that table part of the
// snapshot payload specifically so it survives log compaction and
// InstallSnapshot (see docs/raft/phase9-snapshots.md). This scenario
// checks that guarantee under each of the failure conditions section 28
// calls out: a leader change, a follower restart, and a snapshot/
// snapshot-installation boundary -- not just the simple single-node case
// internal/dbnode/node_test.go's TestNode_DuplicateRequestThroughRaft_
// MutatesStorageExactlyOnce already covers.

func TestScenarioG_DedupAcrossLeaderChange(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	cmd := put("client-1", 1, "x", "10") // one logical request
	mustPropose(t, c, "node0", cmd)
	c.Settle()
	mustConverge(t, c, 10)

	if err := c.CrashNode("node0"); err != nil {
		t.Fatalf("CrashNode(node0): %v", err)
	}
	newLeader := mustWaitForLeader(t, c, 40)

	// The client did not see the first commit succeed (its leader
	// crashed) and retries the identical request against whichever node
	// is leader now.
	mustPropose(t, c, newLeader, cmd)
	c.Settle()
	mustConverge(t, c, 10, c.LiveIDs()...)

	for _, id := range c.LiveIDs() {
		assertGet(t, c, id, "x", "10")
		// Both survivors already held this value from the pre-crash
		// commit; the retry's second commit must resolve as a replay on
		// each of them, never a second real Put -- same value or not.
		if n, err := c.PutCount(id); err != nil {
			t.Fatalf("PutCount(%s): %v", id, err)
		} else if n != 1 {
			t.Fatalf("node %s: real Put calls = %d, want 1 (the retry must resolve as a replay)\n\n%s", id, n, c.Dump())
		}
	}
	requireNoInvariantViolations(t, c)
}

func TestScenarioG_DedupAcrossFollowerRestart(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	cmd := put("client-1", 1, "x", "10")
	mustPropose(t, c, "node0", cmd)
	c.Settle()
	mustConverge(t, c, 10)

	if err := c.CrashNode("node1"); err != nil {
		t.Fatalf("CrashNode(node1): %v", err)
	}
	if err := c.RestartNode("node1"); err != nil {
		t.Fatalf("RestartNode(node1): %v", err)
	}
	c.Settle()

	// Without a snapshot, node1's restart has no durable dedup table to
	// restore (see internal/statemachine's Snapshotter): its fresh
	// KVStateMachine starts with an empty one, and the Settle above just
	// made it re-apply its own still-persisted log from index 1, which
	// re-executes (not replays -- its own dedup table had never seen
	// this request before) client-1/request-1 and genuinely calls
	// Store.Put once. This is correct, not a bug: re-running the
	// identical deterministic command sequence from scratch always
	// re-derives the identical dedup table and KV state. The real "exactly
	// once" property this scenario checks is what happens *next*.
	if n, err := c.PutCount("node1"); err != nil {
		t.Fatalf("PutCount(node1): %v", err)
	} else if n != 1 {
		t.Fatalf("node1: real Put calls after restart's own log replay = %d, want 1\n\n%s", n, c.Dump())
	}

	// The leader itself re-proposes the identical request (e.g. a client
	// retry that reached the still-leading node): Raft logs it as a
	// second entry (it never deduplicates itself -- only the state
	// machine does), and every replica -- including node1, now that its
	// dedup table has been rebuilt -- must resolve this second commit as
	// a replay, not a second real Put.
	mustPropose(t, c, "node0", cmd)
	c.Settle()
	mustConverge(t, c, 10)

	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
	}
	if n, err := c.PutCount("node1"); err != nil {
		t.Fatalf("PutCount(node1): %v", err)
	} else if n != 1 {
		t.Fatalf("node1: real Put calls after the duplicate commit = %d, want unchanged 1 (must resolve as a replay)\n\n%s", n, c.Dump())
	}
	requireNoInvariantViolations(t, c)
}

func TestScenarioG_DedupAcrossSnapshotInstallation(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	// node2 is isolated *before* the original command ever commits, so
	// its dedup state for client-1/request-1 can only ever come from the
	// snapshot payload below -- never from applying the command itself.
	c.Partition("node2")

	cmd := put("client-1", 1, "x", "10")
	mustPropose(t, c, "node0", cmd)
	c.Settle()
	for i := 2; i <= 6; i++ {
		mustPropose(t, c, "node0", put("c2", uint64(i), "filler", "v"))
	}
	c.Settle()
	if err := c.CreateSnapshot("node0", 6); err != nil {
		t.Fatalf("CreateSnapshot(node0, 6): %v", err)
	}
	if err := c.CreateSnapshot("node1", 6); err != nil {
		t.Fatalf("CreateSnapshot(node1, 6): %v", err)
	}

	c.Heal("node2")
	if err := c.WaitForSnapshotIndex("node2", 6, 40); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}

	// node2's dedup state for client-1/request-1 reached it only through
	// the snapshot payload (the log entry that originally carried it was
	// compacted away before node2 ever saw it -- see
	// docs/raft/phase9-snapshots.md on dedup surviving compaction). A
	// retry of the identical request must still resolve as a replay
	// everywhere, including on node2.
	mustPropose(t, c, "node0", cmd)
	c.Settle()
	mustConverge(t, c, 10)

	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
	}
	// node2's only Puts for "x" and "filler" (the snapshot's two live
	// keys) come from RestoreSnapshot reconciling KV state (see
	// statemachine.(*KVStateMachine).RestoreSnapshot), not from replaying
	// the original command -- the post-heal duplicate commit must
	// resolve via node2's own snapshot-restored dedup table and must not
	// call Put a third time.
	if n, err := c.PutCount("node2"); err != nil {
		t.Fatalf("PutCount(node2): %v", err)
	} else if n != 2 {
		t.Fatalf("node2: real Put calls for the snapshot's 2 live KV entries + retry = %d, want 2\n\n%s", n, c.Dump())
	}
	requireNoInvariantViolations(t, c)
}
