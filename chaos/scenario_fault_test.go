package chaos

import (
	"errors"
	"testing"

	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/raft"
)

// --- Scenario I: persistence / storage failure injection -------------------
//
// These reuse the existing failure-injection mechanisms this codebase
// already established (raft.MemoryPersister's FailNextSave/
// FailNextSnapshotSave, generalized to the real FilePersister a
// dbnode.Node actually uses via dbnode.Config.WrapPersister -- see
// faultyPersister; and node_test.go's faultyStore/countingStore pattern,
// generalized the same way via WrapStore -- see faultyStore), rather than
// inventing a new persistence subsystem for chaos testing (section 19).
// Each checks the same thing: a failed durable write must leave
// in-memory state no further ahead than what is actually durable, and
// the node must remain usable (and eventually correct) once the fault
// clears -- never "fail Raft consensus" for what is purely a local
// durability problem (section 20's Raft-committed vs
// state-machine-applied distinction).

func TestScenarioI_PersistenceFailure_SaveStateFailsOnPropose(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	before := c.Node("node0").Raft().LastLogIndex()
	if err := c.FailNextPersist("node0"); err != nil {
		t.Fatalf("FailNextPersist: %v", err)
	}
	if _, _, err := c.Propose("node0", put("c1", 1, "x", "10")); err == nil {
		t.Fatalf("Propose with injected SaveState failure: want error, got nil")
	}
	c.Observe("ProposeWithInjectedPersistFailure")
	if got := c.Node("node0").Raft().LastLogIndex(); got != before {
		t.Fatalf("LastLogIndex after failed persist = %d, want unchanged %d (the append must roll back)\n\n%s", got, before, c.Dump())
	}

	// The fault only fires once: a retry of the identical request now
	// succeeds normally.
	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()
	mustConverge(t, c, 10)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
	}
	requireNoInvariantViolations(t, c)
}

func TestScenarioI_PersistenceFailure_SaveSnapshotFails(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()

	if err := c.FailNextSnapshotPersist("node0"); err != nil {
		t.Fatalf("FailNextSnapshotPersist: %v", err)
	}
	if err := c.CreateSnapshot("node0", 1); err == nil {
		t.Fatalf("CreateSnapshot with injected SaveSnapshot failure: want error, got nil")
	}
	if got := c.Node("node0").SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after failed snapshot persist = %d, want unchanged 0\n\n%s", got, c.Dump())
	}
	// LastLogIndex must be untouched too: CreateSnapshot never compacts
	// the log unless the snapshot itself was confirmed durable first.
	if got := c.Node("node0").Raft().LastLogIndex(); got != 1 {
		t.Fatalf("LastLogIndex after failed snapshot persist = %d, want unchanged 1\n\n%s", got, c.Dump())
	}

	if err := c.CreateSnapshot("node0", 1); err != nil {
		t.Fatalf("retry CreateSnapshot: %v", err)
	}
	if got := c.Node("node0").SnapshotIndex(); got != 1 {
		t.Fatalf("SnapshotIndex after retry = %d, want 1", got)
	}
	requireNoInvariantViolations(t, c)
}

func TestScenarioI_StorageFailure_PutFailsThenRetrySucceeds(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")

	mustPropose(t, c, "node0", put("c1", 1, "a", "1"))
	c.Settle() // Advance's own ApplyAllAvailable already applies this entry.
	if got := c.Node("node0").Raft().LastApplied(); got != 1 {
		t.Fatalf("LastApplied after initial write = %d, want 1", got)
	}

	if err := c.FailNextPut("node0"); err != nil {
		t.Fatalf("FailNextPut: %v", err)
	}
	mustPropose(t, c, "node0", put("c1", 2, "b", "2"))
	mustPropose(t, c, "node0", put("c1", 3, "cc", "3"))
	// Replicate (commit) without applying: Settle/Advance always call
	// ApplyAllAvailable themselves, which -- since the injected fault is
	// single-shot -- would fail once and then auto-retry past it on
	// their own very next round, leaving nothing here to observe. Ticking
	// and draining directly advances CommitIndex the same way
	// settleCommit does (see internal/dbnode/cluster_test.go) without
	// touching LastApplied at all.
	c.TickAll()
	c.DrainAll()
	c.TickAll()
	c.DrainAll()
	c.Observe("Settle(no-apply)")
	if got := c.Node("node0").Raft().CommitIndex(); got != 3 {
		t.Fatalf("CommitIndex = %d, want 3 (Raft consensus is unaffected by a local storage fault)", got)
	}

	// The first ApplyAvailable call after the fault is injected must
	// observe it directly.
	errs := c.ApplyAllAvailable()
	if errs["node0"] == nil {
		t.Fatalf("ApplyAllAvailable: want an error for node0 from the injected storage fault, got nil")
	}
	if got := c.Node("node0").Raft().LastApplied(); got != 1 {
		t.Fatalf("LastApplied after fault = %d, want unchanged 1 (must not advance past the failed entry)\n\n%s", got, c.Dump())
	}
	assertMissing(t, c, "node0", "b")

	// Retrying ApplyAvailable (the next Advance round, or an explicit
	// call) completes it once the fault has cleared.
	applied, err := c.Node("node0").ApplyAvailable()
	if err != nil {
		t.Fatalf("retry ApplyAvailable: %v", err)
	}
	if applied != 2 {
		t.Fatalf("retry ApplyAvailable applied = %d, want 2", applied)
	}
	c.Observe("RetryApplyAvailable(node0)")
	mustConverge(t, c, 10)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "a", "1")
		assertGet(t, c, id, "b", "2")
		assertGet(t, c, id, "cc", "3")
	}
	requireNoInvariantViolations(t, c)
}

// --- Read-consistency chaos (section 18) -----------------------------------
//
// Phase 10 deliberately does not build the full linearizability checker
// Phase 11 owns (see the package doc); it only checks the specific unsafe-
// read cases docs/chaos/phase10-chaos-testing.md calls out, directly
// against the existing ConsistentGet/ReadIndex mechanism.

func TestReadConsistency_FollowerNeverServesConsistentGet(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()

	for _, follower := range []string{"node1", "node2"} {
		if _, err := c.ConsistentGet(follower, []byte("x")); !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("ConsistentGet(%s) (a follower): err = %v, want ErrNotLeader\n\n%s", follower, err, c.Dump())
		}
	}
	requireNoInvariantViolations(t, c)
}

func TestReadConsistency_MinorityLeaderConsistentGetFailsSafely(t *testing.T) {
	c := newScenarioCluster(t, 3)
	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()

	// Isolate the leader into a minority of one: it still locally
	// believes itself leader (see Scenario E), so only ReadIndex's own
	// majority-confirmation round -- not the node's own role check -- can
	// catch this.
	c.Partition("node0")

	if _, err := c.ConsistentGet("node0", []byte("x")); !errors.Is(err, dbnode.ErrReadUnavailable) {
		t.Fatalf("ConsistentGet on an isolated minority leader: err = %v, want ErrReadUnavailable\n\n%s", err, c.Dump())
	}

	c.Heal("node0")
	c.Settle()
	requireNoInvariantViolations(t, c)
}
