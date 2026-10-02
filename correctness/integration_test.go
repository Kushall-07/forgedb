package correctness

import (
	"testing"

	"github.com/Kushall-07/forgedb/chaos"
)

// This file records real operation histories from a real chaos.Cluster
// (never a second, parallel cluster implementation -- §47 of the Phase
// 11 brief) through a Harness, and checks each one with Check. Unlike
// checker_test.go/model_test.go, every History here comes from actually
// calling (*chaos.Cluster).Propose/ConsistentGet against real
// internal/raft, internal/statemachine, and internal/storage code.

func newCorrectnessCluster(t *testing.T, n int) *chaos.Cluster {
	t.Helper()
	c, err := chaos.NewCluster(t.Name(), 0, chaos.Config{NumNodes: n, BaseDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func mustElectLeader(t *testing.T, c *chaos.Cluster, id string) {
	t.Helper()
	if err := c.ElectLeader(id); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
}

// assertLinearizable Checks h and fails the test, printing both Check's
// own report and the cluster's chaos.Dump (so a failure here is
// diagnosable -- and, for a randomized run, reproducible -- exactly like
// a chaos scenario failure), if the result is anything other than
// StatusValid.
func assertLinearizable(t *testing.T, c *chaos.Cluster, h History) CheckResult {
	t.Helper()
	if err := h.Validate(); err != nil {
		t.Fatalf("History.Validate: %v\n\n%s", err, c.Dump())
	}
	result := Check(h, CheckOptions{})
	if result.Status != StatusValid {
		t.Fatalf("%s\n\nchaos log:\n%s", result.Report(h), c.Dump())
	}
	return result
}

// --- §18 deterministic sequential histories ------------------------------

func TestLive_SequentialPutGet(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)
	h.Get("node0", "c1", 2, "x")

	assertLinearizable(t, c, h.History())
}

func TestLive_SequentialOverwrite(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)
	h.Put("node0", "c1", 2, "x", "2", 20)
	h.Get("node0", "c1", 3, "x")

	assertLinearizable(t, c, h.History())
}

func TestLive_Delete(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)
	h.Delete("node0", "c1", 2, "x", 20)
	h.Get("node0", "c1", 3, "x")

	assertLinearizable(t, c, h.History())
}

func TestLive_IndependentKeys(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "a", "1", 20)
	h.Put("node0", "c1", 2, "b", "2", 20)
	h.Get("node0", "c1", 3, "a")
	h.Get("node0", "c1", 4, "b")

	assertLinearizable(t, c, h.History())
}

// --- §20-22 read-after-write / delete / overwrite semantics --------------

func TestLive_ReadAfterWrite(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	put := h.Put("node0", "c1", 1, "x", "1", 20)
	if put.Outcome != OutcomeOK {
		t.Fatalf("Put: outcome = %s, want OK", put.Outcome)
	}
	get := h.Get("node0", "c1", 2, "x")
	if !get.Found || get.Result != "1" {
		t.Fatalf("Get after Put: found=%v value=%q, want (true, \"1\")", get.Found, get.Result)
	}

	assertLinearizable(t, c, h.History())
}

func TestLive_DeleteSemantics(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)
	h.Delete("node0", "c1", 2, "x", 20)
	get := h.Get("node0", "c1", 3, "x")
	if get.Found {
		t.Fatalf("Get after Delete: found=true, want not-found")
	}

	assertLinearizable(t, c, h.History())
}

func TestLive_DeleteThenRecreate(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)
	h.Delete("node0", "c1", 2, "x", 20)
	h.Put("node0", "c1", 3, "x", "2", 20)
	get := h.Get("node0", "c1", 4, "x")
	if !get.Found || get.Result != "2" {
		t.Fatalf("Get after delete+recreate: found=%v value=%q, want (true, \"2\")", get.Found, get.Result)
	}

	assertLinearizable(t, c, h.History())
}

// --- §19/§23/§24 concurrent histories (via Harness's Invoke/Resolve split) --

func TestLive_ConcurrentPutPut(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	// Invoke both before resolving either -- a genuine overlap by
	// construction (see Harness's doc comment).
	a := h.Invoke("c1", 1, OpPut, "node0", "x", "A")
	b := h.Invoke("c2", 1, OpPut, "node0", "x", "B")
	h.Resolve(a, 20)
	h.Resolve(b, 20)
	h.Get("node0", "c1", 2, "x")

	// Either final value is legitimate (the two writes overlap); Check
	// must accept this regardless of which one "physically" landed last.
	assertLinearizable(t, c, h.History())
}

func TestLive_ConcurrentReads(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)

	g1 := h.Invoke("c1", 2, OpGet, "node0", "x", "")
	g2 := h.Invoke("c2", 1, OpGet, "node0", "x", "")
	g3 := h.Invoke("c3", 1, OpGet, "node0", "x", "")
	h.Resolve(g1, 0)
	h.Resolve(g2, 0)
	h.Resolve(g3, 0)

	assertLinearizable(t, c, h.History())
}

func TestLive_ConcurrentPutGet(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	p := h.Invoke("c1", 1, OpPut, "node0", "x", "1")
	g := h.Invoke("c2", 1, OpGet, "node0", "x", "")
	h.Resolve(p, 20)
	h.Resolve(g, 0)

	// The Get may legally observe either the pre-write (not found) or
	// post-write ("1") state -- Check must accept whichever this
	// particular run actually produced.
	assertLinearizable(t, c, h.History())
}

// --- §26 leader change: a successful write must survive, and a later
// successful read from the new leader must observe it ------------------

func TestLive_LeaderChange_WriteSurvivesAndIsVisible(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	put := h.Put("node0", "c1", 1, "x", "1", 20)
	if put.Outcome != OutcomeOK {
		t.Fatalf("Put before crash: outcome = %s, want OK\n\n%s", put.Outcome, c.Dump())
	}

	if err := c.CrashNode("node0"); err != nil {
		t.Fatalf("CrashNode: %v", err)
	}
	newLeader, err := c.WaitForLeader(40)
	if err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}

	get := h.Get(newLeader, "c1", 2, "x")
	if !get.Found || get.Result != "1" {
		t.Fatalf("Get from new leader %s after crash: found=%v value=%q, want (true, \"1\")\n\n%s",
			newLeader, get.Found, get.Result, c.Dump())
	}

	assertLinearizable(t, c, h.History())
}

// --- §39 incomplete operation: a write invoked right before its node
// crashes must not be required to appear in the linearization ----------

func TestLive_IncompleteWrite_InterruptedByCrash(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	p := h.Invoke("c1", 1, OpPut, "node0", "x", "1")
	op := h.ResolveInterrupted(p, func() {
		if err := c.CrashNode("node0"); err != nil {
			t.Fatalf("CrashNode: %v", err)
		}
	})
	if op.Outcome != OutcomeIncomplete {
		t.Fatalf("interrupted Put: outcome = %s, want Incomplete\n\n%s", op.Outcome, c.Dump())
	}

	newLeader, err := c.WaitForLeader(40)
	if err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
	// Whether or not the interrupted write actually landed before the
	// crash, the resulting history must still be linearizable: Check's
	// handling of OutcomeIncomplete (§39/§40) must accept either answer.
	h.Get(newLeader, "c1", 2, "x")

	assertLinearizable(t, c, h.History())
}

// --- §25/§31 partition + minority leader: unavailable reads must never
// be treated as a successful read of an invalid value -------------------

func TestLive_PartitionedMinorityLeader_UnavailableReadExcluded(t *testing.T) {
	c := newCorrectnessCluster(t, 5)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)

	// Isolate the leader into a minority of one; it still locally
	// believes itself leader but cannot confirm a majority (see
	// docs/raft/phase8.5-read-consistency.md and
	// docs/chaos/phase10-chaos-testing.md §12).
	c.Partition("node0")
	c.Advance(2)

	op := h.Get("node0", "c1", 2, "x")
	if op.Outcome != OutcomeFailed {
		t.Fatalf("ConsistentGet against minority leader: outcome = %s, want Failed\n\n%s", op.Outcome, c.Dump())
	}

	c.Heal("node0")
	assertLinearizable(t, c, h.History())
}

// --- §28/§57 deduplication: a retried identical (ClientID, RequestID)
// must not be modeled as a second independent write --------------------

func TestLive_RetryAcrossLeaderChange(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	cmd := h.Put("node0", "c1", 1, "x", "10", 20)
	if cmd.Outcome != OutcomeOK {
		t.Fatalf("initial Put: outcome = %s, want OK\n\n%s", cmd.Outcome, c.Dump())
	}

	if err := c.CrashNode("node0"); err != nil {
		t.Fatalf("CrashNode: %v", err)
	}
	newLeader, err := c.WaitForLeader(40)
	if err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}

	// The client did not see the first commit succeed and retries the
	// identical logical request (same ClientID/RequestID) against
	// whichever node is leader now -- statemachine's dedup table (Phase
	// 7) resolves this as a replay internally, but externally it is
	// just another successful Put of the same value, and Check needs no
	// special-casing for that (§29/§58).
	retry := h.Put(newLeader, "c1", 1, "x", "10", 20)
	if retry.Outcome != OutcomeOK {
		t.Fatalf("retried Put: outcome = %s, want OK\n\n%s", retry.Outcome, c.Dump())
	}

	h.Get(newLeader, "c2", 1, "x")
	assertLinearizable(t, c, h.History())
}

// --- §27 snapshot catch-up: a successful read from a node that caught up
// via InstallSnapshot must be consistent with the recorded history -------

func TestLive_SnapshotCatchUp(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	c.Partition("node2")

	h.Put("node0", "c1", 1, "x", "1", 20)
	for i := 2; i <= 6; i++ {
		h.Put("node0", "c1", uint64(i), "filler", "v", 20)
	}
	if err := c.CreateSnapshot("node0", 6); err != nil {
		t.Fatalf("CreateSnapshot(node0): %v", err)
	}
	if err := c.CreateSnapshot("node1", 6); err != nil {
		t.Fatalf("CreateSnapshot(node1): %v", err)
	}

	c.Heal("node2")
	// A Partition'd (not crashed) node keeps ticking the whole time it is
	// isolated and can time out its own election before healing, which
	// can trigger a disruptive re-election once healed (see
	// WaitForSnapshotIndex's own doc comment and
	// docs/chaos/phase10-chaos-testing.md §7) -- so the leader afterward
	// is not necessarily still node0. ConsistentGet is leader-only by
	// construction (docs/raft/phase8.5-read-consistency.md), so the read
	// below targets whichever node that re-election actually produces,
	// including node2 itself if it becomes leader.
	if err := c.WaitForSnapshotIndex("node2", 6, 40); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
	leader, err := c.WaitForLeader(40)
	if err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}

	get := h.Get(leader, "c1", 7, "x")
	if !get.Found || get.Result != "1" {
		t.Fatalf("Get(x) on leader %s after node2's snapshot catch-up: found=%v value=%q, want (true, \"1\")\n\n%s",
			leader, get.Found, get.Result, c.Dump())
	}

	assertLinearizable(t, c, h.History())
}

// --- §31 local reads must never be fed into this checker ---------------
//
// This is a documentation-level test, not a behavioral one: it confirms
// Harness.Get only ever calls through (*chaos.Cluster).ConsistentGet
// (never a direct LogicalState/Store lookup) by checking that a read
// against an isolated minority leader is reported as Failed, exactly as
// ConsistentGet's own ReadIndex barrier guarantees -- a local
// storage.Store.Get would have returned the (stale but present) value
// instead and this test would see OutcomeOK, not OutcomeFailed.
func TestLive_HarnessNeverUsesLocalReads(t *testing.T) {
	c := newCorrectnessCluster(t, 3)
	mustElectLeader(t, c, "node0")
	h := NewHarness(c)

	h.Put("node0", "c1", 1, "x", "1", 20)
	c.Partition("node0")
	c.Advance(2)

	op := h.Get("node0", "c1", 2, "x")
	if op.Outcome == OutcomeOK {
		t.Fatalf("Get against isolated minority leader returned OK -- Harness must be using ConsistentGet, not a local read\n\n%s", c.Dump())
	}
	c.Heal("node0")
}
