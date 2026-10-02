package chaos

import (
	"os"
	"strconv"
	"testing"
)

// TestRandomChaos_BoundedSeededScenario is docs/chaos/phase10-chaos-testing.md's
// randomized chaos scenario: a fixed cluster size, a fixed (but
// overridable -- see below) seed, and a hard-bounded step count, added
// only *after* every deterministic scenario in this package already
// passes, never as a replacement for them (section 25/37 of the Phase
// 10 brief). Its outcome is fully determined by (seed, numSteps,
// NumNodes): see NewRandomPlan.
//
// Reproducing a specific failure never requires re-deriving the seed:
// EventLog.Dump (via Cluster.Dump) prints the seed it was built from
// directly in its header, so a failure found by this test -- or by
// cmd/forge-chaos, which runs the identical plan generator -- is
// reproduced by pasting that seed back into CHAOS_SEED below (or
// forge-chaos's -seed flag), not by guessing.
func TestRandomChaos_BoundedSeededScenario(t *testing.T) {
	seed := int64(42)
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("CHAOS_SEED=%q: %v", s, err)
		}
		seed = parsed
	}
	const numNodes = 5
	const numSteps = 60

	c := newScenarioClusterSeeded(t, numNodes, seed)
	plan := NewRandomPlan(seed, c.IDs(), numSteps)
	RunRandomScenario(c, plan)

	// Bring the cluster back to a fully healthy state -- heal every
	// possible partition and restart every crashed node -- before
	// checking convergence: a randomized plan has no obligation to end
	// in a healthy state on its own, but the properties worth checking
	// (no lost committed data, eventual convergence) only make sense
	// once it is.
	for _, id := range c.IDs() {
		c.Heal(id)
	}
	c.HealPartitions()
	for _, id := range c.IDs() {
		if c.IsCrashed(id) {
			if err := c.RestartNode(id); err != nil {
				t.Fatalf("final RestartNode(%s): %v\n\n%s", id, err, c.Dump())
			}
		}
	}

	leader, err := c.WaitForLeader(60)
	if err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
	// A leader can only ever commit an entry from an *earlier* term as a
	// side effect of committing one of its own current term (Raft's
	// current-term commit rule -- see
	// docs/raft/phase8-raft-storage-integration.md and
	// TestNode_FullClusterRestart_ConvergesToRecoveredState in
	// internal/dbnode/node_test.go for the same requirement). After a
	// random plan full of leader changes, this leader's own log may be
	// entirely old-term entries nobody has committed yet; without one
	// new proposal here, CommitIndex can get permanently stuck at
	// whatever it already was, and WaitForConvergence below would never
	// return -- not because anything is broken, but because nothing it
	// does can satisfy that rule on its own.
	if _, _, err := c.Propose(leader, put("chaos-closing-marker", 1, "closing-marker", "done")); err != nil {
		t.Fatalf("closing Propose(%s): %v\n\n%s", leader, err, c.Dump())
	}
	c.Settle()

	if err := c.WaitForConvergence(60); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
	if err := c.AssertInvariants(); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
}
