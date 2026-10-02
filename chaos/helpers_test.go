package chaos

import (
	"testing"

	"github.com/Kushall-07/forgedb/internal/statemachine"
)

// newScenarioCluster opens a Cluster of n nodes rooted at a fresh
// t.TempDir(), named after the running test (so EventLog.Dump identifies
// which scenario failed), and registers it for cleanup.
func newScenarioCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	c, err := NewCluster(t.Name(), 0, Config{NumNodes: n, BaseDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// newScenarioClusterSeeded is newScenarioCluster for a randomized
// scenario that needs its seed recorded in the EventLog for reproduction.
func newScenarioClusterSeeded(t *testing.T, n int, seed int64) *Cluster {
	t.Helper()
	c, err := NewCluster(t.Name(), seed, Config{NumNodes: n, BaseDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func mustElectLeader(t *testing.T, c *Cluster, id string) {
	t.Helper()
	if err := c.ElectLeader(id); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
}

func mustPropose(t *testing.T, c *Cluster, id string, cmd statemachine.Command) (index, term uint64) {
	t.Helper()
	index, term, err := c.Propose(id, cmd)
	if err != nil {
		t.Fatalf("Propose(%s): %v\n\n%s", id, err, c.Dump())
	}
	return index, term
}

func mustWaitForLeader(t *testing.T, c *Cluster, maxRounds int) string {
	t.Helper()
	id, err := c.WaitForLeader(maxRounds)
	if err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
	return id
}

func mustConverge(t *testing.T, c *Cluster, maxRounds int, ids ...string) {
	t.Helper()
	if err := c.WaitForConvergence(maxRounds, ids...); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
}

func requireNoInvariantViolations(t *testing.T, c *Cluster) {
	t.Helper()
	if err := c.AssertInvariants(); err != nil {
		t.Fatalf("%v\n\n%s", err, c.Dump())
	}
}

func assertGet(t *testing.T, c *Cluster, id, key, want string) {
	t.Helper()
	state, err := c.LogicalState(id)
	if err != nil {
		t.Fatalf("LogicalState(%s): %v", id, err)
	}
	got, ok := state[key]
	if !ok {
		t.Fatalf("node %s: key %q missing, want %q\n\n%s", id, key, want, c.Dump())
	}
	if got != want {
		t.Fatalf("node %s: Get(%s) = %q, want %q\n\n%s", id, key, got, want, c.Dump())
	}
}

func assertMissing(t *testing.T, c *Cluster, id, key string) {
	t.Helper()
	state, err := c.LogicalState(id)
	if err != nil {
		t.Fatalf("LogicalState(%s): %v", id, err)
	}
	if _, ok := state[key]; ok {
		t.Fatalf("node %s: key %q present, want missing\n\n%s", id, key, c.Dump())
	}
}

func put(clientID string, requestID uint64, key, value string) statemachine.Command {
	return statemachine.NewPutCommand(clientID, requestID, []byte(key), []byte(value))
}

func del(clientID string, requestID uint64, key string) statemachine.Command {
	return statemachine.NewDeleteCommand(clientID, requestID, []byte(key))
}

// --- Smoke test: the harness itself, before any scenario relies on it ---

func TestHarness_ElectProposeCrashRestart_Smoke(t *testing.T) {
	c := newScenarioCluster(t, 3)

	mustElectLeader(t, c, "node0")
	mustPropose(t, c, "node0", put("c1", 1, "x", "10"))
	c.Settle()
	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
	}

	if err := c.CrashNode("node0"); err != nil {
		t.Fatalf("CrashNode: %v", err)
	}
	if c.Node("node0") != nil {
		t.Fatalf("Node(node0) after crash: want nil")
	}

	leader := mustWaitForLeader(t, c, 40)
	if leader == "node0" {
		t.Fatalf("crashed node elected leader")
	}
	mustPropose(t, c, leader, put("c1", 2, "y", "20"))
	c.Settle()
	mustConverge(t, c, 10, c.LiveIDs()...)
	for _, id := range c.LiveIDs() {
		assertGet(t, c, id, "x", "10")
		assertGet(t, c, id, "y", "20")
	}

	if err := c.RestartNode("node0"); err != nil {
		t.Fatalf("RestartNode: %v", err)
	}
	if c.IsCrashed("node0") {
		t.Fatalf("node0 still marked crashed after RestartNode")
	}
	// KV recovers from its own WAL immediately, before any Raft activity.
	assertGet(t, c, "node0", "x", "10")

	c.Settle()
	mustConverge(t, c, 20)
	for _, id := range c.IDs() {
		assertGet(t, c, id, "x", "10")
		assertGet(t, c, id, "y", "20")
	}

	requireNoInvariantViolations(t, c)
}
