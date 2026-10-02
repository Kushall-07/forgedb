package chaos

import "fmt"

// Observe takes a snapshot of every node's current Raft-level state,
// records it in c.Log under action, and feeds it to c.Inv for
// invariant checking. It is called automatically by every Cluster method
// that changes cluster state (Advance, CrashNode, RestartNode, Partition,
// Propose, ...); a scenario may also call it directly after any custom
// step it takes by hand, so the diagnostic log and invariant history
// never have a gap.
func (c *Cluster) Observe(action string) ClusterSnapshot {
	snap := c.snapshot()
	c.Log.record(action, snap)
	c.Inv.observe(snap)
	return snap
}

func (c *Cluster) snapshot() ClusterSnapshot {
	nodes := make([]NodeState, 0, len(c.ids))
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == nil {
			continue
		}
		term, role, leaderID := n.Raft().State()
		nodes = append(nodes, NodeState{
			ID:            id,
			Crashed:       c.crashed[id],
			Term:          term,
			Role:          role.String(),
			LeaderID:      leaderID,
			CommitIndex:   n.Raft().CommitIndex(),
			LastApplied:   n.Raft().LastApplied(),
			SnapshotIndex: n.SnapshotIndex(),
			LastLogIndex:  n.Raft().LastLogIndex(),
		})
	}

	var isolated []string
	for _, id := range c.ids {
		if c.partitioned[id] {
			isolated = append(isolated, id)
		}
	}

	return ClusterSnapshot{Nodes: nodes, Isolated: isolated, Groups: c.groups}
}

// Dump renders this cluster's full recorded event log (every action taken
// and the cluster's state immediately afterward) via c.Log.Dump, for a
// scenario to print when it fails.
func (c *Cluster) Dump() string { return c.Log.Dump() }

// CurrentLeader returns the ID of a live node that currently believes
// itself to be leader, and true, or ("", false) if none does. If more
// than one live node currently claims leadership, this returns the first
// found in ID order -- that situation is itself an invariant violation
// InvariantTracker already flags (see AssertInvariants), not something
// CurrentLeader needs to resolve itself.
func (c *Cluster) CurrentLeader() (string, bool) {
	for _, id := range c.ids {
		n := c.Node(id)
		if n != nil && n.IsLeader() {
			return id, true
		}
	}
	return "", false
}

// CurrentLeaderAmong is CurrentLeader restricted to the given candidate
// IDs -- useful when a scenario must ignore a node it already knows is a
// stale leader (e.g. one just partitioned away from the majority; see
// Scenario E) while waiting for the *other* side to elect a genuine one.
func (c *Cluster) CurrentLeaderAmong(ids ...string) (string, bool) {
	for _, id := range ids {
		n := c.Node(id)
		if n != nil && n.IsLeader() {
			return id, true
		}
	}
	return "", false
}

// WaitForLeaderAmong is WaitForLeader restricted to the given candidate
// IDs -- see CurrentLeaderAmong.
func (c *Cluster) WaitForLeaderAmong(maxRounds int, ids ...string) (string, error) {
	if id, ok := c.CurrentLeaderAmong(ids...); ok {
		return id, nil
	}
	for i := 0; i < maxRounds; i++ {
		c.Advance(1)
		if id, ok := c.CurrentLeaderAmong(ids...); ok {
			return id, nil
		}
	}
	return "", fmt.Errorf("chaos: no leader elected among %v within %d rounds", ids, maxRounds)
}

// AssertInvariants returns an error summarizing every safety-invariant
// violation (see InvariantTracker) observed so far across this cluster's
// entire history, or nil if there have been none.
func (c *Cluster) AssertInvariants() error { return c.Inv.Check() }

// AssertConverged reports whether every one of the given live node IDs
// (c.LiveIDs() if ids is empty) currently holds identical logical KV
// state (see LogicalState) -- deliberately not identical physical Raft
// log/snapshot layout, per docs/chaos/phase10-chaos-testing.md's
// "logical vs physical state" section. It does not itself advance the
// cluster or apply anything; see WaitForConvergence for the bounded,
// retrying version a scenario actually wants after a fault.
func (c *Cluster) AssertConverged(ids ...string) error {
	if len(ids) == 0 {
		ids = c.LiveIDs()
	}
	if len(ids) == 0 {
		return nil
	}
	want, err := c.LogicalState(ids[0])
	if err != nil {
		return err
	}
	for _, id := range ids[1:] {
		got, err := c.LogicalState(id)
		if err != nil {
			return err
		}
		if !stateEqual(want, got) {
			return &convergenceError{a: ids[0], b: id, stateA: want, stateB: got}
		}
	}
	return nil
}

func stateEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

type convergenceError struct {
	a, b           string
	stateA, stateB map[string]string
}

func (e *convergenceError) Error() string {
	return "chaos: nodes " + e.a + " and " + e.b + " have diverged logical state"
}
