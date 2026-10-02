package chaos

import "fmt"

// This file implements docs/chaos/phase10-chaos-testing.md's bounded-wait
// requirement: every condition a scenario might need to wait for
// (leader election, a commit index, an applied index, cluster
// convergence) is checked via a hard-bounded number of Advance rounds,
// never a real-time sleep or an unbounded loop -- see the package doc on
// Cluster and Advance. Every Wait* helper here returns a plain error
// (never calls t.Fatalf itself) so a scenario test can print Cluster.Dump()
// alongside it for full diagnosability on a timeout.

// WaitForLeader advances the cluster up to maxRounds times, returning the
// ID of a live node that becomes leader as soon as one does. It returns
// an error if no node is leader after maxRounds rounds.
func (c *Cluster) WaitForLeader(maxRounds int) (string, error) {
	if id, ok := c.CurrentLeader(); ok {
		return id, nil
	}
	for i := 0; i < maxRounds; i++ {
		c.Advance(1)
		if id, ok := c.CurrentLeader(); ok {
			return id, nil
		}
	}
	return "", fmt.Errorf("chaos: no leader elected within %d rounds", maxRounds)
}

// WaitForCommit advances the cluster up to maxRounds times, returning nil
// as soon as node id's CommitIndex reaches at least index. It returns an
// error if id never catches up (or is not live, or the index is never
// reached) within maxRounds rounds.
func (c *Cluster) WaitForCommit(id string, index uint64, maxRounds int) error {
	for i := 0; i < maxRounds; i++ {
		if n := c.Node(id); n != nil && n.Raft().CommitIndex() >= index {
			return nil
		}
		c.Advance(1)
	}
	if n := c.Node(id); n != nil && n.Raft().CommitIndex() >= index {
		return nil
	}
	return fmt.Errorf("chaos: node %s did not reach commit index %d within %d rounds", id, index, maxRounds)
}

// WaitForApplied advances the cluster up to maxRounds times, returning
// nil as soon as node id's LastApplied reaches at least index.
func (c *Cluster) WaitForApplied(id string, index uint64, maxRounds int) error {
	for i := 0; i < maxRounds; i++ {
		if n := c.Node(id); n != nil && n.Raft().LastApplied() >= index {
			return nil
		}
		c.Advance(1)
	}
	if n := c.Node(id); n != nil && n.Raft().LastApplied() >= index {
		return nil
	}
	return fmt.Errorf("chaos: node %s did not apply through index %d within %d rounds", id, index, maxRounds)
}

// WaitForSnapshotIndex advances the cluster up to maxRounds times,
// returning nil as soon as node id's SnapshotIndex reaches at least
// index -- e.g. waiting for a lagging follower to receive and install a
// leader's InstallSnapshot (see Scenario C in scenario_snapshot_test.go).
//
// A generous maxRounds matters more here than for WaitForCommit/Applied:
// a node that was Partition'd (rather than CrashNode'd) keeps ticking
// and can time out its own election while still isolated (its term then
// keeps climbing the whole time it cannot reach anyone -- see
// docs/chaos/phase10-chaos-testing.md's note on this being a real,
// expected liveness cost of a cluster with no pre-vote phase, not a
// chaos-harness bug); healing it can therefore trigger one or more extra
// rounds of disruptive re-election among the other nodes before a
// legitimate leader emerges and actually sends the InstallSnapshot this
// is waiting for.
func (c *Cluster) WaitForSnapshotIndex(id string, index uint64, maxRounds int) error {
	for i := 0; i < maxRounds; i++ {
		if n := c.Node(id); n != nil && n.SnapshotIndex() >= index {
			return nil
		}
		c.Advance(1)
	}
	if n := c.Node(id); n != nil && n.SnapshotIndex() >= index {
		return nil
	}
	return fmt.Errorf("chaos: node %s did not reach snapshot index %d within %d rounds", id, index, maxRounds)
}

// WaitForConvergence advances the cluster up to maxRounds times, calling
// ApplyAllAvailable on every round, until every one of ids (c.LiveIDs()
// if empty) holds identical logical KV state (see AssertConverged). It
// returns an error, carrying the last observed divergence, if the
// cluster never converges within maxRounds rounds.
func (c *Cluster) WaitForConvergence(maxRounds int, ids ...string) error {
	if err := c.AssertConverged(ids...); err == nil {
		return nil
	}
	var lastErr error
	for i := 0; i < maxRounds; i++ {
		c.Advance(1)
		if err := c.AssertConverged(ids...); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("chaos: cluster did not converge within %d rounds: %w", maxRounds, lastErr)
}
