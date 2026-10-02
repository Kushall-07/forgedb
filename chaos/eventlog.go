package chaos

import (
	"fmt"
	"strings"
)

// NodeState is a point-in-time snapshot of everything about one cluster
// member a chaos scenario needs to diagnose a failure: its Raft-level
// position (term, role, leader, log/commit/apply/snapshot indexes) and
// whether the harness currently considers it crashed. It deliberately
// does not include KV content -- that is compared separately (see
// Cluster.LogicalState) and would make every diagnostic dump unreadably
// large.
type NodeState struct {
	ID            string
	Crashed       bool
	Term          uint64
	Role          string
	LeaderID      string
	CommitIndex   uint64
	LastApplied   uint64
	SnapshotIndex uint64
	LastLogIndex  uint64
}

// ClusterSnapshot is every node's NodeState at one step of a scenario,
// together with the partition groups (if any) in effect at that moment.
type ClusterSnapshot struct {
	Nodes    []NodeState
	Groups   [][]string
	Isolated []string
}

// Step is one recorded entry in an EventLog: a scenario action, and the
// cluster's state immediately after it was taken.
type Step struct {
	Number   int
	Action   string
	Snapshot ClusterSnapshot
}

// EventLog records every step a chaos scenario takes against a Cluster,
// tagged with the scenario's seed (0 for a purely deterministic scenario
// that uses no randomness), so a failure can be diagnosed -- and, for a
// randomized scenario, exactly reproduced -- from the printed log alone.
// See docs/chaos/phase10-chaos-testing.md.
//
// EventLog never fails a test by itself; it only records. Deciding
// whether a given step sequence is a failure is the InvariantTracker's
// and the scenario's own job -- see Cluster.Observe and Dump.
type EventLog struct {
	Scenario string
	Seed     int64
	Steps    []Step
}

func newEventLog(scenario string, seed int64) *EventLog {
	return &EventLog{Scenario: scenario, Seed: seed}
}

func (l *EventLog) record(action string, snap ClusterSnapshot) Step {
	s := Step{Number: len(l.Steps) + 1, Action: action, Snapshot: snap}
	l.Steps = append(l.Steps, s)
	return s
}

// Last returns the most recently recorded step, and false if nothing has
// been recorded yet.
func (l *EventLog) Last() (Step, bool) {
	if len(l.Steps) == 0 {
		return Step{}, false
	}
	return l.Steps[len(l.Steps)-1], true
}

// Dump renders the full recorded action sequence and the cluster state
// after each one, in the format docs/chaos/phase10-chaos-testing.md
// documents. A test calls this (via Cluster.Dump) when a scenario fails,
// so the failure -- and, given Seed, the exact sequence that produced it
// -- can be reproduced without re-running anything.
func (l *EventLog) Dump() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CHAOS FAILURE\nscenario: %s\nseed: %d\ntotal steps: %d\n", l.Scenario, l.Seed, len(l.Steps))
	for _, s := range l.Steps {
		fmt.Fprintf(&b, "\nstep: %d\naction: %s\n", s.Number, s.Action)
		if len(s.Snapshot.Groups) > 0 {
			fmt.Fprintf(&b, "partition groups: %v\n", s.Snapshot.Groups)
		}
		if len(s.Snapshot.Isolated) > 0 {
			fmt.Fprintf(&b, "isolated: %v\n", s.Snapshot.Isolated)
		}
		b.WriteString("cluster:\n")
		for _, n := range s.Snapshot.Nodes {
			state := "running"
			if n.Crashed {
				state = "crashed"
			}
			fmt.Fprintf(&b, "  %s role=%s term=%d leader=%q commit=%d applied=%d snapshot=%d lastlog=%d [%s]\n",
				n.ID, n.Role, n.Term, n.LeaderID, n.CommitIndex, n.LastApplied, n.SnapshotIndex, n.LastLogIndex, state)
		}
	}
	return b.String()
}

// TailDump is like Dump but only renders the last n steps (plus the
// header), for a scenario long enough that the full dump would be
// unwieldy. n <= 0 means "every step" (equivalent to Dump).
func (l *EventLog) TailDump(n int) string {
	if n <= 0 || n >= len(l.Steps) {
		return l.Dump()
	}
	full := &EventLog{Scenario: l.Scenario, Seed: l.Seed, Steps: l.Steps[len(l.Steps)-n:]}
	return full.Dump()
}
