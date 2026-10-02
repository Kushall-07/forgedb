package chaos

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// InvariantTracker accumulates NodeState observations across an entire
// chaos scenario (via Cluster.Observe, called after every Tick/Settle
// round and after every explicit action) and flags any violation of the
// safety invariants docs/chaos/phase10-chaos-testing.md lists that can be
// checked purely from that history: term/commit/applied/snapshot
// monotonicity and "at most one leader per term". It never looks only at
// final state -- see the package doc.
//
// Invariants that require comparing logical KV content across nodes, or
// a specific scenario's knowledge of what "before" and "after" a fault
// mean (committed state survives, a minority cannot commit, an unsafe
// read is rejected, the cluster eventually converges), are deliberately
// not tracked here: they are checked directly by scenario tests using
// Cluster's other methods (LogicalState, WaitForConvergence, Node,
// ConsistentGet), which is simpler than teaching a generic tracker what
// "logically equivalent" or "before the fault" means.
//
// A node currently marked crashed in a NodeState is skipped entirely: its
// reported values are whatever they were frozen at the moment it
// crashed, not a live observation, and checking them would either be a
// no-op (same value, not a new low) or require knowing the restart
// semantics documented on Cluster.RestartNode. Commit/apply indexes are
// volatile and intentionally reset to 0 across a real restart (see
// docs/raft/phase6-raft-persistence.md); Cluster.RestartNode calls
// resetVolatile for exactly this reason before the node rejoins, so this
// tracker's monotonicity baseline for those two fields resets in step
// with the real node's own volatile state. Term and snapshot index are
// durably persisted and are never reset, even across a restart.
type InvariantTracker struct {
	mu sync.Mutex

	lastTerm     map[string]uint64
	lastSnapshot map[string]uint64
	lastCommit   map[string]uint64
	lastApplied  map[string]uint64

	leadersByTerm map[uint64]map[string]bool

	violations []string
}

func newInvariantTracker() *InvariantTracker {
	return &InvariantTracker{
		lastTerm:      make(map[string]uint64),
		lastSnapshot:  make(map[string]uint64),
		lastCommit:    make(map[string]uint64),
		lastApplied:   make(map[string]uint64),
		leadersByTerm: make(map[uint64]map[string]bool),
	}
}

// resetVolatile clears the commit/applied monotonicity baseline for id,
// called by Cluster.RestartNode immediately before the reopened node
// rejoins -- see the type doc comment.
func (inv *InvariantTracker) resetVolatile(id string) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	delete(inv.lastCommit, id)
	delete(inv.lastApplied, id)
}

func (inv *InvariantTracker) observe(snap ClusterSnapshot) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	for _, n := range snap.Nodes {
		if n.Crashed {
			continue
		}

		if prev, ok := inv.lastTerm[n.ID]; ok && n.Term < prev {
			inv.violateLocked("node %s term decreased: %d -> %d", n.ID, prev, n.Term)
		}
		inv.lastTerm[n.ID] = n.Term

		if prev, ok := inv.lastSnapshot[n.ID]; ok && n.SnapshotIndex < prev {
			inv.violateLocked("node %s snapshot index decreased: %d -> %d", n.ID, prev, n.SnapshotIndex)
		}
		inv.lastSnapshot[n.ID] = n.SnapshotIndex

		if prev, ok := inv.lastCommit[n.ID]; ok && n.CommitIndex < prev {
			inv.violateLocked("node %s commit index decreased: %d -> %d", n.ID, prev, n.CommitIndex)
		}
		inv.lastCommit[n.ID] = n.CommitIndex

		if prev, ok := inv.lastApplied[n.ID]; ok && n.LastApplied < prev {
			inv.violateLocked("node %s applied index decreased: %d -> %d", n.ID, prev, n.LastApplied)
		}
		inv.lastApplied[n.ID] = n.LastApplied

		if n.LastApplied > n.CommitIndex {
			inv.violateLocked("node %s lastApplied %d exceeds commitIndex %d", n.ID, n.LastApplied, n.CommitIndex)
		}

		if n.Role == "Leader" {
			set := inv.leadersByTerm[n.Term]
			if set == nil {
				set = make(map[string]bool)
				inv.leadersByTerm[n.Term] = set
			}
			set[n.ID] = true
			if len(set) > 1 {
				ids := make([]string, 0, len(set))
				for id := range set {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				inv.violateLocked("term %d has multiple leaders: %v", n.Term, ids)
			}
		}
	}
}

func (inv *InvariantTracker) violateLocked(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	for _, v := range inv.violations {
		if v == msg {
			return
		}
	}
	inv.violations = append(inv.violations, msg)
}

// Violations returns every distinct invariant violation observed so far,
// in the order first detected.
func (inv *InvariantTracker) Violations() []string {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return append([]string(nil), inv.violations...)
}

// Check returns a single error summarizing every violation observed so
// far, or nil if there have been none.
func (inv *InvariantTracker) Check() error {
	v := inv.Violations()
	if len(v) == 0 {
		return nil
	}
	return fmt.Errorf("chaos: %d invariant violation(s):\n  %s", len(v), strings.Join(v, "\n  "))
}
