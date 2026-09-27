package raft

import "fmt"

// LastApplied returns the highest log index this node's state machine has
// confirmed applying, via MarkApplied. It starts at 0 for both a brand new
// node and a freshly restarted one -- Phase 6 deliberately does not
// persist lastApplied (see docs/raft/phase6-raft-persistence.md), so a
// restarted node's state machine must re-derive its own progress from
// scratch; see docs/raft/phase7-state-machine.md for what that implies in
// practice.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// MarkApplied records that every committed entry up to and including
// index has now been applied by the caller's state machine (see
// internal/statemachine.Applier, the only intended caller). Node itself
// never applies anything to anything -- see the package doc comment --
// this method exists purely so a state machine has somewhere authoritative
// to report its own progress, keeping the invariant lastApplied <=
// commitIndex enforced in one place rather than trusted to every caller.
//
// MarkApplied rejects, advancing nothing, any index that is not a legal
// next value: index must be strictly greater than the current lastApplied
// (progress is monotonic; the same or an earlier index can never be
// re-marked) and must not exceed commitIndex (an entry cannot be marked
// applied before Raft itself considers it committed). A caller that
// applies committed entries one at a time, in order, and calls
// MarkApplied immediately after each one succeeds never has any reason to
// hit either rejection.
func (n *Node) MarkApplied(index uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index <= n.lastApplied {
		return fmt.Errorf("raft: MarkApplied(%d): must be greater than current lastApplied %d", index, n.lastApplied)
	}
	if index > n.commitIndex {
		return fmt.Errorf("raft: MarkApplied(%d): exceeds commitIndex %d", index, n.commitIndex)
	}
	n.lastApplied = index
	return nil
}
