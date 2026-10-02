package raft

// Status is a cheap, point-in-time, read-only snapshot of this node's
// Raft-level state -- everything docs/observability/phase12-observability.md's
// cluster diagnostics need to answer "who is leader, what term, what has
// been committed/applied, is a snapshot present." Status never mutates
// anything and never performs an RPC; it only copies a handful of
// already-tracked fields while holding n.mu briefly, exactly like
// State(), CommitIndex(), and LastApplied() already do. A caller (the
// metrics pull-gauges in internal/dbnode, or the /cluster HTTP handler in
// internal/api) can call it as often as it likes without risking lock
// contention on the hot Raft path.
type Status struct {
	NodeID        string
	Role          string
	Term          uint64
	LeaderID      string
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
	Peers         []string
}

// PeerStatus describes one peer's replication progress as this node's
// leader-side state currently tracks it -- see broadcastAppendEntriesLocked's
// nextIndex/matchIndex. It is meaningless (and never returned) when this
// node is not currently leader, since only a leader tracks per-peer
// replication state at all.
type PeerStatus struct {
	PeerID     string
	NextIndex  uint64
	MatchIndex uint64
	// Lag is how many entries this peer's matchIndex is behind the
	// leader's own last log index -- a direct answer to "how far behind
	// is this follower," per the phase doc's replication-health section.
	Lag uint64
}

// Status returns a snapshot of this node's current Raft state. See the
// Status type doc comment.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		NodeID:        n.id,
		Role:          n.role.String(),
		Term:          n.currentTerm,
		LeaderID:      n.leaderID,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LastLogIndex:  n.log.LastIndex(),
		LastLogTerm:   n.log.LastTerm(),
		SnapshotIndex: n.log.entries[0].Index,
		SnapshotTerm:  n.log.entries[0].Term,
		Peers:         append([]string(nil), n.peers...),
	}
}

// PeerStatuses returns this node's current view of every peer's
// replication progress, or nil if this node is not currently leader (see
// the PeerStatus doc comment). The returned slice is a fresh copy, safe
// for the caller to retain or mutate freely.
func (n *Node) PeerStatuses() []PeerStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return nil
	}
	lastIndex := n.log.LastIndex()
	out := make([]PeerStatus, 0, len(n.peers))
	for _, p := range n.peers {
		match := n.matchIndex[p]
		lag := uint64(0)
		if lastIndex > match {
			lag = lastIndex - match
		}
		out = append(out, PeerStatus{PeerID: p, NextIndex: n.nextIndex[p], MatchIndex: match, Lag: lag})
	}
	return out
}
