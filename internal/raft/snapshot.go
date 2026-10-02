package raft

import (
	"errors"
	"fmt"

	"github.com/Kushall-07/forgedb/internal/metrics"
)

// ErrInvalidSnapshotIndex is returned by CreateSnapshot when index is not
// currently a valid snapshot boundary for this node -- see CreateSnapshot's
// doc comment for the exact rule.
var ErrInvalidSnapshotIndex = errors.New("raft: invalid snapshot index")

// SnapshotIndex returns the index of this node's most recent snapshot --
// the log's current compaction boundary -- or 0 if no snapshot has ever
// been created or installed.
func (n *Node) SnapshotIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.entries[0].Index
}

// CreateSnapshot captures a new snapshot at index, covering every log
// entry up to and including it, and then compacts this node's own log,
// discarding entries at or before index. data is the caller's (a state
// machine's) own opaque serialization of its state after applying through
// index -- internal/raft never interprets it (see Snapshot).
//
// index must be an exact fit: strictly greater than this node's current
// snapshot boundary (SnapshotIndex) -- a request at or behind it is
// rejected with ErrInvalidSnapshotIndex rather than silently treated as a
// no-op, since the caller's data was almost certainly computed assuming a
// different (larger) boundary than one already superseded -- and must not
// exceed this node's own LastApplied() or CommitIndex(): a snapshot may
// only ever represent state that has genuinely been applied, never
// speculative or merely-committed-but-unapplied state (see
// docs/raft/phase9-snapshots.md). index == 0 is always rejected: there is
// nothing to snapshot at the position before the log even begins.
//
// CreateSnapshot persists the new snapshot via Options.Persister.SaveSnapshot
// *before* ever touching this node's in-memory log, and only compacts the
// log and persists the smaller resulting state (via SaveState) once that
// snapshot is confirmed durable. If SaveSnapshot fails, nothing changes at
// all. If the subsequent SaveState fails, the in-memory log compaction is
// rolled back (so in-memory state never gets ahead of the last state this
// node could actually reload after a crash) even though the snapshot
// itself remains durably saved -- this is a safe, recoverable state (see
// the phase doc's crash-window analysis): the next successful
// CreateSnapshot, or this node's own restart, reconciles it correctly.
func (n *Node) CreateSnapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index == 0 {
		return fmt.Errorf("raft: CreateSnapshot(0): %w: there is nothing to snapshot before the log begins", ErrInvalidSnapshotIndex)
	}
	if index <= n.log.entries[0].Index {
		return fmt.Errorf("raft: CreateSnapshot(%d): %w: must be greater than the current snapshot boundary %d", index, ErrInvalidSnapshotIndex, n.log.entries[0].Index)
	}
	if index > n.lastApplied {
		return fmt.Errorf("raft: CreateSnapshot(%d): %w: exceeds lastApplied %d", index, ErrInvalidSnapshotIndex, n.lastApplied)
	}
	if index > n.commitIndex {
		return fmt.Errorf("raft: CreateSnapshot(%d): %w: exceeds commitIndex %d", index, ErrInvalidSnapshotIndex, n.commitIndex)
	}

	term, ok := n.log.TermAt(index)
	if !ok {
		// Unreachable given the checks above (index is between the
		// current boundary and lastApplied <= commitIndex <= LastIndex),
		// but checked explicitly rather than trusted.
		return fmt.Errorf("raft: CreateSnapshot(%d): %w: index not present in this node's own log", index, ErrInvalidSnapshotIndex)
	}

	if err := n.persister.SaveSnapshot(Snapshot{LastIncludedIndex: index, LastIncludedTerm: term, Data: data}); err != nil {
		metrics.RaftSnapshotFailuresTotal.Inc()
		metrics.RecordError("raft")
		return fmt.Errorf("raft: persist snapshot: %w", err)
	}

	oldEntries := append([]LogEntry(nil), n.log.entries...)
	if err := n.log.compact(index); err != nil {
		// Unreachable given the validation above; fail closed rather than
		// leave the (already-saved) snapshot inconsistent with an
		// uncompacted log.
		metrics.RaftSnapshotFailuresTotal.Inc()
		metrics.RecordError("raft")
		return fmt.Errorf("raft: compact log: %w", err)
	}
	if err := n.persistLocked(); err != nil {
		n.log.entries = oldEntries
		metrics.RaftSnapshotFailuresTotal.Inc()
		metrics.RecordError("raft")
		return fmt.Errorf("raft: persist compacted log: %w", err)
	}

	n.snapshotData = data
	metrics.RaftSnapshotsCreatedTotal.Inc()
	metrics.RaftSnapshotBytesTotal.Add(uint64(len(data)))
	logEvent(n.id).Info(eventSnapshotCreated, "snapshot_index", index, "snapshot_term", term, "bytes", len(data))
	return nil
}

// PendingSnapshot returns the snapshot this node has installed (via
// InstallSnapshot) or loaded from its persister at startup that its
// caller's state machine has not yet confirmed restoring, and true. It
// returns (Snapshot{}, false) if there is none.
//
// A caller driving application of committed entries (see
// internal/statemachine.Applier) must check this *before* calling
// CommittedEntries/MarkApplied: if a snapshot is pending, the entries
// covering indexes up through its LastIncludedIndex are no longer in this
// node's log to replay at all -- the state machine's only way to recover
// that state is to restore it from Data, then call
// ConfirmSnapshotRestored, before resuming ordinary apply. See
// docs/raft/phase9-snapshots.md.
func (n *Node) PendingSnapshot() (Snapshot, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pendingSnapshot == nil {
		return Snapshot{}, false
	}
	return *n.pendingSnapshot, true
}

// ConfirmSnapshotRestored records that the caller's state machine has
// successfully restored its entire state from the snapshot most recently
// returned by PendingSnapshot, identified by lastIncludedIndex (so a
// caller that was racing a newer snapshot installed out from under it
// cannot mistakenly confirm the wrong one). On success, it advances
// LastApplied to lastIncludedIndex (if not already past it) and clears the
// pending snapshot.
//
// It returns an error, changing nothing, if lastIncludedIndex does not
// match the currently pending snapshot (including there being none
// pending at all) or exceeds CommitIndex -- the latter should not happen
// in ordinary operation (CommitIndex is always advanced to at least
// LastIncludedIndex at the moment a snapshot becomes pending -- see
// CreateSnapshot's caller contract and HandleInstallSnapshot) but is
// re-validated here rather than trusted, mirroring MarkApplied's own
// defense against ever marking an index applied before Raft considers it
// committed.
func (n *Node) ConfirmSnapshotRestored(lastIncludedIndex uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.pendingSnapshot == nil || n.pendingSnapshot.LastIncludedIndex != lastIncludedIndex {
		return fmt.Errorf("raft: ConfirmSnapshotRestored(%d): no matching pending snapshot", lastIncludedIndex)
	}
	if lastIncludedIndex > n.commitIndex {
		return fmt.Errorf("raft: ConfirmSnapshotRestored(%d): exceeds commitIndex %d", lastIncludedIndex, n.commitIndex)
	}

	if lastIncludedIndex > n.lastApplied {
		n.lastApplied = lastIncludedIndex
		n.notifyAppliedLocked()
	}
	n.pendingSnapshot = nil
	return nil
}

// HandleInstallSnapshot implements the InstallSnapshot RPC (RPCHandler).
// It follows the same term-check and step-down discipline as
// HandleAppendEntries: a stale term is rejected outright, and a newer or
// equal term causes this node to accept args.LeaderID as the current
// leader and step down to Follower.
//
// A stale or duplicate snapshot (LastIncludedIndex at or behind this
// node's own current snapshot boundary -- which also covers the case
// where this node is already fully caught up) is acknowledged as a
// trivial success without touching anything: this node already has
// everything this snapshot would provide.
//
// Otherwise, the snapshot is persisted durably (SaveSnapshot) *before* any
// in-memory log mutation; if that fails, the RPC is rejected and nothing
// changes, so the leader simply retries on its next heartbeat. Once
// durable, the log's compaction boundary is updated via
// Log.installSnapshotBoundary (preserving a matching suffix, or discarding
// a conflicting one -- see that method), the resulting smaller log is
// itself persisted (rolled back on failure, exactly as CreateSnapshot
// does), and only then does this node's in-memory state advance:
// CommitIndex is raised to at least LastIncludedIndex (the snapshot proves
// that much is committed, even if this follower's own CommitIndex hadn't
// caught up yet) and a pending snapshot is recorded for the caller's state
// machine to restore (see PendingSnapshot). LastApplied is deliberately
// *not* touched here -- only the state machine, via
// ConfirmSnapshotRestored once it has actually restored Data, is entitled
// to advance it, preserving the invariant that LastApplied is never raised
// to a position whose effects have not genuinely been applied anywhere
// (see docs/raft/phase9-snapshots.md).
func (n *Node) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		if err := n.becomeFollowerLocked(args.Term); err != nil {
			return InstallSnapshotReply{Term: n.currentTerm, Success: false}
		}
	}
	if args.Term < n.currentTerm {
		return InstallSnapshotReply{Term: n.currentTerm, Success: false}
	}

	n.role = Follower
	if args.LeaderID != "" && args.LeaderID != n.leaderID {
		metrics.RaftLeaderChangesTotal.Inc()
		logEvent(n.id).Info(eventLeaderChanged, "term", n.currentTerm, "leader_id", args.LeaderID)
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	if args.LastIncludedIndex <= n.log.entries[0].Index {
		// Stale or duplicate: this node is already at or past this
		// boundary. Nothing to do.
		return InstallSnapshotReply{Term: n.currentTerm, Success: true}
	}

	if err := n.persister.SaveSnapshot(Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}); err != nil {
		metrics.RaftSnapshotFailuresTotal.Inc()
		metrics.RecordError("raft")
		logEvent(n.id).Info(eventSnapshotFailed, "snapshot_index", args.LastIncludedIndex, "error", err.Error())
		return InstallSnapshotReply{Term: n.currentTerm, Success: false}
	}

	oldEntries := append([]LogEntry(nil), n.log.entries...)
	n.log.installSnapshotBoundary(args.LastIncludedIndex, args.LastIncludedTerm)
	if err := n.persistLocked(); err != nil {
		n.log.entries = oldEntries
		metrics.RaftSnapshotFailuresTotal.Inc()
		metrics.RecordError("raft")
		logEvent(n.id).Info(eventSnapshotFailed, "snapshot_index", args.LastIncludedIndex, "error", err.Error())
		return InstallSnapshotReply{Term: n.currentTerm, Success: false}
	}

	n.snapshotData = args.Data
	n.pendingSnapshot = &Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	if args.LastIncludedIndex > n.commitIndex {
		metrics.RaftEntriesCommittedTotal.Add(args.LastIncludedIndex - n.commitIndex)
		n.commitIndex = args.LastIncludedIndex
		n.notifyCommitLocked()
	}
	metrics.RaftSnapshotsInstalledTotal.Inc()
	metrics.RaftSnapshotBytesTotal.Add(uint64(len(args.Data)))
	logEvent(n.id).Info(eventSnapshotInstalled, "snapshot_index", args.LastIncludedIndex, "snapshot_term", args.LastIncludedTerm, "bytes", len(args.Data))

	return InstallSnapshotReply{Term: n.currentTerm, Success: true}
}
