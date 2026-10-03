package raft

import (
	"fmt"
	"hash/crc32"

	"github.com/Kushall-07/forgedb/internal/metrics"
)

// broadcastAppendEntriesLocked sends one round of AppendEntries to every
// peer: a heartbeat (no entries) if the peer is already fully caught up,
// or replication of whatever log suffix the peer is missing according to
// nextIndex. It is called both from Tick (on the leader's regular
// heartbeat interval) and immediately from becomeLeaderLocked and
// Propose, so new leadership and new entries both start replicating
// without waiting for the next tick. n.mu must be held.
//
// With zero peers, the loop below does nothing -- there is no one to
// replicate to -- so this also re-checks the commit index directly: for a
// single-node cluster, the leader's own log entry already constitutes a
// majority (majority() == 1), and nothing else would ever call
// maybeAdvanceCommitIndexLocked, since that otherwise only happens from a
// peer's AppendEntries/InstallSnapshot reply.
func (n *Node) broadcastAppendEntriesLocked() {
	term := n.currentTerm
	leaderCommit := n.commitIndex
	snapshotBoundary := n.log.entries[0].Index

	for _, peer := range n.peers {
		peer := peer

		// A peer whose nextIndex has fallen at or behind this leader's own
		// compacted log boundary can never be caught up by an ordinary
		// AppendEntries: the entries it would need (anything at or before
		// snapshotBoundary) no longer exist in this leader's log at all --
		// see docs/raft/phase9-snapshots.md. InstallSnapshot is the only
		// correct way to bring it forward.
		if n.nextIndex[peer] <= snapshotBoundary {
			snap := Snapshot{LastIncludedIndex: snapshotBoundary, LastIncludedTerm: n.log.entries[0].Term, Data: n.snapshotData}
			n.trackRPC(func() {
				n.sendInstallSnapshot(peer, term, snap)
			})
			continue
		}

		prevIndex := n.nextIndex[peer] - 1
		prevTerm, _ := n.log.TermAt(prevIndex) // always ok: prevIndex is at or above snapshotBoundary, and never exceeds the leader's own log
		entries := n.log.Slice(prevIndex + 1)

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		n.trackRPC(func() {
			n.sendAppendEntries(peer, term, args)
		})
	}

	n.maybeAdvanceCommitIndexLocked()
}

// sendInstallSnapshot sends snap to peer as a bounded sequence of
// InstallSnapshot chunks (see docs/deployment/phase19-chunked-snapshot-transfer.md),
// each one a separate RPC sent only once the previous chunk's reply has
// been received -- this is what gives the receiver a well-defined,
// strictly increasing offset to validate against even over a transport
// with no ordering guarantee of its own. It runs outside n.mu (see
// trackRPC), taking n.mu only briefly around each reply.
//
// Any failure anywhere in the sequence -- a transport error, a
// higher-term reply (which also triggers this node's own step-down, as
// in sendAppendEntries), this node no longer being leader at term, or an
// explicit Success=false (most commonly the peer's own persistence
// failing transiently, or this exact attempt racing a newer one -- see
// HandleInstallSnapshot) -- aborts the whole attempt immediately,
// sending no further chunks. Unlike sendAppendEntries, there is no
// partial progress to preserve or back off from: the next heartbeat
// round that still finds this peer behind the snapshot boundary simply
// starts an entirely new attempt from chunk 0, exactly as a single
// failed non-chunked InstallSnapshot always retried from scratch.
//
// Only once the Final chunk's reply reports Success does this method
// advance peer's matchIndex to at least snap.LastIncludedIndex and its
// nextIndex to exactly snap.LastIncludedIndex+1 (per Raft's
// InstallSnapshot handling), then re-check whether a new commit index can
// be established, exactly as a successful AppendEntries reply does.
func (n *Node) sendInstallSnapshot(peer string, term uint64, snap Snapshot) {
	chunkSize := n.snapshotChunkSize
	total := uint64(len(snap.Data))

	for offset := uint64(0); ; {
		end := offset + uint64(chunkSize)
		if end > total {
			end = total
		}
		chunk := snap.Data[offset:end]
		final := end == total

		args := InstallSnapshotArgs{
			Term:              term,
			LeaderID:          n.id,
			LastIncludedIndex: snap.LastIncludedIndex,
			LastIncludedTerm:  snap.LastIncludedTerm,
			Data:              chunk,
			Chunked:           true,
			Offset:            offset,
			Final:             final,
			TotalSize:         total,
			Checksum:          crc32.Checksum(chunk, crcTable),
		}
		reply, err := n.transport.SendInstallSnapshot(peer, args)
		if err != nil {
			return // dropped/unreachable; a later heartbeat retries from scratch
		}

		n.mu.Lock()
		if reply.Term > n.currentTerm {
			_ = n.becomeFollowerLocked(reply.Term)
			n.mu.Unlock()
			return
		}
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		if !reply.Success {
			n.mu.Unlock()
			return
		}
		if final {
			if snap.LastIncludedIndex > n.matchIndex[peer] {
				n.matchIndex[peer] = snap.LastIncludedIndex
			}
			if snap.LastIncludedIndex+1 > n.nextIndex[peer] {
				n.nextIndex[peer] = snap.LastIncludedIndex + 1
			}
			n.maybeAdvanceCommitIndexLocked()
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		offset = end
	}
}

// sendAppendEntries sends a single AppendEntries RPC to peer and
// processes the reply: on success, advances that peer's matchIndex /
// nextIndex and re-checks whether a new commit index can be established;
// on failure (a PrevLogIndex/PrevLogTerm mismatch), backs nextIndex off
// by one so the next round retries with one more entry of history, the
// simplest form of the standard back-off-and-retry conflict recovery. It
// runs outside n.mu (see trackRPC).
//
// sendAppendEntries reports whether this round trip counts as peer
// acknowledging this node's leadership at term: true if the RPC was
// delivered, the reply carried no higher term, and this node is still
// leader at term by the time the reply is processed; false otherwise
// (delivery failure, a higher-term reply -- which also triggers this
// node's own step-down below -- or a stale round whose term or leadership
// no longer matches). This is exactly, and only, the signal ReadIndex
// needs to confirm a quorum for a linearizable read (see read.go); it is
// deliberately independent of reply.Success, since a log-matching
// mismatch (Success=false) still confirms the peer recognizes this node
// as leader at term, it just also needs more log history replicated.
// Ordinary heartbeat/replication callers (broadcastAppendEntriesLocked)
// simply ignore the return value, exactly as before this method had one.
func (n *Node) sendAppendEntries(peer string, term uint64, args AppendEntriesArgs) bool {
	metrics.RaftAppendEntriesSentTotal.Inc()
	reply, err := n.transport.SendAppendEntries(peer, args)
	if err != nil {
		metrics.RaftAppendEntriesFailedTotal.Inc()
		return false // dropped/unreachable; a later heartbeat or retry will try again
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		// Best-effort, as in sendRequestVote: becomeFollowerLocked
		// already leaves state untouched on a persistence failure.
		_ = n.becomeFollowerLocked(reply.Term)
		metrics.RaftAppendEntriesFailedTotal.Inc()
		return false
	}
	// This reply may be stale: we may no longer be leader, or a newer
	// term may have started since this RPC was sent.
	if n.role != Leader || n.currentTerm != term {
		metrics.RaftAppendEntriesFailedTotal.Inc()
		return false
	}

	if reply.Success {
		metrics.RaftAppendEntriesSuccessTotal.Inc()
		newMatch := args.PrevLogIndex + uint64(len(args.Entries))
		if newMatch > n.matchIndex[peer] {
			metrics.RaftEntriesReplicatedTotal.Add(newMatch - n.matchIndex[peer])
			n.matchIndex[peer] = newMatch
		}
		if newMatch+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = newMatch + 1
		}
		n.maybeAdvanceCommitIndexLocked()
		return true
	}

	metrics.RaftAppendEntriesFailedTotal.Inc()
	logEvent(n.id).Info(eventAppendFailure, "peer_id", peer, "term", term)
	if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
	return true
}

// maybeAdvanceCommitIndexLocked implements Raft's commit rule: the leader
// may advance commitIndex to N if a majority of the cluster (including
// itself) has replicated an entry at index N, *and* that entry was
// created in the leader's current term. The current-term restriction
// (Raft §5.4.2) is essential for safety -- without it, a leader could
// commit an entry from an earlier term based on a majority match that a
// future leader would be entitled to overwrite, since only a leader's own
// term entries reaching a majority guarantees every future leader's log
// also contains it (that future leader must itself have gotten its vote
// from a node holding this entry). n.mu must be held.
func (n *Node) maybeAdvanceCommitIndexLocked() {
	lastIndex := n.log.LastIndex()
	for N := lastIndex; N > n.commitIndex; N-- {
		term, ok := n.log.TermAt(N)
		if !ok || term != n.currentTerm {
			continue
		}
		count := 1 // the leader itself
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}
		if count >= n.majority() {
			metrics.RaftEntriesCommittedTotal.Add(N - n.commitIndex)
			logEvent(n.id).Debug(eventCommitAdvance, "commit_index", N)
			n.commitIndex = N
			n.notifyCommitLocked()
			return
		}
	}
}

// notifyCommitLocked performs a non-blocking send on commitCh so a
// listener (see CommitCh) wakes up to re-check CommitIndex /
// CommittedEntries. It never blocks and never requires a listener to be
// present -- CommitIndex is always the authoritative source of truth,
// this is purely a wake-up hint. n.mu must be held.
func (n *Node) notifyCommitLocked() {
	select {
	case n.commitCh <- struct{}{}:
	default:
	}
}

// HandleAppendEntries implements the AppendEntries RPC (RPCHandler),
// covering both heartbeats (empty Entries) and log replication. A stale
// term is rejected outright; a newer or equal term causes this node to
// accept args.LeaderID as the current leader and step down to Follower if
// it was a Candidate (or, in the impossible-under-correct-operation case
// of a stale Leader, itself) -- this is the mechanism behind Raft's
// "leader authority" invariant: a node can never keep acting as leader
// once it has seen evidence, via any RPC, of a current term it isn't the
// leader for.
//
// The consistency check (PrevLogIndex/PrevLogTerm must match this node's
// own log) is enforced before any mutation: on mismatch, the RPC is
// rejected with Success=false and the log is left untouched, so the
// leader's next retry (with a lower PrevLogIndex) is the only thing that
// can ever change it. On success, Log.AppendAfter performs the
// conflict-resolution / matching-prefix-preservation append (see log.go);
// if that actually changed the log, the new log is persisted before this
// node replies Success=true, so a leader that has heard this entry was
// accepted can rely on this follower not having silently forgotten it in
// a crash. commitIndex is then advanced to the lesser of the leader's
// LeaderCommit and this node's own new last log index -- never further
// than what this node has actually just accepted into its log.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		if err := n.becomeFollowerLocked(args.Term); err != nil {
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	n.role = Follower
	if args.LeaderID != "" && args.LeaderID != n.leaderID {
		metrics.RaftLeaderChangesTotal.Inc()
		logEvent(n.id).Info(eventLeaderChanged, "term", n.currentTerm, "leader_id", args.LeaderID)
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	if termAtPrev, ok := n.log.TermAt(args.PrevLogIndex); !ok || termAtPrev != args.PrevLogTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	oldEntries := append([]LogEntry(nil), n.log.entries...)
	if n.log.AppendAfter(args.PrevLogIndex, args.Entries) {
		if err := n.persistLocked(); err != nil {
			n.log.entries = oldEntries
			metrics.RecordError("raft")
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		newCommit := args.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			metrics.RaftEntriesCommittedTotal.Add(newCommit - n.commitIndex)
			n.commitIndex = newCommit
			n.notifyCommitLocked()
		}
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// Propose appends command to the leader's log as a new entry at the
// current term and begins replicating it to every peer immediately,
// without waiting for the next heartbeat tick. It returns the index and
// term the entry was assigned, which a caller can later check against
// CommitIndex / CommittedEntries to learn whether (and when) the entry
// was actually committed -- Propose itself does not wait for that; an
// entry appended here can still be lost (e.g. if this node loses
// leadership before replicating it to a majority) rather than committed.
//
// Propose returns ErrNotLeader, appending nothing, if this node is not
// currently the leader. Phase 5 does not implement client redirection to
// the real leader; that is left to a future phase.
//
// The new entry is persisted before Propose broadcasts it to any peer or
// returns to the caller: an entry this node cannot durably remember
// having appended must not be treated as even locally recorded, let alone
// replicated. If persistence fails, the append is rolled back and Propose
// returns that error instead of an index/term -- the caller sees the
// proposal as having not happened at all, and may retry once persistence
// recovers.
func (n *Node) Propose(command Command) (index uint64, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}

	entry := LogEntry{Term: n.currentTerm, Command: append(Command(nil), command...)}
	oldLen := len(n.log.entries)
	index = n.log.Append(entry)
	term = n.currentTerm

	if perr := n.persistLocked(); perr != nil {
		n.log.entries = n.log.entries[:oldLen]
		metrics.RecordError("raft")
		return 0, 0, fmt.Errorf("raft: persist proposed entry: %w", perr)
	}

	n.broadcastAppendEntriesLocked()
	return index, term, nil
}

// CommitIndex returns the highest log index the node currently knows to
// be committed.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// CommittedEntries returns the committed log entries in the inclusive
// range [from, CommitIndex()], or nil if from is past the current commit
// index. This is the boundary a future state machine will poll (or, via
// CommitCh, wait on) to discover newly committed commands in order; Raft
// itself never applies them to anything -- see the package doc comment.
// Passing from=0 is equivalent to from=1 (there is no entry at index 0;
// see Log).
func (n *Node) CommittedEntries(from uint64) []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	if from == 0 {
		from = 1
	}
	if from > n.commitIndex {
		return nil
	}
	return n.log.Range(from, n.commitIndex)
}

// CommitCh returns a channel that receives a value every time
// CommitIndex advances. It is a wake-up hint, not a queue of committed
// entries: a send can be coalesced or missed by a slow reader, so a
// listener should always re-read CommitIndex / CommittedEntries after
// waking, not rely on one channel value per commit.
func (n *Node) CommitCh() <-chan struct{} {
	return n.commitCh
}

// LastLogIndex returns the index of the last entry in the node's own
// log.
func (n *Node) LastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.LastIndex()
}
