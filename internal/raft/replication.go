package raft

// broadcastAppendEntriesLocked sends one round of AppendEntries to every
// peer: a heartbeat (no entries) if the peer is already fully caught up,
// or replication of whatever log suffix the peer is missing according to
// nextIndex. It is called both from Tick (on the leader's regular
// heartbeat interval) and immediately from becomeLeaderLocked and
// Propose, so new leadership and new entries both start replicating
// without waiting for the next tick. n.mu must be held.
func (n *Node) broadcastAppendEntriesLocked() {
	term := n.currentTerm
	leaderCommit := n.commitIndex

	for _, peer := range n.peers {
		peer := peer
		prevIndex := n.nextIndex[peer] - 1
		prevTerm, _ := n.log.TermAt(prevIndex) // always ok: prevIndex never exceeds the leader's own log
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
}

// sendAppendEntries sends a single AppendEntries RPC to peer and
// processes the reply: on success, advances that peer's matchIndex /
// nextIndex and re-checks whether a new commit index can be established;
// on failure (a PrevLogIndex/PrevLogTerm mismatch), backs nextIndex off
// by one so the next round retries with one more entry of history, the
// simplest form of the standard back-off-and-retry conflict recovery. It
// runs outside n.mu (see trackRPC).
func (n *Node) sendAppendEntries(peer string, term uint64, args AppendEntriesArgs) {
	reply, err := n.transport.SendAppendEntries(peer, args)
	if err != nil {
		return // dropped/unreachable; a later heartbeat or retry will try again
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	// This reply may be stale: we may no longer be leader, or a newer
	// term may have started since this RPC was sent.
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + uint64(len(args.Entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		if newMatch+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = newMatch + 1
		}
		n.maybeAdvanceCommitIndexLocked()
		return
	}

	if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
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
// conflict-resolution / matching-prefix-preservation append (see log.go),
// and commitIndex is advanced to the lesser of the leader's LeaderCommit
// and this node's own new last log index -- never further than what this
// node has actually just accepted into its log.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()

	if termAtPrev, ok := n.log.TermAt(args.PrevLogIndex); !ok || termAtPrev != args.PrevLogTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	n.log.AppendAfter(args.PrevLogIndex, args.Entries)

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		newCommit := args.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
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
func (n *Node) Propose(command Command) (index uint64, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}

	entry := LogEntry{Term: n.currentTerm, Command: append(Command(nil), command...)}
	index = n.log.Append(entry)
	term = n.currentTerm

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
