package raft

// startElectionLocked begins a new election: the node increments its
// term, transitions to Candidate, votes for itself, and requests votes
// from every peer concurrently. If the node's own vote is already enough
// to form a majority (only possible in a single-node cluster), it becomes
// leader immediately without sending anything. n.mu must be held; it is
// called from Tick when a Follower or Candidate's election timeout
// elapses.
func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.votesReceived = map[string]bool{n.id: true}
	n.resetElectionTimerLocked()

	term := n.currentTerm
	lastIndex := n.log.LastIndex()
	lastTerm := n.log.LastTerm()

	if len(n.votesReceived) >= n.majority() {
		n.becomeLeaderLocked()
		return
	}

	for _, peer := range n.peers {
		peer := peer
		n.trackRPC(func() {
			n.sendRequestVote(peer, term, lastIndex, lastTerm)
		})
	}
}

// sendRequestVote sends a single RequestVote RPC to peer for the given
// term/log state and processes the reply. It runs outside n.mu (see
// trackRPC) and only re-acquires it once the (possibly slow, in a real
// transport) network call has returned, so a node never blocks its own
// lock on a peer's response.
func (n *Node) sendRequestVote(peer string, term, lastIndex, lastTerm uint64) {
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	reply, err := n.transport.SendRequestVote(peer, args)
	if err != nil {
		return // dropped/unreachable; the election simply proceeds without this vote
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	// The election this reply belongs to may already be over (we lost,
	// won, or a new election has since started) by the time it arrives.
	if n.role != Candidate || n.currentTerm != term {
		return
	}
	if !reply.VoteGranted {
		return
	}
	n.votesReceived[peer] = true
	if len(n.votesReceived) >= n.majority() {
		n.becomeLeaderLocked()
	}
}

// becomeLeaderLocked transitions a winning Candidate to Leader: it
// initializes per-follower replication state (nextIndex optimistically
// set to just past the end of this node's own log, matchIndex to 0) and
// immediately broadcasts a round of AppendEntries, both to establish
// authority over the cluster without waiting for the next heartbeat tick
// and to begin discovering how far each follower's log actually matches.
// n.mu must be held.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	n.votesReceived = nil

	lastIndex := n.log.LastIndex()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = lastIndex + 1
		n.matchIndex[p] = 0
	}

	n.heartbeatElapsed = 0
	n.broadcastAppendEntriesLocked()
}

// HandleRequestVote implements the RequestVote RPC (RPCHandler). It
// enforces every Raft voting rule: stale terms are rejected outright, a
// newer term causes this node to step down to Follower first, and even
// then a vote is granted only if this node has not already voted for a
// different candidate this term and the candidate's log is at least as
// up-to-date as this node's own (logIsUpToDate) -- log length alone is
// never sufficient. Granting a vote resets the election timer, since a
// live candidate worth voting for is reason enough to defer this node's
// own election.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	upToDate := logIsUpToDate(args.LastLogIndex, args.LastLogTerm, n.log.LastIndex(), n.log.LastTerm())

	if !canVote || !upToDate {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	n.votedFor = args.CandidateID
	n.resetElectionTimerLocked()
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
}
