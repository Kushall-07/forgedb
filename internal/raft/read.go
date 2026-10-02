package raft

import "errors"

// ErrReadBarrierUnavailable is returned by ReadIndex when this node cannot
// currently confirm, via a fresh round of AppendEntries to a majority of
// its peers, that it is still safely entitled to serve a linearizable
// read. This covers both an isolated leader (stuck on the minority side
// of a partition) and a leader that loses leadership (to a higher term
// observed from a peer's reply) while the confirmation round is in
// flight. Either way, the caller must treat the read as unsafe and must
// not use any index it may have locally computed. See
// docs/raft/phase8.5-read-consistency.md.
var ErrReadBarrierUnavailable = errors.New("raft: read barrier unavailable: could not confirm quorum for a linearizable read")

// ReadIndex establishes a linearizable read barrier. It is the mechanism
// docs/raft/phase8.5-read-consistency.md calls the "read index": assuming
// this node is currently leader, ReadIndex captures the node's current
// commitIndex and then confirms -- via one fresh round trip of
// AppendEntries to every peer, reusing exactly the RPC a normal heartbeat
// already sends (see sendAppendEntries) -- that a majority of the cluster,
// including itself, still recognizes this node's leadership at the term
// it captured that index under. Only if that confirmation succeeds does
// ReadIndex return the index; a caller that then waits for its own state
// machine to apply through that index before reading local storage (see
// internal/dbnode.Node.ConsistentGet) is guaranteed never to observe state
// staler than anything already committed at the moment ReadIndex was
// called.
//
// A node's local role alone is never sufficient for this: a former leader
// that is actually partitioned into a minority still has role == Leader
// in its own memory (see TestNode_ReadIndex_FailsWithoutMajority and the
// pre-existing TestNode_PartitionedLeader_CannotFalselyCommit, which
// documents the same fact for commit). ReadIndex is what turns that local
// belief into an actually-confirmed fact, every time it is called -- it
// is never cached or assumed to remain valid from one call to the next.
//
// ReadIndex returns ErrNotLeader if this node is not currently leader:
// Phase 8.5 implements linearizable reads as leader-only. A follower
// never has a safe way to confirm it is not stale without itself
// contacting a majority and effectively becoming a temporary reader on
// the leader's behalf, which this phase deliberately does not build (see
// docs/raft/phase8.5-read-consistency.md's "no stale follower reads"
// section) -- a follower's ConsistentGet simply fails with this error
// rather than ever risking a stale local answer.
//
// It returns ErrReadBarrierUnavailable if, once the confirmation round
// finishes, this node can no longer establish that it is still leader at
// the term it captured the index under: a majority of peers failed to
// confirm it (most commonly because this node is on the minority side of
// a partition), or a peer's reply carried a higher term, which causes
// this node to step down immediately, exactly as any other higher-term
// discovery already does (see sendAppendEntries/HandleAppendEntries). In
// either case the index this call computed must never be used.
//
// Like every other method in this package that performs RPCs
// (broadcastAppendEntriesLocked, startElectionLocked), ReadIndex never
// holds n.mu while a transport call is outstanding: it locks only to
// capture the state needed to build each AppendEntries request and,
// separately, to validate the outcome afterward, following the same
// lock -> capture -> unlock -> RPC -> reacquire -> validate pattern the
// rest of internal/raft already uses.
func (n *Node) ReadIndex() (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}

	term := n.currentTerm
	index := n.commitIndex
	majority := n.majority()

	type pendingCall struct {
		peer string
		args AppendEntriesArgs
	}
	calls := make([]pendingCall, 0, len(n.peers))
	for _, peer := range n.peers {
		prevIndex := n.nextIndex[peer] - 1
		prevTerm, _ := n.log.TermAt(prevIndex) // always ok: prevIndex never exceeds this leader's own log
		calls = append(calls, pendingCall{
			peer: peer,
			args: AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      n.log.Slice(prevIndex + 1),
				LeaderCommit: n.commitIndex,
			},
		})
	}
	n.mu.Unlock()

	// A single-node cluster (zero peers) has nobody to confirm with: the
	// leader's own vote already is the entire quorum, exactly as
	// majority() already computes (majority() == 1 when len(n.peers) ==
	// 0). There is no RPC round to send or wait for.
	if majority <= 1 {
		return index, nil
	}

	acked := 1 // the leader always counts toward its own quorum
	ackCh := make(chan bool, len(calls))
	for _, c := range calls {
		c := c
		n.trackRPC(func() {
			ackCh <- n.sendAppendEntries(c.peer, term, c.args)
		})
	}
	for i := 0; i < len(calls); i++ {
		if <-ackCh {
			acked++
		}
	}

	n.mu.Lock()
	stillCurrent := n.role == Leader && n.currentTerm == term
	n.mu.Unlock()

	if !stillCurrent || acked < majority {
		return 0, ErrReadBarrierUnavailable
	}
	return index, nil
}
