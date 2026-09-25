package raft

// Command is an opaque, Raft-agnostic payload proposed by a client. Raft
// treats it purely as bytes to append, replicate, and eventually mark
// committed; it never interprets, validates, or acts on its contents. A
// future state machine (Phase 7) is what will understand what a Command
// actually means for the database -- Raft only guarantees the ordered,
// agreed-upon sequence of Commands that state machine will one day apply.
type Command []byte

// LogEntry is one entry in a Raft log: a Term (the leader's term when the
// entry was created, used for the log-matching and up-to-date checks) and
// an opaque Command. Index is the entry's position in the log; it is
// recorded on the entry itself so a copy of a LogEntry (as returned by
// Log.Slice, or carried in an AppendEntries RPC) still identifies its own
// position without needing the log it came from.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}

// RequestVoteArgs is the RequestVote RPC request. CandidateID identifies
// the caller; LastLogIndex/LastLogTerm describe the end of the
// candidate's log, used by the voter to enforce the up-to-date rule (see
// logIsUpToDate in log.go).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC response. Term lets the
// candidate discover it is stale even when the vote is denied.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC request. Entries is empty
// for a heartbeat. PrevLogIndex/PrevLogTerm identify the log entry the
// follower must already have immediately before Entries, per the
// log-matching property; LeaderCommit lets the follower advance its own
// commitIndex.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC response. Success is false
// whenever the term check fails or the PrevLogIndex/PrevLogTerm
// consistency check fails; Term lets the leader discover it is stale even
// when the RPC is rejected.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}
