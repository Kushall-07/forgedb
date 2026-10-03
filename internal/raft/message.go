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

// InstallSnapshotArgs is the InstallSnapshot RPC request: sent by a
// leader to a follower whose nextIndex has fallen at or behind the
// leader's own compacted log boundary, so ordinary AppendEntries can no
// longer bring it up to date (the leader no longer has the entries that
// would require -- see docs/raft/phase9-snapshots.md). Data is the
// state machine's entire opaque serialized state through
// LastIncludedIndex.
//
// Phase 9 sent the whole of Data in a single RPC. Phase 19
// (docs/deployment/phase19-chunked-snapshot-transfer.md) adds the
// ability to send it as a bounded sequence of chunks instead, via the
// fields below -- modeled on the Raft paper's own offset/done
// InstallSnapshot fields. Chunked is false (the zero value) for every
// pre-Phase-19 caller, in which case Data is, exactly as before, the
// complete payload and every field below is ignored; HandleInstallSnapshot
// preserves this legacy behavior unchanged.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte

	// Chunked, when true, means Data is one bounded chunk of this
	// snapshot's full payload rather than the complete payload, and the
	// fields below describe this chunk's place in the larger transfer.
	Chunked bool

	// Offset is this chunk's byte position within the full snapshot
	// payload. The first chunk of every transfer -- including the first
	// attempt and every retry of a transfer that never completed -- must
	// have Offset 0; receiving one always (re)starts a fresh transfer for
	// this LastIncludedIndex/LastIncludedTerm, discarding any previous
	// in-progress transfer (see HandleInstallSnapshot).
	Offset uint64

	// Final reports whether this is the last chunk of the transfer: once
	// it arrives and the receiver's reconstructed payload is exactly
	// TotalSize bytes, the complete snapshot is validated and installed
	// exactly as the non-chunked path always has.
	Final bool

	// TotalSize is the full snapshot payload's total length in bytes,
	// carried on every chunk so the receiver can validate completion
	// (Final must coincide with the reconstructed payload reaching
	// exactly TotalSize, never more or less) without trusting Final
	// alone.
	TotalSize uint64

	// Checksum is a CRC-32C checksum (the same algorithm and table
	// internal/raft/format.go already uses for on-disk persistence) over
	// this chunk's own Data, letting the receiver detect a corrupted
	// chunk before it is ever appended to the in-progress reconstruction
	// buffer.
	Checksum uint32
}

// InstallSnapshotReply is the InstallSnapshot RPC response. Success is
// false whenever the term check fails or the snapshot could not be
// durably persisted; Term lets the leader discover it is stale even when
// the RPC is rejected.
type InstallSnapshotReply struct {
	Term    uint64
	Success bool
}
