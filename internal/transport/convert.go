package transport

import (
	"github.com/Kushall-07/forgedb/api/proto/raftpb"
	"github.com/Kushall-07/forgedb/internal/raft"
)

// This file holds the pure, allocation-only conversions between
// internal/raft's wire-agnostic RPC structs and their protobuf
// counterparts generated from api/proto/raft.proto. Every field maps
// one-to-one by name; there is no business logic here, only the
// translation Phase 14's real gRPC transport needs on both the client
// side (GRPCTransport, encoding outgoing calls) and the server side
// (Server, decoding incoming calls) -- see grpc_transport.go and
// grpc_server.go.

func requestVoteArgsToProto(a raft.RequestVoteArgs) *raftpb.RequestVoteRequest {
	return &raftpb.RequestVoteRequest{
		Term:         a.Term,
		CandidateId:  a.CandidateID,
		LastLogIndex: a.LastLogIndex,
		LastLogTerm:  a.LastLogTerm,
	}
}

// requestVoteArgsFromProto tolerates a nil request (a malformed or empty
// RPC body) by returning the zero value rather than panicking -- the
// gRPC server must never crash the process on a malformed request (see
// grpc_server.go).
func requestVoteArgsFromProto(r *raftpb.RequestVoteRequest) raft.RequestVoteArgs {
	if r == nil {
		return raft.RequestVoteArgs{}
	}
	return raft.RequestVoteArgs{
		Term:         r.Term,
		CandidateID:  r.CandidateId,
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
	}
}

func requestVoteReplyToProto(r raft.RequestVoteReply) *raftpb.RequestVoteReply {
	return &raftpb.RequestVoteReply{Term: r.Term, VoteGranted: r.VoteGranted}
}

func requestVoteReplyFromProto(r *raftpb.RequestVoteReply) raft.RequestVoteReply {
	if r == nil {
		return raft.RequestVoteReply{}
	}
	return raft.RequestVoteReply{Term: r.Term, VoteGranted: r.VoteGranted}
}

func logEntriesToProto(entries []raft.LogEntry) []*raftpb.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*raftpb.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = &raftpb.LogEntry{Index: e.Index, Term: e.Term, Command: []byte(e.Command)}
	}
	return out
}

func logEntriesFromProto(entries []*raftpb.LogEntry) []raft.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raft.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = raft.LogEntry{Index: e.Index, Term: e.Term, Command: raft.Command(e.Command)}
	}
	return out
}

func appendEntriesArgsToProto(a raft.AppendEntriesArgs) *raftpb.AppendEntriesRequest {
	return &raftpb.AppendEntriesRequest{
		Term:         a.Term,
		LeaderId:     a.LeaderID,
		PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm:  a.PrevLogTerm,
		Entries:      logEntriesToProto(a.Entries),
		LeaderCommit: a.LeaderCommit,
	}
}

func appendEntriesArgsFromProto(r *raftpb.AppendEntriesRequest) raft.AppendEntriesArgs {
	if r == nil {
		return raft.AppendEntriesArgs{}
	}
	return raft.AppendEntriesArgs{
		Term:         r.Term,
		LeaderID:     r.LeaderId,
		PrevLogIndex: r.PrevLogIndex,
		PrevLogTerm:  r.PrevLogTerm,
		Entries:      logEntriesFromProto(r.Entries),
		LeaderCommit: r.LeaderCommit,
	}
}

func appendEntriesReplyToProto(r raft.AppendEntriesReply) *raftpb.AppendEntriesReply {
	return &raftpb.AppendEntriesReply{Term: r.Term, Success: r.Success}
}

func appendEntriesReplyFromProto(r *raftpb.AppendEntriesReply) raft.AppendEntriesReply {
	if r == nil {
		return raft.AppendEntriesReply{}
	}
	return raft.AppendEntriesReply{Term: r.Term, Success: r.Success}
}

func installSnapshotArgsToProto(a raft.InstallSnapshotArgs) *raftpb.InstallSnapshotRequest {
	return &raftpb.InstallSnapshotRequest{
		Term:              a.Term,
		LeaderId:          a.LeaderID,
		LastIncludedIndex: a.LastIncludedIndex,
		LastIncludedTerm:  a.LastIncludedTerm,
		Data:              a.Data,
	}
}

func installSnapshotArgsFromProto(r *raftpb.InstallSnapshotRequest) raft.InstallSnapshotArgs {
	if r == nil {
		return raft.InstallSnapshotArgs{}
	}
	return raft.InstallSnapshotArgs{
		Term:              r.Term,
		LeaderID:          r.LeaderId,
		LastIncludedIndex: r.LastIncludedIndex,
		LastIncludedTerm:  r.LastIncludedTerm,
		Data:              r.Data,
	}
}

func installSnapshotReplyToProto(r raft.InstallSnapshotReply) *raftpb.InstallSnapshotReply {
	return &raftpb.InstallSnapshotReply{Term: r.Term, Success: r.Success}
}

func installSnapshotReplyFromProto(r *raftpb.InstallSnapshotReply) raft.InstallSnapshotReply {
	if r == nil {
		return raft.InstallSnapshotReply{}
	}
	return raft.InstallSnapshotReply{Term: r.Term, Success: r.Success}
}
