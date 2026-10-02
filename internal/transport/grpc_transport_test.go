package transport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Kushall-07/forgedb/api/proto/raftpb"
	"github.com/Kushall-07/forgedb/internal/raft"
)

// fakeHandler is a scriptable raft.RPCHandler, standing in for a real
// *raft.Node so these tests exercise exactly the transport boundary
// (encode -> network -> decode -> deliver -> encode -> network -> decode)
// without depending on internal/raft's election/replication timing. Its
// zero value just echoes back a fixed, recognizable reply; tests that
// need different behavior set the handle funcs directly.
type fakeHandler struct {
	mu sync.Mutex

	onRequestVote     func(raft.RequestVoteArgs) raft.RequestVoteReply
	onAppendEntries   func(raft.AppendEntriesArgs) raft.AppendEntriesReply
	onInstallSnapshot func(raft.InstallSnapshotArgs) raft.InstallSnapshotReply

	lastRequestVote     raft.RequestVoteArgs
	lastAppendEntries   raft.AppendEntriesArgs
	lastInstallSnapshot raft.InstallSnapshotArgs
}

func (f *fakeHandler) HandleRequestVote(args raft.RequestVoteArgs) raft.RequestVoteReply {
	f.mu.Lock()
	f.lastRequestVote = args
	f.mu.Unlock()
	if f.onRequestVote != nil {
		return f.onRequestVote(args)
	}
	return raft.RequestVoteReply{Term: args.Term, VoteGranted: true}
}

func (f *fakeHandler) HandleAppendEntries(args raft.AppendEntriesArgs) raft.AppendEntriesReply {
	f.mu.Lock()
	f.lastAppendEntries = args
	f.mu.Unlock()
	if f.onAppendEntries != nil {
		return f.onAppendEntries(args)
	}
	return raft.AppendEntriesReply{Term: args.Term, Success: true}
}

func (f *fakeHandler) HandleInstallSnapshot(args raft.InstallSnapshotArgs) raft.InstallSnapshotReply {
	f.mu.Lock()
	f.lastInstallSnapshot = args
	f.mu.Unlock()
	if f.onInstallSnapshot != nil {
		return f.onInstallSnapshot(args)
	}
	return raft.InstallSnapshotReply{Term: args.Term, Success: true}
}

func (f *fakeHandler) getLastRequestVote() raft.RequestVoteArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastRequestVote
}

func (f *fakeHandler) getLastAppendEntries() raft.AppendEntriesArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastAppendEntries
}

func (f *fakeHandler) getLastInstallSnapshot() raft.InstallSnapshotArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastInstallSnapshot
}

// startServer starts a real gRPC server on an ephemeral loopback port
// forwarding to handler, and returns its address plus a cleanup func.
func startServer(t *testing.T, handler raft.RPCHandler, opts Options) string {
	t.Helper()
	srv := NewServer(handler, opts)
	addr, err := srv.Serve("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(srv.Stop)
	return addr
}

func TestGRPCTransport_RequestVote_RealNetwork(t *testing.T) {
	handler := &fakeHandler{}
	addr := startServer(t, handler, Options{})
	tr := NewGRPCTransport(map[string]string{"peer": addr}, Options{})
	t.Cleanup(func() { tr.Close() })

	args := raft.RequestVoteArgs{Term: 7, CandidateID: "node-1", LastLogIndex: 3, LastLogTerm: 2}
	reply, err := tr.SendRequestVote("peer", args)
	if err != nil {
		t.Fatalf("SendRequestVote: %v", err)
	}
	if reply.Term != 7 || !reply.VoteGranted {
		t.Fatalf("reply = %+v, want Term=7 VoteGranted=true", reply)
	}
	if got := handler.getLastRequestVote(); got != args {
		t.Fatalf("handler received %+v, want %+v (serialization must round-trip every field)", got, args)
	}
}

func TestGRPCTransport_AppendEntries_RealNetwork(t *testing.T) {
	handler := &fakeHandler{}
	addr := startServer(t, handler, Options{})
	tr := NewGRPCTransport(map[string]string{"peer": addr}, Options{})
	t.Cleanup(func() { tr.Close() })

	entries := []raft.LogEntry{
		{Index: 1, Term: 1, Command: raft.Command("put:a")},
		{Index: 2, Term: 1, Command: raft.Command{}}, // empty, non-nil command must survive the round trip
	}
	args := raft.AppendEntriesArgs{Term: 4, LeaderID: "node-2", PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries, LeaderCommit: 1}
	reply, err := tr.SendAppendEntries("peer", args)
	if err != nil {
		t.Fatalf("SendAppendEntries: %v", err)
	}
	if reply.Term != 4 || !reply.Success {
		t.Fatalf("reply = %+v, want Term=4 Success=true", reply)
	}
	got := handler.getLastAppendEntries()
	if len(got.Entries) != 2 || string(got.Entries[0].Command) != "put:a" || len(got.Entries[1].Command) != 0 {
		t.Fatalf("handler received entries = %+v, want 2 entries with command round-tripped exactly", got.Entries)
	}
}

func TestGRPCTransport_AppendEntries_HeartbeatHasNoEntries(t *testing.T) {
	handler := &fakeHandler{}
	addr := startServer(t, handler, Options{})
	tr := NewGRPCTransport(map[string]string{"peer": addr}, Options{})
	t.Cleanup(func() { tr.Close() })

	args := raft.AppendEntriesArgs{Term: 1, LeaderID: "node-1", Entries: nil}
	if _, err := tr.SendAppendEntries("peer", args); err != nil {
		t.Fatalf("SendAppendEntries: %v", err)
	}
	if got := handler.getLastAppendEntries(); len(got.Entries) != 0 {
		t.Fatalf("handler received %d entries for a heartbeat, want 0", len(got.Entries))
	}
}

func TestGRPCTransport_InstallSnapshot_RealNetwork(t *testing.T) {
	handler := &fakeHandler{}
	addr := startServer(t, handler, Options{})
	tr := NewGRPCTransport(map[string]string{"peer": addr}, Options{})
	t.Cleanup(func() { tr.Close() })

	data := make([]byte, 256*1024) // realistic-ish snapshot payload
	for i := range data {
		data[i] = byte(i)
	}
	args := raft.InstallSnapshotArgs{Term: 9, LeaderID: "node-3", LastIncludedIndex: 100, LastIncludedTerm: 8, Data: data}
	reply, err := tr.SendInstallSnapshot("peer", args)
	if err != nil {
		t.Fatalf("SendInstallSnapshot: %v", err)
	}
	if reply.Term != 9 || !reply.Success {
		t.Fatalf("reply = %+v, want Term=9 Success=true", reply)
	}
	got := handler.getLastInstallSnapshot()
	if len(got.Data) != len(data) {
		t.Fatalf("handler received %d snapshot bytes, want %d (no truncation)", len(got.Data), len(data))
	}
	for i := range data {
		if got.Data[i] != data[i] {
			t.Fatalf("snapshot byte %d corrupted: got %d, want %d", i, got.Data[i], data[i])
		}
	}
}

func TestGRPCTransport_LargeSnapshot_WithinConfiguredLimit(t *testing.T) {
	handler := &fakeHandler{}
	opts := Options{MaxMessageBytes: 8 << 20} // 8 MiB, deliberately smaller than DefaultMaxMessageBytes
	addr := startServer(t, handler, opts)
	tr := NewGRPCTransport(map[string]string{"peer": addr}, opts)
	t.Cleanup(func() { tr.Close() })

	data := make([]byte, 4<<20) // 4 MiB: under the 8 MiB limit, over gRPC's unconfigured 4 MiB default
	args := raft.InstallSnapshotArgs{Term: 1, LeaderID: "node-1", LastIncludedIndex: 1, LastIncludedTerm: 1, Data: data}
	if _, err := tr.SendInstallSnapshot("peer", args); err != nil {
		t.Fatalf("SendInstallSnapshot with configured larger message limit: %v", err)
	}
}

func TestGRPCTransport_UnknownPeer_IsPeerUnreachable(t *testing.T) {
	tr := NewGRPCTransport(map[string]string{}, Options{})
	t.Cleanup(func() { tr.Close() })

	_, err := tr.SendRequestVote("ghost", raft.RequestVoteArgs{})
	if err == nil {
		t.Fatal("SendRequestVote to unknown peer: error = nil, want error")
	}
	if !errors.Is(err, raft.ErrPeerUnreachable) {
		t.Fatalf("error = %v, want errors.Is(err, raft.ErrPeerUnreachable)", err)
	}
}

func TestGRPCTransport_ConnectionRefused_IsPeerUnreachable(t *testing.T) {
	// Nothing is listening on this loopback port: every RPC must fail as
	// a normal, bounded transport-level failure, never a panic or a hang.
	tr := NewGRPCTransport(map[string]string{"peer": "127.0.0.1:1"}, Options{RPCTimeout: 2 * time.Second})
	t.Cleanup(func() { tr.Close() })

	_, err := tr.SendAppendEntries("peer", raft.AppendEntriesArgs{})
	if err == nil {
		t.Fatal("SendAppendEntries to a refused connection: error = nil, want error")
	}
	if !errors.Is(err, raft.ErrPeerUnreachable) {
		t.Fatalf("error = %v, want errors.Is(err, raft.ErrPeerUnreachable)", err)
	}
}

func TestGRPCTransport_DeadlineExceeded_IsPeerUnreachable(t *testing.T) {
	handler := &fakeHandler{
		onAppendEntries: func(raft.AppendEntriesArgs) raft.AppendEntriesReply {
			time.Sleep(200 * time.Millisecond)
			return raft.AppendEntriesReply{Success: true}
		},
	}
	addr := startServer(t, handler, Options{})
	tr := NewGRPCTransport(map[string]string{"peer": addr}, Options{RPCTimeout: 20 * time.Millisecond})
	t.Cleanup(func() { tr.Close() })

	start := time.Now()
	_, err := tr.SendAppendEntries("peer", raft.AppendEntriesArgs{})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("SendAppendEntries past its deadline: error = nil, want deadline-exceeded error")
	}
	if !errors.Is(err, raft.ErrPeerUnreachable) {
		t.Fatalf("error = %v, want errors.Is(err, raft.ErrPeerUnreachable)", err)
	}
	if elapsed > time.Second {
		t.Fatalf("SendAppendEntries took %v, want it bounded close to the 20ms RPCTimeout, not hanging", elapsed)
	}
}

func TestGRPCTransport_HigherTermReply_IsDeliveredUnmodified(t *testing.T) {
	// The transport must not interpret or alter term semantics -- it
	// only carries whatever the handler decided, here a lower term than
	// the caller sent, exactly as a real "I am stale, step down" reply
	// would look over the wire.
	handler := &fakeHandler{
		onRequestVote: func(args raft.RequestVoteArgs) raft.RequestVoteReply {
			return raft.RequestVoteReply{Term: args.Term + 100, VoteGranted: false}
		},
	}
	addr := startServer(t, handler, Options{})
	tr := NewGRPCTransport(map[string]string{"peer": addr}, Options{})
	t.Cleanup(func() { tr.Close() })

	reply, err := tr.SendRequestVote("peer", raft.RequestVoteArgs{Term: 1})
	if err != nil {
		t.Fatalf("SendRequestVote: %v", err)
	}
	if reply.Term != 101 || reply.VoteGranted {
		t.Fatalf("reply = %+v, want Term=101 VoteGranted=false", reply)
	}
}

// TestServer_MalformedRequest_DoesNotCrash exercises Server's nil-request
// guard directly. A real gRPC client can never put a literal nil message
// on the wire -- protobuf unmarshaling always produces a valid, if
// zero-valued, message -- so the only way a handler method actually sees
// req == nil is a caller (today, nothing in this codebase; defensively,
// any future direct raftpb.RaftTransportServer implementor or a
// different client library) invoking it directly. The guard exists so
// that path degrades to a clean error instead of a nil-pointer panic
// that would crash the whole process (see section 8/30's "malformed
// requests must not crash the server" requirement).
func TestServer_MalformedRequest_DoesNotCrash(t *testing.T) {
	handler := &fakeHandler{}
	srv := NewServer(handler, Options{})
	ctx := context.Background()

	if _, err := srv.RequestVote(ctx, nil); err == nil {
		t.Fatal("RequestVote(nil): error = nil, want an InvalidArgument error, not a crash")
	}
	if _, err := srv.AppendEntries(ctx, nil); err == nil {
		t.Fatal("AppendEntries(nil): error = nil, want an InvalidArgument error, not a crash")
	}
	if _, err := srv.InstallSnapshot(ctx, nil); err == nil {
		t.Fatal("InstallSnapshot(nil): error = nil, want an InvalidArgument error, not a crash")
	}

	// The server must still be perfectly usable for a real, well-formed
	// request afterward -- the guard must not have left it in a bad
	// state.
	reply, err := srv.RequestVote(ctx, &raftpb.RequestVoteRequest{Term: 3})
	if err != nil {
		t.Fatalf("RequestVote after malformed request: %v", err)
	}
	if reply.GetTerm() != 3 {
		t.Fatalf("reply.Term = %d, want 3", reply.GetTerm())
	}
}

// TestGRPCTransport_EmptyRequest_OverRealNetwork confirms that a genuine,
// well-formed-but-all-zero-values request (the closest a real gRPC
// client can come to "malformed") round-trips over an actual network
// connection without error, since protobuf has no wire representation
// for "absent message" distinct from "all fields at their zero value."
func TestGRPCTransport_EmptyRequest_OverRealNetwork(t *testing.T) {
	handler := &fakeHandler{}
	addr := startServer(t, handler, Options{})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := raftpb.NewRaftTransportClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{})
	if err != nil {
		t.Fatalf("RequestVote(empty): %v", err)
	}
	if reply.GetTerm() != 0 {
		t.Fatalf("reply.Term = %d, want 0", reply.GetTerm())
	}
}

func TestServer_PeerRestart_ClientRecovers(t *testing.T) {
	handler := &fakeHandler{}
	srv := NewServer(handler, Options{})
	addr, err := srv.Serve("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}

	tr := NewGRPCTransport(map[string]string{"peer": addr}, Options{RPCTimeout: time.Second})
	t.Cleanup(func() { tr.Close() })

	if _, err := tr.SendRequestVote("peer", raft.RequestVoteArgs{Term: 1}); err != nil {
		t.Fatalf("initial SendRequestVote: %v", err)
	}

	srv.Stop()
	if _, err := tr.SendRequestVote("peer", raft.RequestVoteArgs{Term: 2}); err == nil {
		t.Fatal("SendRequestVote after peer stopped: error = nil, want error")
	} else if !errors.Is(err, raft.ErrPeerUnreachable) {
		t.Fatalf("error = %v, want errors.Is(err, raft.ErrPeerUnreachable)", err)
	}

	// "Restart" the peer on the same address and confirm the existing
	// GRPCTransport (same *Node, same addrs map, reused connection)
	// recovers without needing to be reconstructed -- a real node
	// restart inside a Docker Compose cluster looks exactly like this
	// from a peer's point of view.
	srv2 := NewServer(handler, Options{})
	t.Cleanup(srv2.Stop)
	if _, err := srv2.Serve(addr); err != nil {
		t.Skipf("could not rebind %s immediately after Stop (OS port reuse timing): %v", addr, err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := tr.SendRequestVote("peer", raft.RequestVoteArgs{Term: 3}); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("transport did not recover after peer restart within 5s, last error: %v", lastErr)
}
