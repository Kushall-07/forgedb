package transport

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Kushall-07/forgedb/api/proto/raftpb"
	"github.com/Kushall-07/forgedb/internal/raft"
)

// Server exposes a raft.RPCHandler (in production, a dbnode.Node's
// *raft.Node, via Node.Raft()) over real gRPC, so that another process's
// GRPCTransport can reach it. It holds no Raft state of its own and
// makes no consensus decisions -- every incoming RPC is decoded and
// forwarded straight to the handler, exactly as InMemoryTransport's
// direct function call already does for in-process tests; the handler's
// own locking (internal/raft's n.mu) is what makes each call safe, and
// it is held only for the duration of the local, in-memory state
// transition -- never while this package is doing network I/O.
type Server struct {
	raftpb.UnimplementedRaftTransportServer

	handler raft.RPCHandler
	grpcSrv *grpc.Server
}

// NewServer returns a Server forwarding every RPC to handler. It does
// not start listening -- see Serve.
func NewServer(handler raft.RPCHandler, opts Options) *Server {
	opts = opts.withDefaults()
	s := &Server{handler: handler}
	s.grpcSrv = grpc.NewServer(
		grpc.MaxRecvMsgSize(opts.MaxMessageBytes),
		grpc.MaxSendMsgSize(opts.MaxMessageBytes),
	)
	raftpb.RegisterRaftTransportServer(s.grpcSrv, s)
	return s
}

// RequestVote implements raftpb.RaftTransportServer. A nil request (a
// malformed call) is decoded to the zero-valued raft.RequestVoteArgs
// rather than panicking -- see requestVoteArgsFromProto -- so a
// malformed or adversarial RPC can never crash this process; it is
// simply handled as a (very unconvincing) vote request and answered
// normally by internal/raft's own logic.
func (s *Server) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "transport: nil RequestVote request")
	}
	reply := s.handler.HandleRequestVote(requestVoteArgsFromProto(req))
	return requestVoteReplyToProto(reply), nil
}

// AppendEntries implements raftpb.RaftTransportServer.
func (s *Server) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "transport: nil AppendEntries request")
	}
	reply := s.handler.HandleAppendEntries(appendEntriesArgsFromProto(req))
	return appendEntriesReplyToProto(reply), nil
}

// InstallSnapshot implements raftpb.RaftTransportServer.
func (s *Server) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "transport: nil InstallSnapshot request")
	}
	reply := s.handler.HandleInstallSnapshot(installSnapshotArgsFromProto(req))
	return installSnapshotReplyToProto(reply), nil
}

// Serve binds addr (e.g. "0.0.0.0:9091", or ":0" for an ephemeral port,
// which tests use) and begins accepting RPCs on a background goroutine.
// It returns the actual bound address.
func (s *Server) Serve(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	go s.grpcSrv.Serve(ln)
	return ln.Addr().String(), nil
}

// Stop gracefully stops the server: it waits for in-flight RPCs to
// finish and refuses new ones, then returns. It is safe to call more
// than once.
func (s *Server) Stop() {
	s.grpcSrv.GracefulStop()
}
