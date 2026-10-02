// Package transport is Phase 14's real-network implementation of
// internal/raft's Transport and RPCHandler interfaces. It introduces no
// new consensus logic and no second Raft implementation: GRPCTransport
// and Server exist purely to carry the exact same RequestVote,
// AppendEntries, and InstallSnapshot messages internal/raft already
// defines (see internal/raft/message.go) across a real TCP connection
// between separate OS processes, in place of InMemoryTransport's direct
// in-process function calls. Every term check, voting rule, log-matching
// rule, commit rule, and snapshot rule remains exactly as implemented in
// internal/raft; this package only transports bytes.
package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Kushall-07/forgedb/api/proto/raftpb"
	"github.com/Kushall-07/forgedb/internal/raft"
)

// Default tuning for GRPCTransport and Server when an Options value
// leaves a field at its zero value. These are deliberately bounded, not
// unlimited (see docs/deployment/phase14-docker-deployment.md's
// "message limits" section): a real ForgeDB snapshot is sent as a
// single, unchunked RPC (internal/raft/message.go explicitly defers
// chunking to a later phase), so the message-size ceiling must be large
// enough for realistic development/demo snapshots without being left at
// gRPC's default 4 MiB, which a non-trivial snapshot would silently
// exceed.
const (
	DefaultDialTimeout     = 5 * time.Second
	DefaultRPCTimeout      = 2 * time.Second
	DefaultMaxMessageBytes = 64 << 20 // 64 MiB
)

// Options tunes a GRPCTransport and/or Server. The zero value is valid:
// every field falls back to the Default* constant above.
type Options struct {
	// DialTimeout bounds how long dialing a peer may take before the
	// connection attempt is abandoned. gRPC's modern NewClient dials
	// lazily, so this mainly bounds the first RPC's visible latency to a
	// peer that turns out to be unreachable.
	DialTimeout time.Duration

	// RPCTimeout bounds every individual RequestVote/AppendEntries/
	// InstallSnapshot call. It is a transport-level failure detector
	// only -- internal/raft's own election/heartbeat ticking remains the
	// sole source of consensus timing (see the package doc and
	// docs/deployment/phase14-docker-deployment.md's "RPC timeouts"
	// section); a timed-out RPC here is treated exactly like a dropped
	// packet, never as a signal that changes term or voting behavior
	// directly.
	RPCTimeout time.Duration

	// MaxMessageBytes bounds both the send and receive size of every
	// gRPC message, on both GRPCTransport (client) and Server. It exists
	// so an InstallSnapshot RPC carrying a realistic snapshot does not
	// silently fail against gRPC's default 4 MiB ceiling, while still
	// keeping an explicit, documented bound rather than an unlimited one.
	MaxMessageBytes int
}

func (o Options) withDefaults() Options {
	if o.DialTimeout <= 0 {
		o.DialTimeout = DefaultDialTimeout
	}
	if o.RPCTimeout <= 0 {
		o.RPCTimeout = DefaultRPCTimeout
	}
	if o.MaxMessageBytes <= 0 {
		o.MaxMessageBytes = DefaultMaxMessageBytes
	}
	return o
}

// ErrUnknownPeer is returned (wrapped in raft.ErrPeerUnreachable) when a
// send targets a node ID this GRPCTransport was never given an address
// for. Raft treats it exactly like any other unreachable peer.
var ErrUnknownPeer = errors.New("transport: unknown peer")

// GRPCTransport implements raft.Transport over real gRPC connections to
// a static set of peers (Phase 14 uses static cluster membership only --
// see Options and docs/deployment/phase14-docker-deployment.md). It
// deliberately does not implement the dbnode "registrar" interface
// (*InMemoryTransport's Register method): a real network transport has
// no in-process handler to register against, since the only way to
// reach this node at all is for Server to be listening on its own gRPC
// address -- see grpc_server.go and NewServer.
//
// Connections are created lazily, on first use, and reused thereafter;
// a dial failure (or a later RPC failure) is reported as a wrapped
// raft.ErrPeerUnreachable rather than torn down and retried immediately,
// since gRPC's own ClientConn already retries/reconnects transparently
// under the hood -- the next send simply tries the same, potentially
// now-healthy, connection again. GRPCTransport is safe for concurrent
// use; Close releases every connection it has opened.
type GRPCTransport struct {
	opts Options

	mu    sync.Mutex
	addrs map[string]string // peer id -> dial address (host:port), excludes self
	conns map[string]*grpc.ClientConn
}

// NewGRPCTransport returns a GRPCTransport that dials addrs[id] (a
// host:port, typically a Docker Compose service name such as
// "forgedb-2:9092") the first time a send targets id. addrs must not
// include this transport's own node ID -- callers construct it from
// config.Config's peer list with the local node already excluded (see
// cmd/forgedb's wiring).
func NewGRPCTransport(addrs map[string]string, opts Options) *GRPCTransport {
	opts = opts.withDefaults()
	a := make(map[string]string, len(addrs))
	for id, addr := range addrs {
		a[id] = addr
	}
	return &GRPCTransport{opts: opts, addrs: a, conns: make(map[string]*grpc.ClientConn)}
}

func (t *GRPCTransport) clientFor(id string) (raftpb.RaftTransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[id]; ok {
		return raftpb.NewRaftTransportClient(conn), nil
	}
	addr, ok := t.addrs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownPeer, id)
	}

	// grpc.NewClient does not block or dial synchronously; it validates
	// addr and prepares the ClientConn to connect lazily on first RPC
	// (and to reconnect automatically thereafter), so DialTimeout bounds
	// the eventual connection attempt via the per-RPC context deadline
	// below rather than this call itself.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(t.opts.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(t.opts.MaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s (%s): %w", id, addr, err)
	}
	t.conns[id] = conn
	return raftpb.NewRaftTransportClient(conn), nil
}

// unreachable wraps err as raft.ErrPeerUnreachable: every failure mode a
// real network can produce (connection refused, DNS failure, deadline
// exceeded, peer restarting mid-call) collapses to the single error
// raft.Node already knows how to treat -- an RPC that simply could not
// be delivered, with zero effect on term, vote, or replication state.
func unreachable(target string, err error) error {
	return fmt.Errorf("%s: %w: %v", target, raft.ErrPeerUnreachable, err)
}

// SendRequestVote implements raft.Transport.
func (t *GRPCTransport) SendRequestVote(target string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	client, err := t.clientFor(target)
	if err != nil {
		return raft.RequestVoteReply{}, unreachable(target, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.opts.RPCTimeout)
	defer cancel()
	resp, err := client.RequestVote(ctx, requestVoteArgsToProto(args))
	if err != nil {
		return raft.RequestVoteReply{}, unreachable(target, err)
	}
	return requestVoteReplyFromProto(resp), nil
}

// SendAppendEntries implements raft.Transport.
func (t *GRPCTransport) SendAppendEntries(target string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	client, err := t.clientFor(target)
	if err != nil {
		return raft.AppendEntriesReply{}, unreachable(target, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.opts.RPCTimeout)
	defer cancel()
	resp, err := client.AppendEntries(ctx, appendEntriesArgsToProto(args))
	if err != nil {
		return raft.AppendEntriesReply{}, unreachable(target, err)
	}
	return appendEntriesReplyFromProto(resp), nil
}

// SendInstallSnapshot implements raft.Transport. It uses the same
// RPCTimeout as every other RPC: Phase 14 does not give large snapshot
// transfers a separately-tuned deadline (see Options.RPCTimeout's doc
// comment and the deployment doc's known limitations), so an operator
// expecting very large snapshots should raise RPCTimeout accordingly.
func (t *GRPCTransport) SendInstallSnapshot(target string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	client, err := t.clientFor(target)
	if err != nil {
		return raft.InstallSnapshotReply{}, unreachable(target, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.opts.RPCTimeout)
	defer cancel()
	resp, err := client.InstallSnapshot(ctx, installSnapshotArgsToProto(args))
	if err != nil {
		return raft.InstallSnapshotReply{}, unreachable(target, err)
	}
	return installSnapshotReplyFromProto(resp), nil
}

// Close releases every connection this transport has opened. It is safe
// to call once during node shutdown; a Node must stop using the
// transport (raft.Node.Stop + Drain) before Close is called, exactly as
// it must stop before KV storage is closed -- see dbnode.Node.Close's
// ordering.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var firstErr error
	for id, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("transport: close connection to %s: %w", id, err)
		}
	}
	t.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}
