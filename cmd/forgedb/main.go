// Command forgedb runs one ForgeDB node as a long-lived server process:
// Phase 14's real deployment entry point, replacing the pre-Phase-14
// in-process demos this file used to run (see git history / the phase
// docs for those). It wires together, in order:
//
//	config.Load()                  -- environment-driven configuration
//	internal/transport.GRPCTransport -- real gRPC node-to-node transport
//	dbnode.Open                    -- Raft + state machine + storage
//	internal/transport.Server      -- real gRPC server for incoming RPCs
//	dbnode.Node.Run                -- background ticking + applying
//	internal/api.Server            -- client-facing HTTP (/kv, /health,
//	                                   /ready, /cluster, /metrics,
//	                                   /admin/snapshot)
//
// and then blocks until SIGINT/SIGTERM, at which point it shuts every
// one of those down in reverse order. It introduces no new consensus,
// storage, or transport logic of its own -- every piece above is an
// existing package from Phases 0-13 (plus internal/transport, Phase
// 14's new but equally thin gRPC binding); this file is purely
// composition and lifecycle.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Kushall-07/forgedb/internal/api"
	"github.com/Kushall-07/forgedb/internal/config"
	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/logging"
	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/transport"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		// Fail fast on invalid configuration: never start a partially
		// configured distributed node (see
		// docs/deployment/phase14-docker-deployment.md's "configuration
		// validation" section). logging isn't set up yet at this point
		// (it depends on cfg.LogLevel), so this goes straight to stderr.
		fmt.Fprintf(os.Stderr, "forgedb: invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logging.SetLevel(logging.ParseLevel(cfg.LogLevel))
	log := logging.With("node_id", cfg.NodeID, "component", "main")
	log.Info(logging.EventNodeStarted, "http", cfg.HTTP, "grpc", cfg.GRPC, "data_dir", cfg.DataDir, "peers", cfg.PeerIDs())

	srv, err := newServer(cfg)
	if err != nil {
		log.Error("startup failed", "error", err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Info("shutdown signal received")
	srv.shutdown(log)
	log.Info(logging.EventNodeStopped)
}

// server bundles every long-lived piece newServer constructs, so main
// can shut them down together in the right order.
type server struct {
	node      *dbnode.Node
	grpcSrv   *transport.Server
	grpcAddr  string
	apiSrv    *api.Server
	httpAddr  string
	grpcTrans *transport.GRPCTransport
}

// newServer wires one ForgeDB node end to end from cfg and starts it
// listening on both its gRPC (node-to-node) and HTTP (client-facing)
// addresses. It does not block; the caller is responsible for keeping
// the process alive (see main's signal wait) and for calling shutdown.
func newServer(cfg config.Config) (*server, error) {
	raftDir := filepath.Join(cfg.DataDir, "raft")
	kvDir := filepath.Join(cfg.DataDir, "kv")

	grpcTrans := transport.NewGRPCTransport(cfg.PeerGRPCAddrs(), transport.Options{
		RPCTimeout: cfg.RPCTimeout,
	})

	node, err := dbnode.Open(dbnode.Config{
		ID:        cfg.NodeID,
		Peers:     cfg.PeerIDs(),
		Transport: grpcTrans,
		RaftDir:   raftDir,
		KVDir:     kvDir,
	})
	if err != nil {
		grpcTrans.Close()
		return nil, fmt.Errorf("open node: %w", err)
	}

	// The gRPC server forwards incoming RPCs straight to this node's
	// *raft.Node (see internal/transport.Server's doc comment) -- the
	// real-network equivalent of InMemoryTransport.Register, which
	// dbnode.Open already attempts automatically but silently skips for
	// a transport (like GRPCTransport) that isn't a registrar. A real
	// network transport has no in-process handler table to register
	// against; instead, this server must actually be reachable on its
	// own configured gRPC address for peers to deliver RPCs here at all.
	grpcSrv := transport.NewServer(node.Raft(), transport.Options{RPCTimeout: cfg.RPCTimeout})
	grpcAddr, err := grpcSrv.Serve(cfg.GRPC)
	if err != nil {
		node.Close()
		grpcTrans.Close()
		return nil, fmt.Errorf("start gRPC server on %s: %w", cfg.GRPC, err)
	}

	tickInterval := cfg.TickInterval
	if tickInterval <= 0 {
		tickInterval = config.DefaultTickInterval
	}
	node.Run(tickInterval)

	apiSrv := api.NewServer(node, metrics.Default, api.WithPeerHTTPAddrs(cfg.PeerHTTPAddrs()))
	httpAddr, err := apiSrv.Start(cfg.HTTP)
	if err != nil {
		grpcSrv.Stop()
		node.Close()
		grpcTrans.Close()
		return nil, fmt.Errorf("start HTTP server on %s: %w", cfg.HTTP, err)
	}

	logging.With("node_id", cfg.NodeID, "component", "main").Info("server listening", "http", httpAddr, "grpc", grpcAddr)

	return &server{
		node:      node,
		grpcSrv:   grpcSrv,
		grpcAddr:  grpcAddr,
		apiSrv:    apiSrv,
		httpAddr:  httpAddr,
		grpcTrans: grpcTrans,
	}, nil
}

// shutdown stops every component in the reverse order newServer started
// them in: stop accepting new client HTTP requests first, then stop
// accepting new peer RPCs, then stop this node's own Raft/applier/storage
// (dbnode.Node.Close's own internal ordering already handles that
// boundary safely -- see its doc comment), and finally release this
// node's own outgoing gRPC connections. Each step is independently safe
// to call even if an earlier one failed.
func (s *server) shutdown(log *slog.Logger) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.apiSrv.Shutdown(shutdownCtx); err != nil {
		log.Info("http server shutdown error", "error", err.Error())
	}
	s.grpcSrv.Stop()
	if err := s.node.Close(); err != nil {
		log.Info("node close error", "error", err.Error())
	}
	if err := s.grpcTrans.Close(); err != nil {
		log.Info("transport close error", "error", err.Error())
	}
}
