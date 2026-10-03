package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Kushall-07/forgedb/internal/config"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

// discardLogger is a *slog.Logger that writes nowhere, so these tests
// don't spam output with the normal startup/shutdown log lines every
// other package already emits during its own tests.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// testConfig returns a minimal, valid, single-node config.Config
// rooted at a fresh t.TempDir(), listening on ephemeral loopback
// ports. A single node with no peers is a legitimate (if degenerate)
// ForgeDB cluster: internal/raft's majority() is 1 for a peerless
// node, so it can elect itself leader and commit its own proposals
// without any other process involved -- exactly what these lifecycle
// tests need and nothing more.
func testConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		NodeID: "lifecycle-test-node",
		HTTP:   "127.0.0.1:0",
		GRPC:   "127.0.0.1:0",
		// TickInterval only changes how often a real-time tick fires,
		// never the election/heartbeat tick *counts* (internal/raft's
		// own defaults) -- shrinking it just makes this node reach its
		// own default election timeout in milliseconds instead of
		// seconds, which is all these tests need from it.
		TickInterval: 2 * time.Millisecond,
		DataDir:      filepath.Join(t.TempDir(), "data"),
		APIToken:     "lifecycle-test-token",
	}
}

// waitForLeader polls until srv's node reports itself leader or
// timeout elapses, failing the test on timeout. It exists because
// Raft election timing is tick-based, not instantaneous -- see
// testConfig's TickInterval comment.
func waitForLeader(t *testing.T, srv *server, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.node.IsLeader() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("node did not become leader within %s", timeout)
}

// waitGoroutinesSettle polls runtime.NumGoroutine() until it returns
// to baseline (or below -- other tests' own goroutines may still be
// winding down) or timeout elapses. Background goroutines (Raft's
// ticker, the Applier's loop, in-flight RPC sends) do not necessarily
// exit the instant Close/shutdown returns -- Close only blocks for the
// synchronization points its own doc comment promises (stopped
// ticking, drained RPCs, applier loop actually returned) -- so a bare
// before/after NumGoroutine comparison without settling is inherently
// flaky. This is a lightweight, dependency-free stand-in for a
// goroutine-leak-detector library (e.g. goleak), which this phase
// deliberately does not add.
func waitGoroutinesSettle(t *testing.T, baseline int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	for time.Now().Before(deadline) {
		runtime.Gosched()
		last = runtime.NumGoroutine()
		if last <= baseline {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("goroutine count did not settle: have %d, want <= %d (baseline)", last, baseline)
}

// TestNewServer_HTTPServesThenGracefulShutdown exercises the full
// composition root: newServer wires HTTP, gRPC, and the Raft/storage
// node together and starts them; shutdown must stop accepting new
// client HTTP requests (a connection attempt after shutdown returns
// is refused) while cleanly tearing down every other component with
// no error surfaced to the caller.
func TestNewServer_HTTPServesThenGracefulShutdown(t *testing.T) {
	srv, err := newServer(testConfig(t))
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}

	resp, err := http.Get("http://" + srv.httpAddr + "/health")
	if err != nil {
		t.Fatalf("GET /health before shutdown: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /health status = %d, want 200", resp.StatusCode)
	}

	srv.shutdown(discardLogger)

	client := http.Client{Timeout: 2 * time.Second}
	if _, err := client.Get("http://" + srv.httpAddr + "/health"); err == nil {
		t.Fatal("GET /health after shutdown succeeded, want connection refused")
	}
}

// TestServer_Shutdown_Idempotent confirms a second shutdown call -- a
// defensive scenario (e.g. an operator or supervisor sending more than
// one termination signal) rather than one main.go's current single-
// signal-then-exit flow ever triggers on its own -- never panics and
// never re-touches already-released resources (see dbnode.Node.Close's
// own idempotency guard, which this exercises transitively).
func TestServer_Shutdown_Idempotent(t *testing.T) {
	srv, err := newServer(testConfig(t))
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}

	srv.shutdown(discardLogger)
	srv.shutdown(discardLogger) // must not panic or hang
}

// TestServer_Shutdown_NoGoroutineLeak drives several start/shutdown
// cycles and confirms the goroutine count returns to its pre-test
// baseline afterward -- Raft's ticker goroutine, the Applier's loop
// goroutine, and the HTTP/gRPC server goroutines newServer starts
// must all have actually exited, not merely been asked to.
func TestServer_Shutdown_NoGoroutineLeak(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for i := 0; i < 3; i++ {
		srv, err := newServer(testConfig(t))
		if err != nil {
			t.Fatalf("newServer (iteration %d): %v", i, err)
		}
		srv.shutdown(discardLogger)
	}

	waitGoroutinesSettle(t, baseline, 2*time.Second)
}

// TestNewServer_RestartRecoversWrittenValue confirms shutdown's
// ordering never loses a write that was already applied before it
// ran: propose and wait for a value to apply, shut the node down, then
// reopen a fresh server against the exact same DataDir/NodeID and
// confirm the value is still there -- the same Raft-persistence-plus-
// WAL-replay recovery path internal/dbnode's own tests already cover
// in isolation, exercised here through the real composition root
// (newServer/shutdown) instead of dbnode.Open/Close directly.
func TestNewServer_RestartRecoversWrittenValue(t *testing.T) {
	cfg := testConfig(t)

	srv1, err := newServer(cfg)
	if err != nil {
		t.Fatalf("newServer (first run): %v", err)
	}
	waitForLeader(t, srv1, 2*time.Second)

	key, value := []byte("lifecycle-key"), []byte("lifecycle-value")
	index, _, err := srv1.node.Propose(statemachine.NewPutCommand("lifecycle-test-client", 1, key, value))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv1.node.WaitApplied(ctx, index); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}

	srv1.shutdown(discardLogger)

	srv2, err := newServer(cfg)
	if err != nil {
		t.Fatalf("newServer (second run, same DataDir): %v", err)
	}
	defer srv2.shutdown(discardLogger)

	got, err := srv2.node.Store().Get(key)
	if err != nil {
		t.Fatalf("Store().Get after restart: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("recovered value = %q, want %q", got, value)
	}
}
