package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Kushall-07/forgedb/internal/config"
)

// newTestBackend returns an httptest.Server standing in for a ForgeDB
// node's client-facing HTTP API: these tests exercise forge-gateway's
// own HTTP lifecycle (listen, serve, shut down), not leader-following
// behavior, which internal/gateway/proxy_test.go already covers
// directly against Proxy.
func newTestBackend(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func okBackend(t *testing.T) *httptest.Server {
	return newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

// TestGatewayServer_ShutdownWaitsForInFlightRequest confirms shutdown
// does not cut off a proxy request that is already in flight: it
// starts a request against a backend that blocks mid-response, calls
// shutdown concurrently, and requires shutdown to still be waiting
// while the backend is blocked -- only returning, successfully, once
// the backend (and therefore the proxied request) actually completes.
func TestGatewayServer_ShutdownWaitsForInFlightRequest(t *testing.T) {
	reached := make(chan struct{})
	release := make(chan struct{})
	backend := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
		close(reached)
		<-release
		w.WriteHeader(http.StatusOK)
	})

	srv, err := newGatewayServer(config.GatewayConfig{Addr: "127.0.0.1:0", Backends: []string{backend.URL}})
	if err != nil {
		t.Fatalf("newGatewayServer: %v", err)
	}

	type result struct {
		status int
		err    error
	}
	reqDone := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + srv.addr + "/kv/some-key")
		if err != nil {
			reqDone <- result{err: err}
			return
		}
		defer resp.Body.Close()
		reqDone <- result{status: resp.StatusCode}
	}()

	select {
	case <-reached:
	case <-time.After(2 * time.Second):
		t.Fatal("backend was never reached")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- srv.shutdown() }()

	// shutdown must still be waiting on the in-flight request here --
	// the backend handler hasn't been released yet.
	select {
	case <-shutdownDone:
		t.Fatal("shutdown() returned before the in-flight request finished")
	case <-time.After(150 * time.Millisecond):
	}

	close(release)

	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}

	res := <-reqDone
	if res.err != nil {
		t.Fatalf("in-flight request error = %v", res.err)
	}
	if res.status != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200", res.status)
	}
}

// TestGatewayServer_ShutdownIdempotent confirms a second shutdown call
// never panics or blocks -- http.Server.Shutdown's own documented
// behavior, exercised here through this package's wrapper.
func TestGatewayServer_ShutdownIdempotent(t *testing.T) {
	srv, err := newGatewayServer(config.GatewayConfig{Addr: "127.0.0.1:0", Backends: []string{okBackend(t).URL}})
	if err != nil {
		t.Fatalf("newGatewayServer: %v", err)
	}

	if err := srv.shutdown(); err != nil {
		t.Fatalf("first shutdown() error = %v", err)
	}
	if err := srv.shutdown(); err != nil {
		t.Fatalf("second shutdown() error = %v", err)
	}

	if err := <-srv.errCh; err != nil {
		t.Fatalf("errCh after shutdown = %v, want nil", err)
	}
}

// TestGatewayServer_RejectsNewConnectionsAfterShutdown confirms
// shutdown stops accepting new client connections, even though it
// still lets an already-in-flight one finish (see the WaitsForInFlight
// test above).
func TestGatewayServer_RejectsNewConnectionsAfterShutdown(t *testing.T) {
	srv, err := newGatewayServer(config.GatewayConfig{Addr: "127.0.0.1:0", Backends: []string{okBackend(t).URL}})
	if err != nil {
		t.Fatalf("newGatewayServer: %v", err)
	}

	if resp, err := http.Get("http://" + srv.addr + "/kv/k"); err != nil {
		t.Fatalf("pre-shutdown request: %v", err)
	} else {
		resp.Body.Close()
	}

	if err := srv.shutdown(); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}

	client := http.Client{Timeout: 2 * time.Second}
	if _, err := client.Get("http://" + srv.addr + "/kv/k"); err == nil {
		t.Fatal("request after shutdown succeeded, want connection refused")
	}
}
