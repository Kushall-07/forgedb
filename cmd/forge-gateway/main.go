// Command forge-gateway runs ForgeDB's leader-aware public HTTP
// entrypoint: a small, persistent reverse proxy (internal/gateway) that
// sits between a public-facing reverse proxy (e.g. Caddy, behind
// Cloudflare) and a ForgeDB cluster's nodes, forwarding every request
// to whichever node currently identifies itself as the Raft leader --
// following that node's own not-leader hint (see internal/api/kv.go)
// when it isn't. It introduces no new consensus, storage, or
// authentication logic of its own: see internal/gateway's package doc
// comment for the full design rationale, and
// docs/deployment/phase15-leader-aware-gateway.md for the writeup.
//
// Usage:
//
//	GATEWAY_ADDR=0.0.0.0:8090 \
//	PEERS="node-1=forgedb-1:9090|forgedb-1:8080,node-2=forgedb-2:9090|forgedb-2:8080,node-3=forgedb-3:9090|forgedb-3:8080" \
//	forge-gateway
//
// forge-gateway reads the exact same PEERS value every ForgeDB node in
// the cluster already does (see config.LoadGateway) -- there is no
// separate configuration format for the gateway's backend list. It does
// not require FORGEDB_API_TOKEN: it never validates client credentials
// itself, only forwards the Authorization header it received unchanged
// (see internal/gateway's package doc comment).
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kushall-07/forgedb/internal/config"
	"github.com/Kushall-07/forgedb/internal/gateway"
)

// shutdownTimeout bounds how long Shutdown waits for in-flight proxy
// requests (see Proxy.ServeHTTP -- a request that is itself retrying
// across up to gateway.maxAttempts backends) to finish before giving
// up and returning. It does not change proxy/leader-discovery
// semantics; it only bounds how long the process delays exiting.
const shutdownTimeout = 10 * time.Second

// gatewayServer bundles the one long-lived piece newGatewayServer
// starts, so main can shut it down without main itself knowing
// anything about http.Server's own shutdown contract.
type gatewayServer struct {
	httpSrv *http.Server
	addr    string
	errCh   chan error
}

// newGatewayServer binds cfg.Addr (e.g. "0.0.0.0:8090", or "127.0.0.1:0"
// for an ephemeral port, which tests use) and starts serving in the
// background; it does not block. It returns the actual bound address
// (mirroring internal/api.Server.Start and internal/transport.Server.Serve's
// own pattern) and an error only for a bind failure that happens
// synchronously, before any goroutine starts -- a failure after that
// point (e.g. the listener erroring mid-run) is instead delivered on
// errCh, which main's select (below) treats as fatal. errCh also
// receives Shutdown's own eventual, successful drain (reported as
// http.ErrServerClosed, translated to nil here since that return means
// Shutdown worked as intended, not that something failed).
func newGatewayServer(cfg config.GatewayConfig) (*gatewayServer, error) {
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	proxy := gateway.NewProxy(cfg.Backends, gateway.DefaultBackendTimeout, gateway.WithMaxBodyBytes(cfg.MaxBodyBytes))
	httpSrv := &http.Server{Handler: proxy}

	errCh := make(chan error, 1)
	go func() {
		err := httpSrv.Serve(ln)
		if err == http.ErrServerClosed {
			err = nil
		}
		errCh <- err
	}()

	return &gatewayServer{httpSrv: httpSrv, addr: ln.Addr().String(), errCh: errCh}, nil
}

// shutdown gracefully stops the HTTP server, giving any in-flight
// proxy request up to shutdownTimeout to finish (see Proxy.ServeHTTP)
// before new connections and requests stop being accepted. It is safe
// to call more than once: http.Server.Shutdown already tolerates
// repeated calls on its own, returning promptly once the listener and
// every tracked connection are already closed/idle.
func (s *gatewayServer) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return s.httpSrv.Shutdown(ctx)
}

func main() {
	cfg, err := config.LoadGateway()
	if err != nil {
		fmt.Fprintf(os.Stderr, "forge-gateway: invalid configuration: %v\n", err)
		os.Exit(1)
	}

	srv, err := newGatewayServer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "forge-gateway: listen on %s: %v\n", cfg.Addr, err)
		os.Exit(1)
	}
	log.Printf("forge-gateway: listening on %s, backends=%v", srv.addr, cfg.Backends)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Printf("forge-gateway: shutdown signal received")
	case err := <-srv.errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "forge-gateway: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := srv.shutdown(); err != nil {
		log.Printf("forge-gateway: shutdown error: %v", err)
	}
	log.Printf("forge-gateway: shutdown complete")
}
