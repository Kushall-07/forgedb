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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kushall-07/forgedb/internal/config"
	"github.com/Kushall-07/forgedb/internal/gateway"
)

func main() {
	cfg, err := config.LoadGateway()
	if err != nil {
		fmt.Fprintf(os.Stderr, "forge-gateway: invalid configuration: %v\n", err)
		os.Exit(1)
	}

	proxy := gateway.NewProxy(cfg.Backends, gateway.DefaultBackendTimeout)
	srv := &http.Server{Addr: cfg.Addr, Handler: proxy}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("forge-gateway: listening on %s, backends=%v", cfg.Addr, cfg.Backends)
		errCh <- srv.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Printf("forge-gateway: shutdown signal received")
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "forge-gateway: %v\n", err)
			os.Exit(1)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("forge-gateway: shutdown error: %v", err)
	}
}
