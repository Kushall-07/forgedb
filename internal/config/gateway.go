package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// EnvGatewayAddr names the environment variable LoadGateway reads
// cmd/forge-gateway's own listen address from (see GatewayConfig.Addr).
// It is a new variable the leader-aware public HTTP routing phase
// introduces -- it does not rename or replace any existing variable;
// NODE_ID, HTTP_ADDR, GRPC_ADDR, DATA_DIR, PEERS, and FORGEDB_API_TOKEN
// all keep their existing meaning, used exactly as before by
// cmd/forgedb and config.Load.
const EnvGatewayAddr = "GATEWAY_ADDR"

// DefaultGatewayAddr is used when GATEWAY_ADDR is unset or empty.
const DefaultGatewayAddr = ":8090"

// EnvGatewayMaxBodyBytes names the environment variable LoadGateway
// reads GatewayConfig.MaxBodyBytes from. Empty/unset defaults to
// DefaultGatewayMaxBodyBytes.
const EnvGatewayMaxBodyBytes = "GATEWAY_MAX_BODY_BYTES"

// DefaultGatewayMaxBodyBytes is used when GatewayConfig.MaxBodyBytes
// is left zero (i.e. GATEWAY_MAX_BODY_BYTES is unset). It matches
// internal/gateway.DefaultMaxBodyBytes -- see
// Config.DefaultMaxValueBytes's doc comment for why this value is
// duplicated rather than imported across packages.
const DefaultGatewayMaxBodyBytes = 4 << 20 // 4 MiB

// GatewayConfig configures cmd/forge-gateway: a small, leader-following
// HTTP reverse proxy (internal/gateway) that sits in front of a
// ForgeDB cluster's nodes. Unlike Config (one ForgeDB node), a gateway
// is not itself a Raft member and does not need a NodeID, a gRPC
// address, a data directory, or the API token -- it never terminates or
// inspects client authentication itself (see internal/gateway's package
// doc comment), only forwards the Authorization header a client already
// supplied straight through to whichever backend node ultimately
// answers.
type GatewayConfig struct {
	// Addr is the address the gateway listens on (e.g. ":8090").
	Addr string

	// Backends is every node's client-facing HTTP address, as a
	// request-ready base URL (e.g. "http://forgedb-1:8080"), in the
	// order PEERS listed them. It is derived from the exact same PEERS
	// environment variable every ForgeDB node already reads (see
	// Load's doc comment) -- the gateway is given the identical PEERS
	// value every node in the cluster already has, rather than
	// inventing a second, separate way to describe the same roster.
	Backends []string

	// MaxBodyBytes bounds how much of an incoming request body
	// internal/gateway.Proxy buffers in memory (see that package's own
	// DefaultMaxBodyBytes, which this overrides via
	// gateway.WithMaxBodyBytes). LoadGateway reads it from
	// GATEWAY_MAX_BODY_BYTES and defaults it to
	// DefaultGatewayMaxBodyBytes when unset.
	MaxBodyBytes int64
}

// LoadGateway builds a GatewayConfig from the environment: GATEWAY_ADDR
// (default DefaultGatewayAddr) and PEERS (reusing ParsePeers -- see
// Load's doc comment for its "id=grpcAddr|httpAddr" format). It fails
// fast if PEERS names no node with an HTTP address at all, since the
// gateway would then have nowhere to forward a request -- the same
// "fail fast rather than start partially configured" rule Load's own
// Validate already applies to a ForgeDB node.
func LoadGateway() (GatewayConfig, error) {
	cfg := GatewayConfig{Addr: getenvDefault(EnvGatewayAddr, DefaultGatewayAddr), MaxBodyBytes: DefaultGatewayMaxBodyBytes}
	if cfg.Addr == "" {
		return GatewayConfig{}, errors.New("config: GATEWAY_ADDR must not be empty")
	}

	if v := os.Getenv(EnvGatewayMaxBodyBytes); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return GatewayConfig{}, fmt.Errorf("config: invalid %s %q: %w", EnvGatewayMaxBodyBytes, v, err)
		}
		if n <= 0 {
			return GatewayConfig{}, fmt.Errorf("config: %s must be a positive number of bytes, got %d", EnvGatewayMaxBodyBytes, n)
		}
		cfg.MaxBodyBytes = n
	}

	peers, err := ParsePeers(os.Getenv(EnvPeers))
	if err != nil {
		return GatewayConfig{}, err
	}
	for _, p := range peers {
		if p.HTTP == "" {
			continue
		}
		cfg.Backends = append(cfg.Backends, httpURL(p.HTTP))
	}
	if len(cfg.Backends) == 0 {
		return GatewayConfig{}, fmt.Errorf("config: %s must list at least one entry with an HTTP address (id=grpcAddr|httpAddr) for the gateway to forward to", EnvPeers)
	}
	return cfg, nil
}

// httpURL normalizes addr (e.g. "forgedb-1:8080") into a request-ready
// base URL, adding an "http://" scheme when the PEERS entry -- like
// every existing HTTP address this config package parses -- was given
// as a bare host:port.
func httpURL(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "http://" + addr
}
