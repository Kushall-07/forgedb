package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Environment variable names Load reads. These are the only
// configuration inputs Phase 14 introduces -- see
// docs/deployment/phase14-docker-deployment.md's configuration section.
const (
	EnvNodeID         = "NODE_ID"
	EnvHTTPAddr       = "HTTP_ADDR"
	EnvGRPCAddr       = "GRPC_ADDR"
	EnvDataDir        = "DATA_DIR"
	EnvPeers          = "PEERS"
	EnvLogLevel       = "LOG_LEVEL"
	EnvMetricsEnabled = "METRICS_ENABLED"
	EnvMetricsAddr    = "METRICS_ADDR"
	EnvRPCTimeout     = "RPC_TIMEOUT"
	EnvTickInterval   = "TICK_INTERVAL"

	// EnvAPIToken names the environment variable Load reads
	// Config.APIToken from. It has no default -- see Validate.
	EnvAPIToken = "FORGEDB_API_TOKEN"
)

// Load builds a Config from environment variables and validates it (see
// Validate), returning an error immediately if anything is missing or
// inconsistent rather than starting a partially configured node (per
// docs/deployment/phase14-docker-deployment.md's "fail fast" rule).
//
// NODE_ID is required; every other variable has a documented default:
//
//	HTTP_ADDR       default ":8080"
//	GRPC_ADDR       default ":9090"
//	DATA_DIR        default "data"
//	PEERS           default "" (no peers -- a single-node cluster)
//	LOG_LEVEL       default "info"
//	METRICS_ENABLED default "true"
//	METRICS_ADDR    default "" (unused by the production server path --
//	                it exists only for the pre-Phase-14 observability
//	                demo, which listens on its own ephemeral address)
//	RPC_TIMEOUT     default "" (internal/transport.DefaultRPCTimeout)
//	TICK_INTERVAL   default "" (DefaultTickInterval)
//	FORGEDB_API_TOKEN  *(required, no default)* -- the bearer token
//	                internal/api's auth middleware requires on every
//	                protected request (see Config.APIToken). Validate
//	                fails fast when this is unset or empty, rather than
//	                starting a client-facing HTTP API with no
//	                authentication.
//
// PEERS lists the cluster's full static roster (every node, including
// this one -- see Validate) as comma-separated "id=grpcAddr" or
// "id=grpcAddr|httpAddr" entries, e.g.:
//
//	PEERS=node-1=forgedb-1:9091|forgedb-1:8081,node-2=forgedb-2:9092|forgedb-2:8082,node-3=forgedb-3:9093|forgedb-3:8083
//
// The same PEERS value is normally given to every node in a cluster
// (only NODE_ID, HTTP_ADDR, GRPC_ADDR, and DATA_DIR differ per node),
// which is what lets a Docker Compose file share one cluster-wide
// environment entry across all three services.
func Load() (Config, error) {
	cfg := Config{
		NodeID:      os.Getenv(EnvNodeID),
		HTTP:        getenvDefault(EnvHTTPAddr, ":8080"),
		GRPC:        getenvDefault(EnvGRPCAddr, ":9090"),
		DataDir:     getenvDefault(EnvDataDir, "data"),
		LogLevel:    getenvDefault(EnvLogLevel, "info"),
		MetricsAddr: os.Getenv(EnvMetricsAddr),
		APIToken:    os.Getenv(EnvAPIToken),
	}

	peers, err := ParsePeers(os.Getenv(EnvPeers))
	if err != nil {
		return Config{}, err
	}
	cfg.Peers = peers

	cfg.MetricsEnabled = true
	if v := os.Getenv(EnvMetricsEnabled); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid %s %q: %w", EnvMetricsEnabled, v, err)
		}
		cfg.MetricsEnabled = b
	}

	if v := os.Getenv(EnvRPCTimeout); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid %s %q: %w", EnvRPCTimeout, v, err)
		}
		cfg.RPCTimeout = d
	}
	if v := os.Getenv(EnvTickInterval); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid %s %q: %w", EnvTickInterval, v, err)
		}
		cfg.TickInterval = d
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ParsePeers parses the PEERS environment variable's format: zero or
// more comma-separated "id=grpcAddr" or "id=grpcAddr|httpAddr" entries.
// The HTTP half is optional per entry; when omitted, Node.HTTP is left
// empty (only used for the client API's not-leader redirect hint -- see
// internal/api -- and never required for Raft itself to function). An
// empty raw string returns a nil, nil (zero peers): a valid, if
// degenerate, single-node configuration.
func ParsePeers(raw string) ([]Node, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	fields := strings.Split(raw, ",")
	peers := make([]Node, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		idAndAddr := strings.SplitN(field, "=", 2)
		if len(idAndAddr) != 2 {
			return nil, fmt.Errorf("config: invalid %s entry %q: expected format id=grpcAddr[|httpAddr]", EnvPeers, field)
		}
		id := strings.TrimSpace(idAndAddr[0])
		addr := strings.TrimSpace(idAndAddr[1])
		if id == "" || addr == "" {
			return nil, fmt.Errorf("config: invalid %s entry %q: id and gRPC address must not be empty", EnvPeers, field)
		}

		grpcAddr, httpAddr := addr, ""
		if i := strings.IndexByte(addr, '|'); i >= 0 {
			grpcAddr = strings.TrimSpace(addr[:i])
			httpAddr = strings.TrimSpace(addr[i+1:])
			if grpcAddr == "" {
				return nil, fmt.Errorf("config: invalid %s entry %q: gRPC address must not be empty", EnvPeers, field)
			}
		}
		peers = append(peers, Node{ID: id, GRPC: grpcAddr, HTTP: httpAddr})
	}
	return peers, nil
}

// Validate checks cfg for the problems Phase 14 requires failing fast
// on, before any Node, transport, or listener is created (see
// docs/deployment/phase14-docker-deployment.md's "configuration
// validation" section):
//
//   - NodeID, HTTP, GRPC, and DataDir are all non-empty, and HTTP/GRPC
//     parse as valid host:port addresses.
//   - no two peer entries share an ID.
//   - no two distinct peer IDs share a gRPC address.
//   - every peer's gRPC address parses as a valid host:port.
//   - if Peers includes an entry for this node's own ID (the normal
//     case -- see Load's PEERS doc comment), that entry's gRPC address
//     matches cfg.GRPC exactly, so this node's self-identity is
//     coherent with how its peers (and this node's own transport) will
//     address it -- see PeerIDs/PeerGRPCAddrs, which exclude the self
//     entry so Raft and the gRPC transport never attempt to treat this
//     node as its own peer.
//   - APIToken is non-empty, so internal/api's client-facing HTTP
//     surface is never started without authentication configured (see
//     Config.APIToken's doc comment) -- a deployment reachable over a
//     public tunnel or the open internet must never silently fall back
//     to an unauthenticated API.
func (c Config) Validate() error {
	if c.NodeID == "" {
		return errors.New("config: NodeID must not be empty")
	}
	if c.APIToken == "" {
		return fmt.Errorf("config: %s must be set -- refusing to start the HTTP API without authentication configured", EnvAPIToken)
	}
	if err := validateAddr("HTTP", c.HTTP); err != nil {
		return err
	}
	if err := validateAddr("GRPC", c.GRPC); err != nil {
		return err
	}
	if c.DataDir == "" {
		return errors.New("config: DataDir must not be empty")
	}

	seenID := make(map[string]Node, len(c.Peers))
	seenGRPC := make(map[string]string, len(c.Peers))
	var selfEntry *Node
	for _, p := range c.Peers {
		if p.ID == "" {
			return fmt.Errorf("config: %s contains an entry with an empty id", EnvPeers)
		}
		if prev, ok := seenID[p.ID]; ok {
			return fmt.Errorf("config: duplicate peer id %q (addresses %q and %q)", p.ID, prev.GRPC, p.GRPC)
		}
		seenID[p.ID] = p

		if err := validateAddr(fmt.Sprintf("peer %q gRPC", p.ID), p.GRPC); err != nil {
			return err
		}
		if owner, ok := seenGRPC[p.GRPC]; ok && owner != p.ID {
			return fmt.Errorf("config: gRPC address %q is used by both peer %q and peer %q", p.GRPC, owner, p.ID)
		}
		seenGRPC[p.GRPC] = p.ID

		if p.ID == c.NodeID {
			entry := p
			selfEntry = &entry
		}
	}
	if selfEntry != nil {
		if err := checkSelfAddrCoherent(c.NodeID, selfEntry.GRPC, c.GRPC); err != nil {
			return err
		}
	}
	return nil
}

func validateAddr(label, addr string) error {
	if addr == "" {
		return fmt.Errorf("config: %s address must not be empty", label)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("config: %s address %q is invalid: %w", label, addr, err)
	}
	return nil
}

// checkSelfAddrCoherent validates that selfPeerAddr (this node's own
// entry in the PEERS roster -- the address peers are told to dial it at)
// and bindAddr (cfg.GRPC -- the address this process itself binds)
// agree on the port, always, and on the host whenever bindAddr's host is
// not a wildcard bind address. A wildcard bind host (0.0.0.0, ::, or
// simply omitted, as in ":9090") deliberately does NOT have to equal the
// advertised host: binding 0.0.0.0:9090 while being dialed as
// "forgedb-1:9090" (a Docker Compose service name resolving to this same
// container) is the normal, correct pattern, not a misconfiguration --
// see docs/deployment/phase14-docker-deployment.md's network section.
// What must never happen is this node listening on one port while
// telling its peers to dial a different one, which the port check alone
// still catches.
func checkSelfAddrCoherent(nodeID, selfPeerAddr, bindAddr string) error {
	peerHost, peerPort, err := net.SplitHostPort(selfPeerAddr)
	if err != nil {
		return fmt.Errorf("config: peers entry for self (%q) has an invalid gRPC address %q: %w", nodeID, selfPeerAddr, err)
	}
	bindHost, bindPort, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return fmt.Errorf("config: GRPC address %q is invalid: %w", bindAddr, err)
	}
	if peerPort != bindPort {
		return fmt.Errorf("config: peers entry for self (%q) advertises port %q, which does not match the configured GRPC port %q", nodeID, peerPort, bindPort)
	}
	if !isWildcardHost(bindHost) && !isWildcardHost(peerHost) && bindHost != peerHost {
		return fmt.Errorf("config: peers entry for self (%q) advertises host %q, which does not match the configured GRPC host %q", nodeID, peerHost, bindHost)
	}
	return nil
}

// isWildcardHost reports whether host is a "listen on every interface"
// address (including the empty string, as in the host-less ":9090"
// form) rather than a specific one a peer could actually be told to dial.
func isWildcardHost(host string) bool {
	switch host {
	case "", "0.0.0.0", "::":
		return true
	default:
		return false
	}
}

// PeerIDs returns every other cluster member's ID (this node's own ID,
// if present in Peers, is excluded), sorted for determinism. It is
// exactly the raft.Options.Peers / dbnode.Config.Peers value for this
// node, per raft's requirement that Peers "must not include ID itself."
func (c Config) PeerIDs() []string {
	ids := make([]string, 0, len(c.Peers))
	for _, p := range c.Peers {
		if p.ID == c.NodeID {
			continue
		}
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	return ids
}

// PeerGRPCAddrs returns every other cluster member's gRPC dial address
// keyed by ID (this node's own entry is excluded), ready to pass to
// internal/transport.NewGRPCTransport.
func (c Config) PeerGRPCAddrs() map[string]string {
	out := make(map[string]string, len(c.Peers))
	for _, p := range c.Peers {
		if p.ID == c.NodeID {
			continue
		}
		out[p.ID] = p.GRPC
	}
	return out
}

// PeerHTTPAddrs returns every cluster member's HTTP address keyed by ID
// (self included, when known, unlike PeerIDs/PeerGRPCAddrs) for entries
// where it was given; peers whose PEERS entry omitted the optional HTTP
// half are left out. internal/api uses this to resolve a known leader
// ID to an address a client can be redirected to.
func (c Config) PeerHTTPAddrs() map[string]string {
	out := make(map[string]string, len(c.Peers))
	for _, p := range c.Peers {
		if p.HTTP == "" {
			continue
		}
		out[p.ID] = p.HTTP
	}
	return out
}
