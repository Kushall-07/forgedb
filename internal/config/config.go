package config

import "time"

// Config configures one ForgeDB node, including the Phase 12
// observability settings and, as of Phase 14, the static cluster
// membership and real-network deployment settings a caller (cmd/forgedb)
// needs to run as an independent OS process/container. See Load for how
// a Config is normally built (from environment variables) and Validate
// for the fail-fast checks every Config should pass before a Node is
// opened against it.
type Config struct {
	NodeID  string
	HTTP    string
	GRPC    string
	DataDir string

	// APIToken is the static bearer token internal/api's HTTP middleware
	// requires on every request to a protected endpoint (every route
	// except /health and /ready -- see internal/api/auth.go). Load reads
	// it from the FORGEDB_API_TOKEN environment variable and never
	// defaults it: Validate fails fast when it is empty, so a node can
	// never start serving its client-facing HTTP API without
	// authentication configured. It is loaded once here, at startup, and
	// passed into api.NewServer -- nothing in internal/api re-reads the
	// environment per request.
	APIToken string

	// Peers is this cluster's full, static node roster -- every node,
	// including this one (see Validate's self-coherence check) -- keyed
	// by ID. Phase 14 uses only static membership: there is no
	// AddNode/RemoveNode/reconfiguration, so this list is fixed for the
	// process's lifetime, exactly as raft.Options.Peers already assumes.
	// Load parses it from the PEERS environment variable; see ParsePeers.
	Peers []Node

	// LogLevel selects the minimum level internal/logging.Default
	// records, as internal/logging.ParseLevel understands it
	// ("debug"/"info"/"warn"/"error"). An empty value means "info", the
	// right default for normal operation.
	LogLevel string

	// MetricsEnabled controls whether cmd/forgedb starts the
	// internal/api diagnostic + client HTTP server (/health, /ready,
	// /metrics, /cluster, /kv) at all.
	MetricsEnabled bool

	// MetricsAddr is the address the diagnostic HTTP server listens on
	// when MetricsEnabled is true (e.g. ":9090"). An empty value means
	// an OS-assigned ephemeral port (":0"). Phase 14's production server
	// (cmd/forgedb) does not use this field -- it serves everything,
	// diagnostics included, from HTTP -- but it is left in place so the
	// existing observability demo path keeps working unchanged.
	MetricsAddr string

	// RPCTimeout bounds every outgoing node-to-node gRPC call (see
	// internal/transport.Options.RPCTimeout). Zero means
	// transport.DefaultRPCTimeout.
	RPCTimeout time.Duration

	// TickInterval is the real-wall-clock duration between logical Raft
	// ticks when running as a long-lived server (see
	// (*raft.Node).Run / (*dbnode.Node).Run). It does not change any
	// election/heartbeat/commit semantics, which remain governed purely
	// by tick *counts* inside internal/raft -- it only sets how often a
	// tick actually happens in real time. Zero means
	// DefaultTickInterval.
	TickInterval time.Duration

	// MaxValueBytes bounds a /kv PUT request body (see
	// internal/api.Server's own DefaultMaxValueBytes, which this
	// overrides via api.WithMaxValueBytes). Load reads it from the
	// MAX_VALUE_BYTES environment variable and defaults it to
	// DefaultMaxValueBytes when unset -- Phase 13 makes the existing
	// limit configurable without silently lowering it: Validate rejects
	// an explicitly configured value <= 0 rather than treating it as
	// "unlimited" or falling back to the default.
	MaxValueBytes int64

	// CORSOrigins is the exact set of browser Origins internal/api's
	// CORS layer (see internal/api/cors.go) is allowed to reflect into
	// Access-Control-Allow-Origin -- e.g. the dashboard's deployed
	// domain. Load reads it from the comma-separated
	// FORGEDB_CORS_ORIGINS environment variable; a nil/empty value (the
	// default) disables CORS entirely, exactly as internal/api behaved
	// before Phase 13. There is deliberately no wildcard support -- see
	// that package's doc comment for why an explicit allow-list, never
	// "*", is required for an authenticated API.
	CORSOrigins []string
}

// DefaultTickInterval is used when Config.TickInterval is left zero.
const DefaultTickInterval = 100 * time.Millisecond

// DefaultMaxValueBytes is used when Config.MaxValueBytes is left zero
// (i.e. MAX_VALUE_BYTES is unset). It matches
// internal/api.DefaultMaxValueBytes -- the two packages do not import
// one another (config must not depend on api), so this value is kept
// in sync by convention; internal/config/load_test.go and
// internal/api/kv_test.go each pin their own package's constant.
const DefaultMaxValueBytes = 1 << 20 // 1 MiB
