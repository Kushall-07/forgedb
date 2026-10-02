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
}

// DefaultTickInterval is used when Config.TickInterval is left zero.
const DefaultTickInterval = 100 * time.Millisecond
