// Package api is ForgeDB's HTTP surface. Its Phase 12 core is a small,
// read-only diagnostic server exposing /health, /ready, /metrics, and
// /cluster: every one of those handlers only reads already-available
// state and renders it, per
// docs/observability/phase12-observability.md's "diagnostics are
// read-only" rule. Phase 14 (see kv.go) adds the client-facing write/read
// surface ForgeDB previously had none of at all: /kv/{key}
// (PUT/GET/DELETE), built directly on internal/dbnode's existing
// Propose/ConsistentGet, plus an operator-only /admin/snapshot trigger.
// Nothing in kv.go introduces new consensus, replication, or storage
// logic -- see that file's doc comment. Phase E (see auth.go) adds a
// single static-bearer-token authentication middleware in front of
// every route except /health and /ready, since a real deployment of
// this package may now be reachable over a public tunnel rather than
// only a private network.
package api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/logging"
	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// Server is ForgeDB's HTTP server: Phase 12's read-only diagnostics plus
// Phase 14's client-facing /kv surface. A zero Server is not usable;
// construct one with NewServer.
type Server struct {
	node     *dbnode.Node
	registry *metrics.Registry
	httpSrv  *http.Server

	// peerHTTP maps a peer's node ID to its client-facing HTTP address
	// (see config.Config.PeerHTTPAddrs), used only to populate the
	// optional leader_http hint on a not-leader response (see kv.go).
	// Nil or missing entries simply omit the hint -- never an error.
	peerHTTP map[string]string

	// clientID and reqSeq identify this Server's /kv writes as a Raft
	// client for deduplication purposes -- see kv.go's kvClientIDPrefix
	// doc comment for why clientID must be derived from this node's own
	// ID rather than a constant shared across every node in the cluster.
	//
	// reqSeq is seeded (see NewServer) from wall-clock time rather than
	// left at its zero value: this exact ClientID's (ClientID, RequestID)
	// history survives a restart of this node's process in Raft's
	// persisted log, which the state machine replays into its dedup table
	// on every startup (see docs/raft/phase7-state-machine.md), but reqSeq
	// itself is only in-memory and would otherwise restart the sequence
	// from 1 every time -- directly colliding with, and appearing *stale*
	// against, RequestIDs this same ClientID already used in a prior
	// process lifetime. A command the state machine resolves as stale
	// (statemachine.ErrStaleRequest) is still a fully resolved, "applied"
	// outcome from Raft's perspective (committed, LastApplied advances),
	// so a caller that only waits on WaitApplied -- as proposeAndRespond
	// does -- would otherwise see a silently dropped write reported as
	// 200 OK, never reaching storage. Seeding from
	// time.Now().UnixNano() instead makes a fresh Server's first
	// RequestID astronomically likely to exceed every RequestID any
	// previous instance of this same ClientID ever produced (nanosecond
	// resolution against a request rate of, at most, thousands per
	// second), closing this restart collision the same way scoping
	// ClientID to the node's own ID already closes the equivalent
	// cross-node collision at failover.
	clientID string
	reqSeq   atomic.Uint64

	// apiToken is the static bearer token authMiddleware (see auth.go)
	// requires on every protected route. It is set once, in NewServer,
	// from the apiToken parameter -- never re-read from the environment
	// per request -- and is never logged or exposed in any response.
	apiToken string
}

// Option configures optional Server behavior. See WithPeerHTTPAddrs.
type Option func(*Server)

// WithPeerHTTPAddrs supplies the cluster's node-ID-to-HTTP-address map
// (typically config.Config.PeerHTTPAddrs) that /kv's not-leader response
// uses to hint a client at the current leader's address. Omitting this
// option simply means that hint is never populated (leader_id alone is
// still returned) -- it is not required for correctness.
func WithPeerHTTPAddrs(m map[string]string) Option {
	return func(s *Server) { s.peerHTTP = m }
}

// NewServer returns a Server exposing node's diagnostic state through
// reg (typically metrics.Default), plus the /kv client API and
// /admin/snapshot operator trigger. apiToken is the static bearer token
// every protected route requires (see auth.go); it is normally
// config.Config.APIToken, loaded once at startup -- callers must not
// pass an empty token in production (config.Load's Validate already
// fails fast on that before a Server is ever constructed). It does not
// start listening -- see Start.
//
// Route table (see auth.go for the public/protected split rationale):
//
//	GET  /health          public    -- liveness probe
//	GET  /ready            public    -- readiness probe
//	GET  /metrics          protected
//	GET  /cluster          protected
//	GET/PUT/DELETE /kv/{key} protected
//	POST /admin/snapshot   protected
func NewServer(node *dbnode.Node, reg *metrics.Registry, apiToken string, opts ...Option) *Server {
	s := &Server{node: node, registry: reg, clientID: kvClientIDPrefix + node.ID(), apiToken: apiToken}
	s.reqSeq.Store(uint64(time.Now().UnixNano()))
	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()
	// /health and /ready are deliberately never wrapped in authMiddleware
	// -- see auth.go's doc comment for why.
	mux.Handle("/health", s.instrument("/health", readOnly(s.handleHealth)))
	mux.Handle("/ready", s.instrument("/ready", readOnly(s.handleReady)))
	// Every other route requires authentication. authMiddleware wraps
	// instrument (never the reverse), so a failed auth check returns 401
	// before instrument's logging/metrics recording, and well before the
	// underlying handler, ever run.
	mux.Handle("/metrics", authMiddleware(s.apiToken, s.instrument("/metrics", readOnly(s.handleMetrics))))
	mux.Handle("/cluster", authMiddleware(s.apiToken, s.instrument("/cluster", readOnly(s.handleCluster))))
	mux.Handle("/kv/", authMiddleware(s.apiToken, s.instrument("/kv", s.handleKV)))
	mux.Handle("/admin/snapshot", authMiddleware(s.apiToken, s.instrument("/admin/snapshot", s.handleAdminSnapshot)))

	s.httpSrv = &http.Server{Handler: mux}
	return s
}

// readOnly wraps h so every non-GET request is rejected with 405,
// before h ever runs -- the mechanism that makes every endpoint this
// package registers read-only regardless of what its handler does (see
// the package doc comment).
func readOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed: this endpoint is read-only", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

// statusRecorder captures the HTTP status code a handler wrote, so
// instrument can record it after the handler returns without the
// handler itself needing to report it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// instrument wraps h with the API request counters/latency histogram
// and an access log entry, labeled only by route and a coarse
// status_class ("2xx", "4xx", "5xx") -- never a raw URL, query string,
// or any client-supplied value (see the phase doc's cardinality
// policy). route is this package's own fixed path, never
// r.URL.Path itself.
func (s *Server) instrument(route string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		h(rec, r)
		elapsed := time.Since(start)

		statusClass := strconv.Itoa(rec.status/100) + "xx"
		metrics.APIRequestsTotal.WithLabelValues(r.Method + " " + route + " " + statusClass).Inc()
		metrics.APIRequestLatency.WithLabelValues(route).Observe(elapsed.Seconds())
		if rec.status >= 400 {
			metrics.RecordError("api")
		}
		logging.Default.Debug(logging.EventAPIRequest, "component", "api", "method", r.Method, "route", route, "status", rec.status, "duration_ms", elapsed.Milliseconds())
	})
}

// healthResponse is /health's JSON body: liveness only -- "this process
// is up and able to answer HTTP requests," never anything about Raft
// leadership or storage health (see handleReady for that).
type healthResponse struct {
	Status string `json:"status"`
	NodeID string `json:"node_id"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "alive", NodeID: s.node.ID()})
}

// readyResponse is /ready's JSON body: readiness -- "this node has
// finished initializing and its storage engine is reachable," distinct
// from liveness (see handleHealth) and from Raft leadership (a follower
// is just as ready as a leader; readiness is about this node's own
// ability to serve the class of operations it currently can, not about
// whether it happens to be leader right now).
type readyResponse struct {
	Status string `json:"status"`
	NodeID string `json:"node_id"`
	Reason string `json:"reason,omitempty"`
}

// healthCheckKey is a reserved key readyHandler probes to confirm
// storage is reachable, without ever writing anything -- a missing-key
// result is the expected, healthy outcome; any other error indicates a
// genuine storage fault.
var healthCheckKey = []byte("__forgedb_health_check__")

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	_, err := s.node.Store().Get(healthCheckKey)
	if err != nil && err != storage.ErrKeyNotFound {
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{Status: "not_ready", NodeID: s.node.ID(), Reason: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, readyResponse{Status: "ready", NodeID: s.node.ID()})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(s.registry.Render()))
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Status())
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// Start begins listening on addr (e.g. ":9090", or ":0" for an
// ephemeral port) and serves requests on a background goroutine until
// Shutdown is called. It returns the actual address bound, which
// matters when addr asks for an ephemeral port.
func (s *Server) Start(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	go s.httpSrv.Serve(ln)
	return ln.Addr().String(), nil
}

// Shutdown gracefully stops the server, waiting up to the context's
// deadline for in-flight requests to finish. A failure here never
// affects Raft or storage -- see the phase doc's "observability must
// survive failure" rule; this only tears down the HTTP listener.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}
