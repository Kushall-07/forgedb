package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/statemachine"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// This file is Phase 14's addition to internal/api: a minimal
// client-facing /kv/{key} surface (PUT/GET/DELETE), plus an operator-only
// /admin/snapshot trigger. Before Phase 14, this package was read-only by
// design (see the package doc comment) -- ForgeDB had no client-facing
// write path of its own at all, and cmd/forge-client was an empty
// directory. A real deployment cannot be exercised end to end without
// one, so this adds the smallest possible HTTP surface over
// functionality internal/dbnode already fully implements:
// (*dbnode.Node).Propose for writes and (*dbnode.Node).ConsistentGet for
// linearizable reads (see docs/raft/phase8.5-read-consistency.md). It
// introduces no new consensus, replication, or storage logic whatsoever
// -- every byte written or read here still goes through exactly the same
// Raft commit path and ReadIndex barrier every other caller of
// internal/dbnode already uses.
//
// Every key here is a binary-safe path segment: the part of the URL
// after "/kv/", URL-escaped by the caller as needed. Values are sent and
// received as raw request/response bodies (Content-Type:
// application/octet-stream), not JSON, so a value's bytes are never
// re-encoded.

const (
	// maxValueBytes bounds a PUT request body, matching
	// statemachine.Command's own value-size bound so an oversized body is
	// rejected here, with a clear 413, instead of failing later inside
	// Command.Encode.
	maxValueBytes = 1 << 20 // 1 MiB, matches statemachine.maxValueLen

	// defaultWriteTimeout and defaultReadTimeout bound how long a /kv
	// request waits for its write to be applied (PUT/DELETE) or for a
	// linearizable read barrier to be established and caught up to
	// (GET), on top of whatever deadline the client's own request
	// context already carries. This is a transport/API-level safety net,
	// not a consensus timing parameter -- internal/raft's own election
	// and heartbeat ticking is completely unaffected by it.
	defaultWriteTimeout = 5 * time.Second
	defaultReadTimeout  = 5 * time.Second
)

// kvClientIDPrefix, combined with this node's own ID (see Server.clientID,
// set in NewServer), identifies this API server instance as a Raft client
// for the (ClientID, RequestID) deduplication pair every
// statemachine.Command carries (see docs/raft/phase7-state-machine.md).
//
// The per-node ID is not cosmetic: a real deployment's client API runs
// identically on every node, each with its own independent, in-memory
// RequestID counter starting back at zero whenever that process starts.
// A single ClientID shared across every node (this package's first
// Phase 14 implementation used exactly that -- a bare constant) lets two
// different nodes' counters legitimately reach the same RequestID value
// -- trivially so right after a leader failover, where the new leader's
// counter has typically advanced far less than the old one's had. The
// state machine's deduplication (entirely correct, and unchanged by this
// fix -- see docs/raft/phase7-state-machine.md) then has no way to tell
// that apart from an actual client retry, and silently discards the
// second node's command as a duplicate of the first -- a write reported
// as successful (200 OK, committed, LastApplied advanced) that never
// actually reached storage. Scoping the ClientID to this node's own ID
// makes every (ClientID, RequestID) pair this process can ever produce
// unique across the whole cluster, for the lifetime of that node identity
// (which is itself unique and stable -- see config.Config.NodeID).
//
// That node-ID scoping alone does not close the collision for a *restart*
// of this same node, though: Raft's log (and therefore the state
// machine's replayed dedup history for this exact ClientID) survives a
// process restart on disk, while reqSeq -- an in-memory counter -- does
// not; see Server.reqSeq's doc comment for why NewServer seeds it from
// wall-clock time rather than zero, closing that second collision the
// same way node-ID scoping closed the first.
//
// PUT and DELETE are themselves naturally idempotent at the KV level
// (the last write for a key always wins; deleting an already-deleted key
// is a no-op), so a genuine client retry after a lost response --
// producing a new, un-deduplicated RequestID -- can never corrupt state;
// it just re-applies the same end result.
const kvClientIDPrefix = "forgedb-http-api-"

func (s *Server) nextRequestID() uint64 { return s.reqSeq.Add(1) }

// notLeaderResponse is the body returned (with HTTP 421 Misdirected
// Request -- RFC 7540 section 9.1.2 -- the closest standard status for
// "this node cannot serve the request, but another one can") when a /kv
// write or linearizable read is attempted against a node that is not
// currently the Raft leader. LeaderID is this node's best current
// knowledge (empty if unknown, e.g. mid-election); LeaderHTTP is only
// populated when this node was configured with that peer's HTTP address
// (see config.Config.PeerHTTPAddrs) -- Phase 14 does not invent a new
// client routing protocol beyond this hint (see
// docs/deployment/phase14-docker-deployment.md).
type notLeaderResponse struct {
	Error      string `json:"error"`
	LeaderID   string `json:"leader_id,omitempty"`
	LeaderHTTP string `json:"leader_http,omitempty"`
}

func (s *Server) writeNotLeader(w http.ResponseWriter) {
	_, _, leaderID := s.node.Raft().State()
	resp := notLeaderResponse{Error: "not_leader", LeaderID: leaderID}
	if leaderID != "" {
		resp.LeaderHTTP = s.peerHTTP[leaderID]
	}
	writeJSON(w, http.StatusMisdirectedRequest, resp)
}

// handleKV dispatches PUT/GET/DELETE /kv/{key} by method. Unlike every
// other route in this package, it is intentionally not wrapped in
// readOnly -- this is the one surface Phase 14 adds that is allowed to
// mutate state.
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key: expected /kv/{key}", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handleKVPut(w, r, key)
	case http.MethodGet:
		s.handleKVGet(w, r, key)
	case http.MethodDelete:
		s.handleKVDelete(w, r, key)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request, key string) {
	r.Body = http.MaxBytesReader(w, r.Body, maxValueBytes)
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	cmd := statemachine.NewPutCommand(s.clientID, s.nextRequestID(), []byte(key), value)
	s.proposeAndRespond(w, r, cmd)
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request, key string) {
	cmd := statemachine.NewDeleteCommand(s.clientID, s.nextRequestID(), []byte(key))
	s.proposeAndRespond(w, r, cmd)
}

// proposeAndRespond proposes cmd, waits for it to be applied (not merely
// committed -- see (*dbnode.Node).WaitApplied's doc comment), and writes
// the HTTP response. It never reports success before the command has
// actually reached this node's own state machine.
func (s *Server) proposeAndRespond(w http.ResponseWriter, r *http.Request, cmd statemachine.Command) {
	index, _, err := s.node.Propose(cmd)
	if err != nil {
		if errors.Is(err, dbnode.ErrNotLeader) {
			s.writeNotLeader(w)
			return
		}
		metrics.RecordError("api")
		http.Error(w, "propose failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultWriteTimeout)
	defer cancel()
	if err := s.node.WaitApplied(ctx, index); err != nil {
		http.Error(w, "timed out waiting for write to apply: "+err.Error(), http.StatusGatewayTimeout)
		return
	}

	status := "ok"
	if cmd.Op == statemachine.OpDelete {
		status = "deleted"
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), defaultReadTimeout)
	defer cancel()

	value, err := s.node.ConsistentGet(ctx, []byte(key))
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(value)
	case errors.Is(err, storage.ErrKeyNotFound):
		http.Error(w, "key not found", http.StatusNotFound)
	case errors.Is(err, dbnode.ErrNotLeader):
		s.writeNotLeader(w)
	case errors.Is(err, dbnode.ErrReadUnavailable):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "read_unavailable", "reason": err.Error()})
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		http.Error(w, "timed out establishing a consistent read: "+err.Error(), http.StatusGatewayTimeout)
	default:
		metrics.RecordError("api")
		http.Error(w, "read failed: "+err.Error(), http.StatusInternalServerError)
	}
}

// handleAdminSnapshot triggers a Raft snapshot of this node's current
// state at its own LastApplied index, via (*dbnode.Node).CreateSnapshot
// -- the exact Phase 9 mechanism, unmodified. Phase 9 deliberately has no
// automatic snapshot policy (see docs/raft/phase9-snapshots.md); Phase
// 14 does not add one either. This endpoint exists purely as an
// operational trigger -- for an operator, or for a real-network
// integration test (see docs/deployment/phase14-docker-deployment.md's
// snapshot-catch-up test) that needs to force a snapshot boundary without
// an in-process test harness's direct access to a *dbnode.Node. It works
// on any node, leader or follower alike, since snapshotting is a purely
// local log-compaction operation -- see CreateSnapshot's doc comment.
func (s *Server) handleAdminSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed: use POST", http.StatusMethodNotAllowed)
		return
	}
	index := s.node.Raft().LastApplied()
	if err := s.node.CreateSnapshot(index); err != nil {
		http.Error(w, "create snapshot failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "snapshot_index": index})
}
