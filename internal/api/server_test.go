package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

// testAPIToken is the fixed bearer token every test in this package
// configures its Server(s) with (see NewServer's apiToken parameter and
// auth.go). Shared across server_test.go and kv_test.go.
const testAPIToken = "test-token"

func withAuth(req *http.Request) *http.Request {
	req.Header.Set("Authorization", "Bearer "+testAPIToken)
	return req
}

// newTestNode opens a 2-node dbnode.Node cluster and elects the first
// node leader, returning it for the Server under test. A genuine
// zero-peer single-node cluster is deliberately avoided here: this
// package's raft implementation only ever advances CommitIndex from a
// peer's AppendEntries reply (see maybeAdvanceCommitIndexLocked and
// internal/raft/apply_test.go's commitUpTo, which documents the same
// constraint), so a proposal on a truly peerless node would never
// commit. This package tests its HTTP handlers against a real,
// multi-node cluster end to end -- no fake/mock Node implementation.
func newTestNode(t *testing.T) *dbnode.Node {
	t.Helper()
	tr := raft.NewInMemoryTransport()
	base := t.TempDir()

	n0, err := dbnode.Open(dbnode.Config{
		ID:              "api-test-node",
		Peers:           []string{"api-test-follower"},
		Transport:       tr,
		RaftDir:         filepath.Join(base, "raft0"),
		KVDir:           filepath.Join(base, "kv0"),
		ElectionTickMin: 3,
		ElectionTickMax: 3,
		HeartbeatTick:   1,
	})
	if err != nil {
		t.Fatalf("dbnode.Open(node0): %v", err)
	}
	t.Cleanup(func() { n0.Close() })

	n1, err := dbnode.Open(dbnode.Config{
		ID:              "api-test-follower",
		Peers:           []string{"api-test-node"},
		Transport:       tr,
		RaftDir:         filepath.Join(base, "raft1"),
		KVDir:           filepath.Join(base, "kv1"),
		ElectionTickMin: 3,
		ElectionTickMax: 3,
		HeartbeatTick:   1,
	})
	if err != nil {
		t.Fatalf("dbnode.Open(node1): %v", err)
	}
	t.Cleanup(func() { n1.Close() })

	for i := 0; i < 4; i++ {
		n0.Tick()
	}
	n0.Drain()
	n1.Drain()
	if !n0.IsLeader() {
		t.Fatalf("test node did not become leader")
	}
	return n0
}

func TestHandleHealth_AlwaysOK(t *testing.T) {
	n := newTestNode(t)
	srv := NewServer(n, metrics.NewRegistry(), testAPIToken)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	srv.httpSrv.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "alive" {
		t.Errorf("Status = %q, want alive", body.Status)
	}
}

func TestHandleReady_OKWhenStorageReachable(t *testing.T) {
	n := newTestNode(t)
	srv := NewServer(n, metrics.NewRegistry(), testAPIToken)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	srv.httpSrv.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleCluster_ReportsLeaderAndTerm(t *testing.T) {
	n := newTestNode(t)
	if _, _, err := n.Propose(statemachine.NewPutCommand("c1", 1, []byte("k"), []byte("v"))); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.Drain()
	if _, err := n.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}

	srv := NewServer(n, metrics.NewRegistry(), testAPIToken)
	rr := httptest.NewRecorder()
	req := withAuth(httptest.NewRequest(http.MethodGet, "/cluster", nil))
	srv.httpSrv.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var st dbnode.Status
	if err := json.Unmarshal(rr.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if st.Raft.Role != "Leader" {
		t.Errorf("Raft.Role = %q, want Leader", st.Raft.Role)
	}
	if st.Raft.CommitIndex != 1 {
		t.Errorf("Raft.CommitIndex = %d, want 1", st.Raft.CommitIndex)
	}
	if st.Storage.MemTableEntries != 1 {
		t.Errorf("Storage.MemTableEntries = %d, want 1", st.Storage.MemTableEntries)
	}
}

func TestHandleMetrics_RendersRegisteredMetrics(t *testing.T) {
	n := newTestNode(t)
	reg := metrics.NewRegistry()
	c := reg.NewCounter("forgedb_test_marker_total", "a marker metric for this test")
	c.Inc()

	srv := NewServer(n, reg, testAPIToken)
	rr := httptest.NewRecorder()
	req := withAuth(httptest.NewRequest(http.MethodGet, "/metrics", nil))
	srv.httpSrv.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "forgedb_test_marker_total 1") {
		t.Fatalf("response missing expected metric, got:\n%s", rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain prefix", ct)
	}
}

func TestEndpoints_RejectNonGET(t *testing.T) {
	n := newTestNode(t)
	srv := NewServer(n, metrics.NewRegistry(), testAPIToken)

	for _, route := range []string{"/health", "/ready", "/metrics", "/cluster"} {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
			rr := httptest.NewRecorder()
			req := withAuth(httptest.NewRequest(method, route, nil))
			srv.httpSrv.Handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: status = %d, want 405", method, route, rr.Code)
			}
		}
	}
}

func TestEndpoints_NeverMutateNodeState(t *testing.T) {
	n := newTestNode(t)
	before := n.Status()

	srv := NewServer(n, metrics.NewRegistry(), testAPIToken)
	for i := 0; i < 5; i++ {
		for _, route := range []string{"/health", "/ready", "/metrics", "/cluster"} {
			rr := httptest.NewRecorder()
			req := withAuth(httptest.NewRequest(http.MethodGet, route, nil))
			srv.httpSrv.Handler.ServeHTTP(rr, req)
		}
	}

	after := n.Status()
	if before.Raft.Term != after.Raft.Term || before.Raft.CommitIndex != after.Raft.CommitIndex || before.Raft.Role != after.Raft.Role {
		t.Fatalf("node state changed after diagnostic requests: before=%+v after=%+v", before.Raft, after.Raft)
	}
}

func TestInstrument_RecordsAPIMetrics(t *testing.T) {
	n := newTestNode(t)
	reg := metrics.NewRegistry()
	srv := NewServer(n, reg, testAPIToken)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	srv.httpSrv.Handler.ServeHTTP(rr, req)

	// instrument records into the shared global metrics.Default registry
	// (see server.go), not the per-test reg passed to NewServer -- that
	// separation (API request bookkeeping is process-wide; the rendered
	// catalog is caller-supplied) is intentional, so this checks the
	// global counter instead.
	out := metrics.Default.Render()
	if !strings.Contains(out, `forgedb_api_requests_total{key="GET /health 2xx"}`) {
		t.Fatalf("global API metrics missing GET /health 2xx entry, got:\n%s", out)
	}
}
