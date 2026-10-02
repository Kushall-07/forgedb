package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Kushall-07/forgedb/internal/dbnode"
	"github.com/Kushall-07/forgedb/internal/metrics"
	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

// newRunningCluster opens a real 2-node dbnode cluster and deterministically
// elects the first node leader by manually ticking only it -- exactly the
// same pattern server_test.go's newTestNode already relies on, and for the
// same reason: this package's raft implementation only advances
// CommitIndex from a peer's AppendEntries reply, so a truly isolated
// single node can never commit, but ticking the *follower* at all would
// let it start its own competing election. Since real wall-clock Run-based
// ticking on both nodes concurrently is inherently racy (two nodes can
// start ticking in lockstep and repeatedly split-vote), only the leader's
// background ticking/applying is started via Run once leadership is
// already established; the follower stays passive, reacting only to the
// leader's RPCs -- enough to keep its own term/leaderID/log in sync, which
// is all the not-leader-redirect tests below need from it.
func newRunningCluster(t *testing.T) (leader, follower *dbnode.Node) {
	t.Helper()
	tr := raft.NewInMemoryTransport()
	base := t.TempDir()

	n0, err := dbnode.Open(dbnode.Config{
		ID:              "kv-test-node",
		Peers:           []string{"kv-test-follower"},
		Transport:       tr,
		RaftDir:         filepath.Join(base, "raft0"),
		KVDir:           filepath.Join(base, "kv0"),
		ElectionTickMin: 3,
		ElectionTickMax: 3,
		HeartbeatTick:   1,
	})
	if err != nil {
		t.Fatalf("dbnode.Open(leader): %v", err)
	}
	n1, err := dbnode.Open(dbnode.Config{
		ID:              "kv-test-follower",
		Peers:           []string{"kv-test-node"},
		Transport:       tr,
		RaftDir:         filepath.Join(base, "raft1"),
		KVDir:           filepath.Join(base, "kv1"),
		ElectionTickMin: 3,
		ElectionTickMax: 3,
		HeartbeatTick:   1,
	})
	if err != nil {
		n0.Close()
		t.Fatalf("dbnode.Open(follower): %v", err)
	}
	t.Cleanup(func() { n0.Close(); n1.Close() })

	for i := 0; i < 4; i++ {
		n0.Tick()
	}
	n0.Drain()
	n1.Drain()
	if !n0.IsLeader() {
		t.Fatalf("leader was not elected")
	}

	n0.Run(5 * time.Millisecond)
	return n0, n1
}

func doKV(srv *Server, method, path string, body []byte) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	srv.httpSrv.Handler.ServeHTTP(rr, req)
	return rr
}

func TestHandleKV_PutGetDelete_EndToEnd(t *testing.T) {
	leader, _ := newRunningCluster(t)
	srv := NewServer(leader, metrics.NewRegistry())

	if rr := doKV(srv, http.MethodPut, "/kv/foo", []byte("bar")); rr.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body = %s", rr.Code, rr.Body.String())
	}

	rr := doKV(srv, http.MethodGet, "/kv/foo", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != "bar" {
		t.Fatalf("GET body = %q, want %q", rr.Body.String(), "bar")
	}

	if rr := doKV(srv, http.MethodDelete, "/kv/foo", nil); rr.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d, body = %s", rr.Code, rr.Body.String())
	}

	rr = doKV(srv, http.MethodGet, "/kv/foo", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d, want 404", rr.Code)
	}
}

func TestHandleKV_Get_MissingKeyIsNotFound(t *testing.T) {
	leader, _ := newRunningCluster(t)
	srv := NewServer(leader, metrics.NewRegistry())

	rr := doKV(srv, http.MethodGet, "/kv/never-written", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
}

func TestHandleKV_MissingKeySegment(t *testing.T) {
	leader, _ := newRunningCluster(t)
	srv := NewServer(leader, metrics.NewRegistry())

	rr := doKV(srv, http.MethodPut, "/kv/", []byte("x"))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestHandleKV_MethodNotAllowed(t *testing.T) {
	leader, _ := newRunningCluster(t)
	srv := NewServer(leader, metrics.NewRegistry())

	rr := doKV(srv, http.MethodPatch, "/kv/foo", nil)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
}

func TestHandleKV_Put_RejectsOversizedBody(t *testing.T) {
	leader, _ := newRunningCluster(t)
	srv := NewServer(leader, metrics.NewRegistry())

	oversized := bytes.Repeat([]byte("x"), maxValueBytes+1)
	rr := doKV(srv, http.MethodPut, "/kv/big", oversized)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rr.Code)
	}
}

func TestHandleKV_NotLeader_ReturnsLeaderHint(t *testing.T) {
	leader, follower := newRunningCluster(t)

	srv := NewServer(follower, metrics.NewRegistry(), WithPeerHTTPAddrs(map[string]string{
		leader.ID(): "forgedb-1:8081",
	}))

	rr := doKV(srv, http.MethodPut, "/kv/foo", []byte("bar"))
	if rr.Code != http.StatusMisdirectedRequest {
		t.Fatalf("PUT on follower status = %d, want 421", rr.Code)
	}
	var resp notLeaderResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v, body = %s", err, rr.Body.String())
	}
	if resp.Error != "not_leader" {
		t.Fatalf("resp.Error = %q, want not_leader", resp.Error)
	}
	if resp.LeaderID != leader.ID() {
		t.Fatalf("resp.LeaderID = %q, want %q", resp.LeaderID, leader.ID())
	}
	if resp.LeaderHTTP != "forgedb-1:8081" {
		t.Fatalf("resp.LeaderHTTP = %q, want forgedb-1:8081", resp.LeaderHTTP)
	}
}

func TestHandleKV_NotLeader_GetAlsoRedirects(t *testing.T) {
	_, follower := newRunningCluster(t)
	srv := NewServer(follower, metrics.NewRegistry())

	rr := doKV(srv, http.MethodGet, "/kv/foo", nil)
	if rr.Code != http.StatusMisdirectedRequest {
		t.Fatalf("GET on follower status = %d, want 421", rr.Code)
	}
}

func TestHandleAdminSnapshot_CreatesSnapshot(t *testing.T) {
	leader, _ := newRunningCluster(t)
	srv := NewServer(leader, metrics.NewRegistry())

	if rr := doKV(srv, http.MethodPut, "/kv/a", []byte("1")); rr.Code != http.StatusOK {
		t.Fatalf("PUT status = %d", rr.Code)
	}

	rr := doKV(srv, http.MethodPost, "/admin/snapshot", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST /admin/snapshot status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if leader.SnapshotIndex() == 0 {
		t.Fatalf("SnapshotIndex() = 0 after snapshot trigger, want > 0")
	}

	// The node's state must still be fully usable after snapshotting.
	if rr := doKV(srv, http.MethodGet, "/kv/a", nil); rr.Code != http.StatusOK || rr.Body.String() != "1" {
		t.Fatalf("GET after snapshot: status=%d body=%q, want 200 \"1\"", rr.Code, rr.Body.String())
	}
}

// TestClientID_UniquePerNode_PreventsCrossNodeDedupCollision is a direct
// regression test for a real defect found while smoke-testing Phase 14
// across genuinely separate OS processes: if every node's /kv API used
// the same constant ClientID (as this package's first implementation
// did), two different nodes' independently-counted RequestID sequences
// could legitimately collide -- trivially so right after a leader
// failover, when the new leader's counter has advanced far less than the
// old leader's had. The state machine's (ClientID, RequestID)
// deduplication (entirely correct on its own -- see
// docs/raft/phase7-state-machine.md) then indistinguishably treated the
// second node's write as a duplicate of the first and silently discarded
// it, even though the HTTP response reported 200 OK and the entry was
// genuinely committed and applied: a real multi-process repro put "v1"
// through node-1, failed node-1 over to node-2, PUT "v2" through the new
// leader node-2, and read back the stale "v1".
//
// This reproduces the same (ClientID, RequestID) collision directly: both
// commands are proposed through the one real leader (standing in for "two
// different nodes, each leader at a different time"), using each
// Server's own real clientID and the identical RequestID 1 -- exactly
// what a failover can produce once each node's counter restarts at zero.
func TestClientID_UniquePerNode_PreventsCrossNodeDedupCollision(t *testing.T) {
	leader, follower := newRunningCluster(t)
	leaderSrv := NewServer(leader, metrics.NewRegistry())
	followerSrv := NewServer(follower, metrics.NewRegistry())

	if leaderSrv.clientID == followerSrv.clientID {
		t.Fatalf("two different nodes' API servers produced the same clientID %q -- the exact collision this test guards against", leaderSrv.clientID)
	}

	ctx := context.Background()
	index1, _, err := leader.Propose(statemachine.NewPutCommand(leaderSrv.clientID, 1, []byte("k"), []byte("from-node-a")))
	if err != nil {
		t.Fatalf("Propose (node a): %v", err)
	}
	if err := leader.WaitApplied(ctx, index1); err != nil {
		t.Fatalf("WaitApplied (node a): %v", err)
	}

	index2, _, err := leader.Propose(statemachine.NewPutCommand(followerSrv.clientID, 1, []byte("k"), []byte("from-node-b")))
	if err != nil {
		t.Fatalf("Propose (node b): %v", err)
	}
	if err := leader.WaitApplied(ctx, index2); err != nil {
		t.Fatalf("WaitApplied (node b): %v", err)
	}

	value, err := leader.Store().Get([]byte("k"))
	if err != nil {
		t.Fatalf("Store().Get: %v", err)
	}
	if string(value) != "from-node-b" {
		t.Fatalf("value = %q after both writes, want %q -- the second node's write was silently deduplicated against the first", value, "from-node-b")
	}
}

func TestHandleAdminSnapshot_RejectsNonPost(t *testing.T) {
	leader, _ := newRunningCluster(t)
	srv := NewServer(leader, metrics.NewRegistry())

	rr := doKV(srv, http.MethodGet, "/admin/snapshot", nil)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
}
