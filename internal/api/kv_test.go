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
	"github.com/Kushall-07/forgedb/internal/storage"
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

// newSingleNodeRunning opens a literal one-node (zero-peer) dbnode.Node --
// the exact topology the live /kv regression this file's tests below
// reproduce was found on (NODE_ID=node-1, no peers) -- elects it leader
// deterministically (manual Tick/Drain; a zero-peer node never risks a
// split vote, so no InMemoryTransport juggling is needed the way
// newRunningCluster requires), then starts it the same way production
// does (Run, driving both background ticking and the Applier).
func newSingleNodeRunning(t *testing.T) *dbnode.Node {
	t.Helper()
	tr := raft.NewInMemoryTransport()
	base := t.TempDir()

	n, err := dbnode.Open(dbnode.Config{
		ID:              "solo",
		Transport:       tr,
		RaftDir:         filepath.Join(base, "raft"),
		KVDir:           filepath.Join(base, "kv"),
		ElectionTickMin: 2,
		ElectionTickMax: 2,
		HeartbeatTick:   1,
	})
	if err != nil {
		t.Fatalf("dbnode.Open: %v", err)
	}
	t.Cleanup(func() { n.Close() })

	n.Tick()
	n.Tick()
	n.Drain()
	if !n.IsLeader() {
		t.Fatalf("single-node cluster did not elect itself leader")
	}

	n.Run(5 * time.Millisecond)
	return n
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

// --- Single-node /kv regression: committed+applied writes must actually
// reach storage and be readable -----------------------------------------
//
// These reproduce a live defect found running a real single-node backend
// (NODE_ID=node-1, no peers): PUT reported 200 OK, CommitIndex and
// LastApplied both advanced, yet GET returned "key not found" for the
// keys just written, and /cluster's memtable_entries stayed stuck at 1
// despite multiple distinct successful-looking PUTs.
//
// The root cause is not the Raft commit path (already fixed separately
// for the single-node/zero-peer case -- see internal/raft/replication.go)
// and not any storage- or key-encoding bug: PUT and GET use the exact
// same key bytes, the same storage.Store instance (dbnode.Node's n.store,
// read directly by both ConsistentGet and Status/Stats -- see
// internal/dbnode/node.go and observability.go), and there is no second
// cache or store anywhere in the path.
//
// It is a (ClientID, RequestID) deduplication collision in
// internal/statemachine.KVStateMachine.Apply: Server.reqSeq (internal/api)
// is an in-memory counter that restarts at zero every time a new Server
// is constructed (in production, every process restart), but Raft's
// persisted log -- and therefore this exact ClientID's dedup history,
// rebuilt by replaying that log into the state machine on every startup,
// since LastApplied is deliberately never persisted (see
// internal/raft/apply.go) -- survives across that same restart. A
// post-restart RequestID of 1 reads as *stale* (lower than this
// ClientID's last pre-restart RequestID), which the state machine
// resolves deterministically and "applies" (Raft considers the entry
// fully committed and applied -- correctly, per its own contract) without
// ever calling Store.Put/Delete. proposeAndRespond (kv.go) only waits for
// WaitApplied to confirm the entry's Raft index was applied; it has no
// way to see that the *logical* outcome was ErrStaleRequest rather than a
// real mutation, so it reports 200 OK regardless. The fix (Server.reqSeq's
// doc comment, NewServer) seeds reqSeq from wall-clock time instead of
// zero, so a fresh Server's RequestIDs are (for any realistic request
// rate) always higher than anything this ClientID used in a previous
// process lifetime -- closing this collision the same way scoping
// ClientID to the node's own ID already closes the analogous cross-node
// collision at failover (see TestClientID_UniquePerNode_PreventsCrossNodeDedupCollision
// above).

// TestKV_RestartDoesNotCollideRequestIDWithPriorHistory is the direct
// repro: it commits a long run of prior writes under one Server's
// clientID (standing in for "this node's own prior process lifetime"),
// then constructs a brand new Server over the *same* node -- the exact
// shape of a real process restart, since dbnode.Node (and the Raft log
// backing it) is what actually persists, while a Server is recreated from
// scratch -- and checks that a write through the new Server still reaches
// storage instead of being silently resolved as stale.
func TestKV_RestartDoesNotCollideRequestIDWithPriorHistory(t *testing.T) {
	node := newSingleNodeRunning(t)
	srv1 := NewServer(node, metrics.NewRegistry())

	for i := uint64(1); i <= 20; i++ {
		idx, _, err := node.Propose(statemachine.NewPutCommand(srv1.clientID, i, []byte("warmup"), []byte("x")))
		if err != nil {
			t.Fatalf("Propose warmup #%d: %v", i, err)
		}
		if err := node.WaitApplied(context.Background(), idx); err != nil {
			t.Fatalf("WaitApplied warmup #%d: %v", i, err)
		}
	}

	srv2 := NewServer(node, metrics.NewRegistry())
	if srv2.clientID != srv1.clientID {
		t.Fatalf("clientID changed across the simulated restart: %q vs %q -- this test no longer reproduces the real scenario", srv1.clientID, srv2.clientID)
	}

	if rr := doKV(srv2, http.MethodPut, "/kv/after-restart", []byte("hello")); rr.Code != http.StatusOK {
		t.Fatalf("PUT after restart status = %d, body = %s", rr.Code, rr.Body.String())
	}
	rr := doKV(srv2, http.MethodGet, "/kv/after-restart", nil)
	if rr.Code != http.StatusOK || rr.Body.String() != "hello" {
		t.Fatalf("GET after restart: status=%d body=%q, want 200 %q -- the write was silently dropped as a stale duplicate of this ClientID's pre-restart RequestID history", rr.Code, rr.Body.String(), "hello")
	}
}

// TestHandleKV_SingleNodeCluster_MultipleKeysRemainReadable is the
// end-to-end live repro: a true one-node (zero-peer) cluster, driven
// through the real HTTP handlers exactly as the live bug report was,
// writing two distinct keys and confirming both -- not just the last one
// written -- are independently readable, that storage's live entry count
// reflects both, and that a DELETE removes exactly the deleted key.
func TestHandleKV_SingleNodeCluster_MultipleKeysRemainReadable(t *testing.T) {
	node := newSingleNodeRunning(t)
	srv := NewServer(node, metrics.NewRegistry())

	if rr := doKV(srv, http.MethodPut, "/kv/test:dashboard", []byte("hello-forgedb")); rr.Code != http.StatusOK {
		t.Fatalf("PUT test:dashboard status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if rr := doKV(srv, http.MethodPut, "/kv/testkey", []byte("hello")); rr.Code != http.StatusOK {
		t.Fatalf("PUT testkey status = %d, body = %s", rr.Code, rr.Body.String())
	}

	if rr := doKV(srv, http.MethodGet, "/kv/test:dashboard", nil); rr.Code != http.StatusOK || rr.Body.String() != "hello-forgedb" {
		t.Fatalf("GET test:dashboard: status=%d body=%q, want 200 %q", rr.Code, rr.Body.String(), "hello-forgedb")
	}
	if rr := doKV(srv, http.MethodGet, "/kv/testkey", nil); rr.Code != http.StatusOK || rr.Body.String() != "hello" {
		t.Fatalf("GET testkey: status=%d body=%q, want 200 %q", rr.Code, rr.Body.String(), "hello")
	}

	ms, ok := node.Store().(*storage.MemStore)
	if !ok {
		t.Fatalf("node.Store() is not a *storage.MemStore")
	}
	if got := ms.Stats().MemTableEntries; got != 2 {
		t.Fatalf("MemTableEntries = %d, want 2 after two distinct live PUTs", got)
	}

	if rr := doKV(srv, http.MethodDelete, "/kv/testkey", nil); rr.Code != http.StatusOK {
		t.Fatalf("DELETE testkey status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if rr := doKV(srv, http.MethodGet, "/kv/testkey", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("GET testkey after DELETE: status = %d, want 404", rr.Code)
	}
	if rr := doKV(srv, http.MethodGet, "/kv/test:dashboard", nil); rr.Code != http.StatusOK || rr.Body.String() != "hello-forgedb" {
		t.Fatalf("GET test:dashboard after deleting testkey: status=%d body=%q, want 200 %q (DELETE must not touch other keys)", rr.Code, rr.Body.String(), "hello-forgedb")
	}
	if got := ms.Stats().MemTableEntries; got != 1 {
		t.Fatalf("MemTableEntries = %d, want 1 after deleting one of two live keys", got)
	}
}
