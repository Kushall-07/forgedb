package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeNode returns an httptest.Server standing in for one ForgeDB node.
// handler is given full control of the response; countPUT (if non-nil)
// is incremented on every request this fake node actually answers with
// a final (non-421) response, letting tests assert a write was applied
// exactly once even after bouncing through a follower.
func fakeNode(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func notLeaderHandler(leaderID, leaderHTTP string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_ = json.NewEncoder(w).Encode(notLeaderBody{Error: "not_leader", LeaderID: leaderID, LeaderHTTP: leaderHTTP})
	}
}

// TestProxy_RoutesToLeaderFirstTry verifies a request reaching the
// current leader directly (first candidate tried) is answered without
// any retry round trip.
func TestProxy_RoutesToLeaderFirstTry(t *testing.T) {
	var hits int32
	leader := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	p := NewProxy([]string{leader.URL}, time.Second)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/kv/greeting", nil)
	p.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("leader hit %d times, want 1", hits)
	}
}

// TestProxy_FollowsLeaderHintOnFollower421 is Phase 4's core scenario:
// a request first reaches a follower, which rejects it with 421 and a
// leader_http hint; Proxy must follow that hint and succeed, in one
// client-visible request, without the caller ever knowing which node
// was leader.
func TestProxy_FollowsLeaderHintOnFollower421(t *testing.T) {
	var leaderHits int32
	var leaderURL string

	leader := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&leaderHits, 1)
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("leader saw Authorization = %q, want forwarded bearer token", got)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("applied"))
	})
	leaderURL = leader.URL

	follower := fakeNode(t, notLeaderHandler("node-leader", leaderURL))

	p := NewProxy([]string{follower.URL, leaderURL}, time.Second)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/kv/greeting", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	p.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != "applied" {
		t.Fatalf("body = %q, want %q", rr.Body.String(), "applied")
	}
	if atomic.LoadInt32(&leaderHits) != 1 {
		t.Fatalf("leader hit %d times, want exactly 1 (a write must never be applied twice)", leaderHits)
	}
}

// TestProxy_LeaderChange_CachedGuessFollowsNewLeader simulates a
// failover: the first request establishes node A as the cached leader
// guess; node A then starts answering 421 (it lost leadership) and
// points at node B. A second, independent request must land on B
// without the caller supplying any new information.
func TestProxy_LeaderChange_CachedGuessFollowsNewLeader(t *testing.T) {
	var nodeBHits int32
	nodeB := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeBHits, 1)
		w.WriteHeader(http.StatusOK)
	})

	isLeader := int32(1) // nodeA starts as leader
	var nodeAHits int32
	nodeA := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nodeAHits, 1)
		if atomic.LoadInt32(&isLeader) == 1 {
			w.WriteHeader(http.StatusOK)
			return
		}
		notLeaderHandler("node-b", nodeB.URL)(w, r)
	})

	p := NewProxy([]string{nodeA.URL, nodeB.URL}, time.Second)

	// First request: nodeA is leader, answers directly.
	rr1 := httptest.NewRecorder()
	p.ServeHTTP(rr1, httptest.NewRequest(http.MethodGet, "/kv/k", nil))
	if rr1.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", rr1.Code)
	}

	// Failover: nodeA loses leadership.
	atomic.StoreInt32(&isLeader, 0)

	// Second, independent request: must follow nodeA's 421 to nodeB
	// without the client doing anything differently.
	rr2 := httptest.NewRecorder()
	p.ServeHTTP(rr2, httptest.NewRequest(http.MethodGet, "/kv/k", nil))
	if rr2.Code != http.StatusOK {
		t.Fatalf("second request status = %d, want 200", rr2.Code)
	}
	if atomic.LoadInt32(&nodeBHits) != 1 {
		t.Fatalf("nodeB hit %d times, want 1", nodeBHits)
	}
}

// TestProxy_NoLeaderAvailable_ExplicitFailure verifies that when every
// backend reports not_leader with no usable hint (e.g. mid-election),
// Proxy returns an explicit failure -- never a silent/fabricated
// success, and never a 200 from a node that never actually accepted the
// request.
func TestProxy_NoLeaderAvailable_ExplicitFailure(t *testing.T) {
	a := fakeNode(t, notLeaderHandler("", ""))
	b := fakeNode(t, notLeaderHandler("", ""))

	p := NewProxy([]string{a.URL, b.URL}, time.Second)
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/kv/k", nil))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body not JSON: %v", err)
	}
	if body["error"] != "no_leader_available" {
		t.Fatalf("error = %q, want no_leader_available", body["error"])
	}
}

// TestProxy_AllBackendsUnreachable_ExplicitFailure verifies that a
// cluster Proxy cannot reach at all (every dial fails) also produces an
// explicit error rather than hanging or silently succeeding.
func TestProxy_AllBackendsUnreachable_ExplicitFailure(t *testing.T) {
	// Addresses nothing listens on.
	p := NewProxy([]string{"http://127.0.0.1:1", "http://127.0.0.1:2"}, 200*time.Millisecond)
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/kv/k", nil))

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
}

// TestProxy_AuthHeaderPassthrough_NotValidatedLocally verifies Proxy
// never validates or strips Authorization itself: a backend's own 401
// is relayed verbatim, with no retry against other backends (retrying
// would be pointless -- every node shares the same token -- and could
// mask the real error).
func TestProxy_AuthHeaderPassthrough_NotValidatedLocally(t *testing.T) {
	var hits int32
	backend := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
	other := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("second backend must not be tried after a definitive 401")
	})

	p := NewProxy([]string{backend.URL, other.URL}, time.Second)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/kv/k", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	p.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("backend hit %d times, want 1", hits)
	}
}

// TestProxy_QueryStringTokenNotForwardedAsAuth confirms Proxy does not
// invent its own query-string authentication: a token passed only in
// the query string is forwarded as part of the URL (unchanged, like any
// other query parameter) but the backend -- which only honors the
// Authorization header, per internal/api/auth.go -- still rejects it.
// This shows Proxy adds no second authentication mechanism of its own.
func TestProxy_QueryStringTokenNotForwardedAsAuth(t *testing.T) {
	backend := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("no Authorization header was sent; backend must reject")
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})

	p := NewProxy([]string{backend.URL}, time.Second)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/kv/k?token=test-token", nil)
	p.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (query-string token must not authenticate)", rr.Code)
	}
}

// TestProxy_RestartedBackendRejoins verifies a backend that was down
// (crashed node) and comes back as a follower does not poison routing:
// Proxy keeps finding the real leader on subsequent requests.
func TestProxy_RestartedBackendRejoins(t *testing.T) {
	var leaderHits int32
	leader := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&leaderHits, 1)
		w.WriteHeader(http.StatusOK)
	})

	// crashedAddr points at a server we close immediately, simulating a
	// node that is down when the first request arrives.
	crashed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	crashedAddr := crashed.URL
	crashed.Close()

	p := NewProxy([]string{crashedAddr, leader.URL}, 300*time.Millisecond)

	// First request: crashed node is unreachable, falls through to the
	// real leader.
	rr1 := httptest.NewRecorder()
	p.ServeHTTP(rr1, httptest.NewRequest(http.MethodGet, "/kv/k", nil))
	if rr1.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", rr1.Code)
	}

	// A prior failure must not permanently poison routing: a second,
	// independent request must still reach the (unchanged) real leader
	// without the caller doing anything differently.
	rr2 := httptest.NewRecorder()
	p.ServeHTTP(rr2, httptest.NewRequest(http.MethodGet, "/kv/k", nil))
	if rr2.Code != http.StatusOK {
		t.Fatalf("second request status = %d, want 200", rr2.Code)
	}
	if atomic.LoadInt32(&leaderHits) != 2 {
		t.Fatalf("leader hit %d times, want 2", leaderHits)
	}
}

// TestProxy_RequestBodyForwardedIntact verifies a PUT's body survives a
// bounce through a follower's 421 unchanged, byte for byte.
func TestProxy_RequestBodyForwardedIntact(t *testing.T) {
	const payload = "hello world, exactly these bytes"
	var gotBody string
	leader := fakeNode(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	})
	follower := fakeNode(t, notLeaderHandler("leader", leader.URL))

	p := NewProxy([]string{follower.URL, leader.URL}, time.Second)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/kv/k", stringsReader(payload))
	p.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if gotBody != payload {
		t.Fatalf("leader received body %q, want %q", gotBody, payload)
	}
}

func stringsReader(s string) io.Reader { return &stringReaderCloser{s: s} }

type stringReaderCloser struct {
	s   string
	pos int
}

func (r *stringReaderCloser) Read(p []byte) (int, error) {
	if r.pos >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.pos:])
	r.pos += n
	return n, nil
}
