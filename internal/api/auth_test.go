package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Kushall-07/forgedb/internal/metrics"
)

// newAuthTestServer builds a Server backed by a real, already-leading
// single node (see newSingleNodeRunning in kv_test.go), configured with
// testAPIToken, for exercising authMiddleware end to end through the
// real mux rather than calling bearerTokenValid directly.
func newAuthTestServer(t *testing.T) *Server {
	t.Helper()
	node := newSingleNodeRunning(t)
	return NewServer(node, metrics.NewRegistry(), testAPIToken)
}

func doRaw(srv *Server, method, path, authHeader string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	srv.httpSrv.Handler.ServeHTTP(rr, req)
	return rr
}

func TestAuth_MissingHeader_Returns401(t *testing.T) {
	srv := newAuthTestServer(t)
	rr := doRaw(srv, http.MethodGet, "/cluster", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
}

func TestAuth_MalformedHeader_Returns401(t *testing.T) {
	srv := newAuthTestServer(t)
	cases := []string{
		"Bearer",                // no credential at all
		"Bearer ",               // empty credential
		"Basic " + testAPIToken, // wrong scheme
		"Bearer" + testAPIToken, // missing separating space
		testAPIToken,            // no scheme at all
	}
	for _, header := range cases {
		rr := doRaw(srv, http.MethodGet, "/cluster", header)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("header %q: status = %d, want 401", header, rr.Code)
		}
	}
}

func TestAuth_WrongToken_Returns401(t *testing.T) {
	srv := newAuthTestServer(t)
	rr := doRaw(srv, http.MethodGet, "/cluster", "Bearer not-the-real-token")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
}

func TestAuth_CorrectToken_HandlerExecutes(t *testing.T) {
	srv := newAuthTestServer(t)
	rr := doRaw(srv, http.MethodGet, "/cluster", "Bearer "+testAPIToken)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", rr.Code, rr.Body.String())
	}
}

func TestAuth_HealthRemainsPublic(t *testing.T) {
	srv := newAuthTestServer(t)
	rr := doRaw(srv, http.MethodGet, "/health", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("/health without a token: status = %d, want 200", rr.Code)
	}
}

func TestAuth_ReadyRemainsPublic(t *testing.T) {
	srv := newAuthTestServer(t)
	rr := doRaw(srv, http.MethodGet, "/ready", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("/ready without a token: status = %d, want 200", rr.Code)
	}
}

// TestAuth_ProtectedHandlerNotExecutedOnFailure proves authMiddleware
// returns before the wrapped handler runs, not merely that the response
// code is 401: it calls /admin/snapshot (whose handler, if executed,
// would advance the node's snapshot index) with no token and then
// confirms no snapshot was taken.
func TestAuth_ProtectedHandlerNotExecutedOnFailure(t *testing.T) {
	node := newSingleNodeRunning(t)
	srv := NewServer(node, metrics.NewRegistry(), testAPIToken)

	rr := doRaw(srv, http.MethodPost, "/admin/snapshot", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
	if node.SnapshotIndex() != 0 {
		t.Fatalf("SnapshotIndex() = %d, want 0 -- handleAdminSnapshot must not run when authentication fails", node.SnapshotIndex())
	}
}

// TestAuth_KVHandlerNotExecutedOnFailure is the /kv analogue: a PUT
// without a valid token must never reach storage.
func TestAuth_KVHandlerNotExecutedOnFailure(t *testing.T) {
	node := newSingleNodeRunning(t)
	srv := NewServer(node, metrics.NewRegistry(), testAPIToken)

	rr := doRaw(srv, http.MethodPut, "/kv/should-not-be-written", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}

	// Confirm with the correct token that the key was never written.
	getRR := doKV(srv, http.MethodGet, "/kv/should-not-be-written", nil)
	if getRR.Code != http.StatusNotFound {
		t.Fatalf("GET after unauthenticated PUT: status = %d, want 404 (write must not have reached storage)", getRR.Code)
	}
}

func TestAuth_TokenNeverExposedInResponse(t *testing.T) {
	srv := newAuthTestServer(t)

	for _, rr := range []*httptest.ResponseRecorder{
		doRaw(srv, http.MethodGet, "/cluster", ""),
		doRaw(srv, http.MethodGet, "/cluster", "Bearer wrong-token"),
	} {
		if strings.Contains(rr.Body.String(), testAPIToken) {
			t.Fatalf("response body leaked the configured token: %s", rr.Body.String())
		}
		for _, values := range rr.Header() {
			for _, v := range values {
				if strings.Contains(v, testAPIToken) {
					t.Fatalf("response header leaked the configured token: %s", v)
				}
			}
		}
	}
}

// TestAuth_QueryParameterTokenIgnored confirms the only supported
// mechanism is the Authorization header -- a token supplied as a query
// parameter must not authenticate the request.
func TestAuth_QueryParameterTokenIgnored(t *testing.T) {
	srv := newAuthTestServer(t)
	rr := doRaw(srv, http.MethodGet, "/cluster?token="+testAPIToken, "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (query-parameter token must be ignored)", rr.Code)
	}
}
