package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Kushall-07/forgedb/internal/metrics"
)

// newCORSTestServer builds a Server backed by a real, already-leading
// single node (see newSingleNodeRunning in kv_test.go), configured with
// testAPIToken and allowedOrigins, for exercising cors end to end
// through the real mux (see doRaw in auth_test.go).
func newCORSTestServer(t *testing.T, allowedOrigins []string) *Server {
	t.Helper()
	node := newSingleNodeRunning(t)
	return NewServer(node, metrics.NewRegistry(), testAPIToken, WithCORSOrigins(allowedOrigins))
}

func doWithOrigin(srv *Server, method, path, authHeader, origin string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	srv.httpSrv.Handler.ServeHTTP(rr, req)
	return rr
}

// TestCORS_AllowedOrigin_HeaderReflected confirms a request from a
// configured origin, with a valid token, both succeeds and carries
// Access-Control-Allow-Origin echoing that exact origin back -- never
// "*" (see cors.go's package doc comment).
func TestCORS_AllowedOrigin_HeaderReflected(t *testing.T) {
	const origin = "https://dashboard.example.vercel.app"
	srv := newCORSTestServer(t, []string{origin})

	rr := doWithOrigin(srv, http.MethodGet, "/cluster", "Bearer "+testAPIToken, origin)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, origin)
	}
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Access-Control-Allow-Credentials = %q, want unset", got)
	}
}

// TestCORS_DisallowedOrigin_HeaderOmitted confirms a request from an
// origin that is not in the configured allow-list still reaches the
// handler (CORS is not an authorization boundary -- authMiddleware is
// unaffected), but never gets Access-Control-Allow-Origin, which is
// what causes a real browser to block the response from page script.
func TestCORS_DisallowedOrigin_HeaderOmitted(t *testing.T) {
	srv := newCORSTestServer(t, []string{"https://dashboard.example.vercel.app"})

	rr := doWithOrigin(srv, http.MethodGet, "/cluster", "Bearer "+testAPIToken, "https://evil.example")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (an untrusted Origin must not block the request itself), body = %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want unset for a disallowed origin", got)
	}
}

// TestCORS_NoOriginConfigured_HeaderNeverAdded confirms the default
// (no WithCORSOrigins option, matching this package's pre-Phase-13
// behavior) never adds any Access-Control-Allow-Origin header, even
// when the caller does send an Origin header.
func TestCORS_NoOriginConfigured_HeaderNeverAdded(t *testing.T) {
	srv := newCORSTestServer(t, nil)

	rr := doWithOrigin(srv, http.MethodGet, "/cluster", "Bearer "+testAPIToken, "https://dashboard.example.vercel.app")
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want unset when no origins are configured", got)
	}
}

// TestCORS_Preflight_AllowedOrigin confirms an OPTIONS preflight from
// an allowed origin is answered directly -- 204, no body, the expected
// Allow-Methods/Allow-Headers/Max-Age headers -- and never reaches
// authMiddleware (no Authorization header is sent, matching a real
// browser preflight, yet the response is not 401).
func TestCORS_Preflight_AllowedOrigin(t *testing.T) {
	const origin = "https://dashboard.example.vercel.app"
	srv := newCORSTestServer(t, []string{origin})

	rr := doWithOrigin(srv, http.MethodOptions, "/kv/greeting", "", origin)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, origin)
	}
	if got := rr.Header().Get("Access-Control-Allow-Methods"); got != corsAllowedMethods {
		t.Errorf("Access-Control-Allow-Methods = %q, want %q", got, corsAllowedMethods)
	}
	if got := rr.Header().Get("Access-Control-Allow-Headers"); got != corsAllowedHeaders {
		t.Errorf("Access-Control-Allow-Headers = %q, want %q", got, corsAllowedHeaders)
	}
	if got := rr.Header().Get("Access-Control-Max-Age"); got != corsMaxAgeSeconds {
		t.Errorf("Access-Control-Max-Age = %q, want %q", got, corsMaxAgeSeconds)
	}
	if rr.Body.Len() != 0 {
		t.Errorf("preflight body = %q, want empty", rr.Body.String())
	}
}

// TestCORS_Preflight_DisallowedOrigin confirms an OPTIONS preflight
// from an origin outside the allow-list still gets a plain 204 (so it
// never hangs or errors), but none of the Access-Control-Allow-*
// headers a real browser needs to proceed with the actual request.
func TestCORS_Preflight_DisallowedOrigin(t *testing.T) {
	srv := newCORSTestServer(t, []string{"https://dashboard.example.vercel.app"})

	rr := doWithOrigin(srv, http.MethodOptions, "/kv/greeting", "", "https://evil.example")
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rr.Code)
	}
	for _, h := range []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Methods", "Access-Control-Allow-Headers"} {
		if got := rr.Header().Get(h); got != "" {
			t.Errorf("%s = %q, want unset for a disallowed preflight origin", h, got)
		}
	}
}

// TestCORS_HealthRemainsPublic_AcrossOrigins confirms /health's
// pre-Phase-13 public behavior (200, no token required) is unchanged
// by cors, both with and without a cross-origin caller.
func TestCORS_HealthRemainsPublic_AcrossOrigins(t *testing.T) {
	const origin = "https://dashboard.example.vercel.app"
	srv := newCORSTestServer(t, []string{origin})

	for _, o := range []string{"", origin, "https://evil.example"} {
		rr := doWithOrigin(srv, http.MethodGet, "/health", "", o)
		if rr.Code != http.StatusOK {
			t.Errorf("origin %q: /health status = %d, want 200", o, rr.Code)
		}
	}
}
