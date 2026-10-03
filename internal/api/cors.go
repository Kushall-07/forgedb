package api

import "net/http"

// This file is Phase 13's addition to internal/api: a minimal,
// configuration-driven CORS layer, added because the dashboard
// consuming this API is deployed on its own origin (e.g. a separate
// Vercel domain), not the same origin as the public API edge --
// without it, a browser refuses to let the dashboard's own JavaScript
// read any response from here at all, even a successful,
// well-authenticated one. It introduces no new consensus, storage, or
// authentication logic: authMiddleware (see auth.go) remains the sole
// authority on whether a request is authenticated, completely
// unaffected by anything in this file.
//
// CORS is a browser-enforced read restriction, not a server-side
// authorization mechanism -- a non-browser caller (curl,
// forge-client, forge-gateway, a server-to-server integration) never
// sends a preflight and is never affected by this file either way.
// Accordingly, cors never rejects a request on the grounds that its
// Origin is missing or not allowed: it only decides whether to add the
// response headers that let a *browser* expose the result to page
// script, and, for an OPTIONS preflight specifically, answers that
// preflight directly (see below) since a preflight never carries the
// dashboard's Authorization header by design and must not be routed
// through authMiddleware.
//
// Deliberate scope decisions:
//
//   - Only origins in Server.corsOrigins (configured via
//     WithCORSOrigins / FORGEDB_API_TOKEN's sibling env var
//     FORGEDB_CORS_ORIGINS -- see config.Config.CORSOrigins) are ever
//     reflected into Access-Control-Allow-Origin, as an exact string
//     match against the request's Origin header. There is no
//     wildcard ("*") support and no suffix/substring matching -- the
//     empty default allows nothing, which is this package's
//     pre-Phase-13 behavior (no CORS headers at all).
//   - Access-Control-Allow-Credentials is never set. The dashboard
//     authenticates with an explicit Authorization header it attaches
//     itself (see internal/gateway and docs/deployment), never
//     cookies, so this API has no need for -- and never grants --
//     credentialed cross-origin requests.
//   - Access-Control-Expose-Headers is never set. Nothing in this
//     package's responses carries the API token or any other secret
//     in a header, and the dashboard only ever needs the response
//     body (JSON or the raw KV bytes), so there is nothing to
//     deliberately expose.
//   - A disallowed or absent Origin never changes a request's
//     outcome: the wrapped handler still runs (for anything but
//     OPTIONS) and authMiddleware still enforces the bearer token
//     exactly as if this file did not exist. Only a browser,
//     enforcing the missing Access-Control-Allow-Origin header on its
//     own, is the thing that actually blocks that case.
const (
	// corsAllowedMethods lists every HTTP method any route in this
	// package accepts (GET across /health, /ready, /metrics, /cluster,
	// /kv/{key}; PUT/DELETE additionally on /kv/{key}; POST on
	// /admin/snapshot), plus OPTIONS itself. It is returned verbatim on
	// every allowed preflight, rather than computed per route, since a
	// preflight is answered before the mux's own per-route method
	// checks (readOnly, handleKV's switch) ever run.
	corsAllowedMethods = "GET, PUT, POST, DELETE, OPTIONS"

	// corsAllowedHeaders lists every request header the dashboard
	// sends that is not already implicitly allowed by the Fetch
	// specification's CORS-safelisted-request-header list: Authorization
	// (every protected route) and Content-Type (PUT/DELETE's
	// application/octet-stream body -- see kv.go). Accept is included
	// defensively even though the dashboard's current Accept value
	// (application/json) is itself safelisted, so a future change to a
	// non-safelisted Accept value does not silently break preflight.
	corsAllowedHeaders = "Authorization, Content-Type, Accept"

	// corsMaxAgeSeconds bounds how long a browser may cache a
	// preflight's answer before sending a fresh one, purely to reduce
	// preflight round trips -- it has no bearing on authentication
	// (the bearer token is still checked on every actual request) or
	// on how long an allowed-origins list change takes effect for a
	// *new* preflight.
	corsMaxAgeSeconds = "600"
)

// cors wraps h so that:
//   - every response (including one this middleware answers itself)
//     carries "Vary: Origin", so a cache sitting in front of this
//     server never serves one Origin's CORS-headered response to a
//     different Origin;
//   - a request whose Origin header exactly matches one of
//     Server.corsOrigins gets Access-Control-Allow-Origin echoing
//     that exact Origin back (never "*" -- see the package doc
//     comment);
//   - an OPTIONS request (a browser's CORS preflight) is answered
//     directly, with no body and without ever calling h -- a
//     preflight by definition cannot carry the dashboard's
//     Authorization header, so it must never reach authMiddleware.
//
// Every other request (GET/PUT/DELETE/POST, matching Origin or not)
// is passed through to h unchanged; cors never itself produces a 401,
// 403, or any other rejection -- see the package doc comment for why.
func (s *Server) cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")

		origin := r.Header.Get("Origin")
		allowed := origin != "" && s.isAllowedOrigin(origin)
		if allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}

		if r.Method == http.MethodOptions {
			if allowed {
				w.Header().Set("Access-Control-Allow-Methods", corsAllowedMethods)
				w.Header().Set("Access-Control-Allow-Headers", corsAllowedHeaders)
				w.Header().Set("Access-Control-Max-Age", corsMaxAgeSeconds)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}

		h.ServeHTTP(w, r)
	})
}

// isAllowedOrigin reports whether origin exactly matches one of
// Server.corsOrigins. Matching is an exact string comparison -- an
// Origin header is always a full "scheme://host[:port]" value with no
// path, so there is nothing to normalize here; the configured list
// (see config.Config.CORSOrigins) is expected to already be in that
// same form.
func (s *Server) isAllowedOrigin(origin string) bool {
	for _, o := range s.corsOrigins {
		if o == origin {
			return true
		}
	}
	return false
}
