package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// This file is Phase E's addition to internal/api: a single, reusable
// static-token authentication middleware for the client-facing HTTP
// surface, added because the Phase 14 deployment can now be reached
// through a public HTTPS tunnel rather than only a private Docker
// network -- see docs/deployment/phase14-docker-deployment.md's
// authentication section. It introduces no new consensus, storage, or
// state-machine logic, and no new authentication/session subsystem:
// just one http.Handler wrapper, applied once per route in NewServer
// (see server.go), that checks a single static bearer token before the
// wrapped handler ever runs.
//
// Deliberate scope decisions:
//
//   - /health and /ready stay public (unwrapped). They are
//     liveness/readiness probes consumed by infrastructure (Docker
//     HEALTHCHECK, orchestrator probes) that has no way to carry a
//     bearer token, and they leak nothing sensitive -- only "is this
//     process up" / "can it reach its own storage" (see their handlers
//     in server.go). Every other route -- /metrics, /cluster, /kv/*,
//     and /admin/snapshot -- requires authentication, since each
//     exposes either cluster state or the client-facing read/write
//     surface itself.
//   - Only the "Authorization: Bearer <token>" scheme is accepted.
//     Query-parameter tokens (e.g. "?token=...") and cookies are never
//     consulted -- a query string is routinely captured in proxy/access
//     logs, which would otherwise leak the token through a channel this
//     middleware has no control over.
//   - The configured token is compared with crypto/subtle.
//     ConstantTimeCompare, not "==", so a wrong guess cannot be
//     distinguished by timing a byte-by-byte comparison short-circuit.
//   - On any failure (missing header, wrong scheme, empty credential, or
//     a non-matching token) this responds 401 and returns immediately,
//     before the wrapped handler -- and therefore before instrument's
//     logging/metrics recording -- ever runs (see server.go's route
//     table: authMiddleware wraps instrument, never the reverse). The
//     response body is a fixed, generic string; neither it, nor any log
//     line this middleware itself emits, ever includes the configured
//     token, the credential the caller supplied, or the raw
//     Authorization header value.

const bearerScheme = "Bearer"

// authMiddleware returns h wrapped so that every request must present
// "Authorization: Bearer <token>" matching token exactly (constant-time)
// before h runs. token is supplied once by NewServer (ultimately from
// config.Config.APIToken) and is never re-read from the environment
// here -- see config.Load's doc comment on why that lookup happens
// exactly once, at startup.
func authMiddleware(token string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !bearerTokenValid(r, token) {
			w.Header().Set("WWW-Authenticate", bearerScheme)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// bearerTokenValid reports whether r carries an "Authorization: Bearer
// <credential>" header whose credential matches token exactly, under a
// constant-time comparison. Any other scheme (Basic, or anything else),
// a missing header, a missing/empty credential, or a mismatched
// credential all return false -- the caller (authMiddleware) treats
// every one of those identically as 401, so this never needs to
// distinguish "malformed" from "wrong" for the response it sends.
func bearerTokenValid(r *http.Request, token string) bool {
	auth := r.Header.Get("Authorization")
	scheme, credential, ok := strings.Cut(auth, " ")
	if !ok || !strings.EqualFold(scheme, bearerScheme) || credential == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(credential), []byte(token)) == 1
}
