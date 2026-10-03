// Package gateway implements a small, leader-following HTTP reverse
// proxy that makes ForgeDB's client-facing HTTP API resilient to Raft
// leader changes, without teaching the public entrypoint (Caddy) or any
// new component anything about the Raft protocol itself.
//
// Every ForgeDB node already rejects a /kv write or linearizable read it
// cannot serve because it is not the current Raft leader with HTTP 421
// Misdirected Request and a JSON body naming the current leader, when
// known (see internal/api/kv.go's notLeaderResponse). cmd/forge-client
// already follows that hint to find the leader itself, one-shot, from
// the command line. Proxy applies the exact same discovery algorithm,
// but as a persistent server sitting in front of every node, so that
// *every* client -- not just forge-client, and in particular Caddy /
// Cloudflare sitting in front of this process -- gets leader-following
// behavior for free, without ever needing to be told which node is
// currently leader.
//
// Proxy never bypasses internal/api's authentication: it forwards the
// client's Authorization header to the backend exactly as received and
// never inspects, validates, substitutes, or logs it -- the backend's
// own authMiddleware (internal/api/auth.go) remains the sole authority
// on whether a request is authenticated. Proxy also never decides who
// the Raft leader is on its own: it only relays a backend's own
// 421/leader_http answer, and a backend only ever returns that for
// itself, enforced by Raft inside internal/dbnode. A request Proxy
// forwards to a follower is rejected by that follower exactly as if the
// client had reached it directly -- Proxy cannot make a follower accept
// a write, it can only keep looking for a node that will.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// maxBodyBytes bounds how much of an incoming request body Proxy
// buffers in memory in order to be able to retry it against more than
// one backend (an http.Request body is a single-use stream). It is
// intentionally larger than internal/api's own maxValueBytes (1 MiB) so
// that a request already within that node-side limit is never rejected
// here first; an oversized body still gets a clear, explicit 413 from
// Proxy itself rather than an obscure downstream failure.
const maxBodyBytes = 4 << 20 // 4 MiB

// DefaultBackendTimeout bounds how long Proxy waits for a single
// backend's response before treating it as unreachable and trying the
// next candidate. It is independent of, and does not change, any Raft
// election/heartbeat timing or internal/api's own request timeouts --
// this is purely a proxy-hop safety net.
const DefaultBackendTimeout = 5 * time.Second

// maxAttempts bounds how many backends Proxy tries for one incoming
// request before giving up and returning an explicit failure (see
// ServeHTTP), so a persistently unreachable or leaderless cluster can
// never hang a client request forever or loop indefinitely between
// candidates.
const maxAttempts = 8

// excludedHeaders lists headers Proxy never copies verbatim from one
// hop to the next: the true hop-by-hop headers (RFC 7230 section 6.1),
// plus Content-Length and Host, which Go's net/http recomputes
// correctly on its own from the retried request's body and target URL.
// Authorization is deliberately NOT in this list -- it must always be
// forwarded unchanged (see the package doc comment).
var excludedHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding",
	"Te", "Trailer", "Upgrade", "Proxy-Authenticate", "Proxy-Authorization",
	"Content-Length", "Host",
}

// notLeaderBody mirrors internal/api/kv.go's notLeaderResponse -- the
// only coupling Proxy has to that package, and purely a read of an
// already-public wire format, not an import of it.
type notLeaderBody struct {
	Error      string `json:"error"`
	LeaderID   string `json:"leader_id"`
	LeaderHTTP string `json:"leader_http"`
}

// Proxy is a leader-following HTTP reverse proxy over a fixed set of
// ForgeDB nodes' client-facing HTTP addresses. A zero Proxy is not
// usable; construct one with NewProxy.
type Proxy struct {
	backends []string // e.g. "http://forgedb-1:8080", in configured order
	client   *http.Client

	// guess is this Proxy's best current guess at the leader's base
	// URL, used only to pick which backend to try *first* for the next
	// request -- it is never trusted on its own. Every response is
	// still checked for 421 before being treated as final (see
	// ServeHTTP), so a stale or wrong guess costs at most one extra
	// hop, never an incorrect result: a follower that Proxy guesses
	// wrong about still enforces its own not-leader rejection exactly
	// as it would for a direct client request.
	guess atomic.Value // string
}

// NewProxy returns a Proxy that forwards requests to one of backends,
// following each node's own not-leader hint (see the package doc
// comment) until one accepts the request or every candidate has been
// tried. backends must be non-empty -- NewProxy does not validate this
// itself; see config.LoadGateway, which fails fast on an empty list
// before a Proxy is ever constructed. timeout bounds each individual
// backend attempt; a value <= 0 uses DefaultBackendTimeout.
func NewProxy(backends []string, timeout time.Duration) *Proxy {
	if timeout <= 0 {
		timeout = DefaultBackendTimeout
	}
	p := &Proxy{
		backends: append([]string(nil), backends...),
		client: &http.Client{
			Timeout: timeout,
			// Proxy decides for itself, from each response's own status
			// and body, whether to try another backend -- it must never
			// silently follow an HTTP redirect, which would bypass that
			// decision entirely.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	p.guess.Store("")
	return p
}

// ServeHTTP implements http.Handler. It buffers the incoming request
// body (so it can be retried against more than one backend), then tries
// candidate backends -- starting with the last-known-good leader guess,
// then every other configured backend in order -- until one of them
// returns anything other than 421 Misdirected Request, at which point
// that response is relayed to the client verbatim. A follower's 421
// response is never itself returned to the client; it is interpreted
// internally and, when it carries a leader_http hint, that hint is tried
// next. If every candidate is exhausted without a non-421 answer,
// ServeHTTP returns an explicit JSON error -- never a silent success and
// never a response that merely happens to come from an arbitrary node
// (see the package doc comment's "never decides leadership itself").
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := readLimited(r.Body, maxBodyBytes)
	if err != nil {
		writeGatewayError(w, http.StatusRequestEntityTooLarge, "request_too_large", err.Error())
		return
	}

	order := p.candidateOrder()
	tried := make(map[string]bool, len(order))
	var lastErr error
	sawNotLeader := false

	for attempt := 0; attempt < maxAttempts && len(order) > 0; attempt++ {
		addr := order[0]
		order = order[1:]
		if tried[addr] {
			continue
		}
		tried[addr] = true

		status, header, respBody, ferr := p.forward(r.Context(), addr, r, body)
		if ferr != nil {
			lastErr = ferr
			continue
		}

		if status == http.StatusMisdirectedRequest {
			sawNotLeader = true
			if hint := parseLeaderHint(respBody); hint != "" && !tried[hint] {
				p.guess.Store(hint)
				order = append([]string{hint}, order...)
			}
			// No usable hint (e.g. mid-election, or the hint pointed at
			// a candidate already tried): keep going with whatever
			// candidates remain. This single 421 is not yet a final
			// answer for the client.
			continue
		}

		// Any non-421 response -- including a node-level error such as
		// 500 -- is this backend's own final answer for this request;
		// it is not Proxy's place to retry a request a node has
		// already fully handled, which for a write could otherwise mean
		// executing it a second time against a different node.
		p.guess.Store(addr)
		writeResponse(w, status, header, respBody)
		return
	}

	switch {
	case sawNotLeader:
		writeGatewayError(w, http.StatusServiceUnavailable, "no_leader_available",
			"every reachable node reported it is not the current Raft leader, with no usable leader hint (likely mid-election) -- retry shortly")
	case lastErr != nil:
		writeGatewayError(w, http.StatusBadGateway, "no_backend_reachable", lastErr.Error())
	default:
		writeGatewayError(w, http.StatusServiceUnavailable, "no_leader_available", "no configured backend identified itself as the current Raft leader")
	}
}

// candidateOrder returns this Proxy's current backend try-order: the
// cached leader guess first (if any), then every configured backend in
// its original order, the guess itself excluded from that second pass
// to avoid trying it twice back-to-back.
func (p *Proxy) candidateOrder() []string {
	guess, _ := p.guess.Load().(string)
	order := make([]string, 0, len(p.backends)+1)
	if guess != "" {
		order = append(order, guess)
	}
	for _, b := range p.backends {
		if b != guess {
			order = append(order, b)
		}
	}
	return order
}

// forward sends orig (with body substituted for the already-buffered
// body) to addr and returns its response, fully read into memory. An
// error here means addr itself could not be reached at all (dial
// failure, timeout, connection reset) -- never a 4xx/5xx HTTP response,
// which forward reports as a normal, non-error return.
func (p *Proxy) forward(ctx context.Context, addr string, orig *http.Request, body []byte) (status int, header http.Header, respBody []byte, err error) {
	target := strings.TrimRight(addr, "/") + orig.URL.Path
	if orig.URL.RawQuery != "" {
		target += "?" + orig.URL.RawQuery
	}

	var reqBody io.Reader
	if len(body) > 0 {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, orig.Method, target, reqBody)
	if err != nil {
		return 0, nil, nil, err
	}
	copyHeader(req.Header, orig.Header)

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, data, nil
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		if isExcludedHeader(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func isExcludedHeader(name string) bool {
	for _, h := range excludedHeaders {
		if strings.EqualFold(h, name) {
			return true
		}
	}
	return false
}

func writeResponse(w http.ResponseWriter, status int, header http.Header, body []byte) {
	for k, vv := range header {
		if isExcludedHeader(k) {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(status)
	w.Write(body)
}

func parseLeaderHint(body []byte) string {
	var nl notLeaderBody
	if err := json.Unmarshal(body, &nl); err != nil {
		return ""
	}
	hint := strings.TrimSpace(nl.LeaderHTTP)
	if hint == "" {
		return ""
	}
	if !strings.Contains(hint, "://") {
		hint = "http://" + hint
	}
	return hint
}

func writeGatewayError(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "detail": detail})
}

// readLimited reads all of r, failing if it exceeds limit bytes. A nil
// r (no request body at all) returns a nil slice and no error.
func readLimited(r io.Reader, limit int64) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("request body exceeds %d bytes", limit)
	}
	return data, nil
}
