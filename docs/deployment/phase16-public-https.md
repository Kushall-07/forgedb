# Phase 16 — Stable HTTPS Public Endpoint

This document is the design writeup for Phase 16: replacing the
temporary Cloudflare Quick Tunnel in front of Caddy with a stable DNS
hostname and a Caddy-terminated HTTPS certificate, so the public
endpoint stops changing on every tunnel restart. For the hands-on
operator guide, see
[`deploy/README.md`](../../deploy/README.md#18-public-https-endpoint-caddy).

Like Phase 13, this phase touches **only** the deployment/TLS/public-
entrypoint layer. It does not change Raft, the storage engine, the
state machine, `forge-gateway`'s leader-following algorithm, request
deduplication, or authentication semantics -- `FORGEDB_API_TOKEN` is
still checked exactly where it always was
(`internal/api/auth.go`, unmodified).

## 1. The problem

Phase 13's threat boundary diagram
([`phase13-hardening.md`](phase13-hardening.md#1-threat-boundary)) and
its own Known Limitations section both name the Cloudflare Quick
Tunnel as temporary: its `*.trycloudflare.com` hostname is reassigned
on every tunnel restart, which means

- the dashboard's `FORGEDB_CORS_ORIGINS`-governed API calls, and
- any client bookmarking or hardcoding the public URL,

both break every time the tunnel process restarts, with no warning
beyond "it stopped working." A stable hostname that Caddy itself
serves over HTTPS removes that dependency entirely; Cloudflare Quick
Tunnel remains available and documented (`deploy/README.md`'s
Section 1) for local development/testing, but is no longer load-bearing
for production availability.

## 2. What changed

- **`deploy/Caddyfile.example`**: the site label changed from a
  hardcoded literal domain (`example.com`, requiring an operator to
  edit this tracked file directly) to Caddy's own `{$VAR}` environment-
  variable placeholder syntax, resolved from `FORGEDB_PUBLIC_HOSTNAME`
  at Caddy startup. No real hostname is ever written into a tracked
  file. The `reverse_proxy` target was also corrected from the Docker
  Compose service name `forge-gateway:8090` (which only resolves on
  the `forgedb-net` Docker-internal network) to `127.0.0.1:8090` --
  Caddy itself runs natively on the VM, outside that network, and
  reaches `forge-gateway` the same way every curl example in
  `deploy/README.md` already does: via the loopback-bound published
  port (`docker-compose.yml`'s `forge-gateway` service already
  documented this; the example Caddyfile just didn't match it).
- **`deploy/caddy.env.example`** (new): a systemd `EnvironmentFile`
  template providing `FORGEDB_PUBLIC_HOSTNAME`, mirroring
  `.env.example`'s existing pattern for `FORGEDB_API_TOKEN` but for
  Caddy's native systemd service rather than Docker Compose.
- **`deploy/README.md`** (new Section 18): DNS (A/AAAA record),
  Azure NSG rules (443 required; 80 required for ACME HTTP-01 and the
  plain-HTTP redirect, not merely optional), Caddy's own certificate
  behavior (automatic, no cron/scheduling needed), how to validate
  Caddyfile syntax without a real domain, how to validate the issued
  certificate once DNS/NSG are live, and how to verify `forge-gateway`
  and the three node ports stay unreachable from outside the VM.
- **`dashboard/src/services/forgedbApi.ts`**: `API_BASE_PATH` now
  reads `import.meta.env.VITE_FORGEDB_API_BASE_URL`, falling back to
  the existing relative `'/api'` path when unset. This is a one-line,
  purely additive config read -- every request-building, response-
  mapping, and error-handling function in that file is byte-for-byte
  unchanged; see Section 3 below for why this was necessary at all.
- **`dashboard/.env.example`** (new): documents
  `VITE_FORGEDB_API_BASE_URL`, with an explicit instruction to set the
  real value as a Vercel **environment variable** (inlined into the
  built JS at build time by Vite), never in a committed file.

## 3. Why the dashboard needed a one-line change

`internal/api/cors.go` (Phase 13) exists specifically because "the
dashboard is deployed on its own origin (Vercel), separate from the
API's public edge" (its own doc comment). That only matters if the
dashboard's `fetch()` calls are genuinely cross-origin -- i.e. an
absolute URL pointing at the Caddy hostname, not a same-origin relative
path. Before this phase, `forgedbApi.ts` hardcoded the relative
`'/api'` path unconditionally; that path only resolves correctly
through Vite's dev-only proxy (`vite.config.ts`), which does not exist
in a deployed Vercel build. Making the base URL configurable via a
standard Vite build-time environment variable is the minimum change
that lets the already-implemented CORS path actually get exercised in
production, without touching how any request is built, how any
response is parsed, or any component/hook's behavior.

## 4. Alternatives considered

- **A Vercel `rewrites` proxy** (`vercel.json`, mapping `/api/*` to the
  real backend so the browser sees same-origin requests). Rejected:
  this would make the dashboard's requests same-origin again, making
  Phase 13's CORS work moot, and would require tracking the real
  hostname in a `vercel.json` rewrite destination -- violating "do not
  hardcode a real personal domain into application source/config."
  Direct cross-origin calls, governed by `FORGEDB_CORS_ORIGINS`, is the
  design Phase 13 already committed to.
- **Hardcoding the hostname directly in `forgedbApi.ts`.** Rejected
  per this phase's explicit constraints; an environment variable
  resolved at build time is the standard Vite mechanism for exactly
  this case.
- **TLS between Caddy and `forge-gateway`/nodes.** Still out of scope
  (unchanged from Phase 13's own Future Hardening list) -- this phase
  only adds TLS at the public edge, which is where the untrusted
  boundary actually is (see `phase13-hardening.md`'s threat boundary).

## 5. Known limitations

- **Single Azure VM, single Caddy instance.** No external load
  balancer or multi-region failover; losing the VM loses the public
  endpoint regardless of the Raft cluster's own fault tolerance
  (unchanged from Phase 13).
- **Manual DNS/NSG setup.** Nothing in this phase automates creating
  the DNS record or the NSG rule -- both are one-time manual operator
  steps documented in `deploy/README.md` Section 18.
- **Certificate issuance/renewal still depends on port 80 reachability.**
  If the NSG's rule for TCP 80 is later removed (e.g. "we only need
  443"), Caddy's automatic HTTPS will eventually fail to renew.
- **No rate limiting or WAF at the Caddy edge** -- unchanged from
  Phase 13's Future Hardening list.
