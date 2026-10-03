# Phase 13 — Public Deployment Hardening

This document is the design writeup for Phase 13: hardening ForgeDB's
already-working public deployment (Internet -> Cloudflare Quick Tunnel
-> Caddy -> `forge-gateway` -> the current Raft leader -> a 3-node
cluster) without touching Raft, the storage engine, the state machine,
snapshots, compaction, the gateway's routing algorithm, or the
dashboard's design. Every change here is either a configuration
default, a validation check, or a narrow, additive code path; nothing
in `internal/raft`, `internal/storage`, `internal/statemachine`, or
`internal/dbnode` was modified.

For the hands-on operator guide, see
[`deploy/README.md`](../../deploy/README.md). For the gateway's own
design, see
[`phase15-leader-aware-gateway.md`](phase15-leader-aware-gateway.md).

## 1. Threat boundary

```
Browser (dashboard, on its own origin)
    |  HTTPS, Authorization: Bearer <token>, CORS-governed
    v
Internet
    |
Cloudflare Quick Tunnel  (TLS terminates here; see Limitations)
    |
Caddy :80/:443  (native systemd process on the Azure VM -- NOT in Docker)
    |  plaintext HTTP, loopback only (127.0.0.1)
    v
forge-gateway :8090  (Docker container, leader-following reverse proxy)
    |  plaintext HTTP, loopback only (127.0.0.1) + forgedb-net (Docker-internal)
    v
forgedb-1 / forgedb-2 / forgedb-3  (whichever is current Raft leader)
    |  plaintext gRPC, forgedb-net only (Docker-internal, not published beyond loopback)
    v
Raft replication between the three nodes
```

Everything left of Caddy is untrusted. Everything from Caddy inward is
operated by the same deployer and, aside from the Cloudflare Tunnel's
own TLS termination, runs in plaintext on a single Azure VM. The only
thing standing between an unauthenticated caller and ForgeDB's
client-facing read/write surface is `FORGEDB_API_TOKEN`, checked once,
inside whichever ForgeDB node ultimately answers (`internal/api/auth.go`
-- unmodified by this phase). Phase 13 does not change that boundary;
it narrows what else is reachable around it and adds the one piece
(CORS) the dashboard's separate origin requires to use that boundary
from a browser at all.

## 2. IMPLEMENTED

### 2.1 CORS (`internal/api/cors.go`, new)

The dashboard is deployed on its own origin (Vercel), separate from the
API's public edge, and calls the API directly with an `Authorization`
header it attaches itself. Before this phase, `internal/api` had no
CORS handling at all, by design (see `kv.go`'s original doc comment) --
a browser on a different origin could not read any response from this
API, success or failure.

- `Server.cors` (wraps every route, outermost, before `authMiddleware`)
  echoes `Access-Control-Allow-Origin: <origin>` back only when the
  request's `Origin` exactly matches one entry in
  `Server.corsOrigins` -- never a wildcard `*`. `Server.corsOrigins` is
  set via `WithCORSOrigins`, normally from
  `config.Config.CORSOrigins`, itself parsed from the
  `FORGEDB_CORS_ORIGINS` environment variable (comma-separated exact
  origins, e.g. `https://<dashboard>.vercel.app,http://localhost:5173`).
  Unset/empty means no origin is allowed -- CORS stays off by default,
  identical to pre-Phase-13 behavior.
- An `OPTIONS` preflight is answered directly, before `authMiddleware`
  ever runs (a real browser preflight never carries the dashboard's
  Authorization header -- routing it through auth would make every
  preflight fail). For an allowed origin it returns `204` with
  `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`
  (`Authorization, Content-Type, Accept`), and `Access-Control-Max-Age`;
  for a disallowed origin it returns a bare `204` with none of those.
- `Access-Control-Allow-Credentials` is never set (the dashboard uses
  an explicit header, not cookies) and `Access-Control-Expose-Headers`
  is never set (nothing here needs exposing, and the token is never
  sent in a response header to begin with).
- A disallowed or missing `Origin` never changes a *non-preflight*
  request's outcome -- the handler still runs and `authMiddleware`
  still enforces the bearer token exactly as before. CORS is a
  browser-side read restriction, not a second authorization layer;
  only a real browser enforces the missing header on its own. Tests:
  `internal/api/cors_test.go` (allowed origin, disallowed origin,
  preflight, and `/health` unaffected).
- `forge-gateway` needed no changes: it already relays every response
  header a backend node sets that isn't hop-by-hop
  (`internal/gateway/proxy.go`'s `isExcludedHeader` list never excluded
  `Access-Control-*`), so a node's CORS headers — and its 204 preflight
  answers — pass through the gateway untouched.

### 2.2 Gateway leader-hint trust (`internal/gateway/proxy.go`)

`forge-gateway` previously followed a backend's `421` `leader_http`
hint unconditionally. Since node-to-node traffic (gRPC and the HTTP
hint itself) is plaintext inside the Docker network, a compromised or
spoofed node could have pointed the gateway at an arbitrary address.
`ServeHTTP` now only follows a hint when `Proxy.isTrustedBackend`
confirms it exactly matches one of the statically configured `PEERS`
backends; otherwise it is treated exactly like "no usable hint" and
the normal candidate list is used. Test:
`TestProxy_RejectsUntrustedLeaderHint`.

### 2.3 Configurable request-size limits, same defaults

- `internal/api.DefaultMaxValueBytes` (1 MiB, for `/kv` PUT) and
  `internal/gateway.DefaultMaxBodyBytes` (4 MiB, for the gateway's
  buffered request body) are unchanged defaults, now overridable via
  `config.Config.MaxValueBytes` / `MAX_VALUE_BYTES` and
  `config.GatewayConfig.MaxBodyBytes` / `GATEWAY_MAX_BODY_BYTES`
  respectively. Both are validated: an explicitly set non-positive
  value fails fast at startup rather than silently becoming
  "unlimited" or zero.
- Wired through `api.WithMaxValueBytes` / `gateway.WithMaxBodyBytes`
  (new functional options, both backward compatible with every
  existing call site).

### 2.4 Secret handling (`.dockerignore`)

`.dockerignore` did not exclude `.env`, and `Dockerfile`'s builder
stage runs `COPY . .` -- a local `.env` present at build time would
have landed inside the **builder** image layer (not the final runtime
image, which never `COPY`s from it, but still a real exposure if that
intermediate layer is ever cached, pushed, or inspected). `.env` is now
excluded from the build context. `.env.example` remains tracked and
contains only placeholders.

### 2.5 Docker port exposure (`docker-compose.yml`)

Every published port (`forgedb-1/2/3`'s HTTP/gRPC, and `forge-gateway`'s
8090) is now bound to `127.0.0.1` instead of all interfaces
(`"127.0.0.1:8081:8080"` instead of `"8081:8080"`, etc.). This is
defense in depth independent of the Azure NSG: today the NSG already
blocks external access to these ports, but Docker itself previously had
no opinion and would bind `0.0.0.0` if the NSG were ever loosened. Caddy
(native, same host) still reaches `forge-gateway` at `127.0.0.1:8090`
exactly as before; `deploy/scripts/` and the `curl localhost:808N/...`
examples in `deploy/README.md` are unaffected, since they also run on
the same host. Container-to-container traffic on `forgedb-net` is
untouched -- this only changes what the Docker host's own network
interfaces forward from outside the host.

### 2.6 Container hardening (`docker-compose.yml`)

All four services now set `cap_drop: [ALL]` and
`security_opt: [no-new-privileges:true]`. Neither affects the
already-non-root `USER forgedb` in `Dockerfile`, persistent volumes,
healthchecks, or graceful shutdown -- none of ForgeDB's binaries bind a
privileged port, use `setuid`/`setgid`, or need any Linux capability. A
read-only root filesystem was deliberately **not** added (see Future
hardening) to avoid any risk to WAL/runtime behavior.

### 2.7 Caddy baseline headers (`deploy/Caddyfile.example`)

Added `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and
`Referrer-Policy: strict-origin-when-cross-origin` via Caddy's `header`
directive. These are additive response headers only -- they do not
remove or replace any header the gateway/API already set (including
CORS's `Access-Control-*` headers), do not affect `/kv`'s binary
`application/octet-stream` bodies, and do not affect `/health`. CORS
itself is deliberately not handled in Caddy -- see `internal/api/cors.go`
(2.1) for why there is exactly one place that policy lives.

## 3. FUTURE HARDENING (not implemented in this phase)

- **TLS between Caddy and `forge-gateway`/nodes.** All internal HTTP
  and gRPC traffic remains plaintext; today this is mitigated only by
  the loopback/Docker-network boundary (2.5), not by encryption.
- **mTLS or signed node identity for gRPC.** Any node that can reach
  `forgedb-net` is currently trusted as a peer; there is no
  per-connection authentication at the transport layer.
- **Read-only root filesystem / tmpfs for scratch space.** Deferred
  (2.6) specifically to avoid any risk to WAL or runtime behavior
  without dedicated testing.
- **Per-origin or per-client CORS header customization beyond a static
  allow-list** (e.g. origin patterns, dynamic registration).
- **Rate limiting at the Caddy/gateway edge.** Caddy's built-in
  directives do not include a native rate limiter; adding one would
  require a plugin, which this phase deliberately avoids introducing.
- **Token rotation / scoped credentials.** `FORGEDB_API_TOKEN` remains
  one shared bearer secret for every caller.
- **A production-grade public entrypoint in place of the Cloudflare
  Quick Tunnel.** See Limitations below.
- **Dynamic cluster membership, multi-host topology, automatic
  snapshot policy.** Unchanged from Phase 14/15 -- out of scope here.

## 4. Known limitations

- **The Cloudflare Quick Tunnel is temporary.** Its `*.trycloudflare.com`
  hostname changes on every restart and is not a stable, production-
  grade public endpoint. It must never be hardcoded into tracked
  configuration (see `FORGEDB_CORS_ORIGINS`'s own warning in
  `.env.example`); it is set only in a local, untracked `.env` or the
  shell environment.
- **gRPC is plaintext inside the trusted Docker network.** Node-to-node
  replication traffic is unencrypted; this is acceptable only because
  that network is not reachable from outside the host (2.5), not
  because the protocol itself is secured.
- **Cluster membership is static.** `PEERS` is fixed at process start
  for every node and the gateway alike; there is no add/remove-node
  operation.
- **A single Azure VM is not physical fault tolerance.** All three
  Raft nodes, the gateway, and Caddy run on one host; losing that VM
  loses the cluster regardless of Raft's own replication guarantees.
- **One shared bearer secret, no rotation or scopes.**
  `FORGEDB_API_TOKEN` authenticates every caller identically; there is
  no per-user identity, no scoped permission, and no built-in rotation
  mechanism (rotating it today means restarting every node with a new
  value).
- **No complete production secret-management system.** Secrets are
  environment variables sourced from a local, untracked `.env` file;
  there is no integration with a dedicated secrets manager (Azure Key
  Vault, Vault, etc.).
