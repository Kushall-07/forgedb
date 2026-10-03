# Phase 15 — Leader-Aware Public HTTP Routing

This document is the design writeup for Phase 15: why `forge-gateway`
exists, the alternatives considered and rejected, and the correctness
properties verified. For hands-on operator instructions, see
[`deploy/README.md`](../../deploy/README.md#5a-leader-aware-public-routing-forge-gateway).

Phase 15 answers one question: **can ForgeDB's public HTTP entrypoint
stay correct across a Raft leader change, without teaching Raft to
anything outside `internal/raft`/`internal/dbnode`?** It does not
change Raft, the storage engine, the dashboard, or authentication
semantics, and it adds no new consensus mechanism, no duplicated
database state, and no way for a follower to independently apply a
write.

## 1. The problem

Before Phase 15, the deployment's external path was:

```
Internet -> Cloudflare Tunnel -> Caddy -> one ForgeDB node (fixed)
```

`internal/api` already rejects a `/kv` write or linearizable read that
reaches a non-leader node with `HTTP 421 Misdirected Request` and a JSON
body naming the current leader, when known (see
[`internal/api/kv.go`](../../internal/api/kv.go)'s `notLeaderResponse`):

```json
{"error":"not_leader","leader_id":"node-2","leader_http":"forgedb-2:8080"}
```

That is correct and sufficient information -- `cmd/forge-client`
already follows it to find the leader on its own, one request at a
time, from the command line. But Caddy, sitting in front of a *fixed*
node address, has no way to use it: a 421 is a perfectly valid HTTP
response as far as Caddy's reverse proxy is concerned, not a backend
failure, so neither Caddy's active nor passive health checks ever react
to it, and Caddy's configuration language has no way to parse a JSON
response body and extract `leader_http`. The moment the node Caddy
points at stops being leader, every write and linearizable read starts
failing with 421 until an operator manually edits the Caddy
configuration -- exactly the problem this phase closes.

## 2. Alternatives considered

- **(A) Caddy-only routing.** Rejected: Caddy cannot be taught to parse
  `leader_http` out of a 421 body and retry a different upstream
  without a scripting extension outside Caddy's own configuration
  language -- which would amount to writing this phase's proxy *inside*
  Caddy instead of next to it, for no benefit.
- **(B) Rely on the existing not-leader information alone.** Rejected
  on its own: the information already exists (confirmed above), but
  nothing sits between Caddy and the nodes to *act* on it. This phase
  does not add new information to the API -- it adds the one missing
  piece that consumes information `internal/api` already produces.
- **(D) ForgeDB API enhancement.** Rejected as unnecessary: `/cluster`
  already exposes `raft.LeaderID`, and `/kv`'s 421 response already
  carries `leader_id`/`leader_http`. No change to `internal/api` or
  `internal/raft` was needed for this phase.
- **(C) A small dedicated HTTP leader proxy/gateway.** Chosen. It is
  the minimum new component that can act on information the API
  already provides, reusing an algorithm the codebase already proved
  out in `cmd/forge-client`: try a candidate node; if it says "not
  leader" and names one, try that one next; otherwise try the next
  configured node; bounded by a maximum number of attempts.

## 3. Design

```
Internet -> Cloudflare Tunnel -> Caddy -> forge-gateway -> current Raft leader -> ForgeDB API
```

`forge-gateway` (`cmd/forge-gateway`, logic in `internal/gateway`) is a
persistent HTTP server, not a new client protocol or a second
consensus mechanism:

- It holds a fixed list of node HTTP addresses, taken from the same
  `PEERS` environment variable every node already reads
  (`config.LoadGateway`, reusing the existing `config.ParsePeers`) --
  no new address format, no second source of truth for cluster
  membership.
- Per request, it buffers the body (bounded) and tries candidates in
  order: a cached "last known leader" guess first, then every
  configured backend. A `421` response's `leader_http` hint (when
  present) is pushed to the front of the remaining candidates. Any
  other response -- `200`, `404`, `401`, `500`, whatever the node
  itself decided -- is relayed to the client verbatim, and that
  backend's address becomes the new cached guess.
- The `Authorization` header is forwarded byte-for-byte, untouched.
  `forge-gateway` never reads, validates, or logs it, and never
  requires `FORGEDB_API_TOKEN` itself (see `docker-compose.yml`'s
  `forge-gateway` service, which does not set that variable at all).
  Every protected endpoint's authentication is enforced exactly as
  before, inside whichever node answers (`internal/api/auth.go` is
  untouched).
- If every tried candidate answers 421 with no usable hint (e.g.
  mid-election) or none can be reached at all, it returns an explicit
  JSON error (`503 no_leader_available` or `502 no_backend_reachable`)
  -- never a fabricated success, and never a response from a node that
  did not actually decide to answer it.

Because every leader-only decision still happens inside the node that
receives the forwarded request -- `internal/dbnode`'s `Propose` /
`ConsistentGet`, unchanged -- `forge-gateway` cannot cause a follower to
apply a write. A wrong routing guess costs one extra round trip, never
an incorrect result: the follower rejects it exactly as if the client
had reached it directly.

## 4. Correctness properties

1. **Raft remains the only leadership authority.** `forge-gateway`
   never elects, votes, or tracks terms; it only relays what a node's
   own `raft.Status`/`421` response already says.
2. **Followers never apply a client write independently.** Unchanged --
   `internal/dbnode.Propose` still returns `ErrNotLeader` to a
   follower's own HTTP layer, which is what produces the 421
   `forge-gateway` is reacting to, not causing.
3. **A request routed to a follower is not executed as a write
   locally.** Same mechanism as (2); see
   `internal/gateway/proxy_test.go`'s
   `TestProxy_FollowsLeaderHintOnFollower421`, which asserts the
   eventual leader is hit exactly once.
4. **Leader changes are eventually reflected in public routing**,
   without restarting or reconfiguring `forge-gateway` --
   `TestProxy_LeaderChange_CachedGuessFollowsNewLeader`.
5. **No stale leader information causes a silently accepted write on a
   follower** -- the follower's own leader check is the actual gate;
   `forge-gateway`'s cache is only a performance hint, verified fresh on
   every hop against the node's live 421 response.
6. **An explicit failure, never a guess, when no leader is known** --
   `TestProxy_NoLeaderAvailable_ExplicitFailure`.
7. **Never a silent route to an arbitrary node** -- every non-421
   response relayed to the client is that specific node's own
   deliberate answer to the forwarded request, not a response
   `forge-gateway` manufactured or coerced.
8. **Existing authentication middleware preserved** --
   `internal/api/auth.go` is unmodified; `forge-gateway` adds no
   authentication logic of its own (`TestProxy_AuthHeaderPassthrough_NotValidatedLocally`).
9. **`Authorization: Bearer <token>` preserved** -- forwarded
   unchanged; see the same test.
10. **No Raft gRPC ports exposed publicly** -- unchanged; `9091-9093`
    are not touched by this phase.
11. **No individual ForgeDB HTTP ports exposed publicly through the
    Azure NSG** -- see [Section 6](#6-deployment--azure-nsg-notes).
12. **Docker internal network architecture unchanged** -- the three
    `forgedb-N` services, their volumes, and `forgedb-net` are
    untouched; `forge-gateway` is an additional container on the same
    network, not a replacement for any existing one.
13. **Existing health/readiness semantics unchanged** -- `forge-gateway`
    does not special-case `/health`/`/ready`; it proxies them through
    the same generic retry logic as everything else, which succeeds on
    the first reachable node (no node ever answers 421 to `/health` or
    `/ready`), exactly matching the pre-existing "a follower is just as
    healthy/ready as a leader" rule.

## 5. Configuration

One new environment variable, `GATEWAY_ADDR` (default `:8090`,
`config.EnvGatewayAddr`/`config.DefaultGatewayAddr`) -- the address
`forge-gateway` listens on. It reuses the existing `PEERS` variable
unchanged for its backend list (`config.LoadGateway`). No existing
variable (`NODE_ID`, `HTTP_ADDR`, `GRPC_ADDR`, `DATA_DIR`, `PEERS`,
`FORGEDB_API_TOKEN`) was renamed or repurposed, and `forge-gateway`
does not read `FORGEDB_API_TOKEN` at all.

## 6. Deployment / Azure NSG notes

Only `forge-gateway`'s port (and Caddy's public port, in front of it)
should ever be reachable from outside the Docker host. The per-node
ports (`8081-8083`) and the Raft gRPC ports (`9091-9093`) that
`docker-compose.yml` publishes to the host remain there for local
operator convenience (`curl localhost:808N/...`, the existing failure
tests in `deploy/README.md`) -- they are not new, and this phase does
not add to what is exposed. An Azure Network Security Group in front of
this deployment should allow inbound traffic only to Caddy's port (and
therefore, transitively, to `forge-gateway` behind it), not to
`8081-8083` or `9091-9093`.

## 7. Limitations

- `forge-gateway` adds one extra network hop for every request, and up
  to a few more while discovering a fresh leader after a failover (each
  one bounded by `internal/gateway.DefaultBackendTimeout`, 5s).
- Its leader cache is purely a performance hint, held in one process's
  memory -- it is not persisted and starts empty on every
  `forge-gateway` restart, which only costs one discovery round trip,
  never a correctness issue (see [Section 4](#4-correctness-properties)).
- It is a single process/container; Phase 15 does not add its own
  failover for the gateway itself (e.g. running more than one instance
  behind Caddy is possible, since `forge-gateway` is stateless across
  requests beyond that in-memory hint, but is not configured by
  default).
