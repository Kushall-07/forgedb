# Phase 17 — Process Lifecycle and Graceful Shutdown

This is a concise operator/developer note on what `forgedb` and
`forge-gateway`'s process lifecycle guarantees, and what it does not.
It does not change Raft, replication, commit semantics, the state
machine, the storage engine, request deduplication, the gateway's
leader-following algorithm, or authentication -- Phase 17 only hardens
and tests how each process starts up and shuts down.

## What graceful shutdown guarantees

On receiving `SIGINT` or `SIGTERM`, `forgedb` (`cmd/forgedb/main.go`)
shuts down in this order, each step waiting for the previous one:

1. **Stop accepting new client HTTP requests.** `internal/api.Server.Shutdown`
   closes the listener and waits (up to a 10-second bound) for any
   in-flight HTTP request to finish.
2. **Stop accepting new peer gRPC RPCs.** `internal/transport.Server.Stop`
   (`grpc.Server.GracefulStop`) waits for any in-flight `RequestVote`/
   `AppendEntries`/`InstallSnapshot` call this node is currently
   *answering* to finish.
3. **Stop this node's own Raft activity.** `dbnode.Node.Close` stops
   the ticker goroutine (no further election/heartbeat/replication
   work is scheduled), then drains every RPC this node has *sent* so
   far, so no outbound send races the steps after it.
4. **Stop the Applier, waiting for any write already in progress.**
   Still inside `Node.Close`: the Applier's background loop is signaled
   to stop and `Close` blocks until it has actually returned -- not
   merely until it's been asked to -- which matters because that loop
   may be mid-`Store.Put`/`Delete` at the moment shutdown begins.
5. **Only then close KV storage** (the WAL file handle). By
   construction, no further write can reach storage after this point,
   and nothing that was already in flight was cut off before finishing.
6. **Release outgoing gRPC connections** to peers
   (`transport.GRPCTransport.Close`).

`forge-gateway` (`cmd/forge-gateway/main.go`) shuts down more simply:
its HTTP server stops accepting new connections and waits up to a
10-second bound for any in-flight proxy request -- including one still
retrying across backends after a 421 -- to finish, then returns.

Both shutdown paths are **idempotent**: calling them more than once
(a defensive second signal, a test, a failed first attempt) is a
no-op after the first call, not a re-close of an already-released
resource. `dbnode.Node.Close` in particular guards this explicitly --
see its doc comment -- since closing an already-closed WAL file a
second time would otherwise surface a spurious error.

## Shutdown signal behavior

- Both processes treat `SIGINT` and `SIGTERM` identically: begin the
  ordered shutdown above, then exit 0.
- Neither process requires `--force` or a second signal to begin
  shutting down; `docker stop`'s default `SIGTERM`-then-grace-period-
  then-`SIGKILL` behavior is sufficient, and the exec-form
  `ENTRYPOINT ["forgedb"]` / `["forge-gateway"]` in `Dockerfile` means
  the signal reaches the Go process directly as PID 1, not a shell
  wrapper that would need to forward it.
- A startup failure (e.g. a port already in use, invalid
  configuration) exits 1 immediately and does not attempt the
  shutdown sequence on components that never started.

## Expected Docker behavior

- `docker stop <node>` (or `docker compose stop`) now completes well
  within Compose's default 10-second grace period under normal load --
  observed well under 1 second in this phase's own smoke test -- by
  shutting down cleanly rather than waiting out the grace period and
  being `SIGKILL`ed.
- A `SIGKILL` (grace period exceeded, or `docker kill`) still produces
  exactly the same recovery behavior Phase 14 already documented: this
  is a crash, not a graceful shutdown, and `dbnode.Open`'s WAL-replay-
  plus-Raft-persistence recovery (unchanged by this phase) is what
  makes that safe, not graceful shutdown. Graceful shutdown's only job
  is to make the *common* case (a planned stop/restart/redeploy) clean
  and fast; it is not a prerequisite for crash safety.
- Restarting a stopped node (`docker start`, or `docker compose up -d`
  again) rejoins the cluster as a follower and catches up via normal
  Raft replication exactly as before -- this phase does not change
  that path at all, and the Docker lifecycle smoke test below
  exercises it end to end through a real graceful stop of the current
  leader.

## What persistence guarantees remain

Unchanged from every prior phase: a write is durable once `/kv`'s
handler returns success, because the WAL fsync inside `Store.Put`/
`Delete` happens before that handler can return, and Raft's own
persisted log/term/vote survive a restart via `FilePersister`
(`internal/raft`). Graceful shutdown does not add a new durability
guarantee -- it only ensures a write already accepted is never
*interrupted* mid-flight by storage closing out from under it (see
step 4 above), which was already true before this phase for Raft's
own commit rule, and is now also true for the shutdown path
specifically.

## What is NOT guaranteed

- **No guaranteed completion of a request that is still blocked when
  the shutdown timeout (10s) elapses.** `http.Server.Shutdown` returns
  the context's deadline-exceeded error in that case and the process
  still exits; this bounds shutdown latency, it does not make every
  possible in-flight operation finish no matter how long it takes.
- **No draining of Raft replication work queued for *other* nodes.**
  Shutdown stops this node's own ticking and drains what it has
  already sent; it makes no attempt to ensure a majority has received
  a given entry before exiting -- that guarantee, when it exists, comes
  from Raft's own commit rule having already been satisfied before the
  shutdown signal arrived, not from shutdown itself.
- **No zero-downtime single-node restart.** Restarting a node is still
  a visible absence from the cluster between its stop and its
  rejoin -- this phase does not add rolling-restart orchestration.
- **No change to crash-recovery behavior.** A `SIGKILL`/power-loss/
  process-crash scenario is unaffected by anything in this phase; it
  relies entirely on the WAL/Raft-persistence recovery path that
  already existed.

## Docker lifecycle smoke test (manual verification performed this phase)

```
docker compose up -d --build
# all four containers report "healthy"
curl -XPUT .../kv/k1 -> 200                      # write via gateway
docker stop forgedb-3                            # stop the current leader normally
#   -> graceful shutdown logs, completes in <1s
#   -> remaining two nodes elect a new leader
curl .../kv/k1 -> still readable via gateway      # cluster still operating
curl -XPUT .../kv/k2 -> 200                       # cluster still accepts writes
docker start forgedb-3                            # restart the stopped node
#   -> becomes healthy within a few seconds
#   -> rejoins as Follower, CommitIndex/LastApplied catch up to the live nodes
curl .../kv/k1, .../kv/k2 -> both still available via the gateway
docker compose down                               # (no -v: volumes preserved)
```

See the Final Report delivered alongside this phase for the exact
commands and observed output.
