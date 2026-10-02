# Phase 12: Observability & Diagnostics

## 1. Observability goals

Phases 0 through 11 built a working, tested, replicated key/value
database: storage, WAL recovery, SSTables/compaction, Raft consensus and
persistence, a deduplicating state machine, Raft+storage integration,
linearizable reads, snapshots/log compaction, a chaos-testing harness,
and a linearizability checker. None of that work gives an operator (or a
future client) any way to ask the running system, from the outside,
basic questions like:

> What is the cluster doing? Why did a request fail? Which node is
> leader? What term/commit index is active? Is replication healthy? How
> far behind is a follower? Is a snapshot in progress? Are requests
> slow? Is the system experiencing errors?

Phase 12 answers exactly that, and nothing more. It does **not** change
Raft, storage, snapshots, chaos testing, or correctness testing; it
exposes what those layers already track. Its single test is:

> If ForgeDB is behaving incorrectly or slowly, can I understand why
> without attaching a debugger?

## 2. Architecture

```text
                    ForgeDB
                       |
        +--------------+--------------+
        v              v              v
     Metrics         Logs           Health
   (internal/     (internal/     (internal/api
    metrics)        logging)      /health, /ready)
        |              |              |
        +--------------+--------------+
                       v
                 Diagnostics
            (internal/dbnode.Status,
             internal/raft.Status/
             PeerStatuses,
             internal/storage.Stats)
                       |
                       v
                 HTTP endpoints
               (internal/api: /health,
                /ready, /metrics, /cluster)
```

Four new/extended packages carry Phase 12:

| Package | Role |
|---|---|
| `internal/metrics` | Counters, gauges, histograms, and a Prometheus text-exposition renderer. Replaces the old `internal/metrics/logger.go` (which was actually a logger, not a metrics system). |
| `internal/logging` | Structured, leveled logging, built on the standard library's `log/slog` rather than a hand-rolled logger. |
| `internal/api` | A small, strictly read-only HTTP server: `/health`, `/ready`, `/metrics`, `/cluster`. |
| `internal/dbnode` (extended) | `Status()` (full diagnostic snapshot), the pull-gauge wiring that connects live Raft/storage state into `internal/metrics`, and `ConsistentGet`/lifecycle instrumentation. |

Everything else (`internal/raft`, `internal/statemachine`,
`internal/storage`, `internal/storage/wal`) is instrumented in place:
each package calls directly into `internal/metrics` and
`internal/logging` at the exact point an event happens, with no new
abstraction layer in between.

Tracing was considered (per the phase brief) and deliberately **not**
added: structured logs plus metrics plus the diagnostic endpoints below
already answer every question this phase set out to answer, without a
new dependency or a second correlation mechanism to keep consistent (see
§23 and §26).

## 3. Metrics model

`internal/metrics` is a small, dependency-free registry:

* **Counter** -- monotonically increasing (`Inc`/`Add`), backed by
  `atomic.Uint64`.
* **Gauge** -- up or down (`Set`/`Inc`/`Dec`/`Add`), backed by an
  atomically-stored `float64` bit pattern.
* **Histogram** -- a fixed set of bucket boundaries plus a running sum
  and count (`Observe`); never stores individual observations, so its
  memory footprint never grows with traffic.
* **`*Vec` variants** (`CounterVec`, `GaugeVec`, `HistogramVec`) -- one
  metric per distinct value of a single label, for a label whose
  possible values are small and bounded (peer ID, HTTP route, component
  name). There is no mechanism in this package for an unbounded label.
* **Pull-based gauges** (`NewGaugeFunc`/`NewGaugeVecFunc`) -- registered
  once, called at render time. This is how live Raft/storage state
  (term, role, commit index, follower lag, MemTable size...) is exposed
  without any package having to push on every single tick or mutation.

Every operation is lock-free (atomics only); `Render()` briefly locks
the registry just to snapshot which metrics exist, never while
formatting or while any caller's own lock is held.

`internal/metrics.Default` is the single, process-wide registry every
package records into -- the same global-singleton pattern the old
`internal/metrics/logger.go` already used for logging, now applied to
metrics. `internal/metrics/catalog.go` declares every metric ForgeDB
itself records, by name, exactly once, so instrumentation call sites
elsewhere are a single direct call (`metrics.RaftElectionsStartedTotal.Inc()`)
with no risk of a typo silently creating a second, differently-spelled
series.

## 4. Metric naming

Every metric uses the `forgedb_` namespace, one convention throughout:
`forgedb_raft_...`, `forgedb_storage_...`, `forgedb_wal_...`,
`forgedb_state_machine_...`, `forgedb_api_...`, `forgedb_node_...`.

## 5. Counters, gauges, and histograms

The three metric types are used for exactly what they mean:

* A **counter** is used only for a quantity that can never go down
  (an event that happened N times). Replication/election/snapshot/WAL
  event counts, and every `_total`-suffixed metric, are counters.
* A **gauge** is used only for "the current value of something" --
  term, commit index, MemTable size, follower lag, process uptime.
* A **histogram** is used only for a latency distribution worth
  understanding as a distribution, not just an average: currently only
  `forgedb_consistent_get_latency_seconds` and
  `forgedb_api_request_latency_seconds`. Buckets are fixed at
  registration time (`metrics.DefaultLatencyBuckets`, 0.1ms-10s).

## 6. Full metric catalog

### Process / node

| Metric | Type | Meaning |
|---|---|---|
| `forgedb_node_uptime_seconds` | gauge (pull) | Seconds since this process started. |
| `forgedb_node_info{node_id}` | gauge | Always 1; set once at `dbnode.Open`, identifies which node a scrape came from. |

### Raft: state (pull gauges, `internal/dbnode/metrics.go`)

| Metric | Meaning |
|---|---|
| `forgedb_raft_term` | Current Raft term. |
| `forgedb_raft_role{role="leader"\|"candidate"\|"follower"}` | 1 for the current role, 0 for the other two -- never more than one `1` at a time. |
| `forgedb_raft_commit_index` | Current commit index. |
| `forgedb_raft_last_applied` | Current last-applied index. |
| `forgedb_raft_log_entries` | Entries retained in the log past the snapshot boundary (`LastLogIndex - SnapshotIndex`). |
| `forgedb_raft_snapshot_index` | Index of the most recent snapshot (0 if none). |
| `forgedb_raft_follower_lag_entries{peer_id}` | `LastLogIndex - matchIndex[peer]`; leader-only, empty on a follower. |

### Raft: elections and leadership (counters, `internal/raft`)

| Metric | Increments exactly when |
|---|---|
| `forgedb_raft_elections_started_total` | `startElectionLocked` runs (election timeout fired). |
| `forgedb_raft_elections_won_total` | `becomeLeaderLocked` runs (majority of votes, including a single-node cluster's self-vote). |
| `forgedb_raft_elections_lost_total` | A node that was `Candidate` steps down via `becomeFollowerLocked` (saw a higher term before winning). |
| `forgedb_raft_leader_changes_total` | This node's own `leaderID` changes to a new, non-empty value -- either by becoming leader itself, or by learning a new leader's ID from an `AppendEntries`/`InstallSnapshot` RPC. |
| `forgedb_raft_higher_term_steps_down_total` | `becomeFollowerLocked` succeeds (every call site only invokes it after observing a term higher than the node's own). |

### Raft: replication (counters + pull gauge, `internal/raft`)

| Metric | Increments exactly when |
|---|---|
| `forgedb_raft_append_entries_sent_total` | Every `AppendEntries` RPC attempt, leader side -- including the read-barrier confirmation round `ReadIndex` reuses (see §11). |
| `forgedb_raft_append_entries_success_total` | A reply comes back with `Success=true`. |
| `forgedb_raft_append_entries_failed_total` | Delivery failed, the reply carried a higher term, the round was stale, or `Success=false` (log-matching mismatch). |
| `forgedb_raft_entries_replicated_total` | Added by exactly the number of *newly* matched entries on a successful reply (`newMatch - oldMatchIndex`), never recounting entries a peer was already known to have. |
| `forgedb_raft_entries_committed_total` | Added by exactly the number of indexes `commitIndex` just advanced by, on leader (`maybeAdvanceCommitIndexLocked`) or follower (`HandleAppendEntries`/`HandleInstallSnapshot`). |
| `forgedb_raft_follower_lag_entries{peer_id}` | (pull) See above. |

### Raft: snapshots (counters, `internal/raft`)

| Metric | Increments exactly when |
|---|---|
| `forgedb_raft_snapshots_created_total` | `CreateSnapshot` persists successfully. |
| `forgedb_raft_snapshots_installed_total` | `HandleInstallSnapshot` persists a genuinely new (non-stale/duplicate) snapshot successfully. |
| `forgedb_raft_snapshot_failures_total` | Either path's persistence (`SaveSnapshot`, log compaction, or the follow-up `SaveState`) fails. |
| `forgedb_raft_snapshot_bytes_total` | Added by `len(data)` on every successful create or install -- cumulative, not current size (see `forgedb_raft_snapshot_index` for "which snapshot is current"). |

### Raft: linearizable reads (counters + histogram)

| Metric | Increments exactly when |
|---|---|
| `forgedb_raft_read_index_total` | Every `ReadIndex()` call. |
| `forgedb_raft_read_index_success_total` | A read barrier was confirmed (including the single-node, zero-RPC case). |
| `forgedb_raft_read_index_failure_total` | Not leader, or quorum confirmation failed/leadership changed mid-round. |
| `forgedb_consistent_get_total` / `_success_total` / `_failure_total` | `dbnode.Node.ConsistentGet` attempted / completed (including a resolved key-not-found) / failed before ever reading storage. |
| `forgedb_consistent_get_latency_seconds` | Histogram of the full `ConsistentGet` call duration. |

### State machine (counters, `internal/statemachine`)

| Metric | Increments exactly when |
|---|---|
| `forgedb_state_machine_commands_total` | Every `Apply` call (one per committed log entry actually applied -- not per client attempt/retry at the API layer, which does not exist yet). |
| `forgedb_state_machine_apply_success_total` | `Apply` returns a resolved outcome (success, dedup hit/conflict, or a deterministic validation failure) -- i.e. every outcome *except* an unresolved storage fault. |
| `forgedb_state_machine_apply_failure_total` | `Apply` returns a non-nil error (an unresolved storage fault; the entry is not marked applied). |
| `forgedb_state_machine_dedup_hits_total` | A command is resolved as a replay of an already-applied `(ClientID, RequestID)`. |
| `forgedb_state_machine_dedup_conflicts_total` | A `(ClientID, RequestID)` is reused for a logically different command. |

### Storage (counters + pull gauges)

| Metric | Increments exactly when |
|---|---|
| `forgedb_storage_put_total` / `_get_total` / `_delete_total` | Every `MemStore.Put`/`Get`/`Delete` call, including one that fails validation (`ErrEmptyKey`) -- this counts *attempts* against the store, not logical successes. |
| `forgedb_storage_key_not_found_total` | `Get` finds no live value. |
| `forgedb_storage_memtable_entries` / `_memtable_bytes` | (pull) `MemTable.Len()`/`Bytes()`, maintained incrementally by every `Put`/`Delete` -- **never** a full scan (see §24's cardinality/cost rule). |

### WAL (counters, `internal/storage/wal`)

| Metric | Increments exactly when |
|---|---|
| `forgedb_wal_append_total` / `forgedb_wal_bytes_written_total` | Every successful `Append`, by record count / encoded byte size. |
| `forgedb_wal_sync_total` | Every successful `fsync` (`Sync`). |
| `forgedb_wal_recovery_total` | Every `Replay` pass (i.e. every store open), success or failure. |
| `forgedb_wal_recovery_records_total` | Added by however many records that pass actually replayed, even if it then failed partway. |
| `forgedb_wal_corruption_errors_total` | A `Replay` pass stops on `ErrCorrupt` (a checksum or header failure -- never a torn tail, which is expected crash shape and silently truncated, not corruption). |

### API (counters + histogram, `internal/api`)

| Metric | Meaning |
|---|---|
| `forgedb_api_requests_total{key="METHOD route status_class"}` | One composite label (this package's `Vec` type supports a single label dimension) combining method, route, and a coarse 2xx/4xx/5xx class. |
| `forgedb_api_request_latency_seconds{route}` | Histogram of full handler duration, labeled only by route. |

### Errors (aggregate)

| Metric | Meaning |
|---|---|
| `forgedb_errors_total{component}` | One shared, bounded-cardinality counter (`component` ∈ `raft`, `wal`, `storage`, `statemachine`, `api`) incremented via `metrics.RecordError` at every real fault site, instead of a separate near-identical `*_errors_total` per component (see §13's cardinality policy and §49 on avoiding metric sprawl). `forgedb_raft_snapshot_failures_total` already covers snapshot-specific failure counting; a separate `snapshot_errors_total` would just double it. |

## 7. Raft diagnostics

`(*raft.Node).Status()` returns a cheap (`n.mu`-protected, O(1)) copy of
every field above plus `LastLogIndex`/`LastLogTerm`/`SnapshotTerm`/
`Peers`. `(*raft.Node).PeerStatuses()` returns `{PeerID, NextIndex,
MatchIndex, Lag}` per peer, and is `nil` on a non-leader (only a leader
tracks replication progress at all). Neither method performs an RPC or
holds a lock for longer than a field copy.

## 8. Replication / snapshot diagnostics

`PeerStatuses()`'s `Lag` field is the direct answer to "how far behind
is this follower": `leader.LastLogIndex - peer.MatchIndex`. Combined
with `SnapshotIndex`, it also answers "will this follower need a
snapshot to catch up" (once `peer.NextIndex <= leader.SnapshotIndex`,
`broadcastAppendEntriesLocked` switches to `InstallSnapshot` on its own
-- see `docs/raft/phase9-snapshots.md`).

## 9. State-machine / storage diagnostics

`storage.MemStore.Stats()` returns `{MemTableEntries, MemTableBytes}`,
both read from `MemTable`'s own incrementally-maintained counters (see
§24) -- never a scan. `dbnode.Status()` folds this in as `Storage`.

## 10. Structured logging

Built on the standard library's `log/slog`, not a hand-rolled logger:
`internal/logging.Default` is a single process-wide `*slog.Logger`
(mirroring the metrics registry's global-singleton pattern), configured
by `logging.SetLevel`/`logging.ParseLevel`/`logging.SetOutput`.
`logging.With(k, v, ...)` returns a child logger with fields attached
(e.g. `node_id`, `component`), used once per node/package rather than
repeating those fields at every call site.

## 11. Log levels

Debug / Info / Warn / Error, exactly `log/slog`'s four built-in levels.
`logging.ParseLevel` maps a config string (`internal/config.Config.LogLevel`)
to one, defaulting to Info for an empty or unrecognized value. Filtering
happens *before* formatting (`slog`'s own `Enabled` check), so a `Debug`
call site on a hot path (e.g. `wal.Append`, `MemStore.Put`,
`raft.maybeAdvanceCommitIndexLocked`'s commit-advance log) costs nothing
at the default Info level beyond one atomic level comparison.

## 12. Event names

A closed catalog in `internal/logging/events.go`
(`EventRaftLeaderChanged`, `EventRaftSnapshotInstalled`,
`EventStateMachineDedupHit`, `EventWALRecovery`, `EventNodeStarted`,
...), so every occurrence of the same event is spelled identically and
the whole log stream stays greppable. Call sites use these constants,
never ad hoc strings, for every *meaningful state transition*; purely
internal function calls are never logged.

Deliberate level choices, so normal operation at Info stays readable:

| Event | Level | Why |
|---|---|---|
| `raft.election.started`, `raft.election.won`, `raft.leader.changed`, `raft.step_down` | Info | Rare, always meaningful. |
| `raft.snapshot.created`, `raft.snapshot.installed`, `raft.snapshot.failed` | Info | Rare, always meaningful. |
| `raft.append.failure` | Info | Happens during back-off/catch-up, not every heartbeat; informative. |
| `raft.append.success`, `raft.commit.advance` | Debug | Happen on every successful heartbeat/commit -- would flood Info-level logs under normal load. |
| `state_machine.apply` | Debug | Once per applied command -- high volume under load. |
| `state_machine.dedup_hit` | Debug | A normal, frequent outcome of client retries. |
| `state_machine.dedup_conflict`, `state_machine.apply_error` | Warn / Error | Genuinely abnormal. |
| `wal.append`, `wal.sync` | Debug | Once per write -- highest-volume event in the system. |
| `wal.recovery` | Info | Once per store open; always worth seeing. |
| `node.started`, `node.stopped`, `node.recovered` | Info | Lifecycle boundaries. |
| `api.request` | Debug | Once per HTTP request to a diagnostic endpoint. |

## 13. Cardinality policy

No metric or log field in this phase ever uses a request ID, client ID,
raw key, or error message string as a **label** (labels only: peer ID,
HTTP route, role name, or a small fixed component name -- every one
drawn from a bounded, small set fixed by cluster configuration or this
package's own code, never by a caller's input). `client_id`/`request_id`
*do* appear as structured **log fields** (not metric labels) on the
specific dedup-related log lines where they aid debugging (e.g.
`state_machine.dedup_conflict`), which is exactly what §23/§24 of the
phase brief asks for: request correlation in logs, never in metrics.

## 14. Health vs. readiness

* `GET /health` -- liveness only: "this process is up and can answer
  HTTP." Always 200 if the server is reachable at all.
* `GET /ready` -- readiness: "this node's storage engine is reachable."
  Implemented by probing a reserved, never-written key
  (`__forgedb_health_check__`) through `Store.Get`: `ErrKeyNotFound` is
  the expected, healthy answer; any other error means storage is broken
  and returns 503. Deliberately *not* tied to Raft leadership -- a
  follower is exactly as "ready" as a leader; readiness is about this
  node's own ability to serve, not about whether it currently holds
  leadership.

## 15. Cluster diagnostics endpoint

`GET /cluster` renders `dbnode.Status` as JSON: node ID, process uptime,
the full `raft.Status`, `[]raft.PeerStatus` (omitted/empty on a
follower), and `StorageStatus`. It performs no mutation and no RPC --
every field is already-available state, copied under whatever lock
already protects it.

## 16. Metrics endpoint

`GET /metrics` renders `metrics.Default` (or whatever `*metrics.Registry`
the `api.Server` was constructed with) via `Registry.Render()`, in
Prometheus text-exposition format (`# HELP`/`# TYPE` plus one sample
line per series), `Content-Type: text/plain; version=0.0.4`. Every line
corresponds to a real, registered metric -- there is no static or
fabricated value anywhere in this path.

## 17. Read-only enforcement

Every `internal/api` route is wrapped in `readOnly`, which rejects any
method other than `GET` with `405 Method Not Allowed` *before* the
handler itself ever runs (see `server.go`). No handler accepts a body,
and none of the four endpoints can change Raft, storage, or cluster
membership state -- there is no `POST /raft/elect` or equivalent
anywhere in this package, by design (see §38 of the phase brief).

## 18. Configuration

`internal/config.Config` gained three fields: `LogLevel` (string, parsed
by `logging.ParseLevel`), `MetricsEnabled` (bool), `MetricsAddr`
(string, e.g. `":9090"`). These are plain struct fields with sensible
zero-value defaults -- this project has no environment-variable
configuration system, and Phase 12 does not introduce one.

## 19. Thread safety and lock discipline

Every metric operation is lock-free (atomics). Logging calls at
meaningful-but-rare transition points (election/leadership/snapshot
events) happen while `raft.Node`'s own mutex is held, which is a
deliberate, bounded trade-off: these events are infrequent, and a single
buffered `slog` text-handler write is fast enough not to meaningfully
contend the lock; the alternative (capture-then-unlock-then-log at every
such site) would have complicated already-intricate locking for no
measurable benefit. Every *hot-path* log call (`raft.append.success`,
`raft.commit.advance`, `state_machine.apply`, `wal.append`, `wal.sync`)
is Debug-level, so at the default Info level `slog` returns before
formatting anything -- see §11. No metric or log call ever performs
network I/O, and no Raft commit path depends on metrics export or
logging succeeding (see §23).

## 20. Failure behavior

A metrics-registry or logging failure can never affect consensus, by
construction: `internal/metrics` has no failure mode that returns an
error to its caller at all (every operation is a local, infallible
atomic update), and `internal/logging`'s underlying `slog` handler
writes are fire-and-forget from Raft's perspective -- nothing in
`internal/raft`, `internal/statemachine`, or `internal/storage` checks a
logging or metrics call's outcome before proceeding. The diagnostic HTTP
server (`internal/api`) runs in its own goroutine, independent of the
Raft/Applier goroutines; `Server.Shutdown`'s failure only affects the
HTTP listener, never the underlying `dbnode.Node`.

## 21. Testing

New test files, one per touched package:

| File | Covers |
|---|---|
| `internal/metrics/registry_test.go` | Counter/gauge/histogram semantics, vecs, pull gauges, Prometheus rendering, concurrent increments, duplicate-name panic. |
| `internal/logging/logging_test.go` | Level parsing, level filtering, field attachment, concurrent logging. |
| `internal/raft/observability_test.go` | `Status`/`PeerStatuses`, election/leader-change/step-down counters, replication counters, snapshot counters, `ReadIndex` success/failure counters. |
| `internal/statemachine/observability_test.go` | Command/apply-success/apply-failure/dedup-hit/dedup-conflict counters. |
| `internal/storage/memtable/observability_test.go` | Incremental `Len`/`Bytes` tracking across put/update/delete/resurrect. |
| `internal/dbnode/observability_test.go` | `Status()`, pull-gauge wiring, `ConsistentGet` success/failure counters. |
| `internal/api/server_test.go` | All four endpoints against a real `dbnode.Node`; read-only enforcement (405 on non-GET); endpoints never mutate node state; API request metrics recorded. |

All new and pre-existing tests pass; see §27 for exact validation output.

## 22. Known limitations

* **SSTables/compaction are not instrumented.** `internal/storage/sstable`
  and `internal/storage/compaction` are fully implemented and tested in
  isolation, but are not yet wired into the live `MemStore` write path
  `dbnode.Node` actually uses (confirmed by searching the whole
  repository for any call from outside those two packages into either).
  Adding `compaction_*`/`sstable_*` metrics today would describe code
  that never runs in the current write path -- fabricated activity, which
  the phase brief explicitly forbids (§13, §48). These metrics should be
  added together with whatever future phase actually wires SSTables into
  `MemStore`.
* **One "current node" per process for pull-based gauges.** The
  Raft/storage pull gauges (`forgedb_raft_term`, `..._role`, `..._memtable_entries`,
  etc.) are wired to whichever `dbnode.Node` most recently called `Open`
  in this process (`internal/dbnode/metrics.go`'s `currentNode`). In
  production this is not a limitation at all -- exactly one `Node` is
  ever open per process. It only matters in a test binary that opens
  several `Node`s in the same process (as this package's own tests, and
  `internal/raft`'s, already do for unrelated reasons); such a test sees
  gauges for the most recently opened node, not necessarily "the leader"
  -- see `TestCurrentNode_FollowsMostRecentOpen`.
* **`forgedb_storage_errors_total` was not added as a separate metric.**
  `MemStore`'s only failure mode today is a WAL fault, which is already
  counted once, precisely, by `forgedb_errors_total{component="wal"}` (and,
  via `wal_corruption_errors_total`, with more detail). A second,
  `storage`-labeled counter for the identical underlying event would be
  exactly the kind of double-counting across layers §49 warns against.
* **No distributed tracing.** Considered and intentionally left out (see
  §2) -- logs plus metrics plus `/cluster` already make a request
  traceable conceptually through propose -> commit -> apply -> storage,
  and ForgeDB has no client-facing API yet for a trace ID to originate
  from.
* **`go test -race` cannot run in this environment.** `CGO_ENABLED=0` by
  default on this Windows setup and no C toolchain is configured, so
  `-race` fails immediately with "requires cgo" for every package,
  including ones this phase never touched. This is a pre-existing
  environment limitation, not something Phase 12 introduced or could fix
  without altering the toolchain, which was out of scope.
* **`api_requests_total`'s label is a single composite string**
  (`"METHOD route status_class"`), not three separate labels, because
  this phase's `Vec` metric types support exactly one label dimension by
  design (simplicity over a general-purpose label-set implementation,
  which this project's actual scale does not need).

## 23. What Phase 12 proves

* Every piece of Raft/storage/state-machine behavior built in Phases
  0-11 now has a corresponding, accurately-defined metric, diagnostic
  log event, or field in `/cluster` -- nothing here was fabricated to
  "look complete" (see §48's discipline: every counter's exact
  increment condition is documented in §6 above and traceable to a real
  code path).
* Observability was added without changing a single existing test's
  outcome: every pre-existing test across `internal/raft`,
  `internal/statemachine`, `internal/storage` (and subpackages),
  `internal/dbnode`, `chaos`, and `correctness` still passes, including
  repeated (`-count=30`) runs for timing-sensitive packages -- see §27.
* A real chaos run (`cmd/forge-chaos -nodes 5 -steps 150`) produces a
  coherent, readable structured-log trace of every election, leader
  change, step-down, and append failure the scenario induced, and still
  converges and passes its invariant checks -- instrumentation did not
  perturb the behavior it is observing.

## 24. What Phase 13 should build next

Phase 13 (performance benchmarking, explicitly out of scope here) can
build directly on this phase's metrics: `forgedb_consistent_get_latency_seconds`,
`forgedb_api_request_latency_seconds`, and the various `_total` counters
are exactly the signals a throughput/latency benchmark suite would want
to assert against, without needing any new instrumentation first. Phase
14 (deployment, also out of scope here) can point a real Prometheus at
the existing `/metrics` endpoint and a real load balancer's health check
at `/health`/`/ready` with no further changes to this phase's code.

## 25. Validation

```text
gofmt -l .                        -> no output (every changed file formatted)
go vet ./...                      -> clean
go build ./...                    -> clean
go build ./cmd/forgedb            -> clean
go run ./cmd/forgedb              -> exits 0; demonstrates storage recovery
                                      and a full dbnode+api wiring (elects
                                      itself leader, serves /health, /ready,
                                      /cluster, /metrics, shuts down cleanly)
go build ./cmd/forge-chaos        -> clean
go run ./cmd/forge-chaos -nodes 5 -steps 150 -seed 42
                                   -> PASS, cluster converged, no invariant
                                      violations, readable structured log
go test ./...                     -> ok, every package
go test ./internal/raft/...      -count=30  -> ok
go test ./internal/statemachine/... -count=30  -> ok
go test ./internal/dbnode/...    -count=30  -> ok
go test ./chaos/...     -count=5  -> ok
go test ./correctness/... -count=5 -> ok
go test -race ./...               -> "requires cgo" (pre-existing
                                      environment limitation; see §22)
```

No production correctness issue was discovered while implementing or
testing this phase.
