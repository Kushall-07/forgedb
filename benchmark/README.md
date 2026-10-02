# ForgeDB Benchmarks (Phase 13)

This directory, together with `*_test.go` files inside the packages they
measure, is ForgeDB's Phase 13 benchmarking framework: a reproducible way
to answer *how fast is ForgeDB, where does time go, and how does it scale*
-- not just a pile of `BenchmarkXxx` functions.

See `docs/benchmarking/phase13-benchmarking.md` for the full methodology,
recorded results, and bottleneck analysis. This file is the practical
"how to run it and how to read it" reference.

## Where the benchmarks live

Benchmarks are colocated with the package they measure (so they can reuse
each package's own test helpers and, where needed, its unexported
internals), **except** the end-to-end workload benchmarks, which live
here because they compose multiple packages through the public
`chaos.Cluster` harness.

| Layer | Location | What it measures |
|---|---|---|
| MemTable | `internal/storage/memtable/bench_test.go` | Pure in-memory skip list: Put/Get/Delete, mixed workloads, concurrency. No WAL, no disk. |
| WAL | `internal/storage/wal/bench_test.go` | Append vs. Append+Sync (the real durability boundary), recovery/replay time by record count. |
| Storage | `internal/storage/bench_test.go` | `storage.MemStore` (WAL+MemTable) through the real `Store` interface: durable Put/Get/Delete, mixed workloads, reopen/recovery. |
| SSTable | `internal/storage/sstable/bench_test.go` | Standalone SSTable format: write, open, point lookup (hit/miss/bloom-negative), iteration. **Not on the live write path** -- see below. |
| Manifest | `internal/storage/manifest/bench_test.go` | Standalone Manifest/VersionSet: AddTable, reopen. **Not on the live write path.** |
| Compaction | `internal/storage/compaction/bench_test.go` | Standalone `Compact`: non-overlapping, overlapping (duplicate keys), and tombstone-bearing merges. **Not on the live write path.** |
| State machine | `internal/statemachine/bench_test.go` | `KVStateMachine.Apply` against a real `MemStore`: new PUT/DELETE vs. deduplicated replay. |
| Raft | `internal/raft/bench_test.go` | Consensus alone: Propose/commit latency and ReadIndex by cluster size, election convergence, proposal concurrency. No persistence, no storage, in-memory transport. |
| DBNode | `internal/dbnode/bench_test.go` | The full production composition (real `FilePersister` + real `MemStore`): fully durable, fully replicated Put/Delete, local `Get`, linearizable `ConsistentGet`. |
| End-to-end | `benchmark/endtoend/bench_test.go` | Client-shaped workload mixes, key distributions, and value sizes against a real multi-node `chaos.Cluster`. |

## What the live write path actually exercises

**Important for interpreting every number above.** The production write
path, exactly as `dbnode.Node` wires it, is:

```
Propose -> Raft commit -> Applier.ApplyAvailable -> KVStateMachine.Apply -> storage.MemStore.Put/Delete -> WAL Append+Sync -> MemTable
```

`internal/storage/sstable`, `internal/storage/manifest`, and
`internal/storage/compaction` are real, tested, working packages, but as
of this phase **`storage.MemStore` never calls into any of them.** They
are benchmarked here (per the Phase 13 brief's instruction to benchmark
components even when not yet wired into the live path) strictly as
standalone components. Never read their numbers as representative of
DBNode or end-to-end performance -- there is no code path today where a
client write or read touches an SSTable, a Manifest, or a compaction.

## Known codebase limitation: single-node clusters don't commit

There is deliberately no single-node ("1 node") variant of any
*committed*-write or `ConsistentGet` benchmark. `internal/raft`'s commit
rule (`maybeAdvanceCommitIndexLocked`) is only ever invoked from a peer's
AppendEntries reply; with zero peers, that code path never runs, so a
literal one-node Raft cluster never advances its own `CommitIndex`
through `Propose` alone. This is existing, documented behavior of the
real implementation (see `internal/raft/apply_test.go`'s `commitUpTo`
comment and `internal/dbnode/node_test.go`'s section A), not something
Phase 13 introduces or should fix -- see
`docs/benchmarking/phase13-benchmarking.md`'s limitations section.
Everywhere a cluster size matters, this suite uses 3 and 5 nodes.

`internal/raft/bench_test.go` does include one single-node benchmark,
`BenchmarkRaftProposeSingleNodeUnconfirmed`, which measures `Propose`'s
own call cost without waiting for (or claiming) a commit -- its result
must never be compared against the 3-/5-node Propose benchmarks, which
measure a confirmed commit.

## How to run

Standard `go test -bench` is the mechanism; there is no custom CLI.

```powershell
# One package's benchmarks, with allocation stats:
go test ./internal/storage/memtable/... -run '^$' -bench=. -benchmem

# A name filter across the whole repo:
go test ./... -run '^$' -bench='MemTable|WAL|Storage' -benchmem

# Everything (see the caveat below):
go test ./... -run '^$' -bench=. -benchmem
```

### The `-benchtime=Nx` caveat for Raft/DBNode/end-to-end benchmarks

`raft.FilePersister` (the real, production Raft persistence
implementation) rewrites and fsyncs this node's **entire** Raft log on
every single `SaveState` call -- i.e. on every `Propose`. This is a real,
measured property of the production code (see
`docs/benchmarking/phase13-benchmarking.md`'s bottleneck analysis), not a
benchmark bug, but it means a time-based `-benchtime=1s` run against
`internal/dbnode`, `internal/raft`'s commit benchmarks, or
`benchmark/endtoend` lets Go's adaptive iteration count grow the log
indefinitely, making later iterations within the same run progressively
slower and the run itself take far longer than 1 second of wall time.

**Always run these three packages with an explicit iteration count**
instead of a time budget:

```powershell
go test ./internal/raft/...        -run '^$' -bench=. -benchtime=50x -benchmem
go test ./internal/dbnode/...      -run '^$' -bench=. -benchtime=20x -benchmem
go test ./benchmark/endtoend/...   -run '^$' -bench=. -benchtime=10x -benchmem
```

Every other package (MemTable, WAL, Storage, SSTable, Manifest,
Compaction, StateMachine) is safe with the default time-based
`-benchtime`, though Storage's `MemStoreReopen/records=100000` and a few
Manifest/Compaction cases are slow enough that a short explicit bound
(`-benchtime=200ms` to `-benchtime=5x`) keeps a full-suite run practical;
see `scripts/run.ps1`, which already encodes the right flags per package.

## Determinism

Every benchmark that generates data or makes a workload decision (which
key, read vs. write, which key distribution) uses a fixed
`rand.NewSource` seed, never an unseeded or time-seeded source. Re-running
the same benchmark with the same Go version and parameters operates on
byte-identical synthetic data every time.

## Setup vs. measurement

Every benchmark that needs preloaded data (e.g. a `Get` benchmark against
a warm dataset) populates that data with the timer stopped
(`b.StopTimer()`/`b.ResetTimer()`), never inside the measured loop. Where
a benchmark's *point* is setup/recovery cost itself (WAL replay, MemStore
reopen, SSTable open, Manifest reopen), that is its own, separately named
benchmark -- never folded into a steady-state Put/Get number.

## Key distributions

Three key-access patterns are used, always labeled, never silently mixed:

- **uniform** -- every key in a fixed keyspace equally likely.
- **hot-set** -- 80% of operations target the smallest 20% of keys
  (`benchmark/endtoend`'s `hotsetKeys`), modeling a small set of
  frequently-touched records.
- **sequential** -- strictly increasing key index, modeling an
  append/scan-like pattern rather than random lookups.

Most lower-layer benchmarks (MemTable, Storage, DBNode) use a fixed,
bounded keyspace accessed uniformly or sequentially, sized small enough
that every write durably fsyncs without making benchmark setup itself the
dominant cost -- see each file's own `benchKeyspace` comment for the
specific bound and why.

## Latency: ns/op is a mean, not a percentile

Go's own `ns/op` is an average over every iteration; it is never read as
p95 or p99 anywhere in this suite's documentation. `benchmark/endtoend`'s
workload benchmarks additionally record each individual operation's
latency (bounded to at most 20,000 retained samples via a reservoir
replacement, regardless of `b.N` -- see `latencyRecorder` in
`bench_test.go`) and report p50/p95/p99 as custom metrics
(`b.ReportMetric`), visible as extra columns in `go test`'s own output.

## Concurrency

Concurrency scaling is measured only where the thing being driven is
documented safe for concurrent use:

- `internal/storage/memtable`: `MemTable` is `sync.RWMutex`-guarded --
  `BenchmarkMemTableConcurrentMixed` sweeps concurrency 1/2/4/8/16/32.
- `internal/storage`: `MemStore.Put` is safe for concurrent callers (the
  underlying WAL serializes Append+Sync internally) --
  `BenchmarkStoreConcurrentPut` sweeps concurrency 1/2/4/8/16.
- `internal/raft`: `Propose` is safe for concurrent callers (serialized
  by the Node's own mutex) -- `BenchmarkRaftProposeConcurrent` sweeps
  concurrency 1/2/4/8/16.

`chaos.Cluster`, which every end-to-end benchmark uses, is explicitly
documented as **not** safe for concurrent use ("meant to be driven by
exactly one scenario at a time" -- see `chaos/cluster.go`'s package doc).
Every end-to-end benchmark therefore models one sequential client, not
concurrent load.

## InMemoryTransport is not a network

Every Raft, DBNode, and end-to-end benchmark in this suite uses
`raft.InMemoryTransport`: real consensus logic and real synchronization
(locks, goroutines, channels), but RPCs are in-process function calls,
never a socket. These numbers measure *the algorithm plus an in-process
transport*, not TCP/gRPC latency, packet loss, or serialization cost.
Real network characterization is explicitly out of scope for this phase
(it belongs with Phase 14 deployment work) and nothing here should be
quoted as "network throughput."

## What is and isn't durable

No benchmark in this suite disables WAL sync, Raft persistence, majority
commit, state-machine apply, or deduplication to make itself faster. The
two places that might look like an exception are both explicitly labeled:

- `BenchmarkWALAppendNoSync` measures buffered Append with **no** fsync,
  solely so `BenchmarkWALAppendSync`'s fsync overhead can be isolated by
  comparison. Production code never skips the Sync call (see
  `storage/memstore.go`'s Put/Delete).
- `internal/raft/bench_test.go`'s benchmarks use no `Persister` at all
  (defaulting to an in-memory no-op), which is a deliberate choice to
  isolate pure consensus cost from persistence cost -- `internal/dbnode`'s
  benchmarks, right next to them in the matrix, use the real
  `FilePersister` and include that cost.

## Comparing two runs

`scripts/compare.ps1` takes two saved `go test -bench` output files and
prints each shared benchmark's old/new ns/op and the percent delta, for
spotting regressions between a baseline and a later change:

```powershell
./benchmark/scripts/compare.ps1 -Old benchmark/results/phase13-baseline.txt -New benchmark/results/after-change.txt
```

## Reproducibility checklist

Every result quoted in `docs/benchmarking/phase13-benchmarking.md`
records, alongside the number itself:

- Git revision (`git rev-parse HEAD`)
- Go version (`go version`)
- OS/arch/CPU (from `go test`'s own `goos`/`goarch`/`cpu` header lines)
- The exact `-bench`/`-benchtime`/`-benchmem` flags used
- Cluster size, value size, key distribution, and workload mix where
  applicable

Results are **environment-dependent**. A number measured on one machine
is not a universal claim about ForgeDB's performance -- see the report's
own framing.

## Profiling (optional, off by default)

Standard Go tooling works unmodified and needs no project changes:

```powershell
go test ./internal/storage/... -run '^$' -bench=BenchmarkStorePut -cpuprofile=cpu.out -memprofile=mem.out
go tool pprof cpu.out
```

No benchmark enables profiling by default, and nothing in production code
changes to support it.
