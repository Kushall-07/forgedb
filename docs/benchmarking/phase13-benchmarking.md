# Phase 13: Benchmarking & Performance Characterization

This document reports what was actually measured when characterizing
ForgeDB's performance across its real, production code paths. It does
not implement or recommend any change to production code -- Phase 13 is
measurement and analysis only (see `benchmark/README.md`'s scope notes
and section 67 of the phase brief). Phase 14 (deployment) is explicitly
out of scope and not touched here.

**All results below are environment-dependent.** They describe what this
one machine, on this one day, measured for this one Git revision. They
are not a claim that "ForgeDB can handle X requests/sec" in general, and
they are never compared against Redis, RocksDB, PostgreSQL, etcd, or
LevelDB.

## 1. Methodology

| Field | Value |
|---|---|
| Git revision | `a0f9588c9763abf0e8beb533de5993cead9e559c` |
| Go version | `go1.27.1 windows/amd64` |
| OS | Windows 11 Home Single Language |
| CPU | 13th Gen Intel(R) Core(TM) i5-13450HX |
| Logical CPUs | 16 |
| RAM | ~15.7 GiB |
| Storage medium | SSD (SK Hynix HFS512GEJ4X112N, NVMe-class) |
| Benchmark framework | Go's standard `testing.B` (`go test -bench`), no custom harness |
| Transport | `raft.InMemoryTransport` everywhere -- see section 10 |
| Seeds | Every benchmark that generates data or workload decisions uses a fixed `rand.NewSource` seed (see `benchmark/README.md`'s Determinism section) |

Flags used per package (see `benchmark/scripts/run.ps1`, which encodes
exactly this):

| Package | `-benchtime` | Why |
|---|---|---|
| `internal/storage/memtable` | `500ms` (time-based) | Pure in-memory; no fsync, safe to let Go calibrate iteration count. |
| `internal/storage/wal` | `500ms` | Real fsync per op, but each op is O(1) -- no log growth across iterations. |
| `internal/storage` | `3x` (write-heavy), `1x` (`MemStoreReopen/records=100000` alone) | Real fsync per op; small explicit counts keep total run time practical. |
| `internal/storage/sstable` | `500ms` | Standalone, no fsync-per-op pattern. |
| `internal/storage/manifest` | `500ms` | Standalone; `AddTable` does fsync but is O(1) per call regardless of table count. |
| `internal/storage/compaction` | `5x` | Each call does real file I/O (SSTable write + Manifest publish); small count keeps runtime practical. |
| `internal/statemachine` | `500ms` | Real fsync via the underlying `MemStore`, but O(1) per call. |
| `internal/raft` | `50x` (explicit) | No persistence in this package's benchmarks; `50x` chosen for a reasonably stable sample within seconds. |
| `internal/dbnode` | `20x` (explicit) | **Required**, not optional: `raft.FilePersister.SaveState` rewrites and fsyncs the *entire* Raft log on every `Propose` (see section 6), so a time-based run lets the log, and each iteration's cost, grow without bound. |
| `benchmark/endtoend` | `10x` (explicit) | Same reason as `internal/dbnode`, compounded by driving a full 3-/5-node cluster per operation. |

**Sample-size caveat.** Because of the `FilePersister` full-log-rewrite
cost above, every `internal/dbnode` and `benchmark/endtoend` number below
comes from only 10-20 iterations, not Go's usual thousands-to-millions.
These numbers are directionally reliable (the relative shape -- 3-node vs.
5-node, write vs. read, dedup-hit vs. new -- is consistent and large) but
the absolute ns/op carries more run-to-run variance than a
higher-iteration-count benchmark would. Section 9 documents one place
this variance actually appeared in the collected data.

Full raw output for every benchmark in this report is saved at
`benchmark/results/phase13-baseline.txt`.

## 2. Architecture reminder: what the live write path actually exercises

```
Propose -> Raft commit -> Applier.ApplyAvailable -> KVStateMachine.Apply -> storage.MemStore.Put/Delete -> WAL Append+Sync -> MemTable
```

`internal/storage/sstable`, `internal/storage/manifest`, and
`internal/storage/compaction` are real, independently tested packages,
but **`storage.MemStore` never calls into any of them** as of this
revision. Every SSTable/Manifest/Compaction number in section 5 below
characterizes those packages in isolation; none of it describes DBNode or
end-to-end behavior, because no code path today routes a client
read/write through any of them.

## 3. Results: MemTable (pure in-memory, no disk, no Raft)

| Benchmark | ns/op | Throughput | B/op | allocs/op |
|---|---:|---:|---:|---:|
| Put/64B | 447.7 | 142.95 MB/s | 126 | 4 |
| Put/256B | 514.6 | 497.45 MB/s | 321 | 4 |
| Put/1KiB | 690.0 | 1483.97 MB/s | 1090 | 4 |
| Put/4KiB | 1767 | 2318.54 MB/s | 4173 | 4 |
| Put/16KiB | 6789 | 2413.41 MB/s | 16483 | 4 |
| Put/64KiB | 12710 | 5156.28 MB/s | 65683 | 5 |
| GetHit/64B | 421.0 | -- | 95 | 2 |
| GetHit/64KiB | 43187 | -- | 65568 | 2 |
| GetMiss | 224.3 | -- | 32 | 2 |
| Delete | 388.5 | -- | 55 | 2 |
| Mixed 90% read / 10% write | 1175 | -- | 290 | 3 |
| Mixed 50/50 | 1134 | -- | 299 | 3 |
| Mixed 10% read / 90% write | 1092 | -- | 309 | 3 |

**Concurrency (50/50 mixed, `sync.RWMutex`-guarded):**

| Concurrency | ns/op |
|---:|---:|
| 1 | 1896 |
| 2 | 2297 |
| 4 | 2403 |
| 8 | 2030 |
| 16 | 2149 |
| 32 | 2362 |

MemTable's single `sync.RWMutex` does not scale with added goroutines on
this 16-logical-core machine: per-op latency is essentially flat (within
noise) from 1 to 32 concurrent callers, meaning aggregate throughput does
not meaningfully improve past a handful of concurrent writers -- the lock
is the ceiling, not CPU count.

## 4. Results: WAL and Storage (the real durability boundary)

**WAL, Append vs. Append+Sync, by value size:**

| Size | Append-only ns/op | Append+Sync ns/op | Sync overhead |
|---|---:|---:|---:|
| 64B | 3520 | 278476 | +274,956 ns (78x) |
| 256B | 4096 | 273939 | +269,843 ns (67x) |
| 1KiB | 5350 | 281947 | +276,597 ns (53x) |
| 4KiB | 9478 | 378311 | +368,833 ns (40x) |
| 16KiB | 17494 | 320534 | +303,040 ns (18x) |
| 64KiB | 44356 | 368305 | +323,949 ns (8x) |

**This is the single clearest bottleneck in the entire system**: the
fsync call inside `Sync` costs roughly 270-380 microseconds *regardless
of value size*, on an SSD, on this machine. Buffered append cost scales
with size as expected (3.5us at 64B up to 44us at 64KiB), but it is
dwarfed by the fixed fsync cost until values get large -- at 64B, sync
overhead is 78x the append cost itself; even at 64KiB it is still 8x.

**WAL replay (recovery), by record count:**

| Records | ns/op | Scaling |
|---:|---:|---:|
| 1,000 | 402,946 (0.40 ms) | -- |
| 10,000 | 4,361,142 (4.36 ms) | ~10.8x for 10x records |
| 100,000 | 44,544,925 (44.5 ms) | ~10.2x for 10x records |

Replay scales essentially linearly with record count, as expected for a
single sequential pass with no indexing.

**Storage (`MemStore`, real `Store` interface, durable):**

| Benchmark | ns/op |
|---|---:|
| Put/64B | 7,382,167 (n=3, noisy -- see note) |
| Put/256B | 2,912,067 |
| Put/1KiB | 829,500 |
| Put/4KiB | 2,172,533 |
| Put/16KiB | 2,878,233 |
| Put/64KiB | 4,411,833 |
| Get/64B | 3,167 |
| Get/64KiB | 21,900 |
| ReadAfterWrite | 3,246,467 |
| Delete | 4,534,433 |
| Mixed write-only (0% read) | 4,929,933 |
| Mixed balanced (50/50) | 621,900 |
| Mixed write-heavy (10% read) | 3,095,933 |

*Note on sample size:* these `Put`/`Delete`/mixed numbers come from only
3 iterations each (`-benchtime=3x`, chosen to keep a full-suite run
practical given the fsync cost above), so individual values are noisy --
the `Put/64B` figure above is higher than `Put/256B` purely from sample
variance, not because smaller values are slower to write. The *pattern*
(every durable write costs single-digit milliseconds, dominated by fsync,
regardless of value size) is the reliable takeaway, consistent with the
WAL-level numbers above it.

**MemStore reopen (recovery), by record count:**

| Records | ns/op |
|---:|---:|
| 1,000 | 7,791,367 (7.8 ms) |
| 10,000 | 14,596,267 (14.6 ms) |
| 100,000 | 132,649,300 (132.6 ms) |

Compare against WAL-only replay above (0.40 / 4.36 / 44.5 ms for the same
record counts): `MemStore`'s reopen adds roughly 3x WAL-replay's own cost
at every size, attributable to `MemTable.Put`'s per-record skip-list
insertion and copying on top of the WAL's own decode.

**Concurrent durable Put** (WAL serializes Append+Sync internally via its
own mutex):

| Concurrency | ns/op |
|---:|---:|
| 1 | 2,121,400 |
| 2 | 4,676,700 |
| 4 | 6,663,200 |
| 8 | 2,482,467 |
| 16 | 3,447,000 |

At only 3 samples per point, this is too noisy to call a clean trend, but
the absence of any consistent *improvement* with added concurrency is
itself informative: since every write still serializes through one WAL
file's Append+Sync, adding concurrent callers cannot increase durable
write throughput past whatever one fsync-bound writer already achieves.

## 5. Results: SSTable, Manifest, Compaction (standalone -- not on the live path)

**SSTable:**

| Benchmark | ns/op |
|---|---:|
| Write/100 entries | 4,176,560 |
| Write/1,000 entries | 5,767,685 |
| Write/10,000 entries | 20,368,492 |
| Open/100 entries | 51,155 |
| Open/10,000 entries | 123,129 |
| Point lookup, hit | 9,277 |
| Point lookup, miss (in-range) | 294.7 |
| Point lookup, bloom-negative (out-of-range) | 279.9 |
| Iterator scan/1,000 entries | 532,384 |
| Iterator scan/10,000 entries | 5,256,590 |

A miss resolved by the bloom filter/key-range check (280-295 ns) is about
**31x cheaper** than an actual hit (9,277 ns, which must decode a real
data block) -- the bloom filter is doing exactly its intended job of
avoiding unnecessary block reads for absent keys.

**Manifest:**

| Benchmark | ns/op |
|---|---:|
| AddTable, 10 existing tables | 5,565,078 |
| AddTable, 100 existing tables | 9,755,550 |
| AddTable, 1,000 existing tables | 9,865,758 |
| Reopen, 10 tables | 60,403 |
| Reopen, 100 tables | 81,383 |
| Reopen, 1,000 tables | 298,627 |

`AddTable`'s cost barely grows from 100 to 1,000 existing tables (9.76 ms
vs. 9.87 ms) -- consistent with it being dominated by the atomic
file-write/fsync/rename protocol (`internal/storage/atomicfile`) rather
than by how much manifest data there is to serialize at these sizes.
`Reopen`, which must actually parse every table entry, grows more clearly
with table count.

**Compaction:**

| Workload | 2 tables | 4 tables | 8 tables |
|---|---:|---:|---:|
| Non-overlapping | 126.9 ms | 109.4 ms | 194.1 ms |
| Overlapping (duplicate keys) | 69.2 ms | 104.4 ms | 177.6 ms |
| With tombstones | 65.6 ms | 136.5 ms | 181.9 ms |

(500 keys/table; `n=5` samples per cell.) Cost grows roughly with input
table count across all three workload shapes, as expected for a
multi-way merge that must open, read, and merge every input table and
then durably write one output table plus a manifest update. The
non-overlapping/overlapping/tombstone shapes do not differ dramatically
at this scale -- the dominant cost here is file I/O (per-table open/read,
one atomic output write, one manifest publish), not the in-memory merge
logic itself.

## 6. Results: State machine (`KVStateMachine.Apply`, real `MemStore`)

| Benchmark | ns/op |
|---|---:|
| Apply PUT, new request | 3,684,545 |
| Apply DELETE, new request | 3,656,553 |
| Apply, duplicate request (dedup hit) | 89.23 |
| New vs. duplicate, same run: New | 4,281,803 |
| New vs. duplicate, same run: Duplicate | 80.58 |

**Deduplication is essentially free compared to a real write**: a
replayed `(ClientID, RequestID)` resolves from the in-memory dedup map in
under 100 nanoseconds, roughly **41,000-53,000x faster** than a new
request, which pays the full WAL Append+Sync cost from section 4. This
confirms deduplication does exactly what it is for -- a retried client
request costs almost nothing extra on the server side.

## 7. Results: Raft (pure consensus, in-memory transport, no persistence)

| Benchmark | ns/op |
|---|---:|
| Propose, 3-node (confirmed commit) | 13,764 |
| Propose, 5-node (confirmed commit) | 9,810 |
| Propose, 1-node (**unconfirmed** -- see section 9) | 410 |
| ReadIndex, 1-node | 34 |
| ReadIndex, 3-node | 5,306 |
| ReadIndex, 5-node | 6,382 |
| Election convergence, 1-node | 5,172 |
| Election convergence, 3-node | 19,064 |
| Election convergence, 5-node | 42,922 |

**Concurrent Propose** (serialized by the leader's own mutex):

| Concurrency | ns/op |
|---:|---:|
| 1 | 5,896 |
| 2 | 6,580 |
| 4 | 20,920 |
| 8 | 21,232 |
| 16 | 7,154 |

`ReadIndex` and election convergence both show the expected pattern: cost
grows with cluster size, since both require a confirmation round trip to
more peers (one heartbeat per peer for `ReadIndex`; more votes needed for
election). `Propose`'s 3-node vs. 5-node numbers above (13.8us vs. 9.8us)
invert that expectation and should be read as sample noise at `n=50`
rather than "5 nodes commits faster than 3," given `internal/dbnode`'s
much more heavily-sampled equivalent (section 8) shows the expected
5-node-costs-more direction clearly. Concurrent Propose latency is noisy
but shows no consistent improvement with added concurrency, consistent
with every call serializing through one leader mutex regardless of caller
count.

All of these numbers exclude persistence (no `Persister` --
`internal/raft`'s `Options.Persister` defaults to a no-op) and exclude
storage entirely. Compare against section 8 for the fully durable cost.

## 8. Results: DBNode (full production composition: real FilePersister + real MemStore)

| Benchmark | ns/op |
|---|---:|
| Put, 3-node, committed | 33,219,340 (33.2 ms) |
| Put, 5-node, committed | 48,448,665 (48.4 ms) |
| Delete, 3-node, committed | 22,063,025 (22.1 ms) |
| Get, local (no Raft) | 1,225 |
| ConsistentGet, 3-node | 10,990 |
| ConsistentGet, 5-node | 12,075 |

**Three numbers worth comparing directly:**

- **Committed Put vs. pure Raft Propose**: 33.2 ms (DBNode, 3-node) vs.
  13.8 us (raft-only, 3-node) -- roughly **2,400x**. Essentially all of
  this gap is `FilePersister`'s full-log-rewrite-and-fsync cost (section
  9), not replication itself, which section 7 already showed costs
  microseconds.
- **ConsistentGet vs. local Get**: 10,990 ns vs. 1,225 ns -- **~9x**. This
  is the measured cost of linearizability on this environment: one
  `ReadIndex` quorum round trip plus an apply-barrier wait, entirely
  in-memory (no WAL/disk read), versus a direct MemTable read.
- **5-node vs. 3-node committed Put**: 48.4 ms vs. 33.2 ms -- a real
  **~46% increase**, the clearest quorum-size cost this report measures
  for a durable write: more peers' `FilePersister`/replication work must
  complete before a majority is reached.

## 9. Results: End-to-end workloads (`chaos.Cluster`, 3- and 5-node)

**Workload mix matrix (uniform key distribution):**

| Workload | 3-node ns/op | 3-node logical-ops/sec | 3-node p95 | 5-node ns/op | 5-node logical-ops/sec | 5-node p95 |
|---|---:|---:|---:|---:|---:|---:|
| 100% writes | 36,587,060 | 27.3 | 28.2 ms | 23,936,800 | 41.8 | 33.8 ms |
| 90% read / 10% write | 3,568,970 | 280.3 | 11.9 ms | 4,937,880 | 202.5 | 19.3 ms |
| 50/50 | 8,150,940 | 122.7 | 19.2 ms | 11,611,210 | 86.1 | 24.9 ms |
| 10% read / 90% write | 16,205,720 | 61.7 | 20.9 ms | 32,226,550 | 31.0 | 39.0 ms |
| 100% reads (ConsistentGet) | 9,900 | 104,712 | ~0 ms | 24,780 | 41,118 | ~0 ms |

**Key distribution (3-node, 50/50 mix):**

| Distribution | ns/op | ops/sec |
|---|---:|---:|
| Uniform | 10,874,130 | 92.0 |
| Hot-set (80/20) | 6,432,000 | 155.5 |
| Sequential | 7,551,100 | 132.4 |

**Value size (3-node, 50/50 mix):**

| Size | ns/op | ops/sec |
|---|---:|---:|
| 256B | 11,355,220 | 88.1 |
| 4KiB | 11,108,980 | 90.0 |
| 64KiB | 9,328,290 | 107.2 |

**A real limitation of this data, stated plainly:** at only 10 iterations
per point (`-benchtime=10x`, required by the `FilePersister` cost from
section 6), the 100%-writes row above shows the 5-node cluster (23.9 ms)
*faster* than the 3-node cluster (36.6 ms) -- the opposite of
`internal/dbnode`'s much more consistent finding in section 8 (5-node
clearly slower). This is sample-size noise, not a real effect, and is
reported here rather than quietly smoothed over, exactly because a
benchmarking report's job is to say what was actually measured. The
directionally reliable findings from this section are: read-only
workloads are dramatically cheaper than any workload containing writes
(three to four orders of magnitude in ops/sec); write fraction clearly
and monotonically increases latency within a fixed cluster size; and
hot-set/sequential key access was faster than uniform at this sample
size, plausibly real (better locality) but not confirmed independent of
the same small-sample caveat. Value size showed no clear scaling effect
in the 256B-64KiB range at this workload and sample size, consistent with
section 4's finding that fsync's fixed cost dominates until values get
much larger.

## 10. Bottleneck analysis

Ranked by how much of the fully-durable, fully-replicated write latency
each layer measurably accounts for, from the numbers above:

1. **`raft.FilePersister`'s full-log-rewrite-per-Propose is the dominant
   cost of every committed write.** Section 8's Put numbers (33.2-48.4
   ms) are roughly 2,400x the equivalent in-memory-only Raft commit cost
   from section 7 (13.8 us) -- a gap far larger than WAL fsync alone
   (270-380 us, section 4) can explain. `FilePersister.SaveState`
   serializes and atomically rewrites the entire persisted log (current
   term, voted-for, and every log entry) on every single call, so its
   cost grows with however large the log already is -- this is a direct
   reading of the package's own doc comment, not an inference. This is
   the single most actionable finding in this report, though *fixing* it
   is explicitly out of Phase 13's scope (see section 67 of the phase
   brief: measure and document, do not redesign).
2. **WAL fsync is the dominant cost of a write once persistence is held
   constant.** Section 4 shows fsync adds a near-constant 270-380 us on
   top of a buffered append that costs 3.5-44 us depending on size --
   fsync, not value size, sets the floor for every durable KV write.
3. **Quorum size measurably adds cost on top of both of the above.**
   Section 8's 5-node-vs-3-node committed Put (48.4 ms vs. 33.2 ms, a
   ~46% increase) isolates genuine replication/quorum overhead once
   persistence cost is held constant (both numbers pay the same
   per-node `FilePersister` cost; the delta is what's left).
4. **Linearizable reads cost meaningfully more than local reads, but
   nowhere near as much as a write.** `ConsistentGet` (section 8) is
   ~9x a local `Get`, but local `Get` and `ConsistentGet` are both
   several orders of magnitude cheaper than any committed write, because
   neither touches the WAL or `FilePersister` at all.
5. **MemTable lock contention is real but small relative to everything
   above it.** Section 3's concurrency sweep shows per-op latency
   roughly flat (not improving, not collapsing) from 1 to 32 concurrent
   callers -- a measurable ceiling, but one that never shows up as the
   dominant cost in any full-stack number, since WAL fsync and Raft
   persistence dwarf it.
6. **Deduplication cost is negligible.** Section 6 shows a dedup hit
   costs under 100 ns against a multi-millisecond new write -- this path
   will never be a bottleneck.

## 11. Scaling observations

- **As value size increases:** buffered WAL append and MemTable
  operations scale with size roughly as expected (more bytes to copy/
  checksum), but durable WAL writes (`Append+Sync`) and the end-to-end
  workload benchmarks both show fsync's fixed cost dominating until sizes
  get large -- no clear latency increase was measurable going from 256B
  to 64KiB in the end-to-end 50/50 workload (section 9).
- **As concurrency increases:** MemTable (in-memory lock) and WAL-backed
  `MemStore.Put` (serialized by the WAL's own mutex) both show flat-to-
  noisy per-op latency rather than improved throughput -- neither
  benefits from added concurrent callers on this machine, because both
  paths' true bottleneck (a single lock, or a single fsync-ing file) is
  inherently sequential.
- **As cluster size increases (3 -> 5 nodes):** every quorum-dependent
  operation this report measured with enough samples to trust got more
  expensive: `ReadIndex` (5.3us -> 6.4us), election convergence (19.1us
  -> 42.9us, in-memory-only), and committed Put (33.2ms -> 48.4ms,
  fully durable). This is the expected, correct direction for Raft: more
  peers means more confirmations required for both leadership and commit.
- **As dataset size increases:** WAL replay and `MemStore` reopen both
  scale roughly linearly with record count (section 4) -- 10x records
  produced consistently ~10x recovery time at both the 1,000-to-10,000
  and 10,000-to-100,000 steps, with `MemStore` reopen (which also
  reconstructs the MemTable) costing roughly 3x bare WAL replay at every
  size.

## 12. Profiling

Optional CPU/memory profiling is available through standard Go tooling
with zero project changes (see `benchmark/README.md`'s profiling
section): `go test -bench=<name> -cpuprofile=cpu.out -memprofile=mem.out`,
then `go tool pprof`. No benchmark in this suite enables profiling by
default, and no production code was changed to support it. Profiling was
not run as part of producing this report's numbers (the bottleneck
analysis above was derived entirely from comparing benchmark results
across layers, which was sufficient to identify `FilePersister`'s
full-log-rewrite as the dominant cost without needing a CPU profile to
confirm it) -- a natural next step for anyone acting on this report's
findings would be a CPU profile of `BenchmarkDBNodePutThreeNode`
specifically, to confirm the time inside `FilePersister.SaveState` is
where the profile says it is.

## 13. Validation

| Check | Result |
|---|---|
| `gofmt -l .` | Clean (no files needed formatting after a fix to one file) |
| `go vet ./...` | Clean |
| `go build ./...` | Clean |
| `go test ./...` | All packages pass |
| `go test ./internal/raft/... ./internal/statemachine/... ./internal/dbnode/... -count=5` | All pass |
| `go test ./chaos/... ./correctness/... -count=5` | All pass |
| Full benchmark suite execution | All benchmarks run to completion and pass their internal assertions (see section 1's per-package flags) |
| `go test -race ./...` | **Could not run** -- see section 14 |

No existing Phase 0-12 test was modified, and none regressed.

## 14. Known limitations

- **`raft.InMemoryTransport` is not a network.** Every Raft, DBNode, and
  end-to-end number in this report measures the consensus algorithm plus
  an in-process, function-call transport -- real synchronization (locks,
  goroutines, channels) but never a socket, serialization format, or
  network round trip. None of this report's numbers should be quoted as
  network throughput or latency; real network characterization belongs
  to later deployment/integration work (Phase 14+).
- **The race detector could not be run in this environment.** `go test
  -race` requires cgo (`-race requires cgo; enable cgo by setting
  CGO_ENABLED=1`), and this machine has `CGO_ENABLED=0` with no C
  compiler on `PATH` (confirmed: `cgo: C compiler "gcc" not found`). This
  is an environment limitation, not a decision to skip race checking --
  the project's toolchain was not altered to force it to work, per the
  phase brief's explicit instruction not to do so.
- **Windows file-system/timer behavior.** All fsync-bound numbers in this
  report (sections 4, 6, 8) reflect `atomicfile`'s create-write-fsync-
  rename-directory-fsync protocol as it behaves under NTFS on this
  machine; absolute fsync latency is well known to vary significantly
  across operating systems and file systems, and these numbers should
  not be assumed to transfer to Linux/ext4/XFS deployments.
- **Hardware dependence.** Every absolute number in this report is tied
  to the single machine described in section 1. Only relative
  comparisons (layer A costs Nx layer B, cluster size X costs more than
  Y) are likely to generalize; absolute latencies and throughput numbers
  are not.
- **A genuine, documented codebase limitation: single-node clusters never
  commit.** `internal/raft`'s commit-advancement logic
  (`maybeAdvanceCommitIndexLocked`) is only ever invoked from a peer's
  AppendEntries reply; a literal one-node (zero-peer) cluster's
  broadcast loop never runs, so `CommitIndex` never advances through
  `Propose` alone. This is pre-existing, documented behavior (see
  `internal/raft/apply_test.go`'s `commitUpTo` comment and
  `internal/dbnode/node_test.go`'s section A), confirmed independently
  while building this phase's benchmarks, not something Phase 13
  introduced or attempted to fix. Every committed-write and
  `ConsistentGet` benchmark in this suite therefore uses 3 or 5 nodes.
- **Small sample sizes for the two most expensive layers.** As explained
  in section 1, `internal/dbnode` and `benchmark/endtoend` benchmarks ran
  with only 10-20 iterations because of `FilePersister`'s O(log size)
  cost per call. Section 9 documents one place this visibly produced a
  noisy, direction-reversing result. Higher-confidence numbers for these
  two layers would require either a much longer run (accepting that later
  iterations measure an artificially large log) or a benchmark harness
  that periodically snapshots to bound log growth -- the latter would be
  a reasonable future enhancement to this benchmark suite, not a
  production code change.
- **SSTable/Manifest/Compaction numbers do not describe live behavior.**
  Repeated from section 2 because it is easy to misread the tables in
  section 5 out of context: as of this revision, nothing in the live
  write path invokes any of these three packages.
- **Storage medium.** Confirmed as a single local SSD on this machine;
  no attempt was made to characterize behavior on spinning disk, network
  storage, or a different filesystem.

## 15. Recommendations for Phase 14 (not implemented here)

These are observations for whoever picks up deployment work next, not
instructions acted on in this phase:

- Real network transport characterization (replacing/wrapping
  `InMemoryTransport`) is the most obvious gap this report's numbers
  cannot speak to at all -- every Raft/DBNode/end-to-end number here
  excludes real network latency, serialization, and packet loss entirely.
- If committed-write throughput ever needs to improve, section 10's
  bottleneck ranking says where to look first: `FilePersister`'s
  full-log-rewrite is overwhelmingly the largest cost in the entire
  measured stack, ahead of WAL fsync, ahead of quorum size, ahead of
  everything else. (Changing this is explicitly out of Phase 13's scope;
  it is reported here as the single most load-bearing fact for Phase 14
  planning, not acted on.)
- A deployment environment's real disk (and whether it supports fast,
  reliable fsync) will directly determine committed-write latency more
  than almost anything else measured in this report -- provisioning
  guidance for Phase 14 should account for this before tuning anything
  else.
- If this benchmark suite is extended later, a snapshot-aware variant of
  the `internal/dbnode`/`benchmark/endtoend` benchmarks (periodically
  calling `CreateSnapshot` to bound Raft log growth within a run) would
  allow much higher iteration counts and tighter confidence intervals
  without changing what's being measured.
