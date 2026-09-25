# ForgeDB — Phase 4: Compaction

## Scope

Phase 4 adds a **compaction** subsystem (`internal/storage/compaction`)
that merges a set of existing, immutable SSTables into a single new
SSTable, plus the small extension the existing `sstable` and `manifest`
packages needed to support it: a forward iterator on `sstable.Reader`
(`internal/storage/sstable/iterator.go`) and two atomic multi-table
Manifest updates, `manifest.VersionSet.RemoveTables` /
`ReplaceTables`, generalizing the single-table `RemoveTable` /
`ReplaceTable` Phase 3 already had. It does **not** implement a
background scheduler, a size/level policy for when to compact, or any
automatic MemTable-to-SSTable flush integration -- those remain future
work, exactly as Phase 3 left them. Compaction is invoked explicitly by a
caller, demonstrated end to end in `internal/storage/compaction/compaction_test.go`.

## Why compaction is needed

An SSTable is immutable once built (see
`docs/storage/phase3-sstables-manifest.md`): nothing ever edits an
existing SSTable file in place. Every `PUT` or `DELETE` that eventually
gets flushed to disk becomes part of a *new* SSTable, so the same key can
end up recorded in several SSTables over the database's lifetime:

```text
SSTable 1:  name -> Kushal
SSTable 2:  name -> Rahul        (a later PUT)
SSTable 3:  name -> TOMBSTONE    (a later DELETE)
```

All three are still sitting on disk. Left alone forever, this means:

- disk usage grows without bound, even though only one of those three
  records is actually "live" data;
- a read that has to check every SSTable for a key gets slower as more
  tables pile up;
- obsolete values and old tombstones are never reclaimed.

Compaction is what merges tables like these into one, keeping only the
state that still matters.

```text
SSTable 1 ──┐
SSTable 2 ──┼──→ Compaction ──→ New SSTable
SSTable 3 ──┘                         │
                                       ↓
                               Manifest update
                                       │
                                       ↓
                               Old tables retired
```

## Newest-version selection

`manifest.TableMeta.ID` is assigned in strictly increasing order as
tables are added (Phase 3's `VersionSet.nextID`): **a larger ID is
always a newer table.** Phase 3's own documentation already called this
out as "exactly what a later phase's LSM merge read will use," so
Phase 4 uses precisely that rule rather than inventing a different one --
not file modification time, not filename comparison, and not map
iteration order (Go map iteration order is explicitly randomized, so
`Compact` only ever iterates its internal ID->file lookup maps for
existence checks, never to decide output order).

When the same key appears in more than one input table, `mergeTables`
(`internal/storage/compaction/merge.go`) picks the entry from the table
with the largest ID as the winner and discards every other input table's
entry for that key outright -- it is already shadowed and contributes
nothing to the merged state, tombstone or not.

## Tombstones

A tombstone can only be physically dropped from compaction's output when
doing so cannot resurrect an older value. Concretely: if some active
SSTable *outside* the tables being compacted is older than every table
being compacted, that older table might still hold a live value for the
same key, and dropping the tombstone would make a later read fall
through to that stale value.

`Compact` computes this conservatively, without trying to reason about
per-table key ranges:

```text
canDropTombstones = true unless some active table, not in the input
                     set, has an ID smaller than every input table's ID
```

If any such older table exists anywhere in the active Version, every
tombstone produced by the merge is kept in the output table, full stop.
This is deliberately the "correctness over cleverness" choice the phase
calls for: a kept tombstone costs a little disk space; a wrongly dropped
one resurrects deleted data.

```text
Older table (untouched):  key -> old-value
Compacted tables:         key -> TOMBSTONE

canDropTombstones = false (an older, non-input table exists)
      ↓
Output table still contains:  key -> TOMBSTONE
```

When a compaction *does* cover every active table (there is nothing
older left in the database at all), tombstones for keys with no
surviving value are dropped. If literally everything in the merge turns
out to be a droppable tombstone, there is no live state left to write at
all: `Compact` skips building an output table and simply retires the
inputs via `VersionSet.RemoveTables`, rather than asking `sstable.Writer`
to build a table with zero entries (which it already rejects).

## How compaction produces a new SSTable

`Compact(vs, dir, inputIDs, tracker)` in
`internal/storage/compaction/compaction.go`:

1. Validates `inputIDs`: non-empty, no duplicates, and every ID must
   name a table in `vs.Current()` -- the currently active Version.
2. Computes `canDropTombstones` (above) from the full active Version.
3. Opens an `sstable.Reader` for every input table.
4. Runs a streaming **k-way merge** over each input's
   `sstable.Iterator` (see below) using a `container/heap`-based min-heap
   keyed by entry key, resolving duplicate keys by table ID as described
   above. Each input table is read one data block at a time -- never
   loaded whole into memory -- via the small iterator capability added to
   `sstable.Reader` for exactly this purpose (Phase 3's `Reader` never
   needed to walk a table start to end; `Get` only ever reads the one
   block a lookup's key could be in).
5. If the merge produced zero surviving entries, retires the inputs with
   no replacement (`VersionSet.RemoveTables`) and returns.
6. Otherwise, builds a new SSTable from the merged entries with the
   existing `sstable.Writer` -- compaction does not implement its own
   SSTable serialization -- and validates it by reopening the file and
   walking it end to end with the same iterator, checking it reports
   exactly the entries it was built from, in the same order, before
   anything is published.
7. Publishes the result with a single atomic Manifest update,
   `VersionSet.ReplaceTables(inputIDs, outputFileName)`: every input ID
   is removed and the new table added in one on-disk write. The Manifest
   is never observed describing neither the old tables nor the new one.
8. If a non-nil `Tracker` was given, retires each input file's physical
   deletion (see **Reader safety** below) only after step 7 has durably
   succeeded.

If any step before the Manifest update fails, `Compact` returns the
error having touched neither the Manifest nor the active Version at
all -- the database is left exactly as it was. If the Manifest update
itself fails (step 7), the just-built (but never-published) output file
is removed on a best-effort basis and the previous Manifest/active
Version remains exactly as it was before the call, courtesy of
`VersionSet`'s existing rollback behavior (see Phase 3's docs).

### The iterator (`internal/storage/sstable/iterator.go`)

`Reader.NewIterator()` returns an `Iterator` that walks every entry in a
table in key order, reading and decoding one data block at a time (reusing
the same block decode path `Get` already uses), rather than requiring the
whole table to be loaded first. This is the "minimal reusable iterator
capability" the phase asked for if Phase 3's `Reader` turned out not to
have one -- it did not, since `Get`'s single-block lookup never needed it.
No second SSTable read/decode implementation was written; the iterator
calls the same `decodeBlock` the rest of the package already uses.

### Output naming

Phase 3 deliberately left SSTable file naming entirely to the caller
(see its own docs). Compaction's output file name is derived
deterministically from its sorted input table IDs --
`compacted-<id1>-<id2>-...-<idN>.sst` -- so that compacting the same
input set always produces the same name; there is nothing for two
otherwise-equivalent runs to disagree on.

## Reader safety

**Compaction never deletes an input SSTable's file itself as part of
publishing its result.** Retiring a table from the active Version (the
Manifest update in step 7 above) and reclaiming its file on disk are
deliberately two separate steps. A caller that obtained a `Version`
snapshot before compaction ran (via `manifest.VersionSet.Current`, as
Phase 3 always intended callers to do) may still have an `sstable.Reader`
open on one of the tables compaction is about to retire; deleting that
file out from under it the instant the Manifest changes would break that
in-flight read.

`internal/storage/compaction/tracker.go` provides `Tracker`, a small
reference-counted lifecycle for exactly this: a caller brackets every
read of an SSTable file with `tracker.Acquire(path)` / the returned
`release()`, and passes the same `Tracker` to `Compact`. Once an input
table's Manifest removal has durably succeeded, `Compact` calls
`tracker.Retire(path)` for it:

```text
Reader Acquire()s old SSTable's path
        │
Compaction runs, Manifest switches to the new table
        │
tracker.Retire(oldPath) -- but the reader still holds it, so nothing
        │                  is deleted yet
Reader finishes, calls release()
        │
        ↓
Only now is the old file physically deleted
```

If `Retire` is called while no `Acquire` is outstanding for that path
(the common case -- most retired tables have no in-flight reader), the
file is deleted immediately. `Compact` accepts a `nil` `Tracker`, in
which case it never deletes an input file at all, leaving that entirely
to whatever policy a future phase wants to layer on top; nothing in this
phase requires every caller to adopt `Tracker`.

`Tracker` is scoped to this package deliberately: nothing in the
codebase before Phase 4 opened SSTables through any kind of shared
registry (every existing caller, including the Phase 3 integration test,
calls `sstable.Open` directly), so a database-wide reader registry would
be new machinery invented well beyond what compaction itself needs.
`Tracker` only tracks the paths `Compact` and its caller explicitly tell
it about.

## Manifest atomicity

`VersionSet.ReplaceTables` and `RemoveTables` (`internal/storage/manifest/version.go`)
generalize Phase 3's existing `ReplaceTable` / `RemoveTable` -- which are
now thin wrappers delegating to the multi-ID versions -- to atomically
remove *every* input table ID and, for `ReplaceTables`, add the new
output table, all in the single Manifest write Phase 3's `persist()`
already performs (full-snapshot write, `atomicfile.Write`'s
write-temp/fsync/rename/fsync-directory protocol, in-memory rollback on
failure). There is no intermediate on-disk state describing only some of
the input tables removed, or the new table added without its inputs
removed. This is exactly what Phase 3's own `ReplaceTable` doc comment
already flagged as "the building block a future compaction phase will
need."

## Limitations carried over from Phase 3

- Directory fsync remains best-effort on Windows (see Phase 3's docs);
  unchanged by this phase.
- No power-loss testing; unchanged by this phase.
- No race-detector run was possible in this environment (`cgo` requires
  a C compiler this machine does not have) -- confirmed again for this
  phase, not assumed.
- `sstable.Writer.Build` still buffers every entry in memory until
  `Build` is called (a Phase 3 limitation, not something this phase
  changes); compaction's k-way merge reads its inputs one block at a
  time, but the merged result is still fully materialized before being
  handed to `Writer`, matching Writer's existing design rather than
  introducing a second, streaming SSTable writer.

## Not implemented (future phases)

- Any policy for *when* to compact (size thresholds, level structure,
  background scheduling). `Compact` is purely mechanical: given a set of
  input table IDs, it merges them. Deciding which tables to pick and
  when is left to a future phase, exactly as Phase 3 left SSTable
  creation policy to this one.
- Multi-level LSM structure.
- Automatic MemTable-to-SSTable flushing and the merged MemTable+SSTable
  read path (SSTables are still not wired into `MemStore`; see Phase 3's
  "Integration boundary").
- A database-wide reader registry beyond `Tracker`'s explicit,
  caller-driven Acquire/Retire bookkeeping.
- Snapshots, Raft, or any client-facing or cluster networking.
