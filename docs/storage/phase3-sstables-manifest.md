# ForgeDB — Phase 3: SSTables + Manifest/Version Set

## Scope

Phase 3 adds two new, independent storage components: an immutable,
block-based **SSTable** format (`internal/storage/sstable`) and a
**Manifest/Version Set** (`internal/storage/manifest`) that tracks which
SSTables currently make up the active database state. It does **not**
implement compaction, multi-level LSM structure, snapshots, or any part
of Raft. It also does not wire SSTables into `MemStore`'s Put/Get/Delete
path yet -- see **Integration boundary** below for why, and what that
means for what "Phase 3 complete" covers.

## Where this sits in the architecture

```text
WAL
 ↓
MemTable
 ↓
Immutable MemTable            <-- not yet a distinct type; see Integration boundary
 ↓
SSTable                       <-- Phase 3
 ├── data blocks
 ├── index
 ├── Bloom filter
 ├── metadata
 └── footer
        ↓
Manifest / Version Set        <-- Phase 3
```

`internal/storage/sstable` knows nothing about `internal/storage/manifest`,
`MemTable`, the WAL, Raft, or networking: it only reads and writes a
single SSTable file. `internal/storage/manifest` knows nothing about
`sstable`, either -- it only tracks file names and IDs. The two packages
are glued together by a caller (demonstrated in
`internal/storage/manifest/integration_test.go`), not by either package
depending on the other. This mirrors how `wal` and `memtable` were kept
independent of each other in Phase 1/2, with `MemStore` as the only thing
that knows about both.

A new shared package, `internal/storage/atomicfile`, implements the
write-temp/fsync/rename/fsync-directory protocol once and is used by both
`sstable.Writer.Build` (to write a new SSTable file) and
`manifest.VersionSet` (to write the Manifest file), so both components
follow the same atomic-update discipline.

## SSTable format

An SSTable file is a single immutable file, laid out as:

```text
[ data block 0 ]
[ data block 1 ]
...
[ data block N-1 ]
[ index block ]
[ Bloom filter block ]
[ metadata block ]
[ footer ]                <- fixed size, at the very end of the file
```

The reader never depends on a hard-coded offset into this layout except
the footer's own position (the last `footerSize` bytes of the file, which
follows purely from the file's total size). Every other section's
location comes from the footer.

### Footer (`footer.go`)

Fixed size, 64 bytes, always the last 64 bytes of the file:

```text
magic (8 bytes "ForgeSST") | formatVersion (u32 LE) |
indexOffset (u64 LE) | indexLength (u64 LE) |
bloomOffset (u64 LE) | bloomLength (u64 LE) |
metaOffset  (u64 LE) | metaLength  (u64 LE) |
checksum (u32 LE, CRC-32C over every preceding footer byte)
```

`Open` reads exactly these 64 bytes from `fileSize - 64`, validates the
magic, format version, and checksum, and then validates every section's
`(offset, length)` against the file's actual size (excluding the footer)
**before** reading or allocating anything for that section:

- `length` must not be zero.
- `offset` must not exceed the file body size.
- `length` must not exceed `bodySize - offset`.

The last check is computed as a subtraction rather than as
`offset + length > bodySize`, specifically so that an attacker- or
corruption-supplied huge `length` can never overflow the addition and
slip past the check -- `bodySize - offset` cannot overflow once
`offset <= bodySize` has already been established.

### Data blocks (`block.go`)

A data block is a sequence of entries, sorted by key, followed by a
trailing CRC-32C checksum over those entries:

```text
entry 0 | entry 1 | ... | entry N-1 | checksum (u32 LE)
```

Each entry:

```text
flags (u8) | keyLen (u32 LE) | valLen (u32 LE) | key | value
```

`flags` bit 0 is the tombstone bit. When set, `valLen` is always `0` and
no value bytes follow. This mirrors the `wal` package's record format
(explicit little-endian lengths, no dependence on Go's in-memory struct
layout) and keeps decoding a simple, bounded loop: every length is
checked against `maxKeySize`/`maxValueSize` (1 MiB / 64 MiB, the same
bounds the WAL uses) and against the bytes actually remaining in the
block **before** it is used to size a slice, so a corrupted length can
never trigger an unbounded allocation.

`Writer` packs entries into blocks up to a soft target of 4 KiB
(`defaultBlockSize`); a single entry larger than that still gets its own
block rather than being split. Keys are added to a `Writer` in strictly
increasing order (`Writer.Add` rejects anything else), which is exactly
the order an immutable MemTable's skip list already produces, so no
separate sort step is needed when flushing one.

### Index (`index.go`)

A sparse index: one entry per data block, giving that block's first key
and its `(offset, length)` in the file, sorted by key (since blocks are
written in increasing key order):

```text
keyLen (u32 LE) | blockOffset (u64 LE) | blockLength (u64 LE) | key
```
...repeated per block, then a trailing CRC-32C checksum.

Because data blocks are non-overlapping and sorted, `Reader` finds the
one block that could contain a search key by binary-searching the index
for the entry with the largest first-key `<=` the search key
(`Reader.candidateBlock`). The whole index is decoded into memory when
the file is opened -- it is small (one entry per block, not one per key)
-- so a lookup only ever reads one data block from disk.

### Bloom filter (`bloom.go`)

A standard bit-array Bloom filter using the Kirsch-Mitzenmacher
double-hashing technique: two independent hashes of a key, computed with
the standard library's `hash/fnv` (FNV-1a and FNV-1 -- no third-party
dependency, deterministic across runs and platforms), are combined to
derive `numHashes` probe positions:

```text
probe_i = (h1 + i*h2) mod numBits,  for i in [0, numHashes)
```

`add(key)` sets the bit at every probe position; `mightContain(key)`
reports "possibly present" only if every one of those bits is set, and
"definitely absent" as soon as one is not. Because every bit `add` sets
for a key is exactly the set of bits `mightContain` checks for that same
key, a key that was added can never later test as absent -- **no false
negatives are possible by construction.** False positives are possible
(some other key's probe can set the same bit), which is why the actual
answer to a `Get` always still comes from reading and searching the real
data block; the Bloom filter is purely an optimization to skip that block
read for a key that is definitely not present.

`newBloomFilter(n, p)` sizes `numBits` and `numHashes` from the expected
entry count `n` and a target false-positive rate `p` using the standard
formulas:

```text
m = ceil(-n * ln(p) / (ln 2)^2)        (bits)
k = round((m / n) * ln 2)              (probes per key)
```

`Writer.Build` uses `p = 1%` (`defaultFalsePositiveRate`). `k` is clamped
to `[1, 30]` and `m` to a minimum of 64 bits as sanity bounds, not because
the formulas need it in practice.

On-disk block: `numHashes (u8) | numBits (u64 LE) | bitsLen (u32 LE) |
bits | checksum (u32 LE)`. `bitsLen` is redundant with `numBits` (always
`ceil(numBits/8)`) but is stored and cross-checked explicitly, so
`decodeBloom` can catch a corrupted `numBits` or `bitsLen` by their
disagreement, rather than trusting either one alone to size the `bits`
slice. `numBits` is additionally capped at decode time
(`maxBloomBits`, 2 GiB of bits) purely as a corruption guard -- no
filter this package builds is remotely that large.

### Metadata block (`meta.go`)

Table-level summary, so `Reader.Count`/`MinKey`/`MaxKey` don't require
scanning any data block:

```text
numEntries (u64 LE) | minKeyLen (u32 LE) | minKey | maxKeyLen (u32 LE) | maxKey | checksum (u32 LE)
```

### Reader (`reader.go`)

`Open`:

1. Validates the footer (magic, version, checksum, section bounds).
2. Reads and validates the index, Bloom filter, and metadata sections
   (each has its own CRC-32C checksum, checked independently of the
   footer's).
3. Cross-checks every index entry's `(blockOffset, blockLength)` against
   the data region (`[0, indexOffset)`), catching an index whose own
   checksum is intact but whose entries were tampered with to point
   outside the data section.

`Get(key)`:

1. Rejects an empty key.
2. Bloom filter check: if `mightContain(key)` is false, returns `NotFound`
   without touching the file.
3. Binary-searches the index for the candidate block.
4. Reads exactly that block's bytes, validates its checksum, and decodes
   its entries.
5. Binary-searches the decoded entries for `key`.
6. Returns one of three `Result` values -- **not just found/not-found**:
   - `Found` with the value.
   - `Tombstone`: the key was explicitly deleted in this table. This is
     kept distinct from `NotFound` because a tombstone must be able to
     shadow an older value for the same key once a later phase merges
     multiple SSTables (and MemTables) together -- see **Tombstones**
     below.
   - `NotFound`: the key does not appear in this table at all.

No SSTable-level corruption is ever silently accepted: every decode
function (`decodeFooter`, `decodeIndex`, `decodeBloom`, `decodeMeta`,
`decodeBlock`) validates its checksum and every length before using it,
and returns an error wrapping `sstable.ErrCorrupt` rather than returning
a partial or best-effort result.

## Tombstones

An SSTable preserves a delete exactly as a tombstone entry (the `flags`
bit in a data block entry), never by simply omitting the key. This
matters for the same reason it already mattered for the MemTable's skip
list in Phase 1:

```text
PUT a = 1
DELETE a
```

must remain distinguishable, inside the SSTable, from `a` never having
been written at all -- `Get("a")` on this table returns `Tombstone`, not
`NotFound`. This is what lets a later phase's merge read correctly shadow
an older SSTable's (or an older, already-flushed MemTable's) live value
for `a` with this table's delete, rather than accidentally resurrecting
it because the delete looked, from that older table's perspective, like
the key was simply absent from the newer one.

## Manifest format

The Manifest is a **full snapshot** of the active table set, not an
append-only edit log: every update writes out the complete new state and
atomically replaces the previous file, rather than appending an edit
record to a growing log that would need to be replayed. This keeps
recovery trivial -- there is exactly one file to read, with no edit
history to reconstruct -- at the cost of rewriting the whole (typically
small) table list on every change, an acceptable trade at Phase 3's
scale.

```text
magic (8 bytes "ForgeMF1") | formatVersion (u32 LE) | nextID (u64 LE) | numTables (u32 LE) |
  [ id (u64 LE) | fileNameLen (u32 LE) | fileName ] * numTables |
checksum (u32 LE, CRC-32C over every preceding byte)
```

`numTables` and each `fileNameLen` are validated against sane maximums
(`maxTables` = 2^20, `maxFileNameLen` = 4096 bytes) before being used to
size any slice or string, for the same reason the SSTable format bounds
its lengths.

## Version Set

`manifest.VersionSet` (`version.go`) is the in-memory representation of
the current table set:

```go
type TableMeta struct {
    ID       uint64
    FileName string
}

type Version struct {
    Tables []TableMeta
}
```

`ID` is assigned in strictly increasing order as tables are added
(`nextID`, persisted alongside the table list so it survives a restart).
A larger ID is a newer table. No merge-read logic exists yet in Phase 3,
but this ordering is exactly what a later phase's LSM merge read will use
to let a newer table's entry -- including a tombstone -- shadow an older
table's entry for the same key, the same way a later MemTable `Put`
already shadows an earlier one.

`VersionSet` exposes:

- `Open(dir)` -- loads `dir`'s Manifest, or creates an initial empty one.
- `Current()` -- a snapshot `Version`; mutating it does not affect the
  `VersionSet`.
- `AddTable(fileName) (id, error)` -- registers a new table, assigns it
  the next ID, persists.
- `RemoveTable(id) error` -- removes a table; removing an already-absent
  ID is not an error.
- `ReplaceTable(oldID, newFileName) (newID, error)` -- removes `oldID` and
  adds `newFileName` in a single atomic Manifest update. No compaction
  logic uses this yet, but it is the building block a future compaction
  phase will need: an atomic update where the Manifest is never observed
  describing neither the old table nor the new one.

`VersionSet` keeps its own in-memory `tables map[uint64]string` and rolls
it back to a snapshot taken before the change if `persist()` fails, so a
failed update leaves both the in-memory state and the on-disk Manifest
exactly as they were before the call -- see **Atomic Manifest update**.

## Atomic Manifest update

Both `sstable.Writer.Build` and `manifest.VersionSet.persist` go through
`internal/storage/atomicfile.Write(path, data)`, which implements the
required protocol:

1. Write `data` to a temporary file (`path + ".tmp"`) in the same
   directory as `path`.
2. Fsync the temporary file.
3. Rename the temporary file over `path`. A rename either fully completes
   or fully fails -- there is no state where `path` is a partially
   written file. If a previous file exists at `path`, the rename replaces
   it as a single step; `Write` never truncates an existing file in
   place.
4. Fsync the directory containing `path` (best-effort -- see **Known
   limitations**).

If any step before the rename fails, `path` is left completely untouched:
whatever was there before (a previous valid Manifest, or nothing) is
still there, and the temporary file is removed. Combined with
`VersionSet`'s in-memory rollback, a failed `AddTable`/`RemoveTable`/
`ReplaceTable` call leaves the caller with the exact same `VersionSet`
state, backed by the exact same on-disk Manifest, as before the call --
never a torn Manifest and never an in-memory/on-disk mismatch.

## Recovery behavior

`manifest.VersionSet.Open` reconstructs the active table set from disk:

```text
create SSTable(s)
  ↓
VersionSet.AddTable (updates + persists the Manifest)
  ↓
(process restarts / VersionSet.Open on a fresh VersionSet)
  ↓
manifest.Open reads the Manifest file
  ↓
Version.Tables lists the active SSTable file names + IDs
  ↓
caller opens each with sstable.Open
```

This exact sequence is exercised by
`internal/storage/manifest/integration_test.go`
(`TestReopenRecoversActiveSSTablesFromManifest`,
`TestReopenAfterReplaceRecoversOnlyReplacementTable`): real SSTable files
and a real Manifest file in a real temporary directory, with no open
handles or in-memory state kept alive across the simulated restart.

A corrupt Manifest is a hard recovery failure, exactly like a corrupt WAL
in Phase 2: `Open` returns an error wrapping `manifest.ErrCorrupt` rather
than a usable `VersionSet`. The same is true of `sstable.Open` for a
corrupt SSTable file.

## Corruption policy

As in Phase 2's WAL, corruption is never silently accepted or skipped.
Both packages define their own `ErrCorrupt` sentinel (mirroring
`wal.ErrCorrupt`), and every decode path wraps it via `%w` so
`errors.Is(err, sstable.ErrCorrupt)` / `errors.Is(err, manifest.ErrCorrupt)`
works through any amount of wrapping.

Covered explicitly by tests (`sstable/corruption_test.go`,
`manifest/manifest_test.go`):

| Category | How it's tested |
|---|---|
| Invalid footer (magic/version/checksum) | Flip bytes in a real file's footer; hand-craft a footer buffer with a bad version |
| Invalid section offsets | Hand-craft a footer with an offset beyond the declared body size |
| Oversized lengths | Hand-craft a footer with a length exceeding the declared body size |
| Truncated file / truncated mid-section | Truncate a real file before/within a section |
| Truncated block | Corrupt a data block's checksum; decode a truncated block/entry buffer |
| Invalid index | Corrupt an index checksum; truncate an index entry; oversized key length |
| Corrupted Bloom metadata | Corrupt checksum, bits-length/numBits mismatch, zero hash count, excessive bit count |
| Corrupted data | Flip a byte inside a data block; `Get` fails rather than returning a wrong value |
| Invalid Manifest metadata | Bad magic, checksum mismatch, oversized table count, invalid file name length, truncated file, unsupported version |

Every numeric length or count read from an untrusted section is validated
against an explicit bound (`maxKeySize`, `maxValueSize`, `maxBloomBits`,
`maxTables`, `maxFileNameLen`) **before** it is used to size a slice, so a
corrupted length can never itself cause an unbounded or out-of-bounds
allocation -- the same principle Phase 2's WAL record decoding already
followed.

## Integration boundary

**SSTables are not yet wired into `MemStore`.** `internal/storage/storage.go`
and `internal/storage/memstore.go` are unchanged by this phase: `Store`'s
public contract, `MemStore`'s behavior, and every Phase 1/2 test still
pass exactly as before. There is no "immutable MemTable" type yet, no
automatic flush-when-full trigger, and no read path that merges MemTable
state with SSTable state.

This is intentional, not an oversight: automatic flushing needs a policy
for *when* to flush (a size threshold, a background trigger) and a read
path that checks the MemTable first and then falls back to SSTables via
the Manifest's active version -- both of which start to shade into
compaction-adjacent design decisions that are explicitly out of scope for
Phase 3. Building that machinery now, only to have it revisited once
compaction actually exists, would be the "invent a subsystem merely to
force integration" outcome the phase brief warns against. What Phase 3
delivers instead is a complete, independently correct, and thoroughly
tested SSTable format and Manifest/Version Set with explicit,
programmatic creation and recovery APIs (`sstable.NewWriter`/`Build`/
`Open`, `manifest.Open`/`AddTable`/`RemoveTable`/`ReplaceTable`/`Current`)
that a future flush-integration phase can call directly.

## Known limitations

- **Directory fsync is best-effort on this development platform.** Go's
  standard library on Windows cannot open a directory with write access
  at all (`os.OpenFile` on a directory with anything but `O_RDONLY`
  returns "is a directory"), and `Sync` on a read-only directory handle
  fails with "Access is denied". This was verified directly against this
  environment's Go toolchain before implementing `atomicfile.Write`.
  Consequently, step 4 of the atomic-update protocol (fsync the
  directory) is attempted but its failure is not treated as fatal: the
  rename in step 3 has already durably completed the file replacement
  from the path's point of view (NTFS journals the rename's metadata
  update itself), so `atomicfile.Write` still provides "the file at path
  is either the old complete content or the new complete content, never
  torn" on Windows -- it just cannot additionally guarantee, the way a
  POSIX `fsync(dirfd)` would, that the directory entry update itself
  survives an immediate crash before any subsequent flush. This mirrors
  the WAL's own Windows-specific accommodation in Phase 2 (avoiding
  `O_APPEND` because of a similar access-rights gap).
- **This is not power-loss testing**, for the same reason Phase 2's WAL
  tests are not: the corruption and reopen/recovery tests manipulate real
  files on a real filesystem, but none of it simulates actual hardware
  power loss, disk write reordering, or filesystem-level partial-sector
  writes.
- **No race-detector run.** As in Phase 0–2, this development machine has
  no C compiler, so `go test -race` cannot run (`cgo` is required). This
  is an environment limitation, not a result.
- **`Writer` buffers all entries in memory until `Build` is called.**
  There is no streaming/incremental write path. This is a deliberate
  simplification: the intended source of entries is an already fully
  materialized immutable MemTable, so streaming would add complexity
  (in particular, sizing the Bloom filter, which needs the total entry
  count up front) without a corresponding benefit at this phase.
- **No SSTable rotation/naming policy, and no compaction.** This package
  does not decide what to name a new SSTable file, when to create one,
  or when to merge/discard old ones -- all caller responsibilities left
  for a future phase.

## Not implemented (future phases)

- Compaction (size-tiered or level-based)
- Multi-level LSM structure
- Automatic MemTable-to-SSTable flushing and the merged MemTable+SSTable
  read path
- Snapshots
- Raft (leader election, PreVote, RequestVote, AppendEntries, replicated
  log, cluster membership)
- Distributed recovery
- Any client-facing or cluster networking (HTTP, gRPC)
