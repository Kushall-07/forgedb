package raft

import "errors"

// PersistentState is the subset of a Raft node's state that must survive
// a crash for Raft's safety guarantees to hold across restarts:
// currentTerm, votedFor, and the log. Everything else a Node tracks
// (commitIndex, lastApplied, role, election/heartbeat timers, and
// leader-only nextIndex/matchIndex) is volatile and deliberately excluded
// -- see docs/raft/phase6-raft-persistence.md for the full reasoning
// behind exactly this split.
//
// Log holds only the real, 1-indexed entries (the index-0 sentinel -- see
// Log -- is never persisted; it is reconstructed on load).
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []LogEntry
}

// Persister is the abstraction a Node uses to make PersistentState
// durable, so that Raft's consensus logic never depends on a concrete
// on-disk format or storage location. Phase 6 provides two
// implementations: MemoryPersister (in-memory, for tests that want to
// simulate a restart without touching a filesystem, and for fault
// injection) and FilePersister (a real file on disk, using
// internal/storage/atomicfile for crash-safe writes). A Node defaults to
// a no-op Persister when Options.Persister is left nil, which reproduces
// Phase 5's original in-memory-only behavior exactly.
type Persister interface {
	// SaveState durably saves state, replacing whatever was previously
	// saved. It must not return until state is safe to rely on having
	// survived a crash (see FilePersister for what that means on disk).
	SaveState(state PersistentState) error

	// LoadState returns the most recently saved state. It returns
	// ErrNoState, and a zero PersistentState, if nothing has ever been
	// saved -- this is the ordinary case for a brand new node and is not
	// an error condition a caller should treat as startup failure. Any
	// other error (see ErrCorrupt) means a state was previously saved but
	// could not be read back correctly, which a caller should treat very
	// differently: NewNode surfaces it as a startup failure rather than
	// silently proceeding as if the node were new.
	LoadState() (PersistentState, error)

	// SaveSnapshot durably saves snap, replacing whatever snapshot was
	// previously saved. Like SaveState, it must not return until snap is
	// safe to rely on having survived a crash. See Phase 9
	// (docs/raft/phase9-snapshots.md): a Node always calls SaveSnapshot
	// and waits for it to succeed *before* compacting its own in-memory
	// log or persisting that smaller log via SaveState, so a crash can
	// never observe a truncated log without the snapshot that justifies
	// the truncation already being durable.
	SaveSnapshot(snap Snapshot) error

	// LoadSnapshot returns the most recently saved snapshot. It returns
	// ErrNoSnapshot, and a zero Snapshot, if no snapshot has ever been
	// saved -- the ordinary case for a node that has never compacted its
	// log. Any other error (see ErrCorrupt) means a snapshot was
	// previously saved but could not be read back correctly; NewNode
	// surfaces it as a startup failure, exactly as a corrupt LoadState
	// result does.
	LoadSnapshot() (Snapshot, error)
}

// Snapshot is a Raft-level snapshot: an opaque state-machine payload
// (Data) together with the log position it represents. See
// docs/raft/phase9-snapshots.md. internal/raft never interprets Data --
// it is produced and consumed entirely by whatever StateMachine
// implementation sits above this package (see internal/statemachine's
// Snapshotter interface), keeping the same architectural boundary every
// earlier phase established: Raft knows positions, not meanings.
type Snapshot struct {
	// LastIncludedIndex is the highest log index whose effects are
	// reflected in Data. It is always an index this node had itself
	// already applied (see (*Node).CreateSnapshot) at the moment the
	// snapshot was created.
	LastIncludedIndex uint64

	// LastIncludedTerm is the term LastIncludedIndex's entry was created
	// in. Together with LastIncludedIndex it lets this log's compaction
	// boundary still participate correctly in AppendEntries's consistency
	// check and RequestVote's up-to-date check, exactly as if it were
	// still a real retained entry (see Log.TermAt).
	LastIncludedTerm uint64

	// Data is the state machine's own serialization of its entire state
	// through LastIncludedIndex -- opaque bytes from this package's point
	// of view.
	Data []byte
}

// ErrNoState is returned by Persister.LoadState when no state has ever
// been saved. It is deliberately distinct from ErrCorrupt: "nothing here
// yet" and "something here is broken" require different responses from a
// caller (see NewNode).
var ErrNoState = errors.New("raft: no persisted state")

// ErrCorrupt indicates a previously persisted state failed structural
// validation -- a bad magic number, an unsupported format version, a
// truncated header or entry, an invalid length, or a checksum mismatch.
// It is always returned explicitly rather than silently discarding the
// corrupt state and starting fresh, since doing so could silently forget
// a vote or log entries this node had already promised to remember.
var ErrCorrupt = errors.New("raft: corrupt persisted state")

// ErrNoSnapshot is returned by Persister.LoadSnapshot when no snapshot has
// ever been saved. Distinct from ErrCorrupt for the same reason ErrNoState
// is: "nothing here yet" and "something here is broken" require different
// responses from NewNode.
var ErrNoSnapshot = errors.New("raft: no persisted snapshot")

// discardPersister is the zero-value default Persister used when
// Options.Persister is left nil: every save always succeeds without doing
// anything, and every load always reports nothing previously saved. This
// exactly reproduces Phase 5's behavior (a Node that keeps its state in
// memory only, forgetting everything if the process restarts), so every
// Phase 5 test that never mentions persistence continues to behave
// identically.
type discardPersister struct{}

func (discardPersister) SaveState(PersistentState) error { return nil }

func (discardPersister) LoadState() (PersistentState, error) {
	return PersistentState{}, ErrNoState
}

func (discardPersister) SaveSnapshot(Snapshot) error { return nil }

func (discardPersister) LoadSnapshot() (Snapshot, error) {
	return Snapshot{}, ErrNoSnapshot
}
