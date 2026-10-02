package statemachine

import (
	"fmt"
	"sync"

	"github.com/Kushall-07/forgedb/internal/raft"
)

// Applier is the sole owner of application order for one raft.Node: it
// pulls newly committed entries from node (via CommittedEntries, bounded
// by node.LastApplied()), decodes each one's Command, applies them to sm
// one at a time in strictly increasing log-index order, and reports
// progress back to node via MarkApplied. It never applies an entry Raft
// has not marked committed, and never applies two entries out of order or
// concurrently -- see ApplyAvailable.
//
// Applier holds no lock belonging to node while calling sm.Apply: it only
// ever calls node's already-synchronized, non-blocking accessor methods
// (LastApplied, CommittedEntries, MarkApplied), each of which acquires and
// releases node's own mutex internally and returns before Applier does any
// storage work. This is what keeps a potentially slow, blocking storage
// call (see internal/storage.Store) from ever being made while node's
// mutex is held, preserving the concurrency rule internal/raft has
// followed since Phase 5.
type Applier struct {
	// applyMu is Applier's own lock, held for the full duration of
	// ApplyAvailable. It is the single mechanism that prevents two
	// goroutines (e.g. a manual ApplyAvailable call racing with Run's
	// background loop) from ever applying entries concurrently or out of
	// order -- see ApplyAvailable.
	applyMu sync.Mutex

	node *raft.Node
	sm   StateMachine

	stopCh   chan struct{}
	stopOnce sync.Once

	// runMu guards doneCh, which Run creates and closes when its
	// background goroutine returns, and which Stop reads to know whether
	// (and how) to wait for that goroutine -- see Run and Stop.
	runMu  sync.Mutex
	doneCh chan struct{}
}

// NewApplier returns an Applier that applies node's committed entries to
// sm. It does nothing on its own until ApplyAvailable is called directly
// (the deterministic, test-friendly path -- see the raft package's
// analogous Tick/Drain pattern) or Run is started (the production,
// background-goroutine path).
func NewApplier(node *raft.Node, sm StateMachine) *Applier {
	return &Applier{node: node, sm: sm, stopCh: make(chan struct{})}
}

// ApplyAvailable applies every currently committed entry this Applier has
// not yet applied -- the inclusive range (node.LastApplied(), node.CommitIndex()]
// -- in log-index order, one at a time, stopping at the first entry it
// cannot fully resolve. It returns how many entries it successfully
// applied and, if it stopped early, the error that stopped it.
//
// Three distinct things can happen for each entry:
//
//   - Decoding its Command fails (ErrInvalidCommand): this can only mean a
//     committed entry's bytes were not produced by Command.Encode, a
//     serious invariant violation. ApplyAvailable stops immediately,
//     advancing lastApplied no further than the entry before it.
//   - sm.Apply itself returns a non-nil error (an unresolved storage
//     fault -- see StateMachine.Apply): ApplyAvailable stops at that same
//     entry without calling MarkApplied for it, so a later retry resumes
//     at exactly this entry instead of skipping past a command that was
//     never actually durably executed here.
//   - sm.Apply returns (Result, nil), whether or not Result.Err is set:
//     this entry is fully resolved. ApplyAvailable calls node.MarkApplied
//     for its index and continues to the next entry. A resolved failure
//     (ErrRequestIDConflict, ErrStaleRequest, or a validation error such as
//     storage.ErrEmptyKey) still counts as applied -- every replica
//     resolves it identically, and there is nothing left to retry.
//
// ApplyAvailable holds its own mutex for its entire duration, so at most
// one goroutine is ever inside it for a given Applier at a time -- this is
// the ordering guarantee described on the Applier doc comment. It is safe
// (and a no-op, returning (0, nil)) to call when nothing new has been
// committed.
func (a *Applier) ApplyAvailable() (applied int, err error) {
	a.applyMu.Lock()
	defer a.applyMu.Unlock()

	if err := a.restorePendingSnapshotLocked(); err != nil {
		return 0, err
	}

	from := a.node.LastApplied() + 1
	entries := a.node.CommittedEntries(from)

	for _, entry := range entries {
		cmd, decodeErr := DecodeCommand(entry.Command)
		if decodeErr != nil {
			return applied, fmt.Errorf("statemachine: decode committed entry at index %d: %w", entry.Index, decodeErr)
		}

		if _, applyErr := a.sm.Apply(cmd); applyErr != nil {
			return applied, fmt.Errorf("statemachine: apply committed entry at index %d: %w", entry.Index, applyErr)
		}

		if markErr := a.node.MarkApplied(entry.Index); markErr != nil {
			return applied, fmt.Errorf("statemachine: mark entry at index %d applied: %w", entry.Index, markErr)
		}
		applied++
	}

	return applied, nil
}

// restorePendingSnapshotLocked checks whether a.node has a snapshot its
// state machine has not yet caught up to (see (*raft.Node).PendingSnapshot)
// and, if so, restores it before letting the caller (ApplyAvailable)
// proceed to apply any further committed entries. It must be called with
// a.applyMu held, so it can never race a concurrent ApplyAvailable/
// CreateSnapshot call.
//
// This is the mechanism that makes Phase 9 log compaction safe: once a
// snapshot has discarded a prefix of the Raft log, the entries covering
// everything up through its LastIncludedIndex no longer exist to replay
// at all -- restoring sm's entire state from the snapshot's own bytes is
// the only way to recover it (see docs/raft/phase9-snapshots.md). a.sm
// must implement Snapshotter for this to succeed; if it does not, this
// returns a clear error rather than silently skipping the restore and
// later applying entries against a state machine missing everything the
// snapshot represented.
func (a *Applier) restorePendingSnapshotLocked() error {
	snap, ok := a.node.PendingSnapshot()
	if !ok {
		return nil
	}

	restorer, supportsSnapshots := a.sm.(Snapshotter)
	if !supportsSnapshots {
		return fmt.Errorf("statemachine: a snapshot through index %d is pending but this state machine does not implement Snapshotter", snap.LastIncludedIndex)
	}
	if err := restorer.RestoreSnapshot(snap.Data); err != nil {
		return fmt.Errorf("statemachine: restore snapshot through index %d: %w", snap.LastIncludedIndex, err)
	}
	if err := a.node.ConfirmSnapshotRestored(snap.LastIncludedIndex); err != nil {
		return fmt.Errorf("statemachine: confirm snapshot restored through index %d: %w", snap.LastIncludedIndex, err)
	}
	return nil
}

// CreateSnapshot captures the state machine's current state as a new Raft
// snapshot through index, which must exactly equal a.node.LastApplied() at
// the moment it is captured -- see (*raft.Node).CreateSnapshot for the
// full validation CreateSnapshot delegates to. CreateSnapshot holds the
// same lock ApplyAvailable does for its entire duration, so a concurrent
// ApplyAvailable call (e.g. from Run's background goroutine) can never
// advance LastApplied -- and thereby the actual state sm.CreateSnapshot is
// about to capture -- out from under the index this call is about to
// claim the snapshot represents. See docs/raft/phase9-snapshots.md.
//
// It returns an error, persisting nothing, if a.sm does not implement
// Snapshotter, if index does not exactly equal the current LastApplied, or
// if the underlying (*raft.Node).CreateSnapshot call itself fails (an
// invalid index, or a persistence failure).
func (a *Applier) CreateSnapshot(index uint64) error {
	a.applyMu.Lock()
	defer a.applyMu.Unlock()

	creator, ok := a.sm.(Snapshotter)
	if !ok {
		return fmt.Errorf("statemachine: CreateSnapshot: state machine does not implement Snapshotter")
	}
	if got := a.node.LastApplied(); index != got {
		return fmt.Errorf("statemachine: CreateSnapshot(%d): must exactly equal the current LastApplied (%d) -- a snapshot can only capture the state machine's exact current state", index, got)
	}

	data, err := creator.CreateSnapshot()
	if err != nil {
		return fmt.Errorf("statemachine: create snapshot: %w", err)
	}
	if err := a.node.CreateSnapshot(index, data); err != nil {
		return fmt.Errorf("statemachine: persist snapshot: %w", err)
	}
	return nil
}

// Run starts a background goroutine that calls ApplyAvailable once
// immediately (to catch up on anything already committed before Run was
// called) and again every time node's CommitCh signals a new commit,
// until Stop is called. It is the production entry point for driving an
// Applier automatically; tests generally call ApplyAvailable directly
// instead, exactly as raft.Node's own Run is optional and tests drive Tick
// directly.
//
// Run logs nothing and does not panic if ApplyAvailable returns an error
// (e.g. a transient storage fault) -- it simply tries again on the next
// commit notification, or never, if no further commits happen; a
// production caller that needs to observe such failures should call
// ApplyAvailable itself instead of relying on Run.
//
// Run is idempotent: calling it more than once starts at most one
// background goroutine.
func (a *Applier) Run() {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	if a.doneCh != nil {
		return
	}
	done := make(chan struct{})
	a.doneCh = done
	go func() {
		defer close(done)
		a.ApplyAvailable()
		for {
			select {
			case <-a.node.CommitCh():
				a.ApplyAvailable()
			case <-a.stopCh:
				return
			}
		}
	}()
}

// Stop halts the background goroutine started by Run, if any, and -- unlike
// merely closing a channel -- blocks until that goroutine has actually
// returned. This matters because the goroutine may be in the middle of
// ApplyAvailable (and therefore in the middle of a Store.Put/Delete call)
// at the moment Stop is called: closing stopCh alone only prevents the
// *next* iteration of Run's loop from starting, it does not interrupt a
// call already in progress. A caller that closes or otherwise tears down
// the underlying Store as soon as Stop returns (see the composition root
// in internal/dbnode) therefore never races an in-flight storage write.
//
// Stop is safe to call more than once, and safe to call even if Run was
// never called (in which case it returns immediately, having waited on
// nothing).
func (a *Applier) Stop() {
	a.stopOnce.Do(func() { close(a.stopCh) })
	a.runMu.Lock()
	done := a.doneCh
	a.runMu.Unlock()
	if done != nil {
		<-done
	}
}
