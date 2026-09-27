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
func (a *Applier) Run() {
	go func() {
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

// Stop halts the background goroutine started by Run, if any. It is safe
// to call more than once, and safe to call even if Run was never called.
func (a *Applier) Stop() {
	a.stopOnce.Do(func() { close(a.stopCh) })
}
