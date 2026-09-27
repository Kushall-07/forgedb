package raft

import (
	"errors"
	"sync"
)

// MemoryPersister is an in-memory Persister. It exists for two purposes:
//
//   - simulating a crash and restart in a test without touching a real
//     filesystem: construct one MemoryPersister, pass it to two
//     successive NewNode calls (the second standing in for "the process
//     restarted"), and the second Node picks up exactly where the first
//     left off;
//   - fault injection: FailNextSave makes the next SaveState call (and
//     only that one) fail, so a test can verify a Node never treats a
//     durable state transition as having succeeded when persistence
//     actually failed.
//
// MemoryPersister is safe for concurrent use.
type MemoryPersister struct {
	mu       sync.Mutex
	state    PersistentState
	has      bool
	failNext bool
}

// NewMemoryPersister returns an empty MemoryPersister with no saved
// state.
func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

// errSimulatedPersistFailure is returned by SaveState when fault
// injection (see FailNextSave) is active.
var errSimulatedPersistFailure = errors.New("raft: simulated persistence failure")

// FailNextSave arranges for the next call to SaveState to fail (returning
// errSimulatedPersistFailure) without altering the persister's saved
// state, then behave normally again afterward.
func (p *MemoryPersister) FailNextSave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNext = true
}

// SaveState implements Persister. The saved state is deep-copied so a
// caller mutating its own PersistentState.Log slice afterward cannot
// retroactively alter what was "persisted".
func (p *MemoryPersister) SaveState(state PersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.failNext {
		p.failNext = false
		return errSimulatedPersistFailure
	}

	p.state = PersistentState{
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         copyEntries(state.Log),
	}
	p.has = true
	return nil
}

// LoadState implements Persister.
func (p *MemoryPersister) LoadState() (PersistentState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.has {
		return PersistentState{}, ErrNoState
	}
	return PersistentState{
		CurrentTerm: p.state.CurrentTerm,
		VotedFor:    p.state.VotedFor,
		Log:         copyEntries(p.state.Log),
	}, nil
}

// copyEntries returns a copy of entries in which every entry's Command
// byte slice is itself copied, not just the LogEntry struct (whose
// Command field is a slice header that a shallow slice-of-structs copy
// would otherwise still alias to the original). This is what makes it
// safe for a Node to keep appending to (and the caller to keep reusing)
// its own in-memory log after a MemoryPersister.SaveState call: nothing
// the persister holds shares memory with the caller.
func copyEntries(entries []LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = LogEntry{Index: e.Index, Term: e.Term, Command: append(Command(nil), e.Command...)}
	}
	return out
}
