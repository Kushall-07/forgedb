package chaos

import (
	"errors"
	"sync"

	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// errInjectedFault is returned by a faultyPersister/faultyStore when a
// scenario-requested fault fires. It is never returned for any other
// reason, so a scenario asserting on it (via errors.Is) can be certain
// the failure it is observing is the one it injected, not a real bug.
var errInjectedFault = errors.New("chaos: injected fault")

// faultyPersister wraps a real raft.Persister (in production use, the
// raft.FilePersister dbnode.Node.Open already constructs from RaftDir --
// see dbnode.Config.WrapPersister) and lets a scenario make exactly the
// next SaveState or SaveSnapshot call fail, reproducing
// raft.MemoryPersister's own FailNextSave/FailNextSnapshotSave fault
// injection at the dbnode composition-root level instead of only inside
// internal/raft's own unit tests. Every call still either fully succeeds
// against the real underlying persister or fully fails without touching
// it -- faultyPersister injects nothing beyond "pretend this one call
// never happened".
type faultyPersister struct {
	mu sync.Mutex

	inner raft.Persister

	failNextSave     bool
	failNextSnapshot bool
}

func (p *faultyPersister) FailNextSave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNextSave = true
}

func (p *faultyPersister) FailNextSnapshotSave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNextSnapshot = true
}

func (p *faultyPersister) SaveState(state raft.PersistentState) error {
	p.mu.Lock()
	if p.failNextSave {
		p.failNextSave = false
		p.mu.Unlock()
		return errInjectedFault
	}
	p.mu.Unlock()
	return p.inner.SaveState(state)
}

func (p *faultyPersister) LoadState() (raft.PersistentState, error) { return p.inner.LoadState() }

func (p *faultyPersister) SaveSnapshot(snap raft.Snapshot) error {
	p.mu.Lock()
	if p.failNextSnapshot {
		p.failNextSnapshot = false
		p.mu.Unlock()
		return errInjectedFault
	}
	p.mu.Unlock()
	return p.inner.SaveSnapshot(snap)
}

func (p *faultyPersister) LoadSnapshot() (raft.Snapshot, error) { return p.inner.LoadSnapshot() }

// faultyStore wraps a real storage.Store (see dbnode.Config.WrapStore),
// generalizing node_test.go's own faultyStore/countingStore (which only
// ever fails or counts Put) to both of KVStateMachine's mutating
// operations. It supports two independent things a scenario needs: fault
// injection (making exactly the next Put or Delete fail) and mutation
// counting (how many times Put/Delete actually ran against the real
// underlying store) -- the latter is what lets a dedup scenario (see
// scenario_dedup_test.go) tell "applied exactly once" apart from
// "applied twice with an identical value", which looking at the stored
// value alone cannot distinguish.
type faultyStore struct {
	storage.Store

	mu           sync.Mutex
	failNextPut  bool
	failNextDrop bool
	puts         int
	deletes      int
}

func (s *faultyStore) FailNextPut() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNextPut = true
}

func (s *faultyStore) FailNextDelete() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNextDrop = true
}

func (s *faultyStore) Put(key, value []byte) error {
	s.mu.Lock()
	if s.failNextPut {
		s.failNextPut = false
		s.mu.Unlock()
		return errInjectedFault
	}
	s.mu.Unlock()
	if err := s.Store.Put(key, value); err != nil {
		return err
	}
	s.mu.Lock()
	s.puts++
	s.mu.Unlock()
	return nil
}

func (s *faultyStore) Delete(key []byte) error {
	s.mu.Lock()
	if s.failNextDrop {
		s.failNextDrop = false
		s.mu.Unlock()
		return errInjectedFault
	}
	s.mu.Unlock()
	if err := s.Store.Delete(key); err != nil {
		return err
	}
	s.mu.Lock()
	s.deletes++
	s.mu.Unlock()
	return nil
}

func (s *faultyStore) PutCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

func (s *faultyStore) DeleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes
}
