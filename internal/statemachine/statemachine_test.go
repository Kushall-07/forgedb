package statemachine

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/Kushall-07/forgedb/internal/storage"
)

// countingStore wraps a real storage.Store, counting real Put/Delete calls
// and optionally failing the next one of either with a simulated,
// non-deterministic fault -- distinct from storage.ErrEmptyKey, which is a
// deterministic validation error the state machine must treat as a
// resolved outcome rather than a fault (see KVStateMachine.execute).
type countingStore struct {
	storage.Store

	mu       sync.Mutex
	puts     int
	deletes  int
	failNext bool
}

var errSimulatedStorageFault = errors.New("statemachine: simulated storage fault (test only)")

func (s *countingStore) FailNext() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext = true
}

func (s *countingStore) Put(key, value []byte) error {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		s.mu.Unlock()
		return errSimulatedStorageFault
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

func (s *countingStore) Delete(key []byte) error {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		s.mu.Unlock()
		return errSimulatedStorageFault
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

func newTestStateMachine(t *testing.T) (*KVStateMachine, *countingStore) {
	t.Helper()
	base, err := storage.NewMemStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { base.Close() })
	cs := &countingStore{Store: base}
	return NewKVStateMachine(cs), cs
}

// --- PUT / DELETE -------------------------------------------------------

func TestKVStateMachine_Put_WritesToStorage(t *testing.T) {
	sm, cs := newTestStateMachine(t)

	res, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !res.Applied || res.Err != nil {
		t.Fatalf("Apply result = %+v, want Applied=true, Err=nil", res)
	}
	got, err := cs.Get([]byte("x"))
	if err != nil {
		t.Fatalf("Get(x): %v", err)
	}
	if !bytes.Equal(got, []byte("10")) {
		t.Fatalf("Get(x) = %q, want 10", got)
	}
}

func TestKVStateMachine_PutThenDelete_RemovesKey(t *testing.T) {
	sm, cs := newTestStateMachine(t)

	if _, err := sm.Apply(NewPutCommand("c1", 1, []byte("x"), []byte("10"))); err != nil {
		t.Fatalf("Apply put: %v", err)
	}
	if _, err := sm.Apply(NewDeleteCommand("c1", 2, []byte("x"))); err != nil {
		t.Fatalf("Apply delete: %v", err)
	}
	if _, err := cs.Get([]byte("x")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get(x) after delete: got %v, want ErrKeyNotFound", err)
	}
}

func TestKVStateMachine_ApplyOrder_FinalStateCorrect(t *testing.T) {
	sm, cs := newTestStateMachine(t)

	cmds := []Command{
		NewPutCommand("c1", 1, []byte("x"), []byte("1")),
		NewPutCommand("c1", 2, []byte("x"), []byte("2")),
		NewDeleteCommand("c1", 3, []byte("x")),
	}
	for _, cmd := range cmds {
		if _, err := sm.Apply(cmd); err != nil {
			t.Fatalf("Apply(%+v): %v", cmd, err)
		}
	}
	if _, err := cs.Get([]byte("x")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("final Get(x): got %v, want ErrKeyNotFound (deleted last)", err)
	}
}

// --- Deduplication --------------------------------------------------------

func TestKVStateMachine_DuplicateRequest_AppliesOnce(t *testing.T) {
	sm, cs := newTestStateMachine(t)
	cmd := NewPutCommand("client-A", 10, []byte("name"), []byte("Kushal"))

	if _, err := sm.Apply(cmd); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := sm.Apply(cmd); err != nil {
		t.Fatalf("retry Apply: %v", err)
	}
	if cs.puts != 1 {
		t.Fatalf("storage.Put called %d times, want exactly 1", cs.puts)
	}
}

func TestKVStateMachine_DuplicateRequest_ReplaysResult(t *testing.T) {
	sm, _ := newTestStateMachine(t)
	cmd := NewPutCommand("client-A", 10, []byte("name"), []byte("Kushal"))

	first, err := sm.Apply(cmd)
	if err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if first.Replayed {
		t.Fatalf("first Apply reported Replayed=true")
	}

	retry, err := sm.Apply(cmd)
	if err != nil {
		t.Fatalf("retry Apply: %v", err)
	}
	if !retry.Replayed {
		t.Fatalf("retry Apply reported Replayed=false, want true")
	}
	if !bytes.Equal(retry.Key, first.Key) || !bytes.Equal(retry.Value, first.Value) || retry.Applied != first.Applied {
		t.Fatalf("retry result %+v does not match original %+v", retry, first)
	}
}

func TestKVStateMachine_DuplicateDelete_AppliesOnce(t *testing.T) {
	sm, cs := newTestStateMachine(t)
	put := NewPutCommand("client-A", 1, []byte("x"), []byte("10"))
	del := NewDeleteCommand("client-A", 2, []byte("x"))

	if _, err := sm.Apply(put); err != nil {
		t.Fatalf("Apply put: %v", err)
	}
	first, err := sm.Apply(del)
	if err != nil {
		t.Fatalf("first Apply delete: %v", err)
	}
	retry, err := sm.Apply(del)
	if err != nil {
		t.Fatalf("retry Apply delete: %v", err)
	}
	if cs.deletes != 1 {
		t.Fatalf("storage.Delete called %d times, want exactly 1", cs.deletes)
	}
	if !retry.Replayed || retry.Applied != first.Applied {
		t.Fatalf("retry delete result = %+v, want a replay of %+v", retry, first)
	}
}

func TestKVStateMachine_DifferentClients_SameRequestID_Independent(t *testing.T) {
	sm, cs := newTestStateMachine(t)

	if _, err := sm.Apply(NewPutCommand("client-A", 10, []byte("a"), []byte("1"))); err != nil {
		t.Fatalf("Apply A: %v", err)
	}
	if _, err := sm.Apply(NewPutCommand("client-B", 10, []byte("b"), []byte("2"))); err != nil {
		t.Fatalf("Apply B: %v", err)
	}
	if cs.puts != 2 {
		t.Fatalf("storage.Put called %d times, want 2 (independent clients)", cs.puts)
	}
	va, err := cs.Get([]byte("a"))
	if err != nil || !bytes.Equal(va, []byte("1")) {
		t.Fatalf("Get(a) = %q, %v, want 1, nil", va, err)
	}
	vb, err := cs.Get([]byte("b"))
	if err != nil || !bytes.Equal(vb, []byte("2")) {
		t.Fatalf("Get(b) = %q, %v, want 2, nil", vb, err)
	}
}

func TestKVStateMachine_OldDuplicate_NotReexecuted(t *testing.T) {
	sm, cs := newTestStateMachine(t)

	req10 := NewPutCommand("client-A", 10, []byte("x"), []byte("first"))
	req11 := NewPutCommand("client-A", 11, []byte("x"), []byte("second"))

	if _, err := sm.Apply(req10); err != nil {
		t.Fatalf("Apply request 10: %v", err)
	}
	if _, err := sm.Apply(req11); err != nil {
		t.Fatalf("Apply request 11: %v", err)
	}
	if cs.puts != 2 {
		t.Fatalf("storage.Put called %d times before replay, want 2", cs.puts)
	}

	res, err := sm.Apply(req10) // request 10 replayed after 11
	if err != nil {
		t.Fatalf("Apply stale request 10: %v", err)
	}
	if !errors.Is(res.Err, ErrStaleRequest) {
		t.Fatalf("stale replay result = %+v, want Err=ErrStaleRequest", res)
	}
	if cs.puts != 2 {
		t.Fatalf("storage.Put called %d times after stale replay, want unchanged 2", cs.puts)
	}

	got, err := cs.Get([]byte("x"))
	if err != nil || !bytes.Equal(got, []byte("second")) {
		t.Fatalf("Get(x) = %q, %v, want \"second\" (request 11 must not be clobbered)", got, err)
	}
}

func TestKVStateMachine_RequestIDConflict_ReturnsErrorAndLeavesRecordUnchanged(t *testing.T) {
	sm, cs := newTestStateMachine(t)

	original := NewPutCommand("client-A", 42, []byte("x"), []byte("10"))
	conflicting := NewPutCommand("client-A", 42, []byte("x"), []byte("999"))

	if _, err := sm.Apply(original); err != nil {
		t.Fatalf("Apply original: %v", err)
	}
	res, err := sm.Apply(conflicting)
	if err != nil {
		t.Fatalf("Apply conflicting: %v", err)
	}
	if !errors.Is(res.Err, ErrRequestIDConflict) {
		t.Fatalf("conflicting result = %+v, want Err=ErrRequestIDConflict", res)
	}
	if cs.puts != 1 {
		t.Fatalf("storage.Put called %d times, want exactly 1 (conflict must not execute)", cs.puts)
	}

	got, err := cs.Get([]byte("x"))
	if err != nil || !bytes.Equal(got, []byte("10")) {
		t.Fatalf("Get(x) = %q, %v, want original value \"10\" unchanged", got, err)
	}

	// The original dedup record must still replay correctly afterward.
	replay, err := sm.Apply(original)
	if err != nil {
		t.Fatalf("Apply original again: %v", err)
	}
	if !replay.Replayed || !bytes.Equal(replay.Value, []byte("10")) {
		t.Fatalf("replay after conflict = %+v, want a replay of the original", replay)
	}
}

// --- Determinism / replica convergence -----------------------------------

func TestKVStateMachine_MultipleReplicas_Converge(t *testing.T) {
	cmds := []Command{
		NewPutCommand("c1", 1, []byte("a"), []byte("1")),
		NewPutCommand("c2", 1, []byte("b"), []byte("2")),
		NewPutCommand("c1", 2, []byte("a"), []byte("3")),
		NewDeleteCommand("c2", 2, []byte("b")),
		NewPutCommand("c1", 1, []byte("a"), []byte("1")), // duplicate, replayed on every replica
	}

	const numReplicas = 3
	stores := make([]*countingStore, numReplicas)
	for i := range stores {
		sm, cs := newTestStateMachine(t)
		stores[i] = cs
		for _, cmd := range cmds {
			if _, err := sm.Apply(cmd); err != nil {
				t.Fatalf("replica %d Apply(%+v): %v", i, cmd, err)
			}
		}
	}

	for i := 1; i < numReplicas; i++ {
		va, errA := stores[0].Get([]byte("a"))
		vi, errI := stores[i].Get([]byte("a"))
		if !errors.Is(errA, errI) && (errA == nil) != (errI == nil) {
			t.Fatalf("replica 0 and %d disagree on Get(a) errors: %v vs %v", i, errA, errI)
		}
		if !bytes.Equal(va, vi) {
			t.Fatalf("replica 0 and %d disagree on Get(a): %q vs %q", i, va, vi)
		}
		if _, err := stores[i].Get([]byte("b")); !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("replica %d Get(b) = %v, want ErrKeyNotFound (deleted on every replica)", i, err)
		}
	}
}

// --- Storage faults -------------------------------------------------------

func TestKVStateMachine_StorageFault_ReturnsError_NoDedupRecorded(t *testing.T) {
	sm, cs := newTestStateMachine(t)
	cmd := NewPutCommand("client-A", 1, []byte("x"), []byte("10"))

	cs.FailNext()
	if _, err := sm.Apply(cmd); err == nil {
		t.Fatalf("Apply with injected storage fault: want error, got nil")
	}
	if _, err := cs.Get([]byte("x")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get(x) after failed apply: got %v, want ErrKeyNotFound (never applied)", err)
	}

	// A genuine retry (storage now healthy) must actually execute, proving
	// the failed attempt was not mistakenly recorded as already processed.
	res, err := sm.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply retry after fault clears: %v", err)
	}
	if res.Replayed {
		t.Fatalf("retry after a storage fault reported Replayed=true, want a fresh execution")
	}
	if cs.puts != 1 {
		t.Fatalf("storage.Put called %d times, want exactly 1 (the successful retry)", cs.puts)
	}
}

func TestKVStateMachine_EmptyKey_IsResolvedNotHalting(t *testing.T) {
	sm, _ := newTestStateMachine(t)

	res, err := sm.Apply(NewPutCommand("client-A", 1, nil, []byte("v")))
	if err != nil {
		t.Fatalf("Apply with empty key: want resolved (nil) error from Apply, got %v", err)
	}
	if !errors.Is(res.Err, storage.ErrEmptyKey) {
		t.Fatalf("Apply with empty key result = %+v, want Err=storage.ErrEmptyKey", res)
	}
}
