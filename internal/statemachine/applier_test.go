package statemachine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// --- Test cluster helpers --------------------------------------------------
//
// internal/raft's own newTestCluster/electLeader/drainAll helpers are
// unexported test-only functions of that package, so this package builds
// an equivalent minimal cluster directly from raft's exported API.

func mustNewRaftNode(t *testing.T, opts raft.Options) *raft.Node {
	t.Helper()
	n, err := raft.NewNode(opts)
	if err != nil {
		t.Fatalf("raft.NewNode(%+v): %v", opts, err)
	}
	return n
}

func newRaftCluster(t *testing.T, n int) (*raft.InMemoryTransport, []*raft.Node) {
	t.Helper()
	tr := raft.NewInMemoryTransport()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}
	nodes := make([]*raft.Node, n)
	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node := mustNewRaftNode(t, raft.Options{
			ID:              id,
			Peers:           peers,
			Transport:       tr,
			ElectionTickMin: 5,
			ElectionTickMax: 5,
		})
		nodes[i] = node
		tr.Register(id, node)
	}
	return tr, nodes
}

func electLeader(t *testing.T, node *raft.Node) {
	t.Helper()
	for i := 0; i < 5; i++ {
		node.Tick()
	}
	node.Drain()
	if !node.IsLeader() {
		t.Fatalf("node %s did not become leader", node.ID())
	}
}

func drainAll(nodes []*raft.Node) {
	for _, n := range nodes {
		n.Drain()
	}
}

func newApplierTarget(t *testing.T) (*KVStateMachine, *countingStore) {
	t.Helper()
	base, err := storage.NewMemStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { base.Close() })
	cs := &countingStore{Store: base}
	return NewKVStateMachine(cs), cs
}

func encodeOrFatal(t *testing.T, cmd Command) raft.Command {
	t.Helper()
	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode(%+v): %v", cmd, err)
	}
	return encoded
}

// --- Ordering / commit boundary --------------------------------------------

func TestApplier_AppliesCommittedEntriesInOrder(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sm, cs := newApplierTarget(t)
	applier := NewApplier(leader, sm)

	cmds := []Command{
		NewPutCommand("c1", 1, []byte("x"), []byte("1")),
		NewPutCommand("c1", 2, []byte("x"), []byte("2")),
		NewDeleteCommand("c1", 3, []byte("x")),
	}
	for _, cmd := range cmds {
		if _, _, err := leader.Propose(encodeOrFatal(t, cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()
	if leader.CommitIndex() != 3 {
		t.Fatalf("CommitIndex = %d, want 3", leader.CommitIndex())
	}

	applied, err := applier.ApplyAvailable()
	if err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	if applied != 3 {
		t.Fatalf("ApplyAvailable applied = %d, want 3", applied)
	}
	if leader.LastApplied() != 3 {
		t.Fatalf("LastApplied = %d, want 3", leader.LastApplied())
	}
	if _, err := cs.Get([]byte("x")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("final Get(x) = %v, want ErrKeyNotFound (put 1, put 2, delete, in order)", err)
	}
}

func TestApplier_UncommittedEntriesNeverReachStorage(t *testing.T) {
	tr, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sm, cs := newApplierTarget(t)
	applier := NewApplier(leader, sm)

	tr.Partition(nodes[1].ID())
	tr.Partition(nodes[2].ID())

	cmd := NewPutCommand("c1", 1, []byte("x"), []byte("10"))
	if _, _, err := leader.Propose(encodeOrFatal(t, cmd)); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	if leader.CommitIndex() != 0 {
		t.Fatalf("CommitIndex = %d, want 0 (no majority reachable)", leader.CommitIndex())
	}

	applied, err := applier.ApplyAvailable()
	if err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	if applied != 0 {
		t.Fatalf("ApplyAvailable applied = %d, want 0 (entry not committed)", applied)
	}
	if _, err := cs.Get([]byte("x")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get(x) = %v, want ErrKeyNotFound (uncommitted entry must not reach storage)", err)
	}
	if leader.LastApplied() != 0 {
		t.Fatalf("LastApplied = %d, want 0", leader.LastApplied())
	}
}

func TestApplier_ApplyAvailable_NoOpWhenNothingNewCommitted(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sm, _ := newApplierTarget(t)
	applier := NewApplier(leader, sm)

	if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", 1, []byte("x"), []byte("1")))); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()

	if _, err := applier.ApplyAvailable(); err != nil {
		t.Fatalf("first ApplyAvailable: %v", err)
	}
	applied, err := applier.ApplyAvailable()
	if err != nil {
		t.Fatalf("second ApplyAvailable: %v", err)
	}
	if applied != 0 {
		t.Fatalf("second ApplyAvailable applied = %d, want 0 (nothing new committed)", applied)
	}
}

// --- Storage faults halt the loop without advancing lastApplied -----------

func TestApplier_StorageFault_StopsWithoutAdvancingLastApplied(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sm, cs := newApplierTarget(t)
	applier := NewApplier(leader, sm)

	// Commit and apply the first entry normally, so lastApplied starts at
	// a known, non-zero value before the fault is injected.
	if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", 1, []byte("a"), []byte("1")))); err != nil {
		t.Fatalf("Propose 1: %v", err)
	}
	leader.Drain()
	if applied, err := applier.ApplyAvailable(); err != nil || applied != 1 {
		t.Fatalf("ApplyAvailable for entry 1: applied=%d, err=%v, want 1, nil", applied, err)
	}

	// Commit two more entries, but arrange for the *next* real Put call --
	// entry 2's -- to fail with a simulated storage fault.
	cs.FailNext()
	if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", 2, []byte("b"), []byte("2")))); err != nil {
		t.Fatalf("Propose 2: %v", err)
	}
	if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", 3, []byte("c"), []byte("3")))); err != nil {
		t.Fatalf("Propose 3: %v", err)
	}
	leader.Drain()
	if leader.CommitIndex() != 3 {
		t.Fatalf("CommitIndex = %d, want 3", leader.CommitIndex())
	}

	applied, err := applier.ApplyAvailable()
	if err == nil {
		t.Fatalf("ApplyAvailable: want error from the injected storage fault, got nil")
	}
	if applied != 0 {
		t.Fatalf("ApplyAvailable applied = %d for this call, want 0 (entry 2 fails immediately)", applied)
	}
	if leader.LastApplied() != 1 {
		t.Fatalf("LastApplied = %d, want 1 (must not advance past the failed entry)", leader.LastApplied())
	}
	if _, err := cs.Get([]byte("b")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get(b) = %v, want ErrKeyNotFound (failed apply must not claim success)", err)
	}

	// A later retry (storage healthy again) resumes exactly where it left
	// off and completes the remaining entries in order.
	applied, err = applier.ApplyAvailable()
	if err != nil {
		t.Fatalf("retry ApplyAvailable: %v", err)
	}
	if applied != 2 {
		t.Fatalf("retry ApplyAvailable applied = %d, want 2 (entries 2 and 3)", applied)
	}
	if leader.LastApplied() != 3 {
		t.Fatalf("LastApplied after retry = %d, want 3", leader.LastApplied())
	}
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		got, err := cs.Get([]byte(key))
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%s) = %q, %v, want %q, nil", key, got, err, want)
		}
	}
}

// --- Cluster integration: leader + followers converge ----------------------

func TestApplier_ClusterConvergesAfterMajorityCommit(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sms := make([]*KVStateMachine, len(nodes))
	stores := make([]*countingStore, len(nodes))
	appliers := make([]*Applier, len(nodes))
	for i, n := range nodes {
		sm, cs := newApplierTarget(t)
		sms[i] = sm
		stores[i] = cs
		appliers[i] = NewApplier(n, sm)
	}

	if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("client-1", 1, []byte("x"), []byte("10")))); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()
	if leader.CommitIndex() != 1 {
		t.Fatalf("leader CommitIndex = %d, want 1", leader.CommitIndex())
	}

	// The followers only learn the new commit index on the *next*
	// AppendEntries round (this Propose's own broadcast still carried the
	// leader's commitIndex from before this entry committed) -- an extra
	// heartbeat tick propagates it, exactly as it would in production.
	leader.Tick()
	leader.Tick()
	drainAll(nodes)

	for _, n := range nodes {
		if n.CommitIndex() != 1 {
			t.Fatalf("node %s CommitIndex = %d, want 1", n.ID(), n.CommitIndex())
		}
	}

	for i, applier := range appliers {
		if _, err := applier.ApplyAvailable(); err != nil {
			t.Fatalf("node %s ApplyAvailable: %v", nodes[i].ID(), err)
		}
	}

	for i, cs := range stores {
		got, err := cs.Get([]byte("x"))
		if err != nil || !bytes.Equal(got, []byte("10")) {
			t.Fatalf("node %s Get(x) = %q, %v, want 10, nil", nodes[i].ID(), got, err)
		}
	}
}

// --- Run/Stop background loop ----------------------------------------------

func TestApplier_Run_CatchesUpInBackground(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sm, cs := newApplierTarget(t)
	applier := NewApplier(leader, sm)
	applier.Run()
	t.Cleanup(applier.Stop)

	if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", 1, []byte("x"), []byte("10")))); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()

	// CommitCh is a wake-up hint (see raft.Node.CommitCh) consumed by
	// applier.Run's background goroutine; poll LastApplied with a bounded
	// timeout rather than assuming any fixed delay is enough.
	deadline := time.Now().Add(2 * time.Second)
	for leader.LastApplied() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if leader.LastApplied() != 1 {
		t.Fatalf("LastApplied = %d after background Run, want 1", leader.LastApplied())
	}
	got, err := cs.Get([]byte("x"))
	if err != nil || !bytes.Equal(got, []byte("10")) {
		t.Fatalf("Get(x) = %q, %v, want 10, nil", got, err)
	}
}

// blockingStore wraps a real storage.Store, letting a test pause Put's
// return until it explicitly says to continue -- used to prove Stop
// actually waits for an in-flight ApplyAvailable call rather than merely
// signaling it to stop before the next iteration.
type blockingStore struct {
	storage.Store
	putEntered chan struct{}
	release    chan struct{}
	entered    sync.Once
}

func (s *blockingStore) Put(key, value []byte) error {
	s.entered.Do(func() { close(s.putEntered) })
	<-s.release
	return s.Store.Put(key, value)
}

func TestApplier_Stop_WaitsForInFlightApplyBeforeReturning(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	base, err := storage.NewMemStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { base.Close() })
	bs := &blockingStore{Store: base, putEntered: make(chan struct{}), release: make(chan struct{})}
	sm := NewKVStateMachine(bs)
	applier := NewApplier(leader, sm)

	if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", 1, []byte("x"), []byte("10")))); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	leader.Drain()

	applier.Run()

	// Wait until the background goroutine is inside Put (blocked on
	// release), then start Stop concurrently and confirm it has NOT
	// returned yet -- proving it is genuinely waiting on the in-flight
	// call rather than racing past it.
	select {
	case <-bs.putEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("background ApplyAvailable never reached Put")
	}

	stopDone := make(chan struct{})
	go func() {
		applier.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatal("Stop returned before the in-flight Put finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(bs.release)

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the in-flight Put finished")
	}

	if leader.LastApplied() != 1 {
		t.Fatalf("LastApplied = %d, want 1 (the in-flight apply should have completed)", leader.LastApplied())
	}
}
