// Phase 8.5 integration tests: linearizable reads through
// (*Node).ConsistentGet, exercising the full
// Raft.ReadIndex -> apply-barrier -> storage.Get path the same way
// node_test.go already exercises the write path -- real file-backed Raft
// persistence and real WAL-backed KV storage under the shared newCluster
// helper, never a bare in-memory fake standing in for either. See
// docs/raft/phase8.5-read-consistency.md for the design these tests
// verify.
package dbnode

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Kushall-07/forgedb/internal/statemachine"
	"github.com/Kushall-07/forgedb/internal/storage"
)

// --- Case 1/3/8: normal leader read, including after majority confirmation ---

func TestNode_ConsistentGet_LeaderReadsCommittedValue(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	got, err := leader.ConsistentGet(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("ConsistentGet(x): %v", err)
	}
	if string(got) != "10" {
		t.Fatalf("ConsistentGet(x) = %q, want %q", got, "10")
	}
}

// --- Case 2: follower reads must never be served locally -----------------

func TestNode_ConsistentGet_FollowerRejected(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	follower := nodes[1]
	// The follower may even be fully caught up (applied, correct value in
	// its own local storage) -- that must not matter. ConsistentGet on a
	// non-leader always fails rather than ever risk serving a read that
	// cannot be confirmed linearizable.
	if _, err := follower.Store().Get([]byte("x")); err != nil {
		t.Fatalf("precondition: follower should already have applied x locally: %v", err)
	}
	if _, err := follower.ConsistentGet(context.Background(), []byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower ConsistentGet err = %v, want ErrNotLeader", err)
	}
}

// --- Case 4: an isolated leader must fail safely, never return stale data ---

func TestNode_ConsistentGet_IsolatedLeaderFails(t *testing.T) {
	tr, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	// Partition the leader into a minority of one. It still locally
	// believes role == Leader (nothing has told it otherwise), but it can
	// no longer reach the majority a linearizable read requires.
	tr.Partition(leader.ID())

	if _, err := leader.ConsistentGet(context.Background(), []byte("x")); !errors.Is(err, ErrReadUnavailable) {
		t.Fatalf("isolated leader ConsistentGet err = %v, want ErrReadUnavailable", err)
	}
}

// --- Case 6: commit ahead of apply -- GET must wait, not read early ------

func TestNode_ConsistentGet_WaitsForApply(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	// Deliberately do not call ApplyAvailable: commitIndex has advanced
	// but lastApplied has not, reproducing commitIndex > lastApplied.
	if leader.Raft().CommitIndex() == 0 {
		t.Fatalf("precondition: commitIndex should have advanced")
	}
	if leader.Raft().LastApplied() != 0 {
		t.Fatalf("precondition: lastApplied should still be 0, got %d", leader.Raft().LastApplied())
	}

	// A bounded context proves ConsistentGet actually blocks on the apply
	// barrier instead of reading storage early: with apply never
	// happening, it must time out rather than return (correctly or
	// otherwise).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := leader.ConsistentGet(ctx, []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ConsistentGet before apply err = %v, want context.DeadlineExceeded", err)
	}

	// Once the state machine actually catches up, the exact same read
	// barrier is satisfied immediately and returns the correct value.
	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	got, err := leader.ConsistentGet(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("ConsistentGet after apply: %v", err)
	}
	if string(got) != "10" {
		t.Fatalf("ConsistentGet(x) = %q, want %q", got, "10")
	}
}

// --- Case 7: missing key after a valid barrier is a normal not-found -----

func TestNode_ConsistentGet_MissingKeyAfterBarrier(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	// Commit something unrelated so the barrier itself is exercised
	// (non-zero read index), then look up a key that was never written.
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("other"), []byte("v")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	_, err := leader.ConsistentGet(context.Background(), []byte("missing"))
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("ConsistentGet(missing) err = %v, want storage.ErrKeyNotFound", err)
	}
}

// --- Case 8/9: PUT -> GET and DELETE -> GET through the consistent path ---

func TestNode_ConsistentGet_PutThenGet(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("1")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	got, err := leader.ConsistentGet(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("ConsistentGet(x): %v", err)
	}
	if string(got) != "1" {
		t.Fatalf("ConsistentGet(x) = %q, want %q", got, "1")
	}
}

func TestNode_ConsistentGet_DeleteThenGet(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("1")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	proposeOrFatal(t, leader, statemachine.NewDeleteCommand("c1", 2, []byte("x")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	if _, err := leader.ConsistentGet(context.Background(), []byte("x")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("ConsistentGet(x) after delete err = %v, want storage.ErrKeyNotFound", err)
	}
}

// --- Concurrency: concurrent GETs, and GET racing ApplyAvailable ---------

func TestNode_ConsistentGet_ConcurrentGets(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	const n = 8
	results := make([][]byte, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = leader.ConsistentGet(context.Background(), []byte("x"))
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: ConsistentGet err = %v", i, errs[i])
		}
		if string(results[i]) != "10" {
			t.Fatalf("goroutine %d: ConsistentGet = %q, want %q", i, results[i], "10")
		}
	}
}

// GET racing ApplyAvailable: commit without applying, then race a
// ConsistentGet call (waiting on the apply barrier) against the
// ApplyAvailable call that satisfies it. Regardless of scheduling order,
// the read must either block until apply completes or already see it --
// it must never see a value before apply and never error once apply has
// actually happened before ConsistentGet's own barrier check completes.
func TestNode_ConsistentGet_RacesApplyAvailable(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)

	var wg sync.WaitGroup
	var got []byte
	var getErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		got, getErr = leader.ConsistentGet(context.Background(), []byte("x"))
	}()

	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	wg.Wait()

	if getErr != nil {
		t.Fatalf("ConsistentGet: %v", getErr)
	}
	if string(got) != "10" {
		t.Fatalf("ConsistentGet(x) = %q, want %q", got, "10")
	}
}

// --- Case 5: leader failover -- the new leader must serve the read -------

func TestNode_ConsistentGet_LeaderFailoverThenNewLeaderServesGet(t *testing.T) {
	tr, nodes, _ := newCluster(t, 3)
	oldLeader := nodes[0]
	electLeader(t, oldLeader)

	proposeOrFatal(t, oldLeader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	// Partition the old leader away; the remaining two nodes are still a
	// majority and elect a new leader among themselves.
	tr.Partition(oldLeader.ID())
	newLeader := nodes[1]
	electLeader(t, newLeader)
	applyAllAvailable(t, nodes)

	got, err := newLeader.ConsistentGet(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("new leader ConsistentGet(x): %v", err)
	}
	if string(got) != "10" {
		t.Fatalf("new leader ConsistentGet(x) = %q, want %q", got, "10")
	}

	// The stale old leader must not be able to serve a successful
	// consistent read any more, even though its local role has not yet
	// been corrected.
	if _, err := oldLeader.ConsistentGet(context.Background(), []byte("x")); !errors.Is(err, ErrReadUnavailable) {
		t.Fatalf("old (partitioned) leader ConsistentGet err = %v, want ErrReadUnavailable", err)
	}
}

// --- No stale read after a committed write, through a fresh write/read ---

func TestNode_ConsistentGet_NoStaleReadAfterCommittedWrite(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("old")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 2, []byte("x"), []byte("new")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	got, err := leader.ConsistentGet(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("ConsistentGet(x): %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("ConsistentGet(x) = %q, want %q (must not observe the stale prior value)", got, "new")
	}
}
