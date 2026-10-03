// Package dbnode's tests are Phase 8's integration test matrix: they
// exercise the full Raft -> Applier -> KVStateMachine -> storage.Store
// path through Node's public API, using real file-backed Raft persistence
// (raft.FilePersister) and real WAL-backed KV storage (storage.MemStore)
// under distinct per-node temporary directories -- never a bare in-memory
// fake standing in for either. Lower-level unit tests for Raft itself and
// for the state machine/applier in isolation already live in
// internal/raft and internal/statemachine respectively; this package does
// not repeat them, only the behavior that only exists once the three are
// wired together.
package dbnode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/statemachine"
	"github.com/Kushall-07/forgedb/internal/storage"
)

func mustGet(t *testing.T, n *Node, key string) string {
	t.Helper()
	v, err := n.Store().Get([]byte(key))
	if err != nil {
		t.Fatalf("node %s: Get(%s): %v", n.ID(), key, err)
	}
	return string(v)
}

func mustBeMissing(t *testing.T, n *Node, key string) {
	t.Helper()
	if _, err := n.Store().Get([]byte(key)); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("node %s: Get(%s) = %v, want ErrKeyNotFound", n.ID(), key, err)
	}
}

// proposeOrFatal proposes cmd through n and drains n immediately
// afterward, so n's own CommitIndex already reflects majority replication
// (if reachable) before the caller does anything else -- in particular,
// before any further heartbeat tick, whose LeaderCommit is only ever as
// fresh as the leader's CommitIndex at the moment that tick's broadcast is
// sent (see settleCommit).
func proposeOrFatal(t *testing.T, n *Node, cmd statemachine.Command) (index, term uint64) {
	t.Helper()
	index, term, err := n.Propose(cmd)
	if err != nil {
		t.Fatalf("node %s: Propose: %v", n.ID(), err)
	}
	n.Drain()
	return index, term
}

// --- A. Single-node integration: PUT / DELETE through the actual Raft
// commit boundary -------------------------------------------------------
//
// A 3-node cluster's leader is used here, exactly as Phase 7's own
// integration test already does; the point of this test is the
// leader-through-commit-through-storage path, not the cluster size. See
// TestNode_SingleNodeCluster_PutAndDeleteThroughCommit below for the
// literal one-node (zero-peer) case.

func TestNode_SingleLeader_PutAndDeleteThroughCommit(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	if leader.Raft().CommitIndex() != 1 {
		t.Fatalf("CommitIndex = %d, want 1", leader.Raft().CommitIndex())
	}
	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	if got := mustGet(t, leader, "x"); got != "10" {
		t.Fatalf("Get(x) = %q, want %q", got, "10")
	}

	proposeOrFatal(t, leader, statemachine.NewDeleteCommand("c1", 2, []byte("x")))
	settleCommit(nodes)
	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	mustBeMissing(t, leader, "x")
}

// TestNode_SingleNodeCluster_PutAndDeleteThroughCommit covers the literal
// one-node (zero-peer) cluster regression end to end: PUT and DELETE must
// each commit (CommitIndex advances), apply (LastApplied advances), and
// reach storage, exactly as they do with a 3-node cluster above, even
// though there is no follower to replicate to or hear back from -- see
// internal/raft/replication.go's broadcastAppendEntriesLocked, which now
// re-checks the commit index itself once a Propose's AppendEntries round
// has (vacuously) finished dispatching to zero peers.
func TestNode_SingleNodeCluster_PutAndDeleteThroughCommit(t *testing.T) {
	_, nodes, _ := newCluster(t, 1)
	leader := nodes[0]
	electLeader(t, leader)

	putIdx, _ := proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	if got := leader.Raft().CommitIndex(); got != putIdx {
		t.Fatalf("after PUT: CommitIndex = %d, want %d", got, putIdx)
	}
	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	if got := leader.Raft().LastApplied(); got != putIdx {
		t.Fatalf("after PUT: LastApplied = %d, want %d", got, putIdx)
	}
	if got := mustGet(t, leader, "x"); got != "10" {
		t.Fatalf("Get(x) = %q, want %q", got, "10")
	}

	delIdx, _ := proposeOrFatal(t, leader, statemachine.NewDeleteCommand("c1", 2, []byte("x")))
	if got := leader.Raft().CommitIndex(); got != delIdx {
		t.Fatalf("after DELETE: CommitIndex = %d, want %d", got, delIdx)
	}
	if _, err := leader.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	if got := leader.Raft().LastApplied(); got != delIdx {
		t.Fatalf("after DELETE: LastApplied = %d, want %d", got, delIdx)
	}
	mustBeMissing(t, leader, "x")
}

// --- B. Three-node replication: leader -> followers, final states equal ---

func TestNode_ThreeNodeCluster_ReplicatesPutAndDelete(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("client-1", 1, []byte("name"), []byte("Kushal")))
	settleCommit(nodes)
	for _, n := range nodes {
		if n.Raft().CommitIndex() != 1 {
			t.Fatalf("node %s CommitIndex = %d, want 1", n.ID(), n.Raft().CommitIndex())
		}
	}
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		if got := mustGet(t, n, "name"); got != "Kushal" {
			t.Fatalf("node %s Get(name) = %q, want %q", n.ID(), got, "Kushal")
		}
	}

	proposeOrFatal(t, leader, statemachine.NewDeleteCommand("client-1", 2, []byte("name")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		mustBeMissing(t, n, "name")
	}
}

// --- D/E/F. Minority cannot commit or mutate storage; healing lets the
// entry commit and apply everywhere ----------------------------------------
//
// A 5-node cluster is used specifically so one follower can receive (and
// append to its own Raft log) an AppendEntries the leader sends, while the
// cluster as a whole still cannot reach a majority (3 of 5) -- with only 3
// nodes, any single reachable follower plus the leader already *is* a
// majority, which would commit the entry immediately and not exercise this
// path at all.

func TestNode_MinorityPartition_FollowerLogsUncommittedEntry_StorageUnaffected_ThenHeals(t *testing.T) {
	tr, nodes, _ := newCluster(t, 5)
	leader := nodes[0]
	electLeader(t, leader)

	// Partition every follower except one: the leader can reach only
	// itself + nodes[1], which is 2 of 5 -- short of the 3-node majority.
	for _, n := range nodes[2:] {
		tr.Partition(n.ID())
	}

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	leader.Drain()

	if got := leader.Raft().CommitIndex(); got != 0 {
		t.Fatalf("leader CommitIndex = %d, want 0 (no majority reachable)", got)
	}
	// nodes[1] received and appended the AppendEntries carrying this
	// entry -- its own Raft log has it -- but it must never have reached
	// KV storage, since it was never committed.
	if got := nodes[1].Raft().LastLogIndex(); got != 1 {
		t.Fatalf("nodes[1] LastLogIndex = %d, want 1 (follower must still log an uncommitted entry)", got)
	}
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		mustBeMissing(t, n, "x")
	}

	// Heal the partition: the leader can now reach a majority, the entry
	// commits, and applying catches it up everywhere.
	for _, n := range nodes[2:] {
		tr.Heal(n.ID())
	}
	settleCommit(nodes)
	if got := leader.Raft().CommitIndex(); got != 1 {
		t.Fatalf("leader CommitIndex after heal = %d, want 1", got)
	}
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		if got := mustGet(t, n, "x"); got != "10" {
			t.Fatalf("node %s Get(x) after heal = %q, want %q", n.ID(), got, "10")
		}
	}
}

// --- G. Leader failure: surviving majority retains committed state and
// keeps making progress under a new leader --------------------------------

func TestNode_LeaderFailure_SurvivingMajorityRetainsCommittedStateAndElectsNewLeader(t *testing.T) {
	tr, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		if got := mustGet(t, n, "x"); got != "10" {
			t.Fatalf("node %s Get(x) before failure = %q, want %q", n.ID(), got, "10")
		}
	}

	// Simulate the old leader crashing / becoming unavailable: partition
	// it permanently (this test never heals it).
	tr.Partition(leader.ID())

	survivors := []*Node{nodes[1], nodes[2]}
	electLeader(t, survivors[0])
	if survivors[0].Raft().CommitIndex() < 1 {
		t.Fatalf("new leader %s lost track of the previously committed entry: CommitIndex = %d", survivors[0].ID(), survivors[0].Raft().CommitIndex())
	}
	for _, n := range survivors {
		if got := mustGet(t, n, "x"); got != "10" {
			t.Fatalf("surviving node %s Get(x) after leader failure = %q, want %q", n.ID(), got, "10")
		}
	}

	// The surviving majority (2 of 3) can still commit new writes.
	proposeOrFatal(t, survivors[0], statemachine.NewPutCommand("c1", 2, []byte("y"), []byte("20")))
	settleCommit(survivors)
	applyAllAvailable(t, survivors)
	for _, n := range survivors {
		if got := mustGet(t, n, "y"); got != "20" {
			t.Fatalf("surviving node %s Get(y) = %q, want %q", n.ID(), got, "20")
		}
	}
}

// --- H. Node restart: real files, KV recovers independently of Raft's
// volatile commit/apply state ----------------------------------------------

func TestNode_Restart_RecoversKVAndRaftLogFromRealFiles(t *testing.T) {
	tr, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		if got := mustGet(t, n, "x"); got != "10" {
			t.Fatalf("node %s Get(x) before restart = %q, want %q", n.ID(), got, "10")
		}
	}

	// Restart a follower (not the leader): restarting the current leader
	// would require it to win a fresh election in a new term and then
	// have a *new* entry replicate before it could re-derive its own
	// CommitIndex for already-durable, older-term entries (Raft's
	// current-term commit rule -- see docs/raft/phase8-raft-storage-integration.md);
	// restarting a follower avoids that and is enough to prove real-file
	// recovery.
	follower := nodes[1]
	if err := follower.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids := clusterIDs(3)
	reopened, err := Open(newNodeConfig(follower.ID(), peersOf(ids, follower.ID()), tr, dirs[1]))
	if err != nil {
		t.Fatalf("reopen Open: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	// KV storage recovers from its own WAL immediately, before any Raft
	// activity happens at all -- this is the point of the two-durability-
	// mechanisms split (see docs/raft/phase8-raft-storage-integration.md).
	if got := mustGet(t, reopened, "x"); got != "10" {
		t.Fatalf("reopened node Get(x) immediately after reopen = %q, want %q", got, "10")
	}
	// Raft's log (currentTerm/votedFor/log) recovers via FilePersister.
	if got := reopened.Raft().LastLogIndex(); got != 1 {
		t.Fatalf("reopened node LastLogIndex = %d, want 1", got)
	}
	// commitIndex and lastApplied are volatile and always reset to 0 on
	// restart (Phase 6/7's documented behavior; Phase 8 does not change
	// it) -- the reopened node has not yet been told anything is
	// committed by Raft's own mechanics.
	if got := reopened.Raft().CommitIndex(); got != 0 {
		t.Fatalf("reopened node CommitIndex = %d, want 0 immediately after reopen", got)
	}
	if got := reopened.Raft().LastApplied(); got != 0 {
		t.Fatalf("reopened node LastApplied = %d, want 0 immediately after reopen", got)
	}

	// Rejoin the still-running cluster: the still-active leader's next
	// heartbeat carries LeaderCommit, letting the reopened follower's
	// CommitIndex catch back up through ordinary Raft mechanics.
	live := []*Node{leader, nodes[2], reopened}
	settleCommit(live)
	if got := reopened.Raft().CommitIndex(); got != 1 {
		t.Fatalf("reopened node CommitIndex after rejoining = %d, want 1", got)
	}
	if _, err := reopened.ApplyAvailable(); err != nil {
		t.Fatalf("reopened node ApplyAvailable: %v", err)
	}
	if got := reopened.Raft().LastApplied(); got != 1 {
		t.Fatalf("reopened node LastApplied after reapply = %d, want 1", got)
	}
	if got := mustGet(t, reopened, "x"); got != "10" {
		t.Fatalf("reopened node Get(x) after reapply = %q, want %q", got, "10")
	}
}

// --- I. Full cluster restart: all three nodes, logical state converges ----

func TestNode_FullClusterRestart_ConvergesToRecoveredState(t *testing.T) {
	oldTr, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("a"), []byte("1")))
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 2, []byte("b"), []byte("2")))
	proposeOrFatal(t, leader, statemachine.NewDeleteCommand("c1", 3, []byte("a")))
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 4, []byte("c"), []byte("3")))
	settleCommit(nodes)
	for _, n := range nodes {
		if got := n.Raft().CommitIndex(); got != 4 {
			t.Fatalf("node %s CommitIndex = %d, want 4", n.ID(), got)
		}
	}
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		mustBeMissing(t, n, "a")
		if got := mustGet(t, n, "b"); got != "2" {
			t.Fatalf("node %s Get(b) = %q, want 2", n.ID(), got)
		}
		if got := mustGet(t, n, "c"); got != "3" {
			t.Fatalf("node %s Get(c) = %q, want 3", n.ID(), got)
		}
	}

	// Close the whole cluster and reopen every node from the exact same
	// on-disk directories, over a brand new transport (standing in for a
	// full process/cluster restart -- nothing about InMemoryTransport
	// itself is ever persisted).
	for _, n := range nodes {
		if err := n.Close(); err != nil {
			t.Fatalf("node %s Close: %v", n.ID(), err)
		}
	}

	newTr := oldTr // documented below: newCluster's transport is not reused for RPC; only Register matters, and Open re-registers.
	_ = newTr
	ids := clusterIDs(3)
	reopened := make([]*Node, 3)
	for i, id := range ids {
		n, err := Open(newNodeConfig(id, peersOf(ids, id), oldTr, dirs[i]))
		if err != nil {
			t.Fatalf("reopen node %s: %v", id, err)
		}
		reopened[i] = n
		t.Cleanup(func() { n.Close() })
	}

	// KV state is already fully recovered on every node before any Raft
	// activity happens -- this is the primary claim this test proves.
	for _, n := range reopened {
		mustBeMissing(t, n, "a")
		if got := mustGet(t, n, "b"); got != "2" {
			t.Fatalf("reopened node %s Get(b) = %q, want 2", n.ID(), got)
		}
		if got := mustGet(t, n, "c"); got != "3" {
			t.Fatalf("reopened node %s Get(c) = %q, want 3", n.ID(), got)
		}
		if got := n.Raft().CommitIndex(); got != 0 {
			t.Fatalf("reopened node %s CommitIndex = %d, want 0 (volatile state always resets)", n.ID(), got)
		}
	}

	// Re-establishing Raft's own commit tracking after a full restart
	// requires a fresh election and, per Raft's current-term commit rule,
	// at least one new proposal in the new leader's term before
	// CommitIndex can jump forward to cover the recovered (older-term)
	// log too -- see docs/raft/phase8-raft-storage-integration.md.
	newLeader := reopened[0]
	electLeader(t, newLeader)
	proposeOrFatal(t, newLeader, statemachine.NewPutCommand("c2", 1, []byte("restart-marker"), []byte("1")))
	settleCommit(reopened)
	for _, n := range reopened {
		if got := n.Raft().CommitIndex(); got != 5 {
			t.Fatalf("reopened node %s CommitIndex after new proposal = %d, want 5", n.ID(), got)
		}
	}
	applyAllAvailable(t, reopened)
	for _, n := range reopened {
		mustBeMissing(t, n, "a")
		if got := mustGet(t, n, "b"); got != "2" {
			t.Fatalf("reopened node %s Get(b) = %q, want 2", n.ID(), got)
		}
		if got := mustGet(t, n, "c"); got != "3" {
			t.Fatalf("reopened node %s Get(c) = %q, want 3", n.ID(), got)
		}
		if got := mustGet(t, n, "restart-marker"); got != "1" {
			t.Fatalf("reopened node %s Get(restart-marker) = %q, want 1", n.ID(), got)
		}
	}
}

// --- J. Duplicate request through the full path: one logical mutation ----

// countingStore wraps a real storage.Store, counting real Put calls -- used
// to verify a duplicated (ClientID, RequestID) request that Raft itself
// happily logs twice (Raft never deduplicates; see
// docs/raft/phase8-raft-storage-integration.md) still only mutates storage
// once, because KVStateMachine's deduplication catches the replay before
// it ever reaches Store.Put.
type countingStore struct {
	storage.Store
	mu   sync.Mutex
	puts int
}

func (s *countingStore) Put(key, value []byte) error {
	if err := s.Store.Put(key, value); err != nil {
		return err
	}
	s.mu.Lock()
	s.puts++
	s.mu.Unlock()
	return nil
}

func (s *countingStore) Puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

func TestNode_DuplicateRequestThroughRaft_MutatesStorageExactlyOnce(t *testing.T) {
	tr := raft.NewInMemoryTransport()
	ids := clusterIDs(3)
	nodes := make([]*Node, 3)
	stores := make([]*countingStore, 3)
	for i, id := range ids {
		base := t.TempDir()
		dirs := nodeDirs{raftDir: filepath.Join(base, "raft"), kvDir: filepath.Join(base, "kv")}
		var cs *countingStore
		cfg := newNodeConfig(id, peersOf(ids, id), tr, dirs)
		cfg.WrapStore = func(s storage.Store) storage.Store {
			cs = &countingStore{Store: s}
			return cs
		}
		n := mustOpen(t, cfg)
		t.Cleanup(func() { n.Close() })
		nodes[i] = n
		stores[i] = cs
	}

	leader := nodes[0]
	electLeader(t, leader)

	cmd := statemachine.NewPutCommand("client-1", 1, []byte("x"), []byte("10"))
	proposeOrFatal(t, leader, cmd)
	proposeOrFatal(t, leader, cmd) // identical (ClientID, RequestID, Op, Key, Value): a genuine client retry
	settleCommit(nodes)
	if got := leader.Raft().CommitIndex(); got != 2 {
		t.Fatalf("CommitIndex = %d, want 2 (Raft logs the duplicate as a second entry -- it never deduplicates itself)", got)
	}

	applyAllAvailable(t, nodes)
	for i, n := range nodes {
		if got := mustGet(t, n, "x"); got != "10" {
			t.Fatalf("node %s Get(x) = %q, want %q", n.ID(), got, "10")
		}
		if got := stores[i].Puts(); got != 1 {
			t.Fatalf("node %s: real Put calls = %d, want 1 (second entry must be recognized as a replay)", n.ID(), got)
		}
	}
}

// --- K/L. Storage failure through the full path halts application without
// advancing LastApplied, then a retry completes it -------------------------

var errSimulatedStorageFault = errors.New("dbnode: simulated storage fault (test only)")

type faultyStore struct {
	storage.Store
	mu       sync.Mutex
	failNext bool
}

func (s *faultyStore) FailNext() {
	s.mu.Lock()
	s.failNext = true
	s.mu.Unlock()
}

func (s *faultyStore) Put(key, value []byte) error {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		s.mu.Unlock()
		return errSimulatedStorageFault
	}
	s.mu.Unlock()
	return s.Store.Put(key, value)
}

func TestNode_StorageFault_StopsWithoutAdvancingLastApplied_ThenRetrySucceeds(t *testing.T) {
	tr := raft.NewInMemoryTransport()
	ids := clusterIDs(3)
	nodes := make([]*Node, 3)
	var leaderFaultyStore *faultyStore
	for i, id := range ids {
		base := t.TempDir()
		dirs := nodeDirs{raftDir: filepath.Join(base, "raft"), kvDir: filepath.Join(base, "kv")}
		cfg := newNodeConfig(id, peersOf(ids, id), tr, dirs)
		if i == 0 {
			cfg.WrapStore = func(s storage.Store) storage.Store {
				leaderFaultyStore = &faultyStore{Store: s}
				return leaderFaultyStore
			}
		}
		n := mustOpen(t, cfg)
		t.Cleanup(func() { n.Close() })
		nodes[i] = n
	}
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("a"), []byte("1")))
	settleCommit(nodes)
	if applied, err := leader.ApplyAvailable(); err != nil || applied != 1 {
		t.Fatalf("ApplyAvailable for entry 1: applied=%d, err=%v, want 1, nil", applied, err)
	}

	leaderFaultyStore.FailNext()
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 2, []byte("b"), []byte("2")))
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 3, []byte("c"), []byte("3")))
	settleCommit(nodes)
	if got := leader.Raft().CommitIndex(); got != 3 {
		t.Fatalf("CommitIndex = %d, want 3", got)
	}

	applied, err := leader.ApplyAvailable()
	if err == nil {
		t.Fatalf("ApplyAvailable: want error from the injected storage fault, got nil")
	}
	if applied != 0 {
		t.Fatalf("ApplyAvailable applied = %d, want 0 (entry 2 fails immediately)", applied)
	}
	if got := leader.Raft().LastApplied(); got != 1 {
		t.Fatalf("LastApplied = %d, want 1 (must not advance past the failed entry)", got)
	}
	mustBeMissing(t, leader, "b")

	// Raft's own commit index must be completely unaffected by the local
	// storage fault -- consensus and local application are separate.
	if got := leader.Raft().CommitIndex(); got != 3 {
		t.Fatalf("CommitIndex after fault = %d, want unchanged 3", got)
	}

	applied, err = leader.ApplyAvailable()
	if err != nil {
		t.Fatalf("retry ApplyAvailable: %v", err)
	}
	if applied != 2 {
		t.Fatalf("retry ApplyAvailable applied = %d, want 2 (entries 2 and 3)", applied)
	}
	if got := leader.Raft().LastApplied(); got != 3 {
		t.Fatalf("LastApplied after retry = %d, want 3", got)
	}
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got := mustGet(t, leader, key); got != want {
			t.Fatalf("Get(%s) = %q, want %q", key, got, want)
		}
	}
}

// --- M. Apply ordering: entries apply strictly in log-index order --------

func TestNode_ApplyOrdering_StrictlySequential(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("1")))
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 2, []byte("x"), []byte("2")))
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 3, []byte("x"), []byte("3")))
	proposeOrFatal(t, leader, statemachine.NewDeleteCommand("c1", 4, []byte("x")))
	settleCommit(nodes)

	applied, err := leader.ApplyAvailable()
	if err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	if applied != 4 {
		t.Fatalf("applied = %d, want 4", applied)
	}
	mustBeMissing(t, leader, "x") // put 1, put 2, put 3, delete -- in that order
}

// --- N. Concurrent apply protection: no double-application under a race --

func TestNode_ConcurrentApplyAvailable_NoDoubleApplication(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	for i := 1; i <= 5; i++ {
		proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", uint64(i), []byte(fmt.Sprintf("k%d", i)), []byte("v")))
	}
	settleCommit(nodes)
	if got := leader.Raft().CommitIndex(); got != 5 {
		t.Fatalf("CommitIndex = %d, want 5", got)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = leader.ApplyAvailable()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d ApplyAvailable: %v", i, err)
		}
	}
	if got := leader.Raft().LastApplied(); got != 5 {
		t.Fatalf("LastApplied = %d, want 5 (each entry applied exactly once despite the concurrent callers)", got)
	}
	for i := 1; i <= 5; i++ {
		if got := mustGet(t, leader, fmt.Sprintf("k%d", i)); got != "v" {
			t.Fatalf("Get(k%d) = %q, want v", i, got)
		}
	}
}

// --- O. Independent per-node directories -----------------------------------

func TestNode_IndependentPerNodeDirectories(t *testing.T) {
	_, nodes, dirs := newCluster(t, 3)

	seen := make(map[string]bool)
	for i, d := range dirs {
		for _, p := range []string{d.raftDir, d.kvDir} {
			abs, err := filepath.Abs(p)
			if err != nil {
				t.Fatalf("Abs(%s): %v", p, err)
			}
			if seen[abs] {
				t.Fatalf("node%d: directory %s reused across nodes", i, abs)
			}
			seen[abs] = true
		}
	}

	leader := nodes[0]
	electLeader(t, leader)
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	// Every node's own KV directory holds its own real WAL file, on disk,
	// physically separate from every other node's.
	for i, d := range dirs {
		walPath := filepath.Join(d.kvDir, "wal.log")
		info, err := os.Stat(walPath)
		if err != nil {
			t.Fatalf("node%d: stat %s: %v", i, walPath, err)
		}
		if info.Size() == 0 {
			t.Fatalf("node%d: wal.log is empty, want the replicated PUT to have been durably written", i)
		}
	}
}
