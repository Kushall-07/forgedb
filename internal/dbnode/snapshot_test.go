// Phase 9 integration tests: snapshots through the real dbnode.Node
// composition (real raft.FilePersister, real storage.MemStore), exercising
// the same "PUT -> commit -> apply -> snapshot -> more writes -> restart ->
// GET" path and lagging-follower catch-up the phase brief calls for. See
// docs/raft/phase9-snapshots.md.
package dbnode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Kushall-07/forgedb/internal/raft"
	"github.com/Kushall-07/forgedb/internal/statemachine"
)

// --- Basic snapshot + log truncation ----------------------------------------

func TestNode_CreateSnapshot_TruncatesRaftLog(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	for i := 1; i <= 3; i++ {
		proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", uint64(i), []byte("x"), []byte("v")))
	}
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	if err := leader.CreateSnapshot(3); err != nil {
		t.Fatalf("CreateSnapshot(3): %v", err)
	}
	if got := leader.SnapshotIndex(); got != 3 {
		t.Fatalf("SnapshotIndex = %d, want 3", got)
	}
	if got := leader.Raft().LastLogIndex(); got != 3 {
		t.Fatalf("LastLogIndex after snapshot (no new entries since) = %d, want unchanged 3", got)
	}
}

// --- Restart after a snapshot: no suffix ------------------------------------

func TestNode_SnapshotThenRestart_StateSurvives(t *testing.T) {
	tr, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 2, []byte("y"), []byte("20")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	follower := nodes[1]
	if err := follower.CreateSnapshot(2); err != nil {
		t.Fatalf("follower CreateSnapshot(2): %v", err)
	}

	if err := follower.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ids := clusterIDs(3)
	reopened, err := Open(newNodeConfig(follower.ID(), peersOf(ids, follower.ID()), tr, dirs[1]))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	// KV state survives via the node's own independent WAL, exactly as
	// without snapshots.
	if got := mustGet(t, reopened, "x"); got != "10" {
		t.Fatalf("Get(x) after restart = %q, want 10", got)
	}
	if got := mustGet(t, reopened, "y"); got != "20" {
		t.Fatalf("Get(y) after restart = %q, want 20", got)
	}
	// The snapshot boundary itself survives restart.
	if got := reopened.SnapshotIndex(); got != 2 {
		t.Fatalf("SnapshotIndex after restart = %d, want 2", got)
	}
	if got := reopened.Raft().LastLogIndex(); got != 2 {
		t.Fatalf("LastLogIndex after restart = %d, want 2 (no suffix)", got)
	}
}

// --- Restart after a snapshot with a retained suffix ------------------------

func TestNode_SnapshotPlusSuffix_RestartRecoversCorrectState(t *testing.T) {
	tr, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	for i := 1; i <= 3; i++ {
		proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", uint64(i), []byte{byte(i)}, []byte("v1")))
	}
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	follower := nodes[1]
	if err := follower.CreateSnapshot(3); err != nil {
		t.Fatalf("CreateSnapshot(3): %v", err)
	}

	for i := 4; i <= 6; i++ {
		proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", uint64(i), []byte{byte(i)}, []byte("v2")))
	}
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	if err := follower.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ids := clusterIDs(3)
	reopened, err := Open(newNodeConfig(follower.ID(), peersOf(ids, follower.ID()), tr, dirs[1]))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	if got := reopened.SnapshotIndex(); got != 3 {
		t.Fatalf("SnapshotIndex after restart = %d, want 3", got)
	}
	if got := reopened.Raft().LastLogIndex(); got != 6 {
		t.Fatalf("LastLogIndex after restart = %d, want 6 (suffix 4..6 retained)", got)
	}
	for i := 1; i <= 6; i++ {
		key := string([]byte{byte(i)})
		want := "v1"
		if i > 3 {
			want = "v2"
		}
		if got := mustGet(t, reopened, key); got != want {
			t.Fatalf("Get(key %d) after restart = %q, want %q", i, got, want)
		}
	}
}

// --- Leader snapshots, stops, restarts --------------------------------------

func TestNode_LeaderSnapshotThenRestart_RecoversAndCanLeadAgain(t *testing.T) {
	tr, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)
	if err := leader.CreateSnapshot(1); err != nil {
		t.Fatalf("CreateSnapshot(1): %v", err)
	}

	if err := leader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ids := clusterIDs(3)
	reopened, err := Open(newNodeConfig(leader.ID(), peersOf(ids, leader.ID()), tr, dirs[0]))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	if reopened.IsLeader() {
		t.Fatalf("reopened node resumed leadership; a restart must never restore an old leader role")
	}
	if got := reopened.SnapshotIndex(); got != 1 {
		t.Fatalf("SnapshotIndex after restart = %d, want 1", got)
	}
	if got := mustGet(t, reopened, "x"); got != "10" {
		t.Fatalf("Get(x) after restart = %q, want 10", got)
	}

	live := []*Node{reopened, nodes[1], nodes[2]}
	electLeader(t, reopened)
	proposeOrFatal(t, reopened, statemachine.NewPutCommand("c1", 2, []byte("y"), []byte("20")))
	settleCommit(live)
	applyAllAvailable(t, live)
	for _, n := range live {
		if got := mustGet(t, n, "y"); got != "20" {
			t.Fatalf("node %s Get(y) after re-election = %q, want 20", n.ID(), got)
		}
	}
}

// --- Lagging follower installs a snapshot and catches up --------------------

func TestNode_LaggingFollower_InstallsSnapshotAndCatchesUp(t *testing.T) {
	trans, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	laggingID := nodes[2].ID()
	trans.Partition(laggingID)

	for i := 1; i <= 5; i++ {
		proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", uint64(i), []byte{byte(i)}, []byte("v")))
	}
	settleCommit([]*Node{leader, nodes[1]})
	applyAllAvailable(t, []*Node{leader, nodes[1]})

	if err := leader.CreateSnapshot(5); err != nil {
		t.Fatalf("CreateSnapshot(5): %v", err)
	}

	trans.Heal(laggingID)
	// A heartbeat round: the leader notices nodes[2]'s nextIndex is behind
	// the (now compacted) log and sends InstallSnapshot instead.
	settleCommit(nodes)

	lagging := nodes[2]
	if got := lagging.SnapshotIndex(); got != 5 {
		t.Fatalf("lagging node SnapshotIndex after catch-up = %d, want 5", got)
	}
	if _, err := lagging.ApplyAvailable(); err != nil {
		t.Fatalf("lagging node ApplyAvailable: %v", err)
	}
	for i := 1; i <= 5; i++ {
		key := string([]byte{byte(i)})
		if got := mustGet(t, lagging, key); got != "v" {
			t.Fatalf("lagging node Get(key %d) after catch-up = %q, want v", i, got)
		}
	}

	// The cluster continues committing new entries normally afterward.
	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 6, []byte("new"), []byte("value")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)
	for _, n := range nodes {
		if got := mustGet(t, n, "new"); got != "value" {
			t.Fatalf("node %s Get(new) after post-snapshot write = %q, want value", n.ID(), got)
		}
	}
}

// --- Read consistency continues working after a snapshot --------------------

func TestNode_ConsistentGet_WorksAfterSnapshot(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)
	if err := leader.CreateSnapshot(1); err != nil {
		t.Fatalf("CreateSnapshot(1): %v", err)
	}

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 2, []byte("y"), []byte("20")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	v, err := leader.ConsistentGet(context.Background(), []byte("y"))
	if err != nil {
		t.Fatalf("ConsistentGet(y) after snapshot: %v", err)
	}
	if string(v) != "20" {
		t.Fatalf("ConsistentGet(y) = %q, want 20", v)
	}
	v, err = leader.ConsistentGet(context.Background(), []byte("x"))
	if err != nil {
		t.Fatalf("ConsistentGet(x) after snapshot: %v", err)
	}
	if string(v) != "10" {
		t.Fatalf("ConsistentGet(x) = %q, want 10", v)
	}
}

// --- Snapshot safety under partition: never captures uncommitted state -----

func TestNode_PartitionedMinorityLeader_CannotSnapshotBeyondTrueCommitIndex(t *testing.T) {
	trans, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	// Partition the leader into a minority of one; it can still append to
	// its own log locally, but can never commit anything further.
	trans.Partition(leader.ID())
	if _, _, err := leader.Propose(statemachine.NewPutCommand("c1", 2, []byte("y"), []byte("20"))); err != nil {
		t.Fatalf("Propose while partitioned: %v", err)
	}
	leader.Drain()
	if got := leader.Raft().CommitIndex(); got != 1 {
		t.Fatalf("partitioned leader CommitIndex = %d, want unchanged 1 (the new entry cannot commit without a majority)", got)
	}

	// CreateSnapshot(2) must be rejected: index 2 was never applied (it
	// isn't even committed), so there is no way a correct snapshot of it
	// could exist.
	if err := leader.CreateSnapshot(2); err == nil {
		t.Fatalf("CreateSnapshot(2) on a partitioned minority leader (uncommitted): want error, got nil")
	}
	// CreateSnapshot(1), the genuinely applied/committed index, remains
	// valid even while partitioned -- snapshotting one's own already-safe
	// history never requires a majority.
	if err := leader.CreateSnapshot(1); err != nil {
		t.Fatalf("CreateSnapshot(1) (genuinely applied) on a partitioned leader: %v", err)
	}
}

// --- Concurrency: CreateSnapshot racing ApplyAvailable -----------------------

func TestNode_ConcurrentCreateSnapshotAndApplyAvailable_NoCorruption(t *testing.T) {
	_, nodes, _ := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	for i := 1; i <= 10; i++ {
		if _, _, err := leader.Propose(statemachine.NewPutCommand("c1", uint64(i), []byte{byte(i)}, []byte("v"))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()
	settleCommit(nodes)
	if got := leader.Raft().CommitIndex(); got != 10 {
		t.Fatalf("CommitIndex = %d, want 10", got)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var applyErr error
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := leader.ApplyAvailable(); err != nil {
				applyErr = err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		// Best-effort: most attempts will race ahead of LastApplied and
		// correctly fail CreateSnapshot's exact-match check; that is not
		// itself a test failure, only an unexpected *success* with wrong
		// data would be. This loop exists purely to exercise concurrent
		// access to Applier's lock without corrupting anything -- see
		// restorePendingSnapshotLocked/CreateSnapshot both holding applyMu.
		for i := uint64(1); i <= 10; i++ {
			_ = leader.CreateSnapshot(i)
		}
	}()
	wg.Wait()
	if applyErr != nil {
		t.Fatalf("ApplyAvailable under concurrent CreateSnapshot calls: %v", applyErr)
	}
	if got := leader.Raft().LastApplied(); got != 10 {
		t.Fatalf("LastApplied after concurrent access = %d, want 10", got)
	}
	for i := 1; i <= 10; i++ {
		if got := mustGet(t, leader, string([]byte{byte(i)})); got != "v" {
			t.Fatalf("Get(key %d) after concurrent snapshot/apply = %q, want v", i, got)
		}
	}
}

// --- Full cluster restart after every node snapshots ------------------------

func TestNode_FullClusterRestart_AfterSnapshot_Converges(t *testing.T) {
	_, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	for i := 1; i <= 3; i++ {
		proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", uint64(i), []byte{byte(i)}, []byte("v")))
	}
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	for _, n := range nodes {
		if err := n.CreateSnapshot(3); err != nil {
			t.Fatalf("node %s CreateSnapshot(3): %v", n.ID(), err)
		}
	}

	for _, n := range nodes {
		if err := n.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	ids := clusterIDs(3)
	newTr := raft.NewInMemoryTransport()
	reopened := make([]*Node, 3)
	for i, id := range ids {
		n, err := Open(newNodeConfig(id, peersOf(ids, id), newTr, dirs[i]))
		if err != nil {
			t.Fatalf("reopen node %s: %v", id, err)
		}
		t.Cleanup(func() { n.Close() })
		reopened[i] = n
	}

	for _, n := range reopened {
		if got := n.SnapshotIndex(); got != 3 {
			t.Fatalf("node %s SnapshotIndex after full cluster restart = %d, want 3", n.ID(), got)
		}
		for i := 1; i <= 3; i++ {
			key := string([]byte{byte(i)})
			if got := mustGet(t, n, key); got != "v" {
				t.Fatalf("node %s Get(key %d) after restart = %q, want v", n.ID(), i, got)
			}
		}
	}

	electLeader(t, reopened[0])
	proposeOrFatal(t, reopened[0], statemachine.NewPutCommand("c1", 100, []byte("marker"), []byte("post-restart")))
	settleCommit(reopened)
	applyAllAvailable(t, reopened)
	for _, n := range reopened {
		if got := mustGet(t, n, "marker"); got != "post-restart" {
			t.Fatalf("node %s Get(marker) after post-restart proposal = %q, want post-restart", n.ID(), got)
		}
	}
}

// --- Recovery must never silently accept a corrupted/partial snapshot ------

// TestNode_RestartWithCorruptedSnapshotFile_FailsClosed proves the same
// fail-closed guarantee internal/raft's own unit tests establish
// (TestNewNode_CorruptedSnapshotFile_FailsClosed), but end to end through
// the real dbnode.Open composition root and a real FilePersister-backed
// snapshot file on disk: damage to the persisted ".snapshot" file after a
// clean Close (bit rot, a tampered file, a bad restore -- anything other
// than this package's own write path, which the crash-window tests already
// cover) must cause Open to fail outright, never start the node with a
// silently discarded or partially-applied snapshot.
func TestNode_RestartWithCorruptedSnapshotFile_FailsClosed(t *testing.T) {
	tr, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", 1, []byte("x"), []byte("10")))
	settleCommit(nodes)
	applyAllAvailable(t, nodes)

	follower := nodes[1]
	followerDir := dirs[1]
	if err := follower.CreateSnapshot(1); err != nil {
		t.Fatalf("CreateSnapshot(1): %v", err)
	}
	if err := follower.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	snapPath := filepath.Join(followerDir.raftDir, raftStateFileName+".snapshot")
	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("snapshot file %s is empty", snapPath)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(snapPath, data, 0o644); err != nil {
		t.Fatalf("write corrupted snapshot file: %v", err)
	}

	ids := clusterIDs(3)
	if _, err := Open(newNodeConfig(follower.ID(), peersOf(ids, follower.ID()), tr, followerDir)); err == nil {
		t.Fatalf("Open with a corrupted on-disk snapshot file: want error, got nil")
	}
}

// --- Repeated snapshot/recovery cycles remain safe --------------------------

// TestNode_RepeatedSnapshotRestartCycles_StateCorrect drives several
// successive generations of write -> snapshot -> restart against the same
// node, proving each cycle's restart recovers exactly the cumulative state
// so far -- not just that a single snapshot-then-restart works (the other
// tests in this file), but that doing it repeatedly against the same
// on-disk files never accumulates corruption, loses previously-snapshotted
// keys, or regresses the snapshot boundary.
func TestNode_RepeatedSnapshotRestartCycles_StateCorrect(t *testing.T) {
	tr, nodes, dirs := newCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	ids := clusterIDs(3)
	const followerIdx = 1
	follower := nodes[followerIdx]
	followerDir := dirs[followerIdx]

	const cycles = 3
	for cycle := 0; cycle < cycles; cycle++ {
		key := fmt.Sprintf("k%d", cycle)
		val := fmt.Sprintf("v%d", cycle)
		proposeOrFatal(t, leader, statemachine.NewPutCommand("c1", uint64(cycle+1), []byte(key), []byte(val)))
		nodes[followerIdx] = follower
		settleCommit(nodes)
		applyAllAvailable(t, nodes)

		lastApplied := follower.Raft().LastApplied()
		if err := follower.CreateSnapshot(lastApplied); err != nil {
			t.Fatalf("cycle %d: CreateSnapshot(%d): %v", cycle, lastApplied, err)
		}
		if err := follower.Close(); err != nil {
			t.Fatalf("cycle %d: Close: %v", cycle, err)
		}

		reopened, err := Open(newNodeConfig(follower.ID(), peersOf(ids, follower.ID()), tr, followerDir))
		if err != nil {
			t.Fatalf("cycle %d: reopen: %v", cycle, err)
		}
		t.Cleanup(func() { reopened.Close() })

		if got := reopened.SnapshotIndex(); got != lastApplied {
			t.Fatalf("cycle %d: SnapshotIndex after restart = %d, want %d", cycle, got, lastApplied)
		}
		for c := 0; c <= cycle; c++ {
			wantKey := fmt.Sprintf("k%d", c)
			wantVal := fmt.Sprintf("v%d", c)
			if got := mustGet(t, reopened, wantKey); got != wantVal {
				t.Fatalf("cycle %d: Get(%s) after restart = %q, want %q (state from an earlier cycle was lost)", cycle, wantKey, got, wantVal)
			}
		}

		follower = reopened
		nodes[followerIdx] = reopened
	}
}
