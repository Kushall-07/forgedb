package statemachine

import (
	"testing"

	"github.com/Kushall-07/forgedb/internal/raft"
)

// noSnapshotStateMachine is a minimal StateMachine that does *not*
// implement Snapshotter, used to prove ApplyAvailable/CreateSnapshot fail
// clearly rather than silently losing state when snapshotting isn't
// supported.
type noSnapshotStateMachine struct{}

func (noSnapshotStateMachine) Apply(Command) (Result, error) { return Result{}, nil }

var _ StateMachine = noSnapshotStateMachine{}

// --- Applier.CreateSnapshot --------------------------------------------------

func TestApplier_CreateSnapshot_Success(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sm, _ := newApplierTarget(t)
	applier := NewApplier(leader, sm)

	for i := 0; i < 3; i++ {
		if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", uint64(i+1), []byte("x"), []byte("v")))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()
	if _, err := applier.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}

	if err := applier.CreateSnapshot(3); err != nil {
		t.Fatalf("CreateSnapshot(3): %v", err)
	}
	if got := leader.SnapshotIndex(); got != 3 {
		t.Fatalf("SnapshotIndex = %d, want 3", got)
	}
}

func TestApplier_CreateSnapshot_RejectsIndexNotEqualToLastApplied(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	sm, _ := newApplierTarget(t)
	applier := NewApplier(leader, sm)

	for i := 0; i < 3; i++ {
		if _, _, err := leader.Propose(encodeOrFatal(t, NewPutCommand("c1", uint64(i+1), []byte("x"), []byte("v")))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	leader.Drain()
	if _, err := applier.ApplyAvailable(); err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}

	if err := applier.CreateSnapshot(2); err == nil {
		t.Fatalf("CreateSnapshot(2) with LastApplied=3: want error, got nil")
	}
	if got := leader.SnapshotIndex(); got != 0 {
		t.Fatalf("SnapshotIndex after rejected CreateSnapshot = %d, want unchanged 0", got)
	}
}

func TestApplier_CreateSnapshot_RequiresSnapshotter(t *testing.T) {
	_, nodes := newRaftCluster(t, 3)
	leader := nodes[0]
	electLeader(t, leader)

	applier := NewApplier(leader, noSnapshotStateMachine{})
	if err := applier.CreateSnapshot(0); err == nil {
		t.Fatalf("CreateSnapshot against a non-Snapshotter state machine: want error, got nil")
	}
}

// --- ApplyAvailable restoring a pending snapshot -----------------------------

// TestApplier_ApplyAvailable_RestoresPendingSnapshotBeforeApplyingFurther
// simulates what a follower's Applier must do after InstallSnapshot has
// landed at the Raft level (see (*raft.Node).HandleInstallSnapshot): the
// snapshot covers indexes 1..3 (a key "seed" put via a source state
// machine), and one further entry (index 4, a Put of "after") is then
// committed on top of it via an ordinary AppendEntries carrying
// PrevLogIndex/PrevLogTerm at the snapshot boundary. A single
// ApplyAvailable call must restore the snapshot (recovering "seed") *and*
// apply the new entry (recovering "after") in the correct order.
func TestApplier_ApplyAvailable_RestoresPendingSnapshotBeforeApplyingFurther(t *testing.T) {
	source, _ := newApplierTarget(t)
	if _, err := source.Apply(NewPutCommand("c0", 1, []byte("seed"), []byte("value"))); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}
	snapshotData, err := source.CreateSnapshot()
	if err != nil {
		t.Fatalf("source CreateSnapshot: %v", err)
	}

	follower, err := raft.NewNode(raft.Options{ID: "f", Peers: []string{"leader"}, Transport: raft.NewInMemoryTransport()})
	if err != nil {
		t.Fatalf("raft.NewNode: %v", err)
	}
	installReply := follower.HandleInstallSnapshot(raft.InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 3, LastIncludedTerm: 1, Data: snapshotData,
	})
	if !installReply.Success {
		t.Fatalf("HandleInstallSnapshot: %+v", installReply)
	}

	appendReply := follower.HandleAppendEntries(raft.AppendEntriesArgs{
		Term: 1, LeaderID: "leader", PrevLogIndex: 3, PrevLogTerm: 1,
		Entries:      []raft.LogEntry{{Term: 1, Command: encodeOrFatal(t, NewPutCommand("c0", 2, []byte("after"), []byte("value2")))}},
		LeaderCommit: 4,
	})
	if !appendReply.Success {
		t.Fatalf("HandleAppendEntries after install: %+v", appendReply)
	}
	if got := follower.CommitIndex(); got != 4 {
		t.Fatalf("follower CommitIndex = %d, want 4", got)
	}

	sm, cs := newApplierTarget(t)
	applier := NewApplier(follower, sm)

	applied, err := applier.ApplyAvailable()
	if err != nil {
		t.Fatalf("ApplyAvailable: %v", err)
	}
	// Only the one new entry (index 4) is "applied" in the ordinary sense;
	// the snapshot's own indexes (1..3) are recovered via restore, not
	// replay.
	if applied != 1 {
		t.Fatalf("ApplyAvailable applied = %d, want 1 (just the post-snapshot entry)", applied)
	}
	if got := follower.LastApplied(); got != 4 {
		t.Fatalf("LastApplied = %d, want 4", got)
	}
	if _, ok := follower.PendingSnapshot(); ok {
		t.Fatalf("PendingSnapshot still set after ApplyAvailable restored it")
	}

	v, err := cs.Get([]byte("seed"))
	if err != nil || string(v) != "value" {
		t.Fatalf("Get(seed) after restore = %q, %v, want value (recovered from the snapshot)", v, err)
	}
	v, err = cs.Get([]byte("after"))
	if err != nil || string(v) != "value2" {
		t.Fatalf("Get(after) after restore = %q, %v, want value2 (applied normally after the snapshot)", v, err)
	}
}

func TestApplier_ApplyAvailable_PendingSnapshotWithoutSnapshotterSupport_FailsClearly(t *testing.T) {
	follower, err := raft.NewNode(raft.Options{ID: "f", Peers: []string{"leader"}, Transport: raft.NewInMemoryTransport()})
	if err != nil {
		t.Fatalf("raft.NewNode: %v", err)
	}
	reply := follower.HandleInstallSnapshot(raft.InstallSnapshotArgs{Term: 1, LeaderID: "leader", LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("x")})
	if !reply.Success {
		t.Fatalf("HandleInstallSnapshot: %+v", reply)
	}

	applier := NewApplier(follower, noSnapshotStateMachine{})
	if _, err := applier.ApplyAvailable(); err == nil {
		t.Fatalf("ApplyAvailable with a pending snapshot against a non-Snapshotter state machine: want error, got nil")
	}
	if _, ok := follower.PendingSnapshot(); !ok {
		t.Fatalf("PendingSnapshot was cleared despite ApplyAvailable failing to restore it")
	}
}
