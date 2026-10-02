package logging

// Event names every ForgeDB package logs under, passed as the first
// (msg) argument to a slog call -- e.g.
// logging.Default.Info(logging.EventRaftLeaderChanged, "node_id", id, "term", term).
// A fixed, closed catalog of event names (rather than ad hoc strings at
// each call site) is what makes ForgeDB's logs greppable and keeps
// every occurrence of the same event spelled identically. See
// docs/observability/phase12-observability.md's event catalog for what
// each one means and which fields it carries.
const (
	EventRaftElectionStarted = "raft.election.started"
	EventRaftElectionWon     = "raft.election.won"
	EventRaftLeaderChanged   = "raft.leader.changed"
	EventRaftStepDown        = "raft.step_down"

	EventRaftAppendSuccess = "raft.append.success"
	EventRaftAppendFailure = "raft.append.failure"
	EventRaftCommitAdvance = "raft.commit.advance"

	EventRaftSnapshotCreated   = "raft.snapshot.created"
	EventRaftSnapshotInstalled = "raft.snapshot.installed"
	EventRaftSnapshotFailed    = "raft.snapshot.failed"

	EventStateMachineApply         = "state_machine.apply"
	EventStateMachineDedupHit      = "state_machine.dedup_hit"
	EventStateMachineApplyError    = "state_machine.apply_error"
	EventStateMachineDedupConflict = "state_machine.dedup_conflict"

	EventWALAppend   = "wal.append"
	EventWALSync     = "wal.sync"
	EventWALRecovery = "wal.recovery"

	EventStoragePut    = "storage.put"
	EventStorageDelete = "storage.delete"

	EventNodeStarted   = "node.started"
	EventNodeStopped   = "node.stopped"
	EventNodeRecovered = "node.recovered"

	EventAPIRequest = "api.request"
)
