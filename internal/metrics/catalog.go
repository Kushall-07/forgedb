package metrics

import "time"

// This file is the catalog of every metric ForgeDB itself records into
// Default: one package-level handle per metric, registered at process
// startup (Go package init order), so every other package's
// instrumentation call sites are a single, direct call -- e.g.
// metrics.RaftElectionsStartedTotal.Inc() -- with no registry lookup and
// no risk of a typo'd metric name silently creating a second series.
//
// Every metric below exists because some package in this repository
// records into it from a real, reachable code path; see
// docs/observability/phase12-observability.md's metric catalog for what
// each one means and exactly when it changes. Metrics that would require
// inventing behavior ForgeDB does not actually have (e.g. SSTable/
// compaction counters -- see the doc's "known limitations" section, since
// internal/storage/compaction and internal/storage/sstable are not yet
// wired into the live MemStore write path) are deliberately omitted
// rather than fabricated.
const namespace = "forgedb_"

var (
	// Process / node.
	processStartTime = time.Now()

	// NodeInfo is set to 1 for this process's own node_id exactly once, at
	// node startup (see internal/dbnode.Open) -- a standard "info" metric
	// pattern: its value is never meaningful on its own, only its
	// presence and label identify which node a scrape came from.
	NodeInfo = Default.NewGaugeVec(namespace+"node_info", "Always 1; identifies this process by node_id.", "node_id")

	// ErrorsTotal is the single, bounded-cardinality aggregate failure
	// counter every component increments via metrics.RecordError --
	// deliberately one labeled counter instead of a dozen near-identical
	// per-component ones (see the phase doc's cardinality policy).
	ErrorsTotal = Default.NewCounterVec(namespace+"errors_total", "Total failures recorded, by component.", "component")

	// Raft: elections and leadership.
	RaftElectionsStartedTotal    = Default.NewCounter(namespace+"raft_elections_started_total", "Elections this node has started as a candidate.")
	RaftElectionsWonTotal        = Default.NewCounter(namespace+"raft_elections_won_total", "Elections this node has won, becoming leader.")
	RaftElectionsLostTotal       = Default.NewCounter(namespace+"raft_elections_lost_total", "Elections this node started as a candidate but did not win.")
	RaftLeaderChangesTotal       = Default.NewCounter(namespace+"raft_leader_changes_total", "Times this node observed its known leader change (including becoming leader itself).")
	RaftHigherTermStepsDownTotal = Default.NewCounter(namespace+"raft_higher_term_steps_down_total", "Times this node stepped down to Follower after observing a higher term.")

	// Raft: replication.
	RaftAppendEntriesSentTotal    = Default.NewCounter(namespace+"raft_append_entries_sent_total", "AppendEntries RPCs this node has sent as leader.")
	RaftAppendEntriesSuccessTotal = Default.NewCounter(namespace+"raft_append_entries_success_total", "AppendEntries RPCs that were acknowledged with Success=true.")
	RaftAppendEntriesFailedTotal  = Default.NewCounter(namespace+"raft_append_entries_failed_total", "AppendEntries RPCs that failed delivery or were rejected (Success=false).")
	RaftEntriesReplicatedTotal    = Default.NewCounter(namespace+"raft_entries_replicated_total", "Log entries newly confirmed as matched on a follower.")
	RaftEntriesCommittedTotal     = Default.NewCounter(namespace+"raft_entries_committed_total", "Log entries newly committed on this node (leader or follower).")

	// forgedb_raft_follower_lag_entries is deliberately not registered
	// here: it is a pull-based GaugeVecFunc wired up in
	// internal/dbnode.registerMetricsOnce instead, since follower lag is
	// read directly off (*raft.Node).PeerStatuses() at scrape time rather
	// than pushed incrementally -- registering it as a second, unlabeled
	// handle here would conflict with that registration.

	// Raft: snapshots.
	RaftSnapshotsCreatedTotal   = Default.NewCounter(namespace+"raft_snapshots_created_total", "Snapshots this node has created.")
	RaftSnapshotsInstalledTotal = Default.NewCounter(namespace+"raft_snapshots_installed_total", "Snapshots this node has installed from a leader.")
	RaftSnapshotFailuresTotal   = Default.NewCounter(namespace+"raft_snapshot_failures_total", "Snapshot create/install attempts that failed (e.g. a persistence error).")
	RaftSnapshotBytesTotal      = Default.NewCounter(namespace+"raft_snapshot_bytes_total", "Cumulative bytes across every snapshot this node has created or installed.")

	// Raft: linearizable reads (Phase 8.5).
	RaftReadIndexTotal        = Default.NewCounter(namespace+"raft_read_index_total", "ReadIndex calls attempted.")
	RaftReadIndexSuccessTotal = Default.NewCounter(namespace+"raft_read_index_success_total", "ReadIndex calls that established a confirmed read barrier.")
	RaftReadIndexFailureTotal = Default.NewCounter(namespace+"raft_read_index_failure_total", "ReadIndex calls that failed (not leader, or could not confirm quorum).")

	ConsistentGetTotal        = Default.NewCounter(namespace+"consistent_get_total", "ConsistentGet calls attempted.")
	ConsistentGetSuccessTotal = Default.NewCounter(namespace+"consistent_get_success_total", "ConsistentGet calls that completed (including a resolved key-not-found).")
	ConsistentGetFailureTotal = Default.NewCounter(namespace+"consistent_get_failure_total", "ConsistentGet calls that failed before a storage read (not leader, read barrier unavailable, or context cancelled).")
	ConsistentGetLatency      = Default.NewHistogram(namespace+"consistent_get_latency_seconds", "ConsistentGet call latency in seconds.", DefaultLatencyBuckets)

	// State machine.
	StateMachineCommandsTotal       = Default.NewCounter(namespace+"state_machine_commands_total", "Commands presented to the state machine via Apply.")
	StateMachineApplySuccessTotal   = Default.NewCounter(namespace+"state_machine_apply_success_total", "Apply calls that resolved (successfully or with a deterministic, resolved failure) without an unresolved storage fault.")
	StateMachineApplyFailureTotal   = Default.NewCounter(namespace+"state_machine_apply_failure_total", "Apply calls that returned an unresolved storage fault.")
	StateMachineDedupHitsTotal      = Default.NewCounter(namespace+"state_machine_dedup_hits_total", "Commands resolved as a replay of an already-applied (ClientID, RequestID).")
	StateMachineDedupConflictsTotal = Default.NewCounter(namespace+"state_machine_dedup_conflicts_total", "Commands rejected because a (ClientID, RequestID) was reused for a different command.")

	// Storage.
	StoragePutTotal         = Default.NewCounter(namespace+"storage_put_total", "Put calls against the storage engine.")
	StorageGetTotal         = Default.NewCounter(namespace+"storage_get_total", "Get calls against the storage engine.")
	StorageDeleteTotal      = Default.NewCounter(namespace+"storage_delete_total", "Delete calls against the storage engine.")
	StorageKeyNotFoundTotal = Default.NewCounter(namespace+"storage_key_not_found_total", "Get calls that found no live value for the requested key.")

	// WAL.
	WALAppendTotal           = Default.NewCounter(namespace+"wal_append_total", "Records appended to the WAL.")
	WALBytesWrittenTotal     = Default.NewCounter(namespace+"wal_bytes_written_total", "Bytes written to the WAL (encoded record size, including header).")
	WALSyncTotal             = Default.NewCounter(namespace+"wal_sync_total", "fsync calls completed against the WAL.")
	WALRecoveryTotal         = Default.NewCounter(namespace+"wal_recovery_total", "WAL replay (recovery) passes performed, one per store open.")
	WALRecoveryRecordsTotal  = Default.NewCounter(namespace+"wal_recovery_records_total", "Records replayed from the WAL across every recovery pass.")
	WALCorruptionErrorsTotal = Default.NewCounter(namespace+"wal_corruption_errors_total", "WAL replay passes that stopped early due to a checksum or header corruption.")

	// API (internal/api), labeled by route -- a small, fixed set of known
	// HTTP paths, never a raw/unbounded URL.
	APIRequestsTotal  = Default.NewCounterVec(namespace+"api_requests_total", "HTTP requests handled, by \"method route status_class\".", "key")
	APIRequestLatency = Default.NewHistogramVec(namespace+"api_request_latency_seconds", "HTTP request latency in seconds, by route.", "route", DefaultLatencyBuckets)
)

func init() {
	Default.NewGaugeFunc(namespace+"node_uptime_seconds", "Seconds since this process started.", func() float64 {
		return time.Since(processStartTime).Seconds()
	})
}
