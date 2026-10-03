import { activeLabel, indexSamples, labeledValues, parsePrometheusText, scalarValue } from './parsePrometheusText';
import type { LiveMetricsSnapshot } from '../../types/liveMetrics';

/** sum/count -> a mean in milliseconds, or null when nothing has been observed yet. Never a percentile -- see types/liveMetrics.ts's doc comment on why that line is not crossed here. */
function meanMs(sumSeconds: number | undefined, count: number | undefined): number | null {
  if (!count || count <= 0 || sumSeconds === undefined) return null;
  return (sumSeconds / count) * 1000;
}

/**
 * Parses one GET /metrics response body into this dashboard's own
 * LiveMetricsSnapshot shape. Every metric name read here is verified
 * against internal/metrics/catalog.go and internal/dbnode/metrics.go --
 * a metric this function does not recognize is simply absent from the
 * result (scalarValue/labeledValues default to 0/{} for a metric that
 * is not in this scrape), never an error, since a future ForgeDB build
 * adding or removing a metric should not break this parse.
 */
export function buildLiveMetricsSnapshot(rawText: string): LiveMetricsSnapshot {
  const index = indexSamples(parsePrometheusText(rawText));
  const num = (name: string) => scalarValue(index, name) ?? 0;

  const nodeInfoSample = index.get('forgedb_node_info')?.[0];

  const apiLatencySum = labeledValues(index, 'forgedb_api_request_latency_seconds_sum', 'route');
  const apiLatencyCount = labeledValues(index, 'forgedb_api_request_latency_seconds_count', 'route');
  const avgLatencyMsByRoute: Record<string, number | null> = {};
  for (const route of Object.keys(apiLatencySum)) {
    avgLatencyMsByRoute[route] = meanMs(apiLatencySum[route], apiLatencyCount[route]);
  }

  return {
    raft: {
      term: num('forgedb_raft_term'),
      role: activeLabel(index, 'forgedb_raft_role', 'role') ?? 'unknown',
      commitIndex: num('forgedb_raft_commit_index'),
      lastApplied: num('forgedb_raft_last_applied'),
      logEntries: num('forgedb_raft_log_entries'),
      snapshotIndex: num('forgedb_raft_snapshot_index'),
      followerLagEntries: labeledValues(index, 'forgedb_raft_follower_lag_entries', 'peer_id'),
      electionsStarted: num('forgedb_raft_elections_started_total'),
      electionsWon: num('forgedb_raft_elections_won_total'),
      electionsLost: num('forgedb_raft_elections_lost_total'),
      leaderChanges: num('forgedb_raft_leader_changes_total'),
      appendEntriesSent: num('forgedb_raft_append_entries_sent_total'),
      appendEntriesSuccess: num('forgedb_raft_append_entries_success_total'),
      appendEntriesFailed: num('forgedb_raft_append_entries_failed_total'),
      entriesReplicated: num('forgedb_raft_entries_replicated_total'),
      entriesCommitted: num('forgedb_raft_entries_committed_total'),
      snapshotsCreated: num('forgedb_raft_snapshots_created_total'),
      snapshotsInstalled: num('forgedb_raft_snapshots_installed_total'),
      snapshotFailures: num('forgedb_raft_snapshot_failures_total'),
      readIndexTotal: num('forgedb_raft_read_index_total'),
      readIndexSuccess: num('forgedb_raft_read_index_success_total'),
      readIndexFailure: num('forgedb_raft_read_index_failure_total'),
      consistentGetTotal: num('forgedb_consistent_get_total'),
      consistentGetSuccess: num('forgedb_consistent_get_success_total'),
      consistentGetFailure: num('forgedb_consistent_get_failure_total'),
      consistentGetAvgLatencyMs: meanMs(
        scalarValue(index, 'forgedb_consistent_get_latency_seconds_sum'),
        scalarValue(index, 'forgedb_consistent_get_latency_seconds_count'),
      ),
    },
    storage: {
      memtableEntries: num('forgedb_storage_memtable_entries'),
      memtableBytes: num('forgedb_storage_memtable_bytes'),
      putTotal: num('forgedb_storage_put_total'),
      getTotal: num('forgedb_storage_get_total'),
      deleteTotal: num('forgedb_storage_delete_total'),
      keyNotFoundTotal: num('forgedb_storage_key_not_found_total'),
      walAppendTotal: num('forgedb_wal_append_total'),
      walBytesWrittenTotal: num('forgedb_wal_bytes_written_total'),
      walSyncTotal: num('forgedb_wal_sync_total'),
      walRecoveryTotal: num('forgedb_wal_recovery_total'),
      walRecoveryRecordsTotal: num('forgedb_wal_recovery_records_total'),
      walCorruptionErrorsTotal: num('forgedb_wal_corruption_errors_total'),
    },
    api: {
      requestsByKey: labeledValues(index, 'forgedb_api_requests_total', 'key'),
      avgLatencyMsByRoute,
    },
    system: {
      nodeId: nodeInfoSample?.labels['node_id'] ?? '',
      uptimeSeconds: num('forgedb_node_uptime_seconds'),
      errorsByComponent: labeledValues(index, 'forgedb_errors_total', 'component'),
      stateMachineCommands: num('forgedb_state_machine_commands_total'),
      stateMachineApplySuccess: num('forgedb_state_machine_apply_success_total'),
      stateMachineApplyFailure: num('forgedb_state_machine_apply_failure_total'),
      stateMachineDedupHits: num('forgedb_state_machine_dedup_hits_total'),
      stateMachineDedupConflicts: num('forgedb_state_machine_dedup_conflicts_total'),
    },
  };
}
