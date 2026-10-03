/**
 * The dashboard's own normalized view of GET /metrics, built by
 * lib/metrics/buildLiveMetricsSnapshot.ts from the raw Prometheus text.
 * Every field here maps to exactly one metric in internal/metrics/catalog.go
 * or internal/dbnode/metrics.go -- nothing here is invented, and nothing
 * ForgeDB does not actually expose (SSTable/compaction counters, CPU/
 * memory, request-latency percentiles, historical samples) has a field,
 * so there is no way for a caller to accidentally read a fabricated value
 * out of this type.
 */

export interface LiveRaftMetrics {
  term: number;
  /** From forgedb_raft_role's active label (Leader/Follower/Candidate), not guessed from any other state. */
  role: string;
  commitIndex: number;
  lastApplied: number;
  logEntries: number;
  snapshotIndex: number;
  /** peer_id -> entries behind leader. Empty for a single-node deployment, which reports no peers. */
  followerLagEntries: Record<string, number>;
  electionsStarted: number;
  electionsWon: number;
  electionsLost: number;
  leaderChanges: number;
  appendEntriesSent: number;
  appendEntriesSuccess: number;
  appendEntriesFailed: number;
  entriesReplicated: number;
  entriesCommitted: number;
  snapshotsCreated: number;
  snapshotsInstalled: number;
  snapshotFailures: number;
  readIndexTotal: number;
  readIndexSuccess: number;
  readIndexFailure: number;
  consistentGetTotal: number;
  consistentGetSuccess: number;
  consistentGetFailure: number;
  /** DERIVED (sum/count over the histogram's cumulative total) -- a mean, never a percentile; null when count is 0. */
  consistentGetAvgLatencyMs: number | null;
}

export interface LiveStorageMetrics {
  memtableEntries: number;
  memtableBytes: number;
  putTotal: number;
  getTotal: number;
  deleteTotal: number;
  keyNotFoundTotal: number;
  walAppendTotal: number;
  walBytesWrittenTotal: number;
  walSyncTotal: number;
  walRecoveryTotal: number;
  walRecoveryRecordsTotal: number;
  walCorruptionErrorsTotal: number;
}

export interface LiveApiMetrics {
  /** "METHOD route status_class" -> request count, exactly as forgedb_api_requests_total labels it. */
  requestsByKey: Record<string, number>;
  /** route -> DERIVED mean latency in ms (sum/count), or null where count is 0. */
  avgLatencyMsByRoute: Record<string, number | null>;
}

export interface LiveSystemMetrics {
  nodeId: string;
  uptimeSeconds: number;
  errorsByComponent: Record<string, number>;
  stateMachineCommands: number;
  stateMachineApplySuccess: number;
  stateMachineApplyFailure: number;
  stateMachineDedupHits: number;
  stateMachineDedupConflicts: number;
}

export interface LiveMetricsSnapshot {
  raft: LiveRaftMetrics;
  storage: LiveStorageMetrics;
  api: LiveApiMetrics;
  system: LiveSystemMetrics;
}

/**
 * Counter-delta rates computed across two successive polls of the same
 * backend -- never fabricated, and never shown until a second sample
 * actually exists (see hooks/useObservabilityMetrics.ts). All null until
 * then, which the UI renders as "COLLECTING" rather than a fake 0.
 */
export interface DerivedRateMetrics {
  requestsPerSec: number | null;
  errorsPerSec: number | null;
  windowSeconds: number | null;
}
