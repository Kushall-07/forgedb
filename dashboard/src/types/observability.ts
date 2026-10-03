export type ObservabilityStatus = 'healthy' | 'warning' | 'critical';

export type ObservabilityTimeRange = 'AUTO' | '5M' | '15M' | '1H';

/** One sample in a time series, labelled with a relative-minute offset for axis ticks. */
export interface TimeSeriesPoint {
  t: string;
  value: number;
}

export interface RequestRatePoint {
  t: string;
  reads: number;
  writes: number;
}

export interface LatencyPoint {
  t: string;
  p50: number;
  p95: number;
  p99: number;
}

export interface SystemMetrics {
  requestsPerSec: number;
  readsPerSec: number;
  writesPerSec: number;
  latencyMsP50: number;
  latencyMsP95: number;
  latencyMsP99: number;
  errorRatePct: number;
  activeRequests: number;
  commitRatePerSec: number;
  applyRatePerSec: number;
}

export interface RaftTelemetryMetrics {
  currentTerm: number;
  leader: string;
  commitIndex: number;
  appliedIndex: number;
  logLength: number;
  replicationLagMinEntries: number;
  replicationLagMaxEntries: number;
  appendEntriesPerSec: number;
  requestVotePerSec: number;
  electionCount: number;
  heartbeatStatus: 'stable' | 'degraded';
  lastHeartbeatMs: number;
}

export interface StorageTelemetryMetrics {
  wal: {
    writeRateBytesPerSec: number;
    bytesWrittenTotal: number;
    replayStatus: 'idle' | 'replaying' | 'complete';
  };
  memtable: {
    activeSizeBytes: number;
    immutableCount: number;
  };
  sstable: {
    count: number;
    readRatePerSec: number;
    writeRatePerSec: number;
  };
  compaction: {
    pending: number;
    active: number;
    completedTotal: number;
  };
  bytesReadTotal: number;
  bytesWrittenTotal: number;
  tombstoneCount: number;
}

export type NodeTelemetryStatus = 'healthy' | 'warning' | 'critical';

export interface NodeHealthRecord {
  nodeId: string;
  role: 'LEADER' | 'FOLLOWER';
  status: NodeTelemetryStatus;
  uptime: string;
  cpuPct: number;
  memoryPct: number;
  requestsPerSec: number;
  p95LatencyMs: number;
  commitIndex: number;
  appliedIndex: number;
  replicationLagEntries: number;
  lastHeartbeat: string;
  storageState: 'healthy' | 'degraded';
}

export type ObservabilityEventSeverity = 'INFO' | 'WARN' | 'ERROR';

export type ObservabilityEventCategory =
  | 'ELECTION'
  | 'REPLICATION'
  | 'COMPACTION'
  | 'WAL'
  | 'NODE'
  | 'REQUEST';

export interface ObservabilityEvent {
  id: string;
  timestamp: string;
  severity: ObservabilityEventSeverity;
  category: ObservabilityEventCategory;
  source: string;
  message: string;
}

export type IncidentSeverity = 'WARN' | 'ERROR';

export type IncidentStatus = 'investigating' | 'monitoring' | 'resolved';

export interface ObservabilityIncident {
  id: string;
  severity: IncidentSeverity;
  title: string;
  source: string;
  description: string;
  timestamp: string;
  status: IncidentStatus;
}

export interface SystemStatusSignal {
  label: string;
  value: string;
  status: ObservabilityStatus;
}
