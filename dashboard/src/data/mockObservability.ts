import { cluster, nodes, storage } from './mockCluster';
import type {
  LatencyPoint,
  NodeHealthRecord,
  ObservabilityEvent,
  ObservabilityIncident,
  ObservabilityTimeRange,
  RaftTelemetryMetrics,
  RequestRatePoint,
  StorageTelemetryMetrics,
  SystemMetrics,
  SystemStatusSignal,
} from '../types/observability';

/**
 * Deterministic observability mock data, derived from the same mockCluster
 * state the rest of the dashboard reads, so the numbers shown here never
 * contradict Overview/Cluster/Raft (same leader, same term, same commit
 * index). A seeded PRNG (not Math.random/Date.now) drives the time-series
 * shape so every render and every build produces identical output.
 *
 * This is dashboard mock data only. See ObservabilityHeader / DataProvenance
 * for the on-page disclosure: no backend metrics endpoint exists yet.
 */

function mulberry32(seed: number) {
  let s = seed;
  return function random() {
    s |= 0;
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const POINT_COUNT = 60;

/** How many of the last N one-minute samples a time-range selection shows. */
export function pointsForRange(range: ObservabilityTimeRange): number {
  switch (range) {
    case '5M':
      return 5;
    case '15M':
      return 15;
    case '1H':
      return POINT_COUNT;
    case 'AUTO':
    default:
      return 30;
  }
}

/** `-59m .. -1m, now` labels for a 60-minute window sampled once per minute. */
function minuteLabels(count: number): string[] {
  return Array.from({ length: count }, (_, i) => {
    const minutesAgo = count - 1 - i;
    return minutesAgo === 0 ? 'now' : `-${minutesAgo}m`;
  });
}

const LABELS = minuteLabels(POINT_COUNT);

function buildSeries(seed: number, base: number, amplitude: number, noise: number): number[] {
  const rand = mulberry32(seed);
  const values: number[] = [];
  for (let i = 0; i < POINT_COUNT; i += 1) {
    const wave = Math.sin((i / POINT_COUNT) * Math.PI * 2.4) * amplitude;
    const jitter = (rand() - 0.5) * 2 * noise;
    values.push(Math.max(0, base + wave + jitter));
  }
  return values;
}

const readSeries = buildSeries(1001, 860, 90, 40);
const writeSeries = buildSeries(2002, 380, 55, 28);
const p50Series = buildSeries(3003, 6, 1.4, 0.8);
const p95Series = buildSeries(4004, 18, 3.5, 2.1);
const p99Series = buildSeries(5005, 34, 6, 3.4);

export const requestRateSeries: RequestRatePoint[] = LABELS.map((t, i) => ({
  t,
  reads: Math.round(readSeries[i]),
  writes: Math.round(writeSeries[i]),
}));

export const latencySeries: LatencyPoint[] = LABELS.map((t, i) => ({
  t,
  p50: Math.round(p50Series[i] * 10) / 10,
  p95: Math.round(p95Series[i] * 10) / 10,
  p99: Math.round(p99Series[i] * 10) / 10,
}));

const latestReads = requestRateSeries[requestRateSeries.length - 1].reads;
const latestWrites = requestRateSeries[requestRateSeries.length - 1].writes;
const latestLatency = latencySeries[latencySeries.length - 1];

export const systemMetrics: SystemMetrics = {
  requestsPerSec: latestReads + latestWrites,
  readsPerSec: latestReads,
  writesPerSec: latestWrites,
  latencyMsP50: latestLatency.p50,
  latencyMsP95: latestLatency.p95,
  latencyMsP99: latestLatency.p99,
  errorRatePct: 0.12,
  activeRequests: 14,
  commitRatePerSec: Math.round((latestReads + latestWrites) * 0.98),
  applyRatePerSec: Math.round((latestReads + latestWrites) * 0.97),
};

export const raftTelemetry: RaftTelemetryMetrics = {
  currentTerm: cluster.term,
  leader: cluster.leader,
  commitIndex: cluster.commitIndex,
  appliedIndex: cluster.lastApplied,
  logLength: cluster.lastLogIndex,
  replicationLagMinEntries: 0,
  replicationLagMaxEntries: 2,
  appendEntriesPerSec: 24,
  requestVotePerSec: 0,
  electionCount: 1,
  heartbeatStatus: 'stable',
  lastHeartbeatMs: 180,
};

export const storageTelemetry: StorageTelemetryMetrics = {
  wal: {
    writeRateBytesPerSec: 18_400,
    bytesWrittenTotal: 2_184_300,
    replayStatus: 'idle',
  },
  memtable: {
    activeSizeBytes: storage.memtableBytes * 1_000_000,
    immutableCount: 1,
  },
  sstable: {
    count: 18,
    readRatePerSec: 142,
    writeRatePerSec: 6,
  },
  compaction: {
    pending: 2,
    active: 0,
    completedTotal: 47,
  },
  bytesReadTotal: 1_204_582_200,
  bytesWrittenTotal: 318_442_800,
  tombstoneCount: 213,
};

const UPTIME_BY_ROLE: Record<string, string> = {
  'NODE-1': '14d 06h',
  'NODE-2': '14d 06h',
  'NODE-3': '11d 19h',
};

const CPU_BY_NODE: Record<string, number> = { 'NODE-1': 22, 'NODE-2': 38, 'NODE-3': 27 };
const MEM_BY_NODE: Record<string, number> = { 'NODE-1': 41, 'NODE-2': 52, 'NODE-3': 46 };
const RPS_BY_NODE: Record<string, number> = { 'NODE-1': 410, 'NODE-2': 1240, 'NODE-3': 388 };
const P95_BY_NODE: Record<string, number> = { 'NODE-1': 16, 'NODE-2': 18, 'NODE-3': 21 };
const LAG_BY_NODE: Record<string, number> = { 'NODE-1': 0, 'NODE-2': 0, 'NODE-3': 2 };
const HEARTBEAT_BY_NODE: Record<string, string> = {
  'NODE-1': '180ms ago',
  'NODE-2': '—',
  'NODE-3': '240ms ago',
};

export const nodeHealth: NodeHealthRecord[] = nodes.map((n) => {
  const isLeader = n.role === 'leader';
  const lag = LAG_BY_NODE[n.id] ?? 0;
  const status: NodeHealthRecord['status'] = lag > 1 ? 'warning' : 'healthy';
  return {
    nodeId: n.id,
    role: isLeader ? 'LEADER' : 'FOLLOWER',
    status,
    uptime: UPTIME_BY_ROLE[n.id] ?? '—',
    cpuPct: CPU_BY_NODE[n.id] ?? 25,
    memoryPct: MEM_BY_NODE[n.id] ?? 40,
    requestsPerSec: RPS_BY_NODE[n.id] ?? 0,
    p95LatencyMs: P95_BY_NODE[n.id] ?? 18,
    commitIndex: n.commitIndex,
    appliedIndex: n.lastApplied,
    replicationLagEntries: lag,
    lastHeartbeat: HEARTBEAT_BY_NODE[n.id] ?? '—',
    storageState: 'healthy',
  };
});

/** Newest first. */
export const observabilityEvents: ObservabilityEvent[] = [
  {
    id: 'obs-evt-12',
    timestamp: '09:21:14',
    severity: 'INFO',
    category: 'COMPACTION',
    source: 'NODE-1',
    message: 'Compaction completed: 4 SSTables merged, 1 tombstone reclaimed',
  },
  {
    id: 'obs-evt-11',
    timestamp: '09:20:02',
    severity: 'WARN',
    category: 'REPLICATION',
    source: 'NODE-3',
    message: 'Replication lag increased to 2 entries behind leader',
  },
  {
    id: 'obs-evt-10',
    timestamp: '09:19:47',
    severity: 'INFO',
    category: 'REQUEST',
    source: 'cluster',
    message: 'Request rate stable at ~1.2k req/s over the last 5 minutes',
  },
  {
    id: 'obs-evt-9',
    timestamp: '09:19:03',
    severity: 'INFO',
    category: 'COMPACTION',
    source: 'NODE-1',
    message: 'Compaction started: level 0 backlog at 2 pending files',
  },
  {
    id: 'obs-evt-8',
    timestamp: '09:18:41',
    severity: 'INFO',
    category: 'NODE',
    source: 'NODE-3',
    message: 'Follower caught up to leader commit index 4',
  },
  {
    id: 'obs-evt-7',
    timestamp: '09:18:03',
    severity: 'INFO',
    category: 'REPLICATION',
    source: 'cluster',
    message: 'Heartbeat acknowledged by all followers',
  },
  {
    id: 'obs-evt-6',
    timestamp: '09:17:58',
    severity: 'INFO',
    category: 'REPLICATION',
    source: 'NODE-2',
    message: 'Commit index advanced 3 → 4',
  },
  {
    id: 'obs-evt-5',
    timestamp: '09:17:51',
    severity: 'INFO',
    category: 'ELECTION',
    source: 'NODE-2',
    message: 'Leader elected for term 5',
  },
  {
    id: 'obs-evt-4',
    timestamp: '09:12:30',
    severity: 'WARN',
    category: 'WAL',
    source: 'NODE-3',
    message: 'WAL write latency briefly exceeded 40ms during restart',
  },
  {
    id: 'obs-evt-3',
    timestamp: '09:11:58',
    severity: 'INFO',
    category: 'WAL',
    source: 'NODE-3',
    message: 'WAL recovery complete, 212 entries replayed',
  },
  {
    id: 'obs-evt-2',
    timestamp: '09:11:40',
    severity: 'INFO',
    category: 'NODE',
    source: 'NODE-3',
    message: 'Node restarted and rejoined cluster',
  },
  {
    id: 'obs-evt-1',
    timestamp: '09:02:12',
    severity: 'ERROR',
    category: 'REQUEST',
    source: 'NODE-3',
    message: 'KV write rejected: temporarily not leader, redirected to NODE-2',
  },
];

export const activeIncidents: ObservabilityIncident[] = [
  {
    id: 'inc-1',
    severity: 'WARN',
    title: 'Replication delay detected',
    source: 'NODE-3',
    description: 'Follower NODE-3 is up to 2 log entries behind the leader. Within normal bounds, being monitored.',
    timestamp: '09:20:02',
    status: 'monitoring',
  },
  {
    id: 'inc-2',
    severity: 'WARN',
    title: 'Compaction backlog elevated',
    source: 'NODE-1',
    description: 'Level 0 has 2 pending SSTables awaiting compaction, slightly above the steady-state baseline of 0–1.',
    timestamp: '09:19:03',
    status: 'investigating',
  },
];

export const systemStatusSignals: SystemStatusSignal[] = [
  { label: 'Cluster', value: `${nodes.length} / ${nodes.length} nodes`, status: 'healthy' },
  { label: 'Leader', value: cluster.leader, status: 'healthy' },
  { label: 'Raft', value: 'Stable', status: 'healthy' },
  { label: 'Replication', value: 'Synchronized', status: 'healthy' },
  { label: 'Storage', value: 'Healthy', status: 'healthy' },
  { label: 'Requests', value: 'Nominal', status: 'healthy' },
];
