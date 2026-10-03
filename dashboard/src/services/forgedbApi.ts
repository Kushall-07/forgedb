import { cluster, nodes, storage } from '../data/mockCluster';
import { events } from '../data/mockEvents';
import { executeKvRequest, getEntry, getOperations } from '../data/mockKV';
import { raftEvents, raftLog, raftNodes, raftSnapshot } from '../data/mockRaft';
import {
  activeIncidents,
  nodeHealth,
  observabilityEvents,
  raftTelemetry,
  storageTelemetry,
  systemMetrics,
} from '../data/mockObservability';
import type { ClusterState, NodeState, RaftEvent, StorageState } from '../types/cluster';
import type { KVEntry, KVExecutionResult, KVOperation, KVRequestInput } from '../types/kv';
import type {
  RaftEvent as RaftConsensusEvent,
  RaftLogEntry,
  RaftNodeState,
  RaftSnapshotState,
} from '../types/raft';
import type {
  NodeHealthRecord,
  ObservabilityEvent,
  ObservabilityIncident,
  RaftTelemetryMetrics,
  StorageTelemetryMetrics,
  SystemMetrics,
} from '../types/observability';
import type {
  BackendClusterStatus,
  BackendHealthResponse,
  LiveNodeSnapshot,
} from '../types/backend';

/**
 * The future ForgeDB HTTP API boundary. Components in this phase read mock
 * data directly from src/data/ so the Overview page renders synchronously
 * with no loading state; this module exists so that boundary already has a
 * shape to replace it with, once the backend exposes these endpoints:
 *
 *   GET /cluster       -> ClusterState
 *   GET /health        -> { status }
 *   GET /metrics       -> NodeState[] (per-node replication/raft metrics)
 *   PUT    /kv/{key}   -> KVExecutionResult
 *   GET    /kv/{key}   -> KVExecutionResult
 *   DELETE /kv/{key}   -> KVExecutionResult
 *   GET /raft/nodes    -> RaftNodeState[]
 *   GET /raft/log      -> RaftLogEntry[]
 *   GET /raft/events   -> RaftEvent[]
 *   GET /raft/snapshot -> RaftSnapshotState
 *   GET /observability/system    -> SystemMetrics
 *   GET /observability/raft      -> RaftTelemetryMetrics
 *   GET /observability/storage   -> StorageTelemetryMetrics
 *   GET /observability/nodes     -> NodeHealthRecord[]
 *   GET /observability/events    -> ObservabilityEvent[]
 *   GET /observability/incidents -> ObservabilityIncident[]
 *
 * No network calls are made for most of the methods below: swapping their
 * bodies for `fetch(...)` calls is the entire remaining migration, and no UI
 * component needs to change when that happens. The KV Console itself still
 * reads src/data/mockKV.ts directly, and the Raft page still reads
 * src/data/mockRaft.ts directly, the same way Cluster's topology/replication
 * views still read src/data/mockCluster.ts, so most of this interface
 * remains documentation of the target contract rather than code already on
 * the render path. The Observability page follows the same pattern, reading
 * src/data/mockObservability.ts directly rather than through this module.
 *
 * getLiveHealth/getLiveCluster/getLiveSnapshot are the one piece of this
 * module that IS real: they call the actual ForgeDB /health and /cluster
 * endpoints (through Vite's dev proxy at /api/*, see vite.config.ts, since
 * the Go backend has no CORS middleware) and map the response into this
 * module's types. They never fall back to mock data themselves -- they
 * reject/resolve with an explicit 'unavailable' result on any failure, so a
 * caller (see src/hooks/useBackendCluster.ts) decides whether and how to
 * fall back, rather than this layer silently turning a failure into a
 * healthy-looking result.
 */
export interface ForgeDbApi {
  getCluster(): Promise<ClusterState>;
  getNodes(): Promise<NodeState[]>;
  getStorage(): Promise<StorageState>;
  getEvents(): Promise<RaftEvent[]>;
  getHealth(): Promise<{ status: 'healthy' | 'degraded' | 'unhealthy' }>;
  executeKv(request: KVRequestInput): Promise<KVExecutionResult>;
  getKvEntry(key: string): Promise<KVEntry | null>;
  getKvOperations(): Promise<KVOperation[]>;
  getRaftNodes(): Promise<RaftNodeState[]>;
  getRaftLog(): Promise<RaftLogEntry[]>;
  getRaftEvents(): Promise<RaftConsensusEvent[]>;
  getRaftSnapshot(): Promise<RaftSnapshotState>;
  getSystemMetrics(): Promise<SystemMetrics>;
  getRaftMetrics(): Promise<RaftTelemetryMetrics>;
  getStorageMetrics(): Promise<StorageTelemetryMetrics>;
  getNodeHealth(): Promise<NodeHealthRecord[]>;
  getObservabilityEvents(): Promise<ObservabilityEvent[]>;
  getActiveIncidents(): Promise<ObservabilityIncident[]>;
  /** Real GET /health, mapped. Throws BackendRequestError on failure. */
  getLiveHealth(): Promise<{ status: string; nodeId: string }>;
  /** Real GET /cluster, mapped into this node's ClusterState + LiveNodeSnapshot view. Throws BackendRequestError on failure. */
  getLiveCluster(): Promise<{ cluster: ClusterState; node: LiveNodeSnapshot }>;
  /** Both of the above in parallel, collapsed into a tagged result instead of throwing -- the shape src/hooks/useBackendCluster.ts consumes directly. */
  getLiveSnapshot(): Promise<LiveSnapshot>;
}

/** Thrown by the live-backend methods: non-2xx responses, network failures, and malformed JSON all produce one of these with a human-readable message, never a silently "healthy" result. */
export class BackendRequestError extends Error {
  readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = 'BackendRequestError';
    this.cause = cause;
  }
}

const API_BASE_PATH = '/api';

async function requestJson<T>(path: string): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${API_BASE_PATH}${path}`, { headers: { Accept: 'application/json' } });
  } catch (err) {
    throw new BackendRequestError(
      `network error contacting ${path}: ${err instanceof Error ? err.message : String(err)}`,
      err,
    );
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new BackendRequestError(
      `backend returned ${res.status} ${res.statusText} for ${path}${body ? ` -- ${body}` : ''}`,
    );
  }
  try {
    return (await res.json()) as T;
  } catch (err) {
    throw new BackendRequestError(`malformed JSON response from ${path}`, err);
  }
}

function mapBackendHealth(raw: BackendHealthResponse): { status: string; nodeId: string } {
  return { status: raw.status, nodeId: raw.node_id };
}

/**
 * Maps GET /cluster's node-local dbnode.Status into this node's own
 * ClusterState view. leader resolves to raft.LeaderID when known, falling
 * back to this node's own id only when it has no leader on record yet (an
 * empty LeaderID, e.g. before the first election) -- never a guess at who
 * the actual leader is.
 */
function mapBackendClusterState(raw: BackendClusterStatus): ClusterState {
  const r = raw.raft;
  return {
    leader: r.LeaderID || r.NodeID,
    term: r.Term,
    commitIndex: r.CommitIndex,
    lastApplied: r.LastApplied,
    lastLogIndex: r.LastLogIndex,
    lastLogTerm: r.LastLogTerm,
    snapshotIndex: r.SnapshotIndex,
  };
}

function mapBackendLiveNode(raw: BackendClusterStatus): LiveNodeSnapshot {
  const r = raw.raft;
  return {
    nodeId: r.NodeID,
    role: r.Role,
    leaderId: r.LeaderID,
    term: r.Term,
    commitIndex: r.CommitIndex,
    lastApplied: r.LastApplied,
    lastLogIndex: r.LastLogIndex,
    lastLogTerm: r.LastLogTerm,
    snapshotIndex: r.SnapshotIndex,
    snapshotTerm: r.SnapshotTerm,
    memtableEntries: raw.storage.memtable_entries,
    memtableBytes: raw.storage.memtable_bytes,
    processUptimeSeconds: raw.process_uptime_seconds,
  };
}

export type LiveSnapshot =
  | {
      source: 'live';
      health: { status: string; nodeId: string };
      cluster: ClusterState;
      node: LiveNodeSnapshot;
    }
  | {
      source: 'unavailable';
      error: string;
    };

export const forgedbApi: ForgeDbApi = {
  async getCluster() {
    return cluster;
  },
  async getNodes() {
    return nodes;
  },
  async getStorage() {
    return storage;
  },
  async getEvents() {
    return events;
  },
  async getHealth() {
    return { status: 'healthy' };
  },
  async executeKv(request) {
    return executeKvRequest(request);
  },
  async getKvEntry(key) {
    return getEntry(key);
  },
  async getKvOperations() {
    return getOperations();
  },
  async getRaftNodes() {
    return raftNodes;
  },
  async getRaftLog() {
    return raftLog;
  },
  async getRaftEvents() {
    return raftEvents;
  },
  async getRaftSnapshot() {
    return raftSnapshot;
  },
  async getSystemMetrics() {
    return systemMetrics;
  },
  async getRaftMetrics() {
    return raftTelemetry;
  },
  async getStorageMetrics() {
    return storageTelemetry;
  },
  async getNodeHealth() {
    return nodeHealth;
  },
  async getObservabilityEvents() {
    return observabilityEvents;
  },
  async getActiveIncidents() {
    return activeIncidents;
  },
  async getLiveHealth() {
    return mapBackendHealth(await requestJson<BackendHealthResponse>('/health'));
  },
  async getLiveCluster() {
    const raw = await requestJson<BackendClusterStatus>('/cluster');
    return { cluster: mapBackendClusterState(raw), node: mapBackendLiveNode(raw) };
  },
  async getLiveSnapshot() {
    try {
      const [health, clusterRaw] = await Promise.all([
        requestJson<BackendHealthResponse>('/health'),
        requestJson<BackendClusterStatus>('/cluster'),
      ]);
      return {
        source: 'live',
        health: mapBackendHealth(health),
        cluster: mapBackendClusterState(clusterRaw),
        node: mapBackendLiveNode(clusterRaw),
      };
    } catch (err) {
      return {
        source: 'unavailable',
        error: err instanceof Error ? err.message : String(err),
      };
    }
  },
};
