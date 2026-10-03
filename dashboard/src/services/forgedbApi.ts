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
  BackendKvWriteResponse,
  BackendNotLeaderResponse,
  BackendReadUnavailableResponse,
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
 * component needs to change when that happens. The Raft page still reads
 * src/data/mockRaft.ts directly, the same way Cluster's mock topology/
 * replication views still read src/data/mockCluster.ts, so most of this
 * interface remains documentation of the target contract rather than code
 * already on the render path. The Observability page follows the same
 * pattern, reading src/data/mockObservability.ts directly rather than
 * through this module (Phase D).
 *
 * getLiveHealth/getLiveCluster/getLiveSnapshot/getLiveKv/putLiveKv/
 * deleteLiveKv are real (Phase B and Phase C respectively): they call the
 * actual ForgeDB /health, /cluster, and /kv/{key} endpoints (through Vite's
 * dev proxy at /api/*, see vite.config.ts, since the Go backend has no CORS
 * middleware) and map the response into this module's types. The health/
 * cluster methods never fall back to mock data themselves -- they
 * reject/resolve with an explicit 'unavailable' result on any failure, so a
 * caller (see src/hooks/useBackendCluster.ts) decides whether and how to
 * fall back, rather than this layer silently turning a failure into a
 * healthy-looking result. getLiveKv/putLiveKv/deleteLiveKv follow the same
 * rule but go one step further: a 404/421/503/etc. is a legitimate backend
 * answer, not a failure, so those resolve to a tagged result too, and only
 * an actual network error throws BackendRequestError -- see
 * src/hooks/useKvConsole.ts, which is the KV Console's equivalent of
 * useBackendCluster.ts and decides live-vs-mock per request.
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
  /**
   * Real GET /kv/{key}. Resolves to a tagged LiveKvGetResult for every
   * outcome the backend itself can produce (found, not found, not leader,
   * read unavailable, or another non-2xx) -- those are all legitimate
   * protocol answers, not failures. Only throws BackendRequestError when
   * the request never reached the backend at all (network error).
   */
  getLiveKv(key: string): Promise<LiveKvGetResult>;
  /** Real PUT /kv/{key} with value sent verbatim as the request body. See getLiveKv for the success-vs-failure split. */
  putLiveKv(key: string, value: string): Promise<LiveKvWriteResult>;
  /** Real DELETE /kv/{key}. See getLiveKv for the success-vs-failure split. */
  deleteLiveKv(key: string): Promise<LiveKvWriteResult>;
  /** Real GET /metrics, returned as the raw Prometheus text exposition body (see internal/api/server.go's handleMetrics). Parsing it is lib/metrics/buildLiveMetricsSnapshot.ts's job, not this layer's. Throws BackendRequestError on failure. */
  getLiveMetricsText(): Promise<string>;
}

/**
 * Tagged outcome of a real GET /kv/{key} call -- see internal/api/kv.go's
 * handleKVGet for the exact status codes this mirrors. 'bytes' is the raw
 * response body, only present on 'found'; decoding it (UTF-8 vs binary)
 * is left to the caller (see hooks/useKvConsole.ts) since this layer must
 * not decide how a binary value gets displayed.
 */
export type LiveKvGetResult =
  | { kind: 'found'; status: 200; bytes: Uint8Array; clientLatencyMs: number }
  | { kind: 'not_found'; status: 404; clientLatencyMs: number }
  | { kind: 'not_leader'; status: 421; leaderId?: string; leaderHttp?: string; clientLatencyMs: number }
  | { kind: 'read_unavailable'; status: 503; reason: string; clientLatencyMs: number }
  | { kind: 'error'; status: number; statusText: string; detail: string; clientLatencyMs: number };

/** Tagged outcome of a real PUT or DELETE /kv/{key} call. */
export type LiveKvWriteResult =
  | { kind: 'ok'; status: 200; backendStatus: string; clientLatencyMs: number }
  | { kind: 'not_leader'; status: 421; leaderId?: string; leaderHttp?: string; clientLatencyMs: number }
  | { kind: 'error'; status: number; statusText: string; detail: string; clientLatencyMs: number };

/** Thrown by the live-backend methods: non-2xx responses, network failures, and malformed JSON all produce one of these with a human-readable message, never a silently "healthy" result. */
export class BackendRequestError extends Error {
  readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = 'BackendRequestError';
    this.cause = cause;
  }
}

/**
 * Local dev keeps the relative '/api' path, which Vite's dev proxy
 * forwards to FORGEDB_HTTP_ADDR server-side (see vite.config.ts). A
 * deployed build (e.g. on Vercel) has no such proxy, so it instead
 * needs an absolute URL to the public ForgeDB endpoint -- set
 * VITE_FORGEDB_API_BASE_URL at build time (a Vercel project
 * environment variable, never a committed file) to the stable HTTPS
 * hostname Caddy terminates TLS for (see deploy/README.md's HTTPS
 * section). This is exactly the cross-origin case internal/api/cors.go
 * exists for: FORGEDB_CORS_ORIGINS on the backend must list this
 * dashboard's own deployed origin.
 */
const API_BASE_PATH: string = import.meta.env.VITE_FORGEDB_API_BASE_URL || '/api';

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

function kvPath(key: string): string {
  return `/kv/${encodeURIComponent(key)}`;
}

/**
 * 502 Bad Gateway is never a response the ForgeDB handler itself can
 * produce (see internal/api/kv.go -- every status it writes is 200, 400,
 * 404, 413, 421, 500, 503, or 504); it is Vite's dev proxy's own signal
 * that it could not reach the upstream at all (connection refused), the
 * same condition as a direct network failure. Treating it as a generic
 * 'error' kind would show it as a live-but-broken response instead of
 * triggering the mock fallback a genuinely unreachable backend should --
 * so this is promoted to a thrown BackendRequestError right alongside an
 * actual fetch() failure, before either KV helper inspects the status.
 */
function assertProxyReachedBackend(res: Response, path: string): void {
  if (res.status === 502) {
    throw new BackendRequestError(`backend unreachable: the dev proxy could not reach ForgeDB for ${path}`);
  }
}

/** HTTP status -> this UI's own short status text, since several of these paths (404/413/500/504) come back as plain text, not JSON, with no statusText worth reusing. */
function kvStatusText(status: number, fallback: string): string {
  switch (status) {
    case 404:
      return 'NOT FOUND';
    case 413:
      return 'PAYLOAD TOO LARGE';
    case 504:
      return 'TIMEOUT';
    case 500:
      return 'INTERNAL ERROR';
    default:
      return fallback || `HTTP ${status}`;
  }
}

async function liveKvGet(key: string): Promise<LiveKvGetResult> {
  const start = performance.now();
  let res: Response;
  try {
    res = await fetch(`${API_BASE_PATH}${kvPath(key)}`, { method: 'GET' });
  } catch (err) {
    throw new BackendRequestError(
      `network error contacting GET ${kvPath(key)}: ${err instanceof Error ? err.message : String(err)}`,
      err,
    );
  }
  assertProxyReachedBackend(res, kvPath(key));
  const clientLatencyMs = performance.now() - start;

  if (res.status === 200) {
    const bytes = new Uint8Array(await res.arrayBuffer());
    return { kind: 'found', status: 200, bytes, clientLatencyMs };
  }
  if (res.status === 404) {
    return { kind: 'not_found', status: 404, clientLatencyMs };
  }
  if (res.status === 421) {
    const body = (await res.json().catch(() => null)) as BackendNotLeaderResponse | null;
    return { kind: 'not_leader', status: 421, leaderId: body?.leader_id, leaderHttp: body?.leader_http, clientLatencyMs };
  }
  if (res.status === 503) {
    const body = (await res.json().catch(() => null)) as BackendReadUnavailableResponse | null;
    return { kind: 'read_unavailable', status: 503, reason: body?.reason ?? body?.error ?? 'read unavailable', clientLatencyMs };
  }
  const detail = await res.text().catch(() => '');
  return { kind: 'error', status: res.status, statusText: kvStatusText(res.status, res.statusText), detail, clientLatencyMs };
}

async function liveKvWrite(key: string, method: 'PUT' | 'DELETE', body?: string): Promise<LiveKvWriteResult> {
  const start = performance.now();
  let res: Response;
  try {
    res = await fetch(`${API_BASE_PATH}${kvPath(key)}`, {
      method,
      headers: body !== undefined ? { 'Content-Type': 'application/octet-stream' } : undefined,
      body,
    });
  } catch (err) {
    throw new BackendRequestError(
      `network error contacting ${method} ${kvPath(key)}: ${err instanceof Error ? err.message : String(err)}`,
      err,
    );
  }
  assertProxyReachedBackend(res, kvPath(key));
  const clientLatencyMs = performance.now() - start;

  if (res.status === 200) {
    const parsed = (await res.json().catch(() => null)) as BackendKvWriteResponse | null;
    return { kind: 'ok', status: 200, backendStatus: parsed?.status ?? 'ok', clientLatencyMs };
  }
  if (res.status === 421) {
    const body2 = (await res.json().catch(() => null)) as BackendNotLeaderResponse | null;
    return { kind: 'not_leader', status: 421, leaderId: body2?.leader_id, leaderHttp: body2?.leader_http, clientLatencyMs };
  }
  const detail = await res.text().catch(() => '');
  return { kind: 'error', status: res.status, statusText: kvStatusText(res.status, res.statusText), detail, clientLatencyMs };
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
  async getLiveKv(key) {
    return liveKvGet(key);
  },
  async putLiveKv(key, value) {
    return liveKvWrite(key, 'PUT', value);
  },
  async deleteLiveKv(key) {
    return liveKvWrite(key, 'DELETE');
  },
  async getLiveMetricsText() {
    let res: Response;
    try {
      res = await fetch(`${API_BASE_PATH}/metrics`);
    } catch (err) {
      throw new BackendRequestError(
        `network error contacting /metrics: ${err instanceof Error ? err.message : String(err)}`,
        err,
      );
    }
    if (!res.ok) {
      throw new BackendRequestError(`backend returned ${res.status} ${res.statusText} for /metrics`);
    }
    return res.text();
  },
};
