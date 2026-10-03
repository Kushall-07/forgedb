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
 * No network calls are made here. Swapping the bodies below for `fetch(...)`
 * calls is the entire migration; no UI component needs to change. The KV
 * Console itself still reads src/data/mockKV.ts directly, and the Raft page
 * still reads src/data/mockRaft.ts directly, the same way Overview and
 * Cluster read src/data/mockCluster.ts, so this interface is documentation of
 * the target contract rather than code already on the render path. The
 * Observability page follows the same pattern, reading
 * src/data/mockObservability.ts directly rather than through this module.
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
}

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
};
