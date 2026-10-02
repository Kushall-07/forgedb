import { cluster, nodes, storage } from '../data/mockCluster';
import { events } from '../data/mockEvents';
import { executeKvRequest, getEntry, getOperations } from '../data/mockKV';
import { raftEvents, raftLog, raftNodes, raftSnapshot } from '../data/mockRaft';
import type { ClusterState, NodeState, RaftEvent, StorageState } from '../types/cluster';
import type { KVEntry, KVExecutionResult, KVOperation, KVRequestInput } from '../types/kv';
import type {
  RaftEvent as RaftConsensusEvent,
  RaftLogEntry,
  RaftNodeState,
  RaftSnapshotState,
} from '../types/raft';

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
 *
 * No network calls are made here. Swapping the bodies below for `fetch(...)`
 * calls is the entire migration; no UI component needs to change. The KV
 * Console itself still reads src/data/mockKV.ts directly, and the Raft page
 * still reads src/data/mockRaft.ts directly, the same way Overview and
 * Cluster read src/data/mockCluster.ts, so this interface is documentation of
 * the target contract rather than code already on the render path.
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
};
