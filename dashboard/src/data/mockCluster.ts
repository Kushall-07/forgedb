import type { ClusterState, NodeState, StorageState } from '../types/cluster';

/**
 * Mock ForgeDB cluster state, shaped exactly like the real Raft cluster will
 * report it (see src/services/forgedbApi.ts for the future GET /cluster
 * contract). Centralized here so the real API can replace this module
 * without any UI component changing.
 */
export const nodes: NodeState[] = [
  {
    id: 'NODE-1',
    role: 'follower',
    term: 5,
    commitIndex: 4,
    lastApplied: 4,
    lastLogIndex: 4,
    lastLogTerm: 4,
    matchIndex: 4,
    lag: 0,
    status: 'synced',
  },
  {
    id: 'NODE-2',
    role: 'leader',
    term: 5,
    commitIndex: 4,
    lastApplied: 4,
    lastLogIndex: 4,
    lastLogTerm: 4,
    matchIndex: 4,
    lag: 0,
    status: 'active',
  },
  {
    id: 'NODE-3',
    role: 'follower',
    term: 5,
    commitIndex: 4,
    lastApplied: 4,
    lastLogIndex: 4,
    lastLogTerm: 4,
    matchIndex: 4,
    lag: 0,
    status: 'synced',
  },
];

export const cluster: ClusterState = {
  leader: 'NODE-2',
  term: 5,
  commitIndex: 4,
  lastApplied: 4,
  lastLogIndex: 4,
  lastLogTerm: 4,
  snapshotIndex: 0,
};

export const storage: StorageState = {
  memtableEntries: 2,
  memtableBytes: 51,
  wal: 'active',
  sstables: 0,
  snapshotIndex: 0,
};
