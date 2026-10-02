import { cluster, nodes } from './mockCluster';
import { fullySyncedCount } from './clusterSelectors';
import { raftNodes } from './mockRaft';
import type { RaftNodeState } from '../types/raft';

/**
 * Derived reads over the Raft mock state, mirroring clusterSelectors.ts so
 * every component computes "the leader" or "the majority size" the same way.
 */
export function getRaftLeader(): RaftNodeState | undefined {
  return raftNodes.find((n) => n.role === 'LEADER');
}

export function getRaftFollowers(): RaftNodeState[] {
  return raftNodes.filter((n) => n.role === 'FOLLOWER');
}

export function getRaftNode(nodeId: string): RaftNodeState | undefined {
  return raftNodes.find((n) => n.nodeId === nodeId);
}

export const majorityCount = Math.floor(nodes.length / 2) + 1;

/** The consensus-level facts shown across the Raft page header, summary, and invariants. */
export const raftConsensus = {
  cluster: 'forgedb-cluster',
  leader: cluster.leader,
  term: cluster.term,
  commitIndex: cluster.commitIndex,
  lastApplied: cluster.lastApplied,
  logLength: cluster.lastLogIndex,
  totalNodes: nodes.length,
  majorityCount,
  electionState: 'STABLE' as const,
  replicationCount: fullySyncedCount(),
};
