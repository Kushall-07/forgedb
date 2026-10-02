import { cluster, nodes } from './mockCluster';
import { nextIndexFor } from './clusterSelectors';
import type { RaftEvent, RaftLogEntry, RaftNodeState, RaftSnapshotState } from '../types/raft';

/**
 * Raft-specific mock state, derived from the same cluster mock (mockCluster.ts)
 * the Overview and Cluster pages already read, so the Raft dashboard can never
 * disagree with them about who the leader is, what term the cluster is in, or
 * where the commit/apply boundary sits. Only the fields Raft itself needs
 * (votedFor, nextIndex/matchIndex semantics, the log, events, snapshot state)
 * are added here.
 */
export const raftNodes: RaftNodeState[] = nodes.map((n) => {
  const isLeader = n.role === 'leader';
  return {
    nodeId: n.id,
    role: isLeader ? 'LEADER' : 'FOLLOWER',
    term: n.term,
    votedFor: cluster.leader,
    lastLogIndex: n.lastLogIndex,
    lastLogTerm: n.lastLogTerm,
    commitIndex: n.commitIndex,
    lastApplied: n.lastApplied,
    nextIndex: isLeader ? null : nextIndexFor(n),
    matchIndex: isLeader ? null : n.matchIndex,
  };
});

/**
 * The replicated log itself. currentTerm (5, see mockCluster.cluster.term) is
 * intentionally ahead of this log's last term (4): no entry has been proposed
 * yet in the current term, which is a normal, valid Raft state and must not
 * be "corrected" to make the numbers match.
 */
export const raftLog: RaftLogEntry[] = [
  {
    index: 1,
    term: 1,
    commandType: 'PUT',
    key: 'user:10042',
    value: { name: 'Jordan Ellis', active: true },
    committed: true,
    applied: true,
  },
  {
    index: 2,
    term: 2,
    commandType: 'PUT',
    key: 'config:theme',
    value: 'midnight',
    committed: true,
    applied: true,
  },
  {
    index: 3,
    term: 3,
    commandType: 'DELETE',
    key: 'cache:item:77',
    committed: true,
    applied: true,
  },
  {
    index: 4,
    term: 4,
    commandType: 'PUT',
    key: 'session:abc123',
    value: { userId: 10042, expiresAt: '2026-10-03T14:02:00Z' },
    committed: true,
    applied: true,
  },
];

/** Mock recent Raft events, newest first. */
export const raftEvents: RaftEvent[] = [
  {
    id: 'raft-evt-5',
    timestamp: '09:18:03',
    type: 'HEARTBEAT',
    title: 'Heartbeat acknowledged',
    description: 'leader NODE-2 → NODE-1 / NODE-3',
    nodeIds: ['NODE-2', 'NODE-1', 'NODE-3'],
  },
  {
    id: 'raft-evt-4',
    timestamp: '09:17:58',
    type: 'COMMIT',
    title: 'Commit index advanced',
    description: '3 → 4',
    nodeIds: ['NODE-2'],
  },
  {
    id: 'raft-evt-3',
    timestamp: '09:17:57',
    type: 'REPLICATION',
    title: 'AppendEntries acknowledged',
    description: 'NODE-3 matchIndex = 4',
    nodeIds: ['NODE-3'],
  },
  {
    id: 'raft-evt-2',
    timestamp: '09:17:56',
    type: 'REPLICATION',
    title: 'AppendEntries acknowledged',
    description: 'NODE-1 matchIndex = 4',
    nodeIds: ['NODE-1'],
  },
  {
    id: 'raft-evt-1',
    timestamp: '09:17:51',
    type: 'ELECTION',
    title: 'Leader elected',
    description: 'NODE-2 / term 5',
    nodeIds: ['NODE-2'],
  },
];

/** No snapshot has been installed in this mock state; the full log is retained from index 1. */
export const raftSnapshot: RaftSnapshotState = {
  snapshotIndex: 0,
  snapshotTerm: 0,
  logStartIndex: 1,
  compactedEntries: 0,
  state: 'NONE',
};
