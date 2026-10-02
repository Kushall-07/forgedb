export type RaftRole = 'LEADER' | 'FOLLOWER' | 'CANDIDATE';

export interface RaftNodeState {
  nodeId: string;
  role: RaftRole;
  term: number;
  votedFor: string | null;
  lastLogIndex: number;
  lastLogTerm: number;
  commitIndex: number;
  lastApplied: number;
  /** Leader-only concept: null for the leader itself, since it has no "next index" into its own log. */
  nextIndex: number | null;
  /** Leader-only concept: null for the leader itself; see RaftNodeSelector/ReplicationMatrix for why it is hidden rather than faked. */
  matchIndex: number | null;
}

export type RaftCommandType = 'PUT' | 'DELETE';

export interface RaftLogEntry {
  index: number;
  term: number;
  commandType: RaftCommandType;
  key: string;
  value?: unknown;
  committed: boolean;
  applied: boolean;
}

export type RaftEventType = 'ELECTION' | 'REPLICATION' | 'COMMIT' | 'HEARTBEAT' | 'SNAPSHOT';

export interface RaftEvent {
  id: string;
  timestamp: string;
  type: RaftEventType;
  title: string;
  description: string;
  nodeIds: string[];
}

export type RaftSnapshotStatus = 'NONE' | 'AVAILABLE';

export interface RaftSnapshotState {
  snapshotIndex: number;
  snapshotTerm: number;
  logStartIndex: number;
  compactedEntries: number;
  state: RaftSnapshotStatus;
}
