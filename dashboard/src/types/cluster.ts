export type NodeRole = 'leader' | 'follower';

export type NodeHealth = 'active' | 'synced' | 'lagging' | 'unreachable';

export interface NodeState {
  id: string;
  role: NodeRole;
  term: number;
  commitIndex: number;
  lastApplied: number;
  lastLogIndex: number;
  lastLogTerm: number;
  matchIndex: number;
  lag: number;
  status: NodeHealth;
}

export interface ClusterState {
  leader: string;
  term: number;
  commitIndex: number;
  lastApplied: number;
  lastLogIndex: number;
  lastLogTerm: number;
  snapshotIndex: number;
}

export interface StorageState {
  memtableEntries: number;
  memtableBytes: number;
  wal: 'active' | 'inactive';
  sstables: number;
  snapshotIndex: number;
}

export type RaftEventKind = 'election' | 'append' | 'commit' | 'apply' | 'sync';

export interface RaftEvent {
  id: string;
  timestamp: string;
  message: string;
  kind: RaftEventKind;
}

export type RaftPipelineStage = 'propose' | 'replicate' | 'majority' | 'commit' | 'apply';

export type SystemHealthStatus = 'healthy' | 'warning' | 'critical';

export interface SystemHealthItem {
  label: string;
  value: string;
  status: SystemHealthStatus;
}
