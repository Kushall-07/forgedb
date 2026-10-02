export type KVOperationType = 'GET' | 'PUT' | 'DELETE';

export type ConsistencyMode = 'LINEARIZABLE' | 'LOCAL';

/** A mock target selector. AUTO and LEADER both resolve to the current Raft leader. */
export type KVTargetSelection = 'AUTO' | 'LEADER' | 'NODE-1' | 'NODE-2' | 'NODE-3';

export type KVEntryState = 'PRESENT' | 'TOMBSTONE';

export interface KVEntry {
  key: string;
  value: unknown;
  valueType: string;
  sizeBytes: number;
  /** The Raft log index this entry's current state was applied at, not a generic "version". */
  raftLogIndex: number;
  updatedAt: string;
  state: KVEntryState;
}

export interface KVOperation {
  id: string;
  timestamp: string;
  operation: KVOperationType;
  key: string;
  target: string;
  status: number;
  latencyMs: number;
  consistency: ConsistencyMode;
}

export interface KVRequestState {
  operation: KVOperationType;
  key: string;
  value: string;
  target: KVTargetSelection;
  consistency: ConsistencyMode;
}

export interface KVResponseBody {
  key: string;
  value?: unknown;
}

export interface KVExecutionResponse {
  requestId: string;
  status: number;
  statusText: string;
  ok: boolean;
  latencyMs: number;
  node: string;
  term: number;
  commitIndex: number;
  appliedIndex: number;
  consistency: ConsistencyMode;
  found?: boolean;
  deleted?: boolean;
  notLeader?: boolean;
  leader?: string;
  body: KVResponseBody | null;
  errorMessage?: string;
}

export interface KVExecutionResult {
  response: KVExecutionResponse;
  operation: KVOperation;
  entry: KVEntry | null;
}

export interface KVRequestInput {
  operation: KVOperationType;
  key: string;
  value?: string;
  target: KVTargetSelection;
  consistency: ConsistencyMode;
}
