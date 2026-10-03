export type KVOperationType = 'GET' | 'PUT' | 'DELETE';

export type ConsistencyMode = 'LINEARIZABLE' | 'LOCAL';

/** A mock target selector. AUTO and LEADER both resolve to the current Raft leader. */
export type KVTargetSelection = 'AUTO' | 'LEADER' | 'NODE-1' | 'NODE-2' | 'NODE-3';

export type KVEntryState = 'PRESENT' | 'TOMBSTONE';

/**
 * Where one KV result/entry/operation actually came from. 'live' means it
 * was answered by the real ForgeDB HTTP API; 'mock' means the dashboard's
 * mock KV store answered it instead, either because the selected target
 * has no configured live address or because the live request failed and
 * the console fell back (see hooks/useKvConsole.ts). Never left ambiguous.
 */
export type KVDataSource = 'live' | 'mock';

export interface KVEntry {
  key: string;
  value: unknown;
  valueType: string;
  sizeBytes: number;
  /**
   * The Raft log index this entry's current state was applied at.
   * Undefined for a live entry: the real PUT/DELETE response body is just
   * {"status":"ok"|"deleted"} (see internal/api/kv.go) and does not report
   * which index the write landed at, so this is never guessed.
   */
  raftLogIndex?: number;
  updatedAt: string;
  state: KVEntryState;
  /**
   * True when a live GET returned bytes that do not decode as valid UTF-8.
   * The value is intentionally not rendered in that case rather than
   * lossily reinterpreted as text (see services/forgedbApi.ts's getLiveKv).
   */
  isBinary?: boolean;
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
  source: KVDataSource;
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
  /** True when a live GET found a value but it was binary (not valid UTF-8) and so is deliberately not included above. */
  binary?: boolean;
  sizeBytes?: number;
}

export interface KVExecutionResponse {
  requestId: string;
  status: number;
  statusText: string;
  ok: boolean;
  /**
   * For a live request this is client-measured wall-clock round-trip time,
   * not a server processing metric -- the backend does not report its own
   * handling duration anywhere in the /kv response (see
   * services/forgedbApi.ts). Labelled "Client Latency" in the UI for live
   * responses to avoid implying otherwise.
   */
  latencyMs: number;
  node: string;
  /**
   * term/commitIndex/appliedIndex are only ever populated for mock
   * responses. The real /kv write/read responses carry no Raft metadata at
   * all (see internal/api/kv.go) -- a live response leaves these
   * undefined rather than inventing a value, and the UI renders "NOT
   * EXPOSED" for them.
   */
  term?: number;
  commitIndex?: number;
  appliedIndex?: number;
  consistency: ConsistencyMode;
  /** Set when a live GET was requested under LOCAL consistency: the HTTP API only ever performs a linearizable read, so this explains the discrepancy instead of silently claiming LOCAL behavior occurred. */
  consistencyNote?: string;
  found?: boolean;
  deleted?: boolean;
  notLeader?: boolean;
  leader?: string;
  body: KVResponseBody | null;
  errorMessage?: string;
  source: KVDataSource;
  /** Only set for a 'mock' response produced as a fallback after a live attempt failed or was not configured -- see hooks/useKvConsole.ts. */
  fallbackReason?: string;
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
