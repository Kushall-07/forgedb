import { cluster } from './mockCluster';
import type {
  KVEntry,
  KVExecutionResponse,
  KVExecutionResult,
  KVOperation,
  KVRequestInput,
  KVRequestState,
  KVTargetSelection,
} from '../types/kv';

/**
 * Mock KV store + request log for the KV Console. Shaped like the real
 * ForgeDB KV contract will be once src/services/forgedbApi.ts talks to:
 *
 *   PUT    /kv/{key}
 *   GET    /kv/{key}
 *   DELETE /kv/{key}
 *
 * The module-level `store`/`operationLog` below stand in for state that will
 * live on the server once that API exists; every mutation goes through
 * executeKvRequest so the eventual swap to real fetch calls only touches this
 * file, not the components that read it.
 */

const seedEntries: Record<string, KVEntry> = {
  'user:10042': {
    key: 'user:10042',
    value: { name: 'Kushal', status: 'active' },
    valueType: 'JSON',
    sizeBytes: 38,
    raftLogIndex: 4,
    updatedAt: '2026-10-02 09:14:02',
    state: 'PRESENT',
  },
  'session:abc123': {
    key: 'session:abc123',
    value: { token: 'sess_9f2e', expiresIn: 3600 },
    valueType: 'JSON',
    sizeBytes: 42,
    raftLogIndex: 3,
    updatedAt: '2026-10-02 09:12:41',
    state: 'PRESENT',
  },
  'config:theme': {
    key: 'config:theme',
    value: 'dark',
    valueType: 'STRING',
    sizeBytes: 4,
    raftLogIndex: 2,
    updatedAt: '2026-10-02 08:55:17',
    state: 'PRESENT',
  },
  'cache:item:77': {
    key: 'cache:item:77',
    value: null,
    valueType: 'NULL',
    sizeBytes: 0,
    raftLogIndex: 5,
    updatedAt: '2026-10-02 09:16:09',
    state: 'TOMBSTONE',
  },
};

const seedOperations: KVOperation[] = [
  { id: 'req-0001', timestamp: '09:10:04', operation: 'PUT', key: 'config:theme', target: 'NODE-2', status: 200, latencyMs: 6.1, consistency: 'LINEARIZABLE', source: 'mock' },
  { id: 'req-0002', timestamp: '09:12:41', operation: 'PUT', key: 'session:abc123', target: 'NODE-2', status: 200, latencyMs: 7.2, consistency: 'LINEARIZABLE', source: 'mock' },
  { id: 'req-0003', timestamp: '09:13:05', operation: 'GET', key: 'session:abc123', target: 'NODE-2', status: 200, latencyMs: 3.4, consistency: 'LOCAL', source: 'mock' },
  { id: 'req-0004', timestamp: '09:14:02', operation: 'PUT', key: 'user:10042', target: 'NODE-2', status: 200, latencyMs: 6.9, consistency: 'LINEARIZABLE', source: 'mock' },
  { id: 'req-0005', timestamp: '09:14:30', operation: 'GET', key: 'user:10042', target: 'NODE-2', status: 200, latencyMs: 4.8, consistency: 'LINEARIZABLE', source: 'mock' },
  { id: 'req-0006', timestamp: '09:15:12', operation: 'GET', key: 'config:theme', target: 'NODE-2', status: 200, latencyMs: 3.9, consistency: 'LOCAL', source: 'mock' },
  { id: 'req-0007', timestamp: '09:16:09', operation: 'DELETE', key: 'cache:item:77', target: 'NODE-2', status: 200, latencyMs: 6.4, consistency: 'LINEARIZABLE', source: 'mock' },
  { id: 'req-0008', timestamp: '09:17:51', operation: 'GET', key: 'cache:item:99', target: 'NODE-2', status: 404, latencyMs: 2.7, consistency: 'LINEARIZABLE', source: 'mock' },
];

/** A few real keys to surface as one-click examples, not a full key browser. */
export const exampleKeys = ['user:10042', 'session:abc123', 'config:theme', 'cache:item:77'];

let store: Record<string, KVEntry> = { ...seedEntries };
let operationLog: KVOperation[] = [...seedOperations];
let requestCounter = seedOperations.length;
let logIndex = cluster.commitIndex;

/** Shared across live and mock requests so the operation table's request IDs stay in one sequence regardless of source -- see hooks/useKvConsole.ts. */
export function nextRequestId(): string {
  requestCounter += 1;
  return `req-${String(requestCounter).padStart(4, '0')}`;
}

function nowClock(): string {
  const d = new Date();
  return d.toLocaleTimeString('en-GB', { hour12: false });
}

function valueTypeOf(value: unknown): string {
  if (value === null) return 'NULL';
  if (typeof value === 'string') return 'STRING';
  if (typeof value === 'number') return 'NUMBER';
  if (typeof value === 'boolean') return 'BOOLEAN';
  return 'JSON';
}

function byteSizeOf(value: unknown): number {
  try {
    return new TextEncoder().encode(typeof value === 'string' ? value : JSON.stringify(value)).length;
  } catch {
    return 0;
  }
}

/** AUTO and LEADER both resolve to whichever node the cluster currently reports as leader. */
function resolveNode(target: KVTargetSelection): string {
  if (target === 'AUTO' || target === 'LEADER') return cluster.leader;
  return target;
}

function deterministicLatency(operation: KVRequestInput['operation'], key: string): number {
  const keyWeight = (key.length % 4) * 0.3;
  if (operation === 'GET') return Math.round((4.2 + keyWeight) * 10) / 10;
  if (operation === 'PUT') return Math.round((6.8 + keyWeight) * 10) / 10;
  return Math.round((5.9 + keyWeight) * 10) / 10;
}

export function getEntry(key: string): KVEntry | null {
  return store[key] ?? null;
}

export function getOperations(): KVOperation[] {
  return [...operationLog];
}

/** The KV page's own view of commit/apply progress, tracked independently of the Cluster page's static mock state. */
export function getKvRaftState() {
  return { leader: cluster.leader, term: cluster.term, commitIndex: logIndex, appliedIndex: logIndex };
}

/**
 * Simulates one KV request against the mock store. Writes targeted at a
 * follower node are rejected as NOT_LEADER, same as the real leader-oriented
 * API would, without ever pretending Raft replication actually ran.
 */
export function executeKvRequest(input: KVRequestInput): KVExecutionResult {
  const requestId = nextRequestId();
  const node = resolveNode(input.target);
  const isExplicitFollower = input.target !== 'AUTO' && input.target !== 'LEADER' && input.target !== cluster.leader;
  const timestamp = nowClock();

  if (input.operation !== 'GET' && isExplicitFollower) {
    const latencyMs = deterministicLatency(input.operation, input.key);
    const response = {
      requestId,
      status: 307,
      statusText: 'NOT LEADER',
      ok: false,
      latencyMs,
      node,
      term: cluster.term,
      commitIndex: logIndex,
      appliedIndex: logIndex,
      consistency: input.consistency,
      notLeader: true,
      leader: cluster.leader,
      body: null,
      errorMessage: `${node} is not the leader. Writes must go through ${cluster.leader}.`,
      source: 'mock' as const,
    };
    const operation: KVOperation = {
      id: requestId,
      timestamp,
      operation: input.operation,
      key: input.key,
      target: node,
      status: 307,
      latencyMs,
      consistency: input.consistency,
      source: 'mock',
    };
    operationLog = [operation, ...operationLog];
    return { response, operation, entry: getEntry(input.key) };
  }

  const latencyMs = deterministicLatency(input.operation, input.key);
  const base = {
    requestId,
    latencyMs,
    node,
    term: cluster.term,
    consistency: input.consistency,
    source: 'mock' as const,
  };

  if (input.operation === 'GET') {
    const entry = store[input.key];
    const found = Boolean(entry && entry.state === 'PRESENT');
    const response = {
      ...base,
      status: found ? 200 : 404,
      statusText: found ? 'OK' : 'NOT FOUND',
      ok: found,
      commitIndex: logIndex,
      appliedIndex: logIndex,
      found,
      body: found ? { key: input.key, value: entry!.value } : { key: input.key },
      errorMessage: found ? undefined : 'The requested key does not exist.',
    };
    const operation: KVOperation = {
      id: requestId,
      timestamp,
      operation: 'GET',
      key: input.key,
      target: node,
      status: response.status,
      latencyMs,
      consistency: input.consistency,
      source: 'mock',
    };
    operationLog = [operation, ...operationLog];
    return { response, operation, entry: getEntry(input.key) };
  }

  logIndex += 1;
  const nextLogIndex = logIndex;

  if (input.operation === 'PUT') {
    let parsedValue: unknown;
    try {
      parsedValue = input.value ? JSON.parse(input.value) : '';
    } catch {
      parsedValue = input.value;
    }
    const entry: KVEntry = {
      key: input.key,
      value: parsedValue,
      valueType: valueTypeOf(parsedValue),
      sizeBytes: byteSizeOf(parsedValue),
      raftLogIndex: nextLogIndex,
      updatedAt: `${new Date().toISOString().slice(0, 10)} ${timestamp}`,
      state: 'PRESENT',
    };
    store = { ...store, [input.key]: entry };
    const response = {
      ...base,
      status: 200,
      statusText: 'OK',
      ok: true,
      commitIndex: nextLogIndex,
      appliedIndex: nextLogIndex,
      body: { key: input.key, value: parsedValue },
    };
    const operation: KVOperation = {
      id: requestId,
      timestamp,
      operation: 'PUT',
      key: input.key,
      target: node,
      status: 200,
      latencyMs,
      consistency: input.consistency,
      source: 'mock',
    };
    operationLog = [operation, ...operationLog];
    return { response, operation, entry };
  }

  // DELETE
  const tombstone: KVEntry = {
    key: input.key,
    value: null,
    valueType: 'NULL',
    sizeBytes: 0,
    raftLogIndex: nextLogIndex,
    updatedAt: `${new Date().toISOString().slice(0, 10)} ${timestamp}`,
    state: 'TOMBSTONE',
  };
  store = { ...store, [input.key]: tombstone };
  const response = {
    ...base,
    status: 200,
    statusText: 'OK',
    ok: true,
    commitIndex: nextLogIndex,
    appliedIndex: nextLogIndex,
    deleted: true,
    body: { key: input.key },
  };
  const operation: KVOperation = {
    id: requestId,
    timestamp,
    operation: 'DELETE',
    key: input.key,
    target: node,
    status: 200,
    latencyMs,
    consistency: input.consistency,
    source: 'mock',
  };
  operationLog = [operation, ...operationLog];
  return { response, operation, entry: tombstone };
}

export const defaultRequestState: KVRequestState = {
  operation: 'GET',
  key: 'user:10042',
  value: '',
  target: 'LEADER',
  consistency: 'LINEARIZABLE',
};

/** Matches the GET user:10042 example response shown before any Execute click, so the console never opens empty. */
export const initialResponse: KVExecutionResponse = {
  requestId: 'req-0005',
  status: 200,
  statusText: 'OK',
  ok: true,
  latencyMs: 4.8,
  node: cluster.leader,
  term: cluster.term,
  commitIndex: cluster.commitIndex,
  appliedIndex: cluster.lastApplied,
  consistency: 'LINEARIZABLE',
  found: true,
  body: { key: 'user:10042', value: seedEntries['user:10042'].value },
  source: 'mock',
};
