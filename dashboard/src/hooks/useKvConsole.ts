import { useState } from 'react';
import { forgedbApi, type LiveKvGetResult, type LiveKvWriteResult } from '../services/forgedbApi';
import {
  defaultRequestState,
  executeKvRequest,
  getEntry,
  getOperations,
  initialResponse,
  nextRequestId,
} from '../data/mockKV';
import type {
  ConsistencyMode,
  KVEntry,
  KVExecutionResponse,
  KVOperation,
  KVOperationType,
  KVRequestState,
  KVTargetSelection,
} from '../types/kv';
import type { RequestPhase } from '../components/kv/KVRequestWorkspace';

/**
 * How long the mock fallback path pretends a request took, matching the
 * console's original (Phase pre-C) feel. Only ever applied to a 'mock'
 * result -- a real request's latencyMs is always the actual measured
 * round trip (see services/forgedbApi.ts's liveKvGet/liveKvWrite), never
 * padded or delayed artificially.
 */
const MOCK_FALLBACK_DELAY_MS = 450;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function nowClock(): string {
  return new Date().toLocaleTimeString('en-GB', { hour12: false });
}

function decodeKvValue(bytes: Uint8Array): { text: string; isBinary: false } | { isBinary: true } {
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    return { text, isBinary: false };
  } catch {
    return { isBinary: true };
  }
}

interface LiveBuildContext {
  requestId: string;
  key: string;
  value: string;
  node: string;
  timestamp: string;
  consistency: ConsistencyMode;
}

interface BuiltResult {
  response: KVExecutionResponse;
  /** undefined means "leave the currently displayed entry alone" -- used whenever a request failed without telling us anything new about the key's actual state. null means "known to not exist / have no PRESENT value". */
  entry: KVEntry | null | undefined;
  operation: KVOperation;
}

function buildLiveGetResult(result: LiveKvGetResult, ctx: LiveBuildContext): BuiltResult {
  const { requestId, key, node, timestamp, consistency } = ctx;
  const consistencyNote =
    consistency === 'LOCAL'
      ? "ForgeDB's HTTP API always performs a linearizable read (ConsistentGet); a LOCAL read is not exposed over HTTP."
      : undefined;
  const baseOperation = { id: requestId, timestamp, operation: 'GET' as const, key, target: node, consistency: 'LINEARIZABLE' as const, source: 'live' as const };

  switch (result.kind) {
    case 'found': {
      const decoded = decodeKvValue(result.bytes);
      const entry: KVEntry = decoded.isBinary
        ? { key, value: null, valueType: 'BINARY', sizeBytes: result.bytes.byteLength, updatedAt: timestamp, state: 'PRESENT', isBinary: true }
        : { key, value: decoded.text, valueType: 'STRING', sizeBytes: result.bytes.byteLength, updatedAt: timestamp, state: 'PRESENT' };
      return {
        entry,
        response: {
          requestId, status: 200, statusText: 'OK', ok: true, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE', consistencyNote, found: true,
          body: decoded.isBinary
            ? { key, binary: true, sizeBytes: result.bytes.byteLength }
            : { key, value: decoded.text },
          source: 'live',
        },
        operation: { ...baseOperation, status: 200, latencyMs: result.clientLatencyMs },
      };
    }
    case 'not_found':
      return {
        entry: null,
        response: {
          requestId, status: 404, statusText: 'NOT FOUND', ok: false, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE', consistencyNote, found: false, body: { key },
          errorMessage: 'No value exists for this key.', source: 'live',
        },
        operation: { ...baseOperation, status: 404, latencyMs: result.clientLatencyMs },
      };
    case 'not_leader':
      return {
        entry: undefined,
        response: {
          requestId, status: 421, statusText: 'NOT LEADER', ok: false, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE', consistencyNote, notLeader: true, leader: result.leaderId || undefined,
          body: null, errorMessage: 'Read rejected: this node is not the Raft leader.', source: 'live',
        },
        operation: { ...baseOperation, status: 421, latencyMs: result.clientLatencyMs },
      };
    case 'read_unavailable':
      return {
        entry: undefined,
        response: {
          requestId, status: 503, statusText: 'READ UNAVAILABLE', ok: false, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE', consistencyNote, body: null,
          errorMessage: `Read unavailable: ${result.reason}`, source: 'live',
        },
        operation: { ...baseOperation, status: 503, latencyMs: result.clientLatencyMs },
      };
    case 'error':
    default:
      return {
        entry: undefined,
        response: {
          requestId, status: result.status, statusText: result.statusText, ok: false, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE', consistencyNote, body: null,
          errorMessage: result.detail || 'The backend returned an unexpected error.', source: 'live',
        },
        operation: { ...baseOperation, status: result.status, latencyMs: result.clientLatencyMs },
      };
  }
}

function buildLiveWriteResult(opType: 'PUT' | 'DELETE', result: LiveKvWriteResult, ctx: LiveBuildContext): BuiltResult {
  const { requestId, key, value, node, timestamp } = ctx;
  const baseOperation = { id: requestId, timestamp, operation: opType, key, target: node, consistency: 'LINEARIZABLE' as const, source: 'live' as const };

  switch (result.kind) {
    case 'ok': {
      const entry: KVEntry =
        opType === 'PUT'
          ? { key, value, valueType: 'STRING', sizeBytes: new TextEncoder().encode(value).length, updatedAt: timestamp, state: 'PRESENT' }
          : { key, value: null, valueType: 'NULL', sizeBytes: 0, updatedAt: timestamp, state: 'TOMBSTONE' };
      return {
        entry,
        response: {
          requestId, status: 200, statusText: 'OK', ok: true, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE',
          deleted: opType === 'DELETE' ? true : undefined,
          body: opType === 'PUT' ? { key, value } : { key },
          source: 'live',
        },
        operation: { ...baseOperation, status: 200, latencyMs: result.clientLatencyMs },
      };
    }
    case 'not_leader':
      return {
        entry: undefined,
        response: {
          requestId, status: 421, statusText: 'NOT LEADER', ok: false, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE', notLeader: true, leader: result.leaderId || undefined,
          body: null,
          errorMessage: 'Write rejected by the selected ForgeDB node: it is not the current Raft leader.',
          source: 'live',
        },
        operation: { ...baseOperation, status: 421, latencyMs: result.clientLatencyMs },
      };
    case 'error':
    default:
      return {
        entry: undefined,
        response: {
          requestId, status: result.status, statusText: result.statusText, ok: false, latencyMs: result.clientLatencyMs, node,
          consistency: 'LINEARIZABLE', body: null,
          errorMessage: result.detail || 'The backend returned an unexpected error.', source: 'live',
        },
        operation: { ...baseOperation, status: result.status, latencyMs: result.clientLatencyMs },
      };
  }
}

/**
 * Attempts the request against the real backend. Resolves to a BuiltResult
 * for every protocol-level outcome; only throws (via forgedbApi's
 * BackendRequestError) when the request never reached the backend at all,
 * which is the one case runRequest below treats as groundsto fall back to
 * mock. knownNodeId, when provided, skips the extra GET /health this would
 * otherwise race against the KV call purely to label which node answered.
 */
async function runLiveRequest(
  request: KVRequestState,
  key: string,
  timestamp: string,
  knownNodeId?: string,
): Promise<BuiltResult> {
  const requestId = nextRequestId();
  const healthPromise =
    knownNodeId !== undefined ? Promise.resolve(knownNodeId) : forgedbApi.getLiveHealth().then((h) => h.nodeId).catch(() => null);

  if (request.operation === 'GET') {
    const [result, nodeId] = await Promise.all([forgedbApi.getLiveKv(key), healthPromise]);
    const node = nodeId ? nodeId.toUpperCase() : request.target;
    return buildLiveGetResult(result, { requestId, key, value: request.value, node, timestamp, consistency: request.consistency });
  }

  const opType = request.operation;
  const [result, nodeId] =
    opType === 'PUT'
      ? await Promise.all([forgedbApi.putLiveKv(key, request.value), healthPromise])
      : await Promise.all([forgedbApi.deleteLiveKv(key), healthPromise]);
  const node = nodeId ? nodeId.toUpperCase() : request.target;
  return buildLiveWriteResult(opType, result, { requestId, key, value: request.value, node, timestamp, consistency: request.consistency });
}

/**
 * Decides live vs. mock for one request and returns a normalized result.
 * AUTO and LEADER both always attempt the configured backend directly
 * (there is only one address configured -- see vite.config.ts's FORGEDB_HTTP_ADDR).
 * An explicit NODE-1/2/3 selection is only attempted live when it matches
 * the node_id the configured backend actually reports for itself; ForgeDB's
 * dashboard has no address configured for any other node, so that case
 * (and any live network failure) falls back to the existing mock KV store,
 * labeled 'mock' and carrying a human-readable reason rather than silently
 * pretending to be live.
 */
async function runRequest(request: KVRequestState): Promise<BuiltResult> {
  const key = request.key.trim();
  const timestamp = nowClock();
  const isDirectTarget = request.target === 'AUTO' || request.target === 'LEADER';

  let fallbackReason: string | undefined;
  let knownNodeId: string | undefined;

  if (!isDirectTarget) {
    try {
      const health = await forgedbApi.getLiveHealth();
      const normalized = health.nodeId.toUpperCase();
      if (normalized === request.target) {
        knownNodeId = health.nodeId;
      } else {
        fallbackReason = `${request.target} has no address configured in this dashboard -- the reachable backend reports itself as ${health.nodeId}.`;
      }
    } catch (err) {
      fallbackReason = `Backend unreachable while resolving ${request.target}: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  if (isDirectTarget || knownNodeId !== undefined) {
    try {
      return await runLiveRequest(request, key, timestamp, knownNodeId);
    } catch (err) {
      fallbackReason = `Backend unreachable: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  await sleep(MOCK_FALLBACK_DELAY_MS);
  const mock = executeKvRequest({ operation: request.operation, key, value: request.value, target: request.target, consistency: request.consistency });
  return {
    entry: mock.entry,
    response: { ...mock.response, fallbackReason },
    operation: mock.operation,
  };
}

export interface UseKvConsoleResult {
  request: KVRequestState;
  phase: RequestPhase;
  response: KVExecutionResponse;
  entry: KVEntry | null;
  viewedKey: string;
  operations: KVOperation[];
  validationError: string | null;
  onOperationChange: (operation: KVOperationType) => void;
  onTargetChange: (target: KVTargetSelection) => void;
  onConsistencyChange: (consistency: ConsistencyMode) => void;
  onKeyChange: (key: string) => void;
  onValueChange: (value: string) => void;
  onExampleKey: (key: string) => void;
  onExecute: () => void;
}

/**
 * The KV Console's single state owner: request form, last response, the
 * key's current entry, and the operation history, all driven through
 * runRequest above rather than components touching forgedbApi or mockKV
 * directly. Mirrors the role hooks/useBackendCluster.ts plays for Cluster,
 * except this one acts on demand (one request at a time) instead of
 * polling, since KV operations are user-initiated.
 */
export function useKvConsole(): UseKvConsoleResult {
  const [request, setRequest] = useState<KVRequestState>(defaultRequestState);
  const [phase, setPhase] = useState<RequestPhase>('done');
  const [response, setResponse] = useState<KVExecutionResponse>(initialResponse);
  const [entry, setEntry] = useState<KVEntry | null>(() => getEntry(defaultRequestState.key));
  const [viewedKey, setViewedKey] = useState(defaultRequestState.key);
  const [operations, setOperations] = useState<KVOperation[]>(() => getOperations());
  const [validationError, setValidationError] = useState<string | null>(null);

  function clearError() {
    if (validationError) setValidationError(null);
  }

  function onOperationChange(operation: KVOperationType) {
    clearError();
    setRequest((prev) => ({ ...prev, operation }));
  }

  function onTargetChange(target: KVTargetSelection) {
    clearError();
    setRequest((prev) => ({ ...prev, target }));
  }

  function onConsistencyChange(consistency: ConsistencyMode) {
    clearError();
    setRequest((prev) => ({ ...prev, consistency }));
  }

  function onKeyChange(key: string) {
    clearError();
    setRequest((prev) => ({ ...prev, key }));
  }

  function onValueChange(value: string) {
    clearError();
    setRequest((prev) => ({ ...prev, value }));
  }

  function onExampleKey(key: string) {
    clearError();
    setRequest((prev) => ({ ...prev, key }));
  }

  function onExecute() {
    if (phase === 'executing') return;

    const key = request.key.trim();
    if (!key) {
      setValidationError('A key is required to execute this request.');
      return;
    }
    if (request.operation === 'PUT' && !request.value.trim()) {
      setValidationError('A PUT operation requires a value.');
      return;
    }

    setValidationError(null);
    setPhase('executing');

    runRequest(request).then(({ response: nextResponse, entry: nextEntry, operation }) => {
      setResponse(nextResponse);
      if (nextEntry !== undefined) setEntry(nextEntry);
      setViewedKey(key);
      setOperations((prev) => [operation, ...prev]);
      setPhase('done');
    });
  }

  return {
    request,
    phase,
    response,
    entry,
    viewedKey,
    operations,
    validationError,
    onOperationChange,
    onTargetChange,
    onConsistencyChange,
    onKeyChange,
    onValueChange,
    onExampleKey,
    onExecute,
  };
}
