import { useState } from 'react';
import { useScrollCraft } from '../lib/scrollcraft/useScrollCraft';
import { defaultRequestState, executeKvRequest, getEntry, getOperations, initialResponse } from '../data/mockKV';
import type { ConsistencyMode, KVEntry, KVExecutionResponse, KVOperation, KVOperationType, KVRequestState, KVTargetSelection } from '../types/kv';
import { KVHeader } from '../components/kv/KVHeader';
import { KVRequestWorkspace, type RequestPhase } from '../components/kv/KVRequestWorkspace';
import { KVResponsePanel } from '../components/kv/KVResponsePanel';
import { KVValueInspector } from '../components/kv/KVValueInspector';
import { KVOperationTable } from '../components/kv/KVOperationTable';
import { KVRequestContext } from '../components/kv/KVRequestContext';
import { KVSystemSummary } from '../components/kv/KVSystemSummary';
import './KVConsole.css';

const CLIENT_ID = 'dashboard-client';
const EXECUTE_DELAY_MS = 450;

export default function KVConsole() {
  const scopeRef = useScrollCraft<HTMLDivElement>();

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

  function handleOperationChange(operation: KVOperationType) {
    clearError();
    setRequest((prev) => ({ ...prev, operation }));
  }

  function handleTargetChange(target: KVTargetSelection) {
    clearError();
    setRequest((prev) => ({ ...prev, target }));
  }

  function handleConsistencyChange(consistency: ConsistencyMode) {
    clearError();
    setRequest((prev) => ({ ...prev, consistency }));
  }

  function handleKeyChange(key: string) {
    clearError();
    setRequest((prev) => ({ ...prev, key }));
  }

  function handleValueChange(value: string) {
    clearError();
    setRequest((prev) => ({ ...prev, value }));
  }

  function handleExampleKey(key: string) {
    clearError();
    setRequest((prev) => ({ ...prev, key }));
  }

  function handleExecute() {
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

    window.setTimeout(() => {
      const result = executeKvRequest({
        operation: request.operation,
        key,
        value: request.value,
        target: request.target,
        consistency: request.consistency,
      });
      setResponse(result.response);
      setEntry(result.entry);
      setViewedKey(key);
      setOperations(getOperations());
      setPhase('done');
    }, EXECUTE_DELAY_MS);
  }

  return (
    <div ref={scopeRef}>
      <KVHeader />

      <div className="kv-console__split">
        <KVRequestWorkspace
          request={request}
          phase={phase}
          validationError={validationError}
          requestId={response.requestId}
          clientId={CLIENT_ID}
          onOperationChange={handleOperationChange}
          onTargetChange={handleTargetChange}
          onConsistencyChange={handleConsistencyChange}
          onKeyChange={handleKeyChange}
          onValueChange={handleValueChange}
          onExampleKey={handleExampleKey}
          onExecute={handleExecute}
        />
        <KVResponsePanel response={response} phase={phase} />
      </div>

      <KVValueInspector entryKey={viewedKey} entry={entry} />
      <KVOperationTable operations={operations} />
      <KVRequestContext />
      <KVSystemSummary readModel={request.consistency} />
    </div>
  );
}
