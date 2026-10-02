import { useEffect, useId } from 'react';
import { exampleKeys } from '../../data/mockKV';
import type { ConsistencyMode, KVOperationType, KVRequestState, KVTargetSelection } from '../../types/kv';
import { SectionHeader } from '../ui/SectionHeader';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './KVRequestWorkspace.css';

const OPERATIONS: KVOperationType[] = ['GET', 'PUT', 'DELETE'];
const TARGETS: KVTargetSelection[] = ['AUTO', 'LEADER', 'NODE-1', 'NODE-2', 'NODE-3'];
const CONSISTENCY_MODES: ConsistencyMode[] = ['LINEARIZABLE', 'LOCAL'];

export type RequestPhase = 'idle' | 'executing' | 'done';

interface KVRequestWorkspaceProps {
  request: KVRequestState;
  phase: RequestPhase;
  validationError: string | null;
  requestId: string;
  clientId: string;
  onOperationChange: (operation: KVOperationType) => void;
  onTargetChange: (target: KVTargetSelection) => void;
  onConsistencyChange: (consistency: ConsistencyMode) => void;
  onKeyChange: (key: string) => void;
  onValueChange: (value: string) => void;
  onExampleKey: (key: string) => void;
  onExecute: () => void;
}

/**
 * The main request editor: operation, target node, consistency mode, key and
 * (for PUT) a value body, plus the request trace strip and the Execute
 * action. Everything here edits local state; nothing is sent over the
 * network in this phase.
 */
export function KVRequestWorkspace({
  request,
  phase,
  validationError,
  requestId,
  clientId,
  onOperationChange,
  onTargetChange,
  onConsistencyChange,
  onKeyChange,
  onValueChange,
  onExampleKey,
  onExecute,
}: KVRequestWorkspaceProps) {
  const keyInputId = useId();
  const valueInputId = useId();
  const targetSelectId = useId();
  const consistencySelectId = useId();

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.ctrlKey && event.key === 'Enter') {
        event.preventDefault();
        onExecute();
      }
    }
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onExecute]);

  const isExecuting = phase === 'executing';

  return (
    <section className="kv-workspace" aria-label="Request workspace">
      <SectionHeader eyebrow="Workspace" title="Request Workspace" description="Build and execute a KV operation against the mock cluster." />

      <form
        className="kv-workspace__panel panel"
        data-sc-in
        onSubmit={(e) => {
          e.preventDefault();
          onExecute();
        }}
      >
        <div className="kv-workspace__row">
          <TechnicalLabel as="div">Operation</TechnicalLabel>
          <div className="kv-workspace__tabs" role="tablist" aria-label="Operation">
            {OPERATIONS.map((op) => (
              <button
                key={op}
                type="button"
                role="tab"
                aria-selected={request.operation === op}
                className={`kv-workspace__tab kv-workspace__tab--${op.toLowerCase()} ${request.operation === op ? 'kv-workspace__tab--active' : ''}`}
                onClick={() => onOperationChange(op)}
              >
                {op}
              </button>
            ))}
          </div>
        </div>

        <div className="kv-workspace__row kv-workspace__row--split">
          <div className="kv-workspace__field">
            <label className="technical-label" htmlFor={targetSelectId}>
              Target Node
            </label>
            <select
              id={targetSelectId}
              className="kv-workspace__select mono"
              value={request.target}
              onChange={(e) => onTargetChange(e.target.value as KVTargetSelection)}
            >
              {TARGETS.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>

          <div className="kv-workspace__field">
            <label className="technical-label" htmlFor={consistencySelectId}>
              Consistency
            </label>
            <select
              id={consistencySelectId}
              className="kv-workspace__select mono"
              value={request.consistency}
              onChange={(e) => onConsistencyChange(e.target.value as ConsistencyMode)}
            >
              {CONSISTENCY_MODES.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="kv-workspace__row">
          <label className="technical-label" htmlFor={keyInputId}>
            Key
          </label>
          <input
            id={keyInputId}
            className="kv-workspace__input mono"
            type="text"
            placeholder="user:10042"
            value={request.key}
            onChange={(e) => onKeyChange(e.target.value)}
            autoComplete="off"
            spellCheck={false}
          />
          <div className="kv-workspace__examples">
            <span className="kv-workspace__examples-label mono">Examples</span>
            {exampleKeys.map((key) => (
              <button type="button" key={key} className="kv-workspace__example mono" onClick={() => onExampleKey(key)}>
                {key}
              </button>
            ))}
          </div>
        </div>

        <div className="kv-workspace__row">
          <label className="technical-label" htmlFor={valueInputId}>
            Value
          </label>
          {request.operation === 'PUT' ? (
            <textarea
              id={valueInputId}
              className="kv-workspace__textarea mono"
              placeholder={'{\n  "name": "Kushal",\n  "status": "active"\n}'}
              value={request.value}
              onChange={(e) => onValueChange(e.target.value)}
              rows={5}
              spellCheck={false}
            />
          ) : request.operation === 'GET' ? (
            <div className="kv-workspace__disabled-note mono" id={valueInputId}>
              VALUE NOT REQUIRED FOR GET
            </div>
          ) : (
            <div className="kv-workspace__disabled-note mono" id={valueInputId}>
              VALUE NOT APPLICABLE FOR DELETE
            </div>
          )}
        </div>

        <div className="kv-workspace__meta" aria-label="Request metadata">
          <div className="kv-workspace__meta-item">
            <TechnicalLabel>Request ID</TechnicalLabel>
            <span className="mono kv-workspace__meta-value">{requestId}</span>
          </div>
          <div className="kv-workspace__meta-item">
            <TechnicalLabel>Client ID</TechnicalLabel>
            <span className="mono kv-workspace__meta-value">{clientId}</span>
          </div>
          <div className="kv-workspace__meta-item">
            <TechnicalLabel>Target</TechnicalLabel>
            <span className="mono kv-workspace__meta-value">{request.target}</span>
          </div>
          <div className="kv-workspace__meta-item">
            <TechnicalLabel>Consistency</TechnicalLabel>
            <span className="mono kv-workspace__meta-value">{request.consistency}</span>
          </div>
          <div className="kv-workspace__meta-item">
            <TechnicalLabel>Operation</TechnicalLabel>
            <span className="mono kv-workspace__meta-value">{request.operation}</span>
          </div>
        </div>

        {validationError ? (
          <div className="kv-workspace__status kv-workspace__status--error" role="alert">
            <span className="mono kv-workspace__status-title">INVALID REQUEST</span>
            <span className="kv-workspace__status-body">{validationError}</span>
          </div>
        ) : isExecuting ? (
          <div className="kv-workspace__status kv-workspace__status--executing" role="status">
            <span className="mono kv-workspace__status-title">EXECUTING REQUEST</span>
            <span className="kv-workspace__status-body">Submitting mock request&hellip;</span>
          </div>
        ) : null}

        <div className="kv-workspace__actions">
          <button type="submit" className={`kv-workspace__execute kv-workspace__execute--${request.operation.toLowerCase()}`} disabled={isExecuting}>
            {isExecuting ? 'EXECUTING…' : `EXECUTE ${request.operation}`}
          </button>
          <span className="kv-workspace__hint mono">Ctrl + Enter</span>
        </div>
      </form>
    </section>
  );
}
