import type { KVExecutionResponse } from '../../types/kv';
import { MetricValue } from '../ui/MetricValue';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import type { RequestPhase } from './KVRequestWorkspace';
import './KVResponsePanel.css';

interface KVResponsePanelProps {
  response: KVExecutionResponse | null;
  phase: RequestPhase;
}

function toneForStatus(response: KVExecutionResponse): StatusTone {
  if (response.notLeader) return 'identity';
  if (response.ok) return 'sync';
  if (response.status === 404) return 'warn';
  return 'critical';
}

function formatBody(response: KVExecutionResponse): string {
  if (response.notLeader) {
    return JSON.stringify({ error: 'NOT_LEADER', leader: response.leader }, null, 2);
  }
  if (!response.body) return '{}';
  if (response.found === false) {
    return JSON.stringify({ key: response.body.key, found: false }, null, 2);
  }
  const payload: Record<string, unknown> = { key: response.body.key };
  if ('value' in response.body) payload.value = response.body.value;
  if (response.found !== undefined) payload.found = response.found;
  if (response.deleted !== undefined) payload.deleted = response.deleted;
  return JSON.stringify(payload, null, 2);
}

/** The response/trace inspector: the second major surface, always showing the most recent mock result. */
export function KVResponsePanel({ response, phase }: KVResponsePanelProps) {
  const isExecuting = phase === 'executing';

  return (
    <section className="kv-response" aria-label="Response inspector">
      <SectionHeader eyebrow="Inspector" title="Response Inspector" description="What the mock cluster returned for the last request." />

      <div className="kv-response__panel panel" data-sc-in>
        {isExecuting ? (
          <div className="kv-response__waiting" role="status">
            <TechnicalLabel as="div">Executing Request</TechnicalLabel>
            <p className="kv-response__waiting-text">Submitting mock request&hellip;</p>
          </div>
        ) : response ? (
          <>
            <div className="kv-response__status-row">
              <StatusIndicator tone={toneForStatus(response)} label={`${response.status} ${response.statusText}`} size="md" />
              <span className="kv-response__request-id mono">{response.requestId}</span>
            </div>

            <div className="kv-response__metrics">
              <MetricValue label="Latency" value={`${response.latencyMs.toFixed(1)} ms`} />
              <MetricValue label="Node" value={response.node} tone="identity" />
              <MetricValue label="Term" value={response.term} />
              <MetricValue label="Commit Index" value={response.commitIndex} />
              <MetricValue label="Applied Index" value={response.appliedIndex} />
              <MetricValue label="Consistency" value={response.consistency} />
            </div>

            {response.notLeader ? (
              <div className="kv-response__note kv-response__note--identity">
                <TechnicalLabel>Leader</TechnicalLabel>
                <span className="mono">{response.leader}</span>
                <p>{response.errorMessage}</p>
              </div>
            ) : null}

            <div className="kv-response__body">
              <TechnicalLabel as="div">Response Body</TechnicalLabel>
              <pre className="kv-response__body-block mono">{formatBody(response)}</pre>
            </div>

            {response.errorMessage && !response.notLeader ? (
              <p className="kv-response__error">{response.errorMessage}</p>
            ) : null}
          </>
        ) : (
          <div className="kv-response__waiting">
            <TechnicalLabel as="div">Waiting For Request</TechnicalLabel>
            <p className="kv-response__waiting-text">Select an operation and execute it to inspect the response.</p>
          </div>
        )}
      </div>
    </section>
  );
}
