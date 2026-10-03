import type { KVOperation } from '../../types/kv';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import { DataSourceBadge } from '../ui/DataSourceBadge';
import './KVOperationTable.css';

interface KVOperationTableProps {
  operations: KVOperation[];
}

function toneForStatus(status: number): StatusTone {
  if (status >= 200 && status < 300) return 'sync';
  if (status === 307) return 'identity';
  if (status === 404 || status === 409) return 'warn';
  return 'critical';
}

const OP_TONE: Record<KVOperation['operation'], string> = {
  GET: 'kv-op-table__op--get',
  PUT: 'kv-op-table__op--put',
  DELETE: 'kv-op-table__op--delete',
};

/** Recent KV operations, newest first. Scrolls horizontally inside its own panel on narrow screens. */
export function KVOperationTable({ operations }: KVOperationTableProps) {
  return (
    <section className="kv-op-table" aria-label="Recent operations">
      <SectionHeader eyebrow="History" title="Recent Operations" description="The last KV requests issued from this console, newest first. Source distinguishes a real ForgeDB response from a mock fallback." />

      <div className="kv-op-table__wrap panel" data-sc-in>
        <table className="kv-op-table__table">
          <thead>
            <tr>
              <th scope="col">Time</th>
              <th scope="col">Operation</th>
              <th scope="col">Key</th>
              <th scope="col">Target</th>
              <th scope="col">Status</th>
              <th scope="col">Latency</th>
              <th scope="col">Consistency</th>
              <th scope="col">Source</th>
              <th scope="col">Request ID</th>
            </tr>
          </thead>
          <tbody>
            {operations.map((op) => (
              <tr key={op.id}>
                <td className="mono">{op.timestamp}</td>
                <td className={`mono ${OP_TONE[op.operation]}`}>{op.operation}</td>
                <td className="mono">{op.key}</td>
                <td className="mono">{op.target}</td>
                <td>
                  <StatusIndicator tone={toneForStatus(op.status)} label={String(op.status)} size="sm" />
                </td>
                <td className="mono">{op.latencyMs.toFixed(1)} ms</td>
                <td className="mono">{op.consistency}</td>
                <td>
                  <DataSourceBadge status={op.source} />
                </td>
                <td className="mono kv-op-table__id">{op.id}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
