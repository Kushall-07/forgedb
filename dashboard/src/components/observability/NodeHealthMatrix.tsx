import { useState } from 'react';
import { nodeHealth } from '../../data/mockObservability';
import type { NodeTelemetryStatus } from '../../types/observability';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './NodeHealthMatrix.css';

const STATUS_TONE: Record<NodeTelemetryStatus, StatusTone> = {
  healthy: 'sync',
  warning: 'warn',
  critical: 'critical',
};

/** All nodes at a glance, with a selectable row for a compact per-node detail readout. */
export function NodeHealthMatrix() {
  const [selectedId, setSelectedId] = useState(nodeHealth[0]?.nodeId ?? '');
  const selected = nodeHealth.find((n) => n.nodeId === selectedId);

  return (
    <section className="observability-node-matrix" aria-label="Node health matrix">
      <SectionHeader
        eyebrow="Fleet"
        title="Node Health"
        description="Per-node resource usage, throughput, and replication state. Select a node for detail."
      />

      <div className="observability-node-matrix__table-wrap panel" data-sc-in>
        <table className="observability-node-matrix__table">
          <thead>
            <tr>
              <th scope="col">Node</th>
              <th scope="col">Role</th>
              <th scope="col">Status</th>
              <th scope="col">CPU</th>
              <th scope="col">Memory</th>
              <th scope="col">Req/s</th>
              <th scope="col">P95</th>
              <th scope="col">Commit</th>
              <th scope="col">Apply</th>
              <th scope="col">Lag</th>
            </tr>
          </thead>
          <tbody>
            {nodeHealth.map((node) => (
              <tr key={node.nodeId} className={node.nodeId === selectedId ? 'observability-node-matrix__row--selected' : ''}>
                <th scope="row">
                  <button
                    type="button"
                    className="observability-node-matrix__select mono"
                    aria-pressed={node.nodeId === selectedId}
                    onClick={() => setSelectedId(node.nodeId)}
                  >
                    {node.nodeId}
                  </button>
                </th>
                <td>
                  <StatusIndicator tone={node.role === 'LEADER' ? 'identity' : 'sync'} label={node.role} size="sm" />
                </td>
                <td>
                  <StatusIndicator tone={STATUS_TONE[node.status]} label={node.status.toUpperCase()} size="sm" />
                </td>
                <td className="mono">{node.cpuPct}%</td>
                <td className="mono">{node.memoryPct}%</td>
                <td className="mono">{node.requestsPerSec}</td>
                <td className="mono">{node.p95LatencyMs}ms</td>
                <td className="mono">{node.commitIndex}</td>
                <td className="mono">{node.appliedIndex}</td>
                <td className={`mono ${node.replicationLagEntries > 0 ? 'observability-node-matrix__lag--warn' : ''}`}>
                  {node.replicationLagEntries}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {selected ? (
        <div className="observability-node-matrix__detail panel" data-sc-in aria-live="polite">
          <TechnicalLabel as="div">{selected.nodeId} Detail</TechnicalLabel>
          <dl className="observability-node-matrix__detail-rows">
            <div className="observability-node-matrix__detail-row">
              <dt>Uptime</dt>
              <dd className="mono">{selected.uptime}</dd>
            </div>
            <div className="observability-node-matrix__detail-row">
              <dt>Last Heartbeat</dt>
              <dd className="mono">{selected.lastHeartbeat}</dd>
            </div>
            <div className="observability-node-matrix__detail-row">
              <dt>Storage State</dt>
              <dd>
                <StatusIndicator
                  tone={selected.storageState === 'healthy' ? 'sync' : 'warn'}
                  label={selected.storageState.toUpperCase()}
                  size="sm"
                />
              </dd>
            </div>
            <div className="observability-node-matrix__detail-row">
              <dt>Replication Lag</dt>
              <dd className="mono">{selected.replicationLagEntries} entries</dd>
            </div>
          </dl>
        </div>
      ) : null}
    </section>
  );
}
