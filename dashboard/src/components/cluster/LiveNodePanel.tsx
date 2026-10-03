import { SectionHeader } from '../ui/SectionHeader';
import { DataSourceBadge } from '../ui/DataSourceBadge';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import type { BackendStatus } from '../../hooks/useBackendCluster';
import type { LiveNodeSnapshot } from '../../types/backend';
import './LiveNodePanel.css';

interface LiveNodePanelProps {
  status: BackendStatus;
  node: LiveNodeSnapshot | null;
  error: string | null;
}

/**
 * The one real node this dashboard can currently reach, shown honestly as a
 * single-node readout -- never spliced into the mock 3-node topology below
 * it (see Phase B's "GET /cluster is node-local, not federated" rule).
 * Renders nothing fabricated: when status isn't 'live' this shows why, not
 * stand-in numbers.
 */
export function LiveNodePanel({ status, node, error }: LiveNodePanelProps) {
  return (
    <section className="live-node-panel" aria-label="Live node">
      <SectionHeader
        eyebrow="Real Backend"
        title="Live Node"
        description="The single node answering GET /health and GET /cluster right now -- not a cluster-wide view. The topology below remains a mock illustration of a 3-node roster until multi-node polling ships."
      />

      <div className="live-node-panel__panel panel" data-sc-in>
        <div className="live-node-panel__status">
          <DataSourceBadge status={status} />
          {status !== 'live' && error ? <span className="live-node-panel__error mono">{error}</span> : null}
        </div>

        {node ? (
          <div className="live-node-panel__groups">
            <div className="live-node-panel__group">
              <TechnicalLabel as="div">Identity</TechnicalLabel>
              <dl className="live-node-panel__rows">
                <div className="live-node-panel__row">
                  <dt>Node ID</dt>
                  <dd className="mono">{node.nodeId}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Role</dt>
                  <dd className="mono">{node.role}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Leader ID</dt>
                  <dd className="mono">{node.leaderId || '—'}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Uptime</dt>
                  <dd className="mono">{node.processUptimeSeconds.toFixed(1)}s</dd>
                </div>
              </dl>
            </div>

            <div className="live-node-panel__group">
              <TechnicalLabel as="div">Raft</TechnicalLabel>
              <dl className="live-node-panel__rows">
                <div className="live-node-panel__row">
                  <dt>Term</dt>
                  <dd className="mono">{node.term}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Commit Index</dt>
                  <dd className="mono">{node.commitIndex}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Last Applied</dt>
                  <dd className="mono">{node.lastApplied}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Last Log Index</dt>
                  <dd className="mono">{node.lastLogIndex}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Last Log Term</dt>
                  <dd className="mono">{node.lastLogTerm}</dd>
                </div>
              </dl>
            </div>

            <div className="live-node-panel__group">
              <TechnicalLabel as="div">Snapshot &amp; Storage</TechnicalLabel>
              <dl className="live-node-panel__rows">
                <div className="live-node-panel__row">
                  <dt>Snapshot Index</dt>
                  <dd className="mono">{node.snapshotIndex}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>Snapshot Term</dt>
                  <dd className="mono">{node.snapshotTerm}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>MemTable Entries</dt>
                  <dd className="mono">{node.memtableEntries}</dd>
                </div>
                <div className="live-node-panel__row">
                  <dt>MemTable Bytes</dt>
                  <dd className="mono">{node.memtableBytes}</dd>
                </div>
              </dl>
            </div>
          </div>
        ) : (
          <p className="live-node-panel__empty mono">
            {status === 'loading'
              ? 'Connecting to the ForgeDB backend...'
              : 'Backend unreachable -- showing the mock topology below only. Start the ForgeDB node and refresh to see live state here.'}
          </p>
        )}
      </div>
    </section>
  );
}
