import { cluster, nodes } from '../../data/mockCluster';
import { fullySyncedCount } from '../../data/clusterSelectors';
import { StatusIndicator } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './ConsistencySummary.css';

export function ConsistencySummary() {
  const synced = fullySyncedCount();

  return (
    <section className="consistency-summary" aria-label="Cluster consistency summary">
      <div className="consistency-summary__panel panel" data-sc-in>
        <div className="consistency-summary__item">
          <TechnicalLabel>Raft Consensus</TechnicalLabel>
          <StatusIndicator tone="sync" label="Healthy" size="sm" />
        </div>
        <div className="consistency-summary__item">
          <TechnicalLabel>Leader</TechnicalLabel>
          <span className="mono consistency-summary__value">{cluster.leader}</span>
        </div>
        <div className="consistency-summary__item">
          <TechnicalLabel>Majority</TechnicalLabel>
          <span className="mono consistency-summary__value">
            {synced} / {nodes.length}
          </span>
        </div>
        <div className="consistency-summary__item">
          <TechnicalLabel>Commit / Apply</TechnicalLabel>
          <span className="mono consistency-summary__value">
            {cluster.commitIndex} / {cluster.lastApplied}
          </span>
        </div>
        <div className="consistency-summary__item">
          <TechnicalLabel>Replication</TechnicalLabel>
          <span className="mono consistency-summary__value">
            {synced} / {nodes.length} SYNCED
          </span>
        </div>
      </div>
    </section>
  );
}
