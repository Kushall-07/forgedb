import { nodes } from '../../data/mockCluster';
import { getKvRaftState } from '../../data/mockKV';
import type { ConsistencyMode } from '../../types/kv';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './KVSystemSummary.css';

interface KVSystemSummaryProps {
  readModel: ConsistencyMode;
}

/** Compact storage/consistency summary, same grammar as Cluster's ConsistencySummary strip. */
export function KVSystemSummary({ readModel }: KVSystemSummaryProps) {
  const { leader, commitIndex, appliedIndex } = getKvRaftState();

  return (
    <section className="kv-system-summary" aria-label="Storage and consistency summary">
      <div className="kv-system-summary__panel panel" data-sc-in>
        <div className="kv-system-summary__item">
          <TechnicalLabel>Read Model</TechnicalLabel>
          <span className="mono kv-system-summary__value">{readModel}</span>
        </div>
        <div className="kv-system-summary__item">
          <TechnicalLabel>Leader</TechnicalLabel>
          <span className="mono kv-system-summary__value kv-system-summary__value--identity">{leader}</span>
        </div>
        <div className="kv-system-summary__item">
          <TechnicalLabel>Commit Index</TechnicalLabel>
          <span className="mono kv-system-summary__value">{commitIndex}</span>
        </div>
        <div className="kv-system-summary__item">
          <TechnicalLabel>Last Applied</TechnicalLabel>
          <span className="mono kv-system-summary__value">{appliedIndex}</span>
        </div>
        <div className="kv-system-summary__item">
          <TechnicalLabel>Replication</TechnicalLabel>
          <span className="mono kv-system-summary__value">
            {nodes.length} / {nodes.length}
          </span>
        </div>
        <div className="kv-system-summary__item">
          <TechnicalLabel>Storage</TechnicalLabel>
          <span className="mono kv-system-summary__value">WAL + MEMTABLE</span>
        </div>
        <div className="kv-system-summary__item">
          <TechnicalLabel>State</TechnicalLabel>
          <span className="mono kv-system-summary__value kv-system-summary__value--sync">CONVERGED</span>
        </div>
      </div>
    </section>
  );
}
