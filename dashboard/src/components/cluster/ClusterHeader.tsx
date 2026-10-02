import { Share2 } from 'lucide-react';
import { cluster, nodes } from '../../data/mockCluster';
import { StatusIndicator } from '../ui/StatusIndicator';
import './ClusterHeader.css';

/** Compact page header, not a hero: this console is read many times a day. */
export function ClusterHeader() {
  const leaderCount = nodes.filter((n) => n.role === 'leader').length;

  return (
    <section className="cluster-header" aria-label="Cluster header" data-sc-in>
      <div className="cluster-header__identity">
        <span className="cluster-header__icon" aria-hidden="true">
          <Share2 size={20} strokeWidth={1.75} />
        </span>
        <div>
          <span className="cluster-header__eyebrow mono">FORGEDB / CLUSTER</span>
          <h1 className="cluster-header__title">Cluster</h1>
          <p className="cluster-header__subtitle">Distributed consensus topology</p>
        </div>
      </div>

      <div className="cluster-header__badges">
        <span className="cluster-header__badge mono">{nodes.length} NODES</span>
        <span className="cluster-header__badge mono">{leaderCount} LEADER</span>
        <span className="cluster-header__badge mono">TERM {cluster.term}</span>
        <StatusIndicator tone="sync" label="Healthy" pulse />
      </div>
    </section>
  );
}
