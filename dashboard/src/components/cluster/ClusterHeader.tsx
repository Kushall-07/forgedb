import { Share2 } from 'lucide-react';
import { nodes } from '../../data/mockCluster';
import { DataSourceBadge } from '../ui/DataSourceBadge';
import type { BackendStatus } from '../../hooks/useBackendCluster';
import type { ClusterState } from '../../types/cluster';
import './ClusterHeader.css';

interface ClusterHeaderProps {
  cluster: ClusterState;
  status: BackendStatus;
}

/**
 * Compact page header, not a hero: this console is read many times a day.
 * TERM comes from the live node's own view when status is 'live' (see
 * hooks/useBackendCluster.ts); the node/leader counts stay sourced from the
 * mock 3-node roster, since GET /cluster is node-local, not federated.
 */
export function ClusterHeader({ cluster, status }: ClusterHeaderProps) {
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
        <span className="cluster-header__badge mono">{nodes.length} NODES (MOCK)</span>
        <span className="cluster-header__badge mono">{leaderCount} LEADER (MOCK)</span>
        <span className="cluster-header__badge mono">TERM {cluster.term}</span>
        <DataSourceBadge status={status} />
      </div>
    </section>
  );
}
