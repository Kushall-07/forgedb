import { nodes } from '../../data/mockCluster';
import { MetricValue } from '../ui/MetricValue';
import { DataSourceBadge } from '../ui/DataSourceBadge';
import type { BackendStatus } from '../../hooks/useBackendCluster';
import type { ClusterState } from '../../types/cluster';
import './ClusterSummary.css';

interface ClusterSummaryProps {
  cluster: ClusterState;
  status: BackendStatus;
}

/**
 * Leader/Term/Commit Index/Last Applied/Last Log Index come from `cluster`,
 * which is this node's live GET /cluster view when status is 'live' and the
 * mock ClusterState otherwise (see hooks/useBackendCluster.ts). Node/leader
 * COUNTS stay sourced from the mock 3-node roster unconditionally: /cluster
 * is node-local (one node's own state), never a federated view of the
 * cluster, so there is no real multi-node count to report yet.
 */
export function ClusterSummary({ cluster, status }: ClusterSummaryProps) {
  const leaderCount = nodes.filter((n) => n.role === 'leader').length;

  return (
    <section className="cluster-summary" aria-label="Cluster summary">
      <div className="cluster-summary__header" data-sc-in>
        <div className="cluster-summary__heading">
          <span className="cluster-summary__eyebrow mono">FORGEDB</span>
          <h1 className="cluster-summary__title">Distributed Database Control Plane</h1>
          <p className="cluster-summary__subtitle">Consensus, replication, durability and cluster state.</p>
        </div>
        <div className="cluster-summary__badges">
          <DataSourceBadge status={status} />
          <span className="cluster-summary__badge mono">{nodes.length} NODES (MOCK)</span>
          <span className="cluster-summary__badge mono">{leaderCount} LEADER (MOCK)</span>
        </div>
      </div>

      <div className="cluster-summary__metrics" data-sc-in data-sc-stagger="70">
        <MetricValue label="Leader" value={cluster.leader} emphasis tone="sync" />
        <MetricValue label="Term" value={cluster.term} />
        <MetricValue label="Commit Index" value={cluster.commitIndex} />
        <MetricValue label="Last Applied" value={cluster.lastApplied} />
        <MetricValue label="Last Log Index" value={cluster.lastLogIndex} />
      </div>
    </section>
  );
}
