import { cluster, nodes } from '../../data/mockCluster';
import { MetricValue } from '../ui/MetricValue';
import { StatusIndicator } from '../ui/StatusIndicator';
import './ClusterSummary.css';

export function ClusterSummary() {
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
          <StatusIndicator tone="sync" label="Healthy" pulse />
          <span className="cluster-summary__badge mono">{nodes.length} NODES</span>
          <span className="cluster-summary__badge mono">{leaderCount} LEADER</span>
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
