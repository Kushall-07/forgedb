import { cluster, nodes } from '../../data/mockCluster';
import { fullySyncedCount } from '../../data/clusterSelectors';
import { MetricValue } from '../ui/MetricValue';
import './ClusterSummary.css';

export function ClusterSummary() {
  const synced = fullySyncedCount();

  return (
    <section className="cluster-live-summary" aria-label="Live cluster summary" data-sc-in data-sc-stagger="60">
      <MetricValue label="Nodes" value={nodes.length} />
      <MetricValue label="Leader" value={cluster.leader} emphasis tone="sync" />
      <MetricValue label="Term" value={cluster.term} />
      <MetricValue label="Commit Index" value={cluster.commitIndex} />
      <MetricValue label="Last Applied" value={cluster.lastApplied} />
      <MetricValue label="Log Index" value={cluster.lastLogIndex} />
      <MetricValue
        label="Replication"
        value={`${synced} / ${nodes.length} SYNCED`}
        tone="sync"
        footnote="All peers acknowledging"
      />
    </section>
  );
}
