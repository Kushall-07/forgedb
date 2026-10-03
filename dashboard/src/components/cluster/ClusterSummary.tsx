import { nodes } from '../../data/mockCluster';
import { fullySyncedCount } from '../../data/clusterSelectors';
import { MetricValue } from '../ui/MetricValue';
import type { ClusterState } from '../../types/cluster';
import './ClusterSummary.css';

interface ClusterSummaryProps {
  cluster: ClusterState;
}

/**
 * Leader/Term/Commit Index/Last Applied/Log Index come from `cluster` (this
 * node's live view when available -- see hooks/useBackendCluster.ts).
 * Replication sync count stays derived from the mock 3-node roster: it is
 * inherently a multi-node calculation and GET /cluster reports only one
 * node, so there is nothing real to compute it from yet.
 */
export function ClusterSummary({ cluster }: ClusterSummaryProps) {
  const synced = fullySyncedCount();

  return (
    <section className="cluster-live-summary" aria-label="Live cluster summary" data-sc-in data-sc-stagger="60">
      <MetricValue label="Nodes" value={nodes.length} footnote="Mock roster" />
      <MetricValue label="Leader" value={cluster.leader} emphasis tone="sync" />
      <MetricValue label="Term" value={cluster.term} />
      <MetricValue label="Commit Index" value={cluster.commitIndex} />
      <MetricValue label="Last Applied" value={cluster.lastApplied} />
      <MetricValue label="Log Index" value={cluster.lastLogIndex} />
      <MetricValue
        label="Replication"
        value={`${synced} / ${nodes.length} SYNCED`}
        tone="sync"
        footnote="All peers acknowledging (mock roster)"
      />
    </section>
  );
}
