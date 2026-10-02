import { useState } from 'react';
import { useScrollCraft } from '../lib/scrollcraft/useScrollCraft';
import { cluster } from '../data/mockCluster';
import { ClusterHeader } from '../components/cluster/ClusterHeader';
import { ClusterSummary } from '../components/cluster/ClusterSummary';
import { ClusterTopology } from '../components/cluster/ClusterTopology';
import { NodeInspector } from '../components/cluster/NodeInspector';
import { ReplicationMatrix } from '../components/cluster/ReplicationMatrix';
import { RaftStateComparison } from '../components/cluster/RaftStateComparison';
import { ClusterTimeline } from '../components/cluster/ClusterTimeline';
import { ConsistencySummary } from '../components/cluster/ConsistencySummary';

export default function Cluster() {
  const scopeRef = useScrollCraft<HTMLDivElement>();
  const [selectedId, setSelectedId] = useState(cluster.leader);

  return (
    <div ref={scopeRef}>
      <ClusterHeader />
      <ClusterSummary />
      <ClusterTopology selectedId={selectedId} onSelect={setSelectedId} />
      <NodeInspector nodeId={selectedId} />
      <ReplicationMatrix />
      <RaftStateComparison />
      <ClusterTimeline />
      <ConsistencySummary />
    </div>
  );
}
