import { useState } from 'react';
import { useScrollCraft } from '../lib/scrollcraft/useScrollCraft';
import { cluster } from '../data/mockCluster';
import { useBackendCluster } from '../hooks/useBackendCluster';
import { ClusterHeader } from '../components/cluster/ClusterHeader';
import { LiveNodePanel } from '../components/cluster/LiveNodePanel';
import { ClusterSummary } from '../components/cluster/ClusterSummary';
import { ClusterTopology } from '../components/cluster/ClusterTopology';
import { NodeInspector } from '../components/cluster/NodeInspector';
import { ReplicationMatrix } from '../components/cluster/ReplicationMatrix';
import { RaftStateComparison } from '../components/cluster/RaftStateComparison';
import { ClusterTimeline } from '../components/cluster/ClusterTimeline';
import { ConsistencySummary } from '../components/cluster/ConsistencySummary';

export default function Cluster() {
  const scopeRef = useScrollCraft<HTMLDivElement>();
  const backend = useBackendCluster();
  // The selectable mock topology below still starts on the mock leader --
  // GET /cluster is node-local and has no bearing on which of the three
  // mock nodes is "selected" in that illustration.
  const [selectedId, setSelectedId] = useState(cluster.leader);

  return (
    <div ref={scopeRef}>
      <ClusterHeader cluster={backend.cluster} status={backend.status} />
      <LiveNodePanel status={backend.status} node={backend.liveNode} error={backend.error} />
      <ClusterSummary cluster={backend.cluster} />
      <ClusterTopology selectedId={selectedId} onSelect={setSelectedId} />
      <NodeInspector nodeId={selectedId} />
      <ReplicationMatrix />
      <RaftStateComparison />
      <ClusterTimeline />
      <ConsistencySummary />
    </div>
  );
}
