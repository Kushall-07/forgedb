import { useScrollCraft } from '../lib/scrollcraft/useScrollCraft';
import { useBackendCluster } from '../hooks/useBackendCluster';
import { ClusterSummary } from '../components/overview/ClusterSummary';
import { ClusterTopology } from '../components/overview/ClusterTopology';
import { EventTimeline } from '../components/overview/EventTimeline';
import { RaftPipeline } from '../components/overview/RaftPipeline';
import { ReplicationStatus } from '../components/overview/ReplicationStatus';
import { StorageSnapshot } from '../components/overview/StorageSnapshot';
import { SystemHealth } from '../components/overview/SystemHealth';

export default function Overview() {
  const scopeRef = useScrollCraft<HTMLDivElement>();
  const backend = useBackendCluster();

  return (
    <div ref={scopeRef}>
      <ClusterSummary cluster={backend.cluster} status={backend.status} />
      <ClusterTopology />
      <RaftPipeline />
      <ReplicationStatus />
      <StorageSnapshot storage={backend.storage} status={backend.status} />
      <EventTimeline />
      <SystemHealth />
    </div>
  );
}
