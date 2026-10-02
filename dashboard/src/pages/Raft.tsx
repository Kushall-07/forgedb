import { useState } from 'react';
import { useScrollCraft } from '../lib/scrollcraft/useScrollCraft';
import { cluster } from '../data/mockCluster';
import { raftLog } from '../data/mockRaft';
import { RaftHeader } from '../components/raft/RaftHeader';
import { RaftSummary } from '../components/raft/RaftSummary';
import { RaftNodeSelector } from '../components/raft/RaftNodeSelector';
import { LeaderStatePanel } from '../components/raft/LeaderStatePanel';
import { FollowerStatePanel } from '../components/raft/FollowerStatePanel';
import { ReplicationPipeline } from '../components/raft/ReplicationPipeline';
import { RaftLogInspector } from '../components/raft/RaftLogInspector';
import { ReplicationMatrix } from '../components/raft/ReplicationMatrix';
import { CommitApplyBoundary } from '../components/raft/CommitApplyBoundary';
import { RaftEventTimeline } from '../components/raft/RaftEventTimeline';
import { SnapshotState } from '../components/raft/SnapshotState';
import { RaftInvariants } from '../components/raft/RaftInvariants';

export default function Raft() {
  const scopeRef = useScrollCraft<HTMLDivElement>();
  const [selectedNodeId, setSelectedNodeId] = useState(cluster.leader);
  const [selectedLogIndex, setSelectedLogIndex] = useState<number | null>(
    raftLog[raftLog.length - 1]?.index ?? null,
  );

  return (
    <div ref={scopeRef}>
      <RaftHeader />
      <RaftSummary />
      <RaftNodeSelector selectedNodeId={selectedNodeId} onSelect={setSelectedNodeId} />
      <LeaderStatePanel />
      <FollowerStatePanel />
      <ReplicationPipeline />
      <RaftLogInspector selectedIndex={selectedLogIndex} onSelect={setSelectedLogIndex} />
      <ReplicationMatrix />
      <CommitApplyBoundary />
      <RaftEventTimeline />
      <SnapshotState />
      <RaftInvariants />
    </div>
  );
}
