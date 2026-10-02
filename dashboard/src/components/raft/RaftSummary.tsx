import { raftConsensus } from '../../data/raftSelectors';
import { MetricValue } from '../ui/MetricValue';
import './RaftSummary.css';

/** The eight Raft indexes/counts that answer "what state is consensus in right now" at a glance. No invented health score. */
export function RaftSummary() {
  return (
    <section className="raft-summary" aria-label="Consensus state summary" data-sc-in data-sc-stagger="60">
      <MetricValue label="Current Term" value={raftConsensus.term} />
      <MetricValue label="Leader" value={raftConsensus.leader} emphasis tone="identity" />
      <MetricValue label="Commit Index" value={raftConsensus.commitIndex} />
      <MetricValue label="Last Applied" value={raftConsensus.lastApplied} />
      <MetricValue label="Log Length" value={raftConsensus.logLength} />
      <MetricValue label="Majority" value={`${raftConsensus.majorityCount} / ${raftConsensus.totalNodes}`} tone="sync" />
      <MetricValue label="Election State" value={raftConsensus.electionState} tone="sync" />
      <MetricValue
        label="Replication"
        value={`${raftConsensus.replicationCount} / ${raftConsensus.totalNodes}`}
        tone="sync"
        footnote="All peers acknowledging"
      />
    </section>
  );
}
