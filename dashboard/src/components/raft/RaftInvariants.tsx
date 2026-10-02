import { nodes } from '../../data/mockCluster';
import { raftConsensus } from '../../data/raftSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import './RaftInvariants.css';

interface Invariant {
  label: string;
  value: string;
  pass: boolean;
}

/** Explicit correctness conditions, not a generic health score. Each one is independently checkable against the mock state. */
export function RaftInvariants() {
  const leaderCount = nodes.filter((n) => n.role === 'leader').length;
  const termsConsistent = nodes.every((n) => n.term === raftConsensus.term);
  const commitWithinLog = raftConsensus.commitIndex <= raftConsensus.logLength;
  const appliedWithinCommit = raftConsensus.lastApplied <= raftConsensus.commitIndex;
  const followerCount = nodes.filter((n) => n.role === 'follower').length;
  const followersLagging = nodes.filter((n) => n.role === 'follower' && n.lag > 0).length;
  const majorityMet = raftConsensus.replicationCount >= raftConsensus.majorityCount;

  const invariants: Invariant[] = [
    { label: 'Leader', value: `${leaderCount} active leader`, pass: leaderCount === 1 },
    { label: 'Current Term', value: `${raftConsensus.term} consistent`, pass: termsConsistent },
    { label: 'Commit ≤ Log', value: `${raftConsensus.commitIndex} ≤ ${raftConsensus.logLength}`, pass: commitWithinLog },
    {
      label: 'Applied ≤ Commit',
      value: `${raftConsensus.lastApplied} ≤ ${raftConsensus.commitIndex}`,
      pass: appliedWithinCommit,
    },
    { label: 'Follower Lag', value: `${followersLagging} / ${followerCount}`, pass: followersLagging === 0 },
    { label: 'Majority', value: `${raftConsensus.majorityCount} / ${raftConsensus.totalNodes}`, pass: majorityMet },
  ];

  const allPass = invariants.every((i) => i.pass);

  return (
    <section className="raft-invariants" aria-label="Raft invariants">
      <SectionHeader
        eyebrow="Correctness"
        title="Raft Invariants"
        description="Explicit correctness conditions Raft guarantees, checked against the current mock state."
      />

      <div className="raft-invariants__grid panel" data-sc-in data-sc-stagger="50">
        {invariants.map((inv) => (
          <div className="raft-invariants__item" key={inv.label}>
            <span className="raft-invariants__label mono">{inv.label.toUpperCase()}</span>
            <span className="raft-invariants__value mono">{inv.value}</span>
            <StatusIndicator tone={inv.pass ? 'sync' : 'critical'} label={inv.pass ? 'PASS' : 'FAIL'} size="sm" />
          </div>
        ))}
        <div className="raft-invariants__item raft-invariants__item--state">
          <span className="raft-invariants__label mono">STATE</span>
          <StatusIndicator tone={allPass ? 'sync' : 'critical'} label={allPass ? 'CONSISTENT' : 'INCONSISTENT'} />
        </div>
      </div>
    </section>
  );
}
