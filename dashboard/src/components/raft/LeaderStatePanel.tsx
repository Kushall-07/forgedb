import { getRaftLeader, raftConsensus } from '../../data/raftSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './LeaderStatePanel.css';

/** The leader's authoritative Raft fields, plus the heartbeat cycle that currently keeps it leader. */
export function LeaderStatePanel() {
  const leader = getRaftLeader();
  if (!leader) return null;

  return (
    <section className="leader-state" aria-label="Leader and election state">
      <SectionHeader
        eyebrow="Leadership"
        title="Leader / Election State"
        description="The current leader's authoritative Raft state and the heartbeat cycle that keeps it leader."
      />

      <div className="leader-state__panel panel" data-sc-in>
        <div className="leader-state__head">
          <StatusIndicator tone="identity" label="LEADER" pulse />
          <span className="leader-state__node mono">{leader.nodeId}</span>
        </div>

        <dl className="leader-state__rows">
          <div className="leader-state__row">
            <dt>Term</dt>
            <dd className="mono">{leader.term}</dd>
          </div>
          <div className="leader-state__row">
            <dt>Voted For</dt>
            <dd className="mono">{leader.votedFor}</dd>
          </div>
          <div className="leader-state__row">
            <dt>Last Log Index</dt>
            <dd className="mono">{leader.lastLogIndex}</dd>
          </div>
          <div className="leader-state__row">
            <dt>Last Log Term</dt>
            <dd className="mono">{leader.lastLogTerm}</dd>
          </div>
          <div className="leader-state__row">
            <dt>Commit Index</dt>
            <dd className="mono">{leader.commitIndex}</dd>
          </div>
          <div className="leader-state__row">
            <dt>Last Applied</dt>
            <dd className="mono">{leader.lastApplied}</dd>
          </div>
        </dl>

        <div className="leader-state__divider" role="presentation" />

        <TechnicalLabel as="div">Mocked Control-Plane State</TechnicalLabel>
        <dl className="leader-state__rows leader-state__rows--heartbeat">
          <div className="leader-state__row">
            <dt>Election Timer</dt>
            <dd>
              <StatusIndicator tone="sync" label={raftConsensus.electionState} size="sm" />
            </dd>
          </div>
          <div className="leader-state__row">
            <dt>Heartbeat</dt>
            <dd>
              <StatusIndicator tone="sync" label="ACTIVE" size="sm" pulse />
            </dd>
          </div>
          <div className="leader-state__row">
            <dt>Quorum</dt>
            <dd className="mono">
              {raftConsensus.majorityCount} / {raftConsensus.totalNodes}
            </dd>
          </div>
          <div className="leader-state__row">
            <dt>Last Heartbeat Sent</dt>
            <dd className="mono">~180 ms ago</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}
