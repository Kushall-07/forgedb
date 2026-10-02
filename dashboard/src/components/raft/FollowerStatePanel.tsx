import { getNode } from '../../data/clusterSelectors';
import { getRaftFollowers } from '../../data/raftSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import './FollowerStatePanel.css';

/** Each follower's own view of the log it has received and the state machine it has applied. */
export function FollowerStatePanel() {
  const followers = getRaftFollowers();

  return (
    <section className="follower-state" aria-label="Follower state comparison">
      <SectionHeader
        eyebrow="Replication"
        title="Follower State"
        description="Each follower's own view of the log it has received and the state machine it has applied."
      />

      <div className="follower-state__grid" data-sc-in data-sc-stagger="70">
        {followers.map((node) => {
          const clusterNode = getNode(node.nodeId);
          const caughtUp = clusterNode ? clusterNode.lag === 0 : false;
          return (
            <div className="follower-state__card panel" key={node.nodeId}>
              <div className="follower-state__head">
                <StatusIndicator tone="sync" label="FOLLOWER" size="sm" />
                <span className="follower-state__node mono">{node.nodeId}</span>
              </div>

              <dl className="follower-state__rows">
                <div className="follower-state__row">
                  <dt>Term</dt>
                  <dd className="mono">{node.term}</dd>
                </div>
                <div className="follower-state__row">
                  <dt>Last Log Index</dt>
                  <dd className="mono">{node.lastLogIndex}</dd>
                </div>
                <div className="follower-state__row">
                  <dt>Last Log Term</dt>
                  <dd className="mono">{node.lastLogTerm}</dd>
                </div>
                <div className="follower-state__row">
                  <dt>Commit Index</dt>
                  <dd className="mono">{node.commitIndex}</dd>
                </div>
                <div className="follower-state__row">
                  <dt>Last Applied</dt>
                  <dd className="mono">{node.lastApplied}</dd>
                </div>
                <div className="follower-state__row">
                  <dt>Next Index</dt>
                  <dd className="mono">{node.nextIndex}</dd>
                </div>
                <div className="follower-state__row">
                  <dt>Match Index</dt>
                  <dd className="mono">{node.matchIndex}</dd>
                </div>
              </dl>

              <div className="follower-state__footer">
                <StatusIndicator
                  tone={caughtUp ? 'sync' : 'warn'}
                  label={caughtUp ? 'CAUGHT UP' : 'CATCHING UP'}
                  size="sm"
                />
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
