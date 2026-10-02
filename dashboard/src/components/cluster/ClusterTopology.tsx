import { getFollowers, getLeader, isClusterConverged } from '../../data/clusterSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { ClusterNode } from './ClusterNode';
import './ClusterTopology.css';

const FOLLOWER_X = [50, 16];
const FOLLOWER_Y = [6, 72];

interface ClusterTopologyProps {
  selectedId: string;
  onSelect: (id: string) => void;
}

/**
 * The centerpiece: leader bottom-right, replicating AppendEntries out to
 * every follower. Followers only ever acknowledge; they never originate a
 * write, which is why the arrows are one-directional and the convergence
 * point reads MAJORITY rather than a second write path.
 */
export function ClusterTopology({ selectedId, onSelect }: ClusterTopologyProps) {
  const leader = getLeader();
  const followers = getFollowers();
  const converged = isClusterConverged();

  if (!leader) return null;

  return (
    <section className="cluster-topology" aria-label="Cluster topology">
      <SectionHeader
        eyebrow="Replication"
        title="Raft Topology"
        description="The leader replicates every committed entry to each follower through AppendEntries. Followers acknowledge; they never mutate the state machine independently."
      />

      <div className="cluster-topology__stage" data-sc-in>
        <svg
          className="cluster-topology__lines"
          viewBox="0 0 100 100"
          preserveAspectRatio="none"
          aria-hidden="true"
          focusable="false"
        >
          <defs>
            <marker id="cluster-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
              <path d="M0,0 L10,5 L0,10 Z" fill="var(--status-sync)" />
            </marker>
          </defs>
          {followers.map((follower, i) => (
            <line
              key={follower.id}
              className="cluster-topology__line"
              x1="84"
              y1="88"
              x2={FOLLOWER_X[i]}
              y2={FOLLOWER_Y[i] + 12}
              markerEnd="url(#cluster-arrow)"
            />
          ))}
        </svg>

        {followers.map((follower, i) => (
          <span
            key={follower.id}
            className="cluster-topology__flow-label mono"
            style={{ top: `${(FOLLOWER_Y[i] + 12 + 88) / 2 - 4}%`, left: `${(FOLLOWER_X[i] + 84) / 2}%` }}
          >
            AppendEntries
          </span>
        ))}

        <div className="cluster-topology__node cluster-topology__node--leader">
          <ClusterNode node={leader} selected={selectedId === leader.id} onSelect={onSelect} />
        </div>

        {followers.map((follower, i) => (
          <div
            key={follower.id}
            className="cluster-topology__node cluster-topology__node--follower"
            style={{ top: `${FOLLOWER_Y[i]}%`, left: `${FOLLOWER_X[i]}%` }}
          >
            <ClusterNode node={follower} selected={selectedId === follower.id} onSelect={onSelect} />
          </div>
        ))}

        <div className={`cluster-topology__majority ${converged ? 'cluster-topology__majority--achieved' : ''}`}>
          <span className="mono">MAJORITY</span>
        </div>
      </div>

      <p className="cluster-topology__caption mono">
        Leader <span className="cluster-topology__caption-arrow">&rarr;</span> AppendEntries{' '}
        <span className="cluster-topology__caption-arrow">&rarr;</span> Majority Ack{' '}
        <span className="cluster-topology__caption-arrow">&rarr;</span> Commit
      </p>
    </section>
  );
}
