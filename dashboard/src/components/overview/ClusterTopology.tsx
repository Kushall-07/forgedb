import { nodes } from '../../data/mockCluster';
import { SectionHeader } from '../ui/SectionHeader';
import { NodeCard } from './NodeCard';
import './ClusterTopology.css';

const FOLLOWER_X = [16, 84];

export function ClusterTopology() {
  const leader = nodes.find((n) => n.role === 'leader');
  const followers = nodes.filter((n) => n.role === 'follower');

  if (!leader) return null;

  return (
    <section className="topology" aria-label="Cluster topology">
      <SectionHeader
        eyebrow="Replication"
        title="Cluster Topology"
        description="The leader replicates committed entries to followers through AppendEntries. Followers acknowledge; they never write independently to storage."
      />

      <div className="topology__stage" data-sc-in>
        <svg
          className="topology__lines"
          viewBox="0 0 100 100"
          preserveAspectRatio="none"
          aria-hidden="true"
          focusable="false"
        >
          <defs>
            <marker id="topology-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
              <path d="M0,0 L10,5 L0,10 Z" fill="var(--status-sync)" />
            </marker>
          </defs>
          {followers.map((follower, i) => (
            <line
              key={follower.id}
              className="topology__line"
              x1="50"
              y1="26"
              x2={FOLLOWER_X[i]}
              y2="60"
              markerEnd="url(#topology-arrow)"
            />
          ))}
        </svg>

        <div className="topology__node topology__node--leader">
          <NodeCard node={leader} />
        </div>

        <div className="topology__followers">
          {followers.map((follower) => (
            <div className="topology__node topology__node--follower" key={follower.id}>
              <NodeCard node={follower} />
            </div>
          ))}
        </div>
      </div>

      <p className="topology__caption mono">
        Leader <span className="topology__caption-arrow">&rarr;</span> AppendEntries <span className="topology__caption-arrow">&rarr;</span> Majority Ack <span className="topology__caption-arrow">&rarr;</span> Commit
      </p>
    </section>
  );
}
