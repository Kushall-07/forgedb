import { getFollowers, nextIndexFor } from '../../data/clusterSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import './ReplicationMatrix.css';

const TONE: Record<string, StatusTone> = {
  active: 'sync',
  synced: 'sync',
  lagging: 'warn',
  unreachable: 'critical',
};

export function ReplicationMatrix() {
  const followers = getFollowers();

  return (
    <section className="replication-matrix" aria-label="Replication matrix">
      <SectionHeader
        eyebrow="Peers"
        title="Replication Matrix"
        description="What the leader knows about each follower's log position right now."
      />

      <div className="replication-matrix__table-wrap panel" data-sc-in>
        <table className="replication-matrix__table">
          <thead>
            <tr>
              <th scope="col">Peer</th>
              <th scope="col">Role</th>
              <th scope="col">Next Index</th>
              <th scope="col">Match Index</th>
              <th scope="col">Lag</th>
              <th scope="col">State</th>
            </tr>
          </thead>
          <tbody>
            {followers.map((node) => (
              <tr key={node.id}>
                <td className="mono">{node.id}</td>
                <td className="mono">{node.role.toUpperCase()}</td>
                <td className="mono">{nextIndexFor(node)}</td>
                <td className="mono">{node.matchIndex}</td>
                <td className="mono">{node.lag}</td>
                <td>
                  <div className="replication-matrix__state">
                    <StatusIndicator tone={TONE[node.status]} label={node.status.toUpperCase()} size="sm" />
                    <span
                      className={`replication-matrix__bar replication-matrix__bar--${node.lag === 0 ? 'full' : 'partial'}`}
                      aria-hidden="true"
                    />
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
