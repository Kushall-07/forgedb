import { nodes } from '../../data/mockCluster';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import './ReplicationStatus.css';

const TONE: Record<string, StatusTone> = {
  active: 'sync',
  synced: 'sync',
  lagging: 'warn',
  unreachable: 'critical',
};

export function ReplicationStatus() {
  const followers = nodes.filter((n) => n.role === 'follower');

  return (
    <section className="replication-status" aria-label="Replication status">
      <SectionHeader
        eyebrow="Peers"
        title="Replication Status"
        description="Match index and lag for every follower acknowledging the leader's log."
      />

      <div className="replication-status__table-wrap panel" data-sc-in>
        <table className="replication-status__table">
          <thead>
            <tr>
              <th scope="col">Peer</th>
              <th scope="col">Role</th>
              <th scope="col">Term</th>
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
                <td className="mono">{node.term}</td>
                <td className="mono">{node.matchIndex}</td>
                <td className="mono">{node.lag}</td>
                <td>
                  <StatusIndicator tone={TONE[node.status]} label={node.status.toUpperCase()} size="sm" />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
