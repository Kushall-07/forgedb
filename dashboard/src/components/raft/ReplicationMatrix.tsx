import { nodes } from '../../data/mockCluster';
import { raftNodes } from '../../data/mockRaft';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import './ReplicationMatrix.css';

const ROLE_TONE: Record<string, StatusTone> = {
  LEADER: 'identity',
  FOLLOWER: 'sync',
  CANDIDATE: 'warn',
};

/** Every node's log position, replication progress, and apply state in one surface. The leader's nextIndex/matchIndex are hidden rather than faked. */
export function ReplicationMatrix() {
  return (
    <section className="raft-replication-matrix" aria-label="Replication progress matrix">
      <SectionHeader
        eyebrow="Peers"
        title="Replication Progress Matrix"
        description="Every node's log position, replication progress, and apply state in one surface."
      />

      <div className="raft-replication-matrix__table-wrap panel" data-sc-in>
        <table className="raft-replication-matrix__table">
          <thead>
            <tr>
              <th scope="col">Node</th>
              <th scope="col">Role</th>
              <th scope="col">Term</th>
              <th scope="col">Last Log</th>
              <th scope="col">Last Log Term</th>
              <th scope="col">Next Index</th>
              <th scope="col">Match Index</th>
              <th scope="col">Commit Index</th>
              <th scope="col">Applied</th>
              <th scope="col">Lag</th>
              <th scope="col">State</th>
            </tr>
          </thead>
          <tbody>
            {raftNodes.map((node) => {
              const clusterNode = nodes.find((n) => n.id === node.nodeId);
              const isLeader = node.role === 'LEADER';
              const caughtUp = clusterNode ? clusterNode.lag === 0 : true;
              return (
                <tr key={node.nodeId}>
                  <td className="mono">{node.nodeId}</td>
                  <td>
                    <StatusIndicator tone={ROLE_TONE[node.role]} label={node.role} size="sm" />
                  </td>
                  <td className="mono">{node.term}</td>
                  <td className="mono">{node.lastLogIndex}</td>
                  <td className="mono">{node.lastLogTerm}</td>
                  <td className="mono">{node.nextIndex ?? '—'}</td>
                  <td className="mono">{isLeader ? '—' : node.matchIndex}</td>
                  <td className="mono">{node.commitIndex}</td>
                  <td className="mono">{node.lastApplied}</td>
                  <td className="mono">{clusterNode?.lag ?? 0}</td>
                  <td>
                    <StatusIndicator
                      tone={isLeader ? 'identity' : caughtUp ? 'sync' : 'warn'}
                      label={isLeader ? 'LEADER' : caughtUp ? 'CAUGHT UP' : 'CATCHING UP'}
                      size="sm"
                    />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
