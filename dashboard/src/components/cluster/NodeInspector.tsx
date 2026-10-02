import { getFollowers, getLeader, getNode, nextIndexFor } from '../../data/clusterSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './NodeInspector.css';

const TONE: Record<string, StatusTone> = {
  active: 'sync',
  synced: 'sync',
  lagging: 'warn',
  unreachable: 'critical',
};

interface NodeInspectorProps {
  nodeId: string;
}

/**
 * The selected node's full diagnostic readout: identity, Raft position, and
 * its replication relationship to the rest of the cluster. A leader shows
 * every peer it is replicating to; a follower shows only its own
 * relationship to the leader, since that is the only link it has.
 */
export function NodeInspector({ nodeId }: NodeInspectorProps) {
  const node = getNode(nodeId);
  const leader = getLeader();
  const followers = getFollowers();

  if (!node) return null;

  const isLeader = node.role === 'leader';
  const peers = isLeader ? followers : leader ? [leader] : [];

  return (
    <section className="node-inspector" aria-label="Node inspector">
      <SectionHeader
        eyebrow="Inspection"
        title="Node Inspector"
        description="Observation only: the detailed Raft and replication state of the selected node."
      />

      <div className="node-inspector__panel panel" data-sc-in>
        <div className="node-inspector__selected mono">
          SELECTED NODE <span className="node-inspector__selected-id">{node.id}</span>
        </div>

        <div className="node-inspector__groups">
          <div className="node-inspector__group">
            <TechnicalLabel as="div">Identity</TechnicalLabel>
            <dl className="node-inspector__rows">
              <div className="node-inspector__row">
                <dt>Node ID</dt>
                <dd className="mono">{node.id}</dd>
              </div>
              <div className="node-inspector__row">
                <dt>Role</dt>
                <dd className="mono">{isLeader ? 'Leader' : 'Follower'}</dd>
              </div>
              <div className="node-inspector__row">
                <dt>Status</dt>
                <dd>
                  <StatusIndicator tone={TONE[node.status]} label={node.status.toUpperCase()} size="sm" />
                </dd>
              </div>
              <div className="node-inspector__row">
                <dt>Peers</dt>
                <dd className="mono">{peers.map((p) => p.id).join(', ') || '—'}</dd>
              </div>
            </dl>
          </div>

          <div className="node-inspector__group">
            <TechnicalLabel as="div">Raft</TechnicalLabel>
            <dl className="node-inspector__rows">
              <div className="node-inspector__row">
                <dt>Current Term</dt>
                <dd className="mono">{node.term}</dd>
              </div>
              <div className="node-inspector__row">
                <dt>Commit Index</dt>
                <dd className="mono">{node.commitIndex}</dd>
              </div>
              <div className="node-inspector__row">
                <dt>Last Applied</dt>
                <dd className="mono">{node.lastApplied}</dd>
              </div>
              <div className="node-inspector__row">
                <dt>Last Log Index</dt>
                <dd className="mono">{node.lastLogIndex}</dd>
              </div>
              <div className="node-inspector__row">
                <dt>Last Log Term</dt>
                <dd className="mono">{node.lastLogTerm}</dd>
              </div>
            </dl>
          </div>

          <div className="node-inspector__group">
            <TechnicalLabel as="div">{isLeader ? 'Replication to Peers' : 'Replication from Leader'}</TechnicalLabel>
            <div className="node-inspector__peers">
              {peers.map((peer) => (
                <div className="node-inspector__peer" key={peer.id}>
                  <span className="node-inspector__peer-id mono">{peer.id}</span>
                  <dl className="node-inspector__peer-rows">
                    <div className="node-inspector__row">
                      <dt>Next Index</dt>
                      <dd className="mono">{nextIndexFor(isLeader ? peer : node)}</dd>
                    </div>
                    <div className="node-inspector__row">
                      <dt>Match Index</dt>
                      <dd className="mono">{isLeader ? peer.matchIndex : node.matchIndex}</dd>
                    </div>
                    <div className="node-inspector__row">
                      <dt>Lag</dt>
                      <dd className="mono">{isLeader ? peer.lag : node.lag}</dd>
                    </div>
                    <div className="node-inspector__row">
                      <dt>State</dt>
                      <dd>
                        <StatusIndicator
                          tone={TONE[isLeader ? peer.status : node.status]}
                          label={(isLeader ? peer.status : node.status).toUpperCase()}
                          size="sm"
                        />
                      </dd>
                    </div>
                  </dl>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
