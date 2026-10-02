import type { NodeState } from '../../types/cluster';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import './ClusterNode.css';

interface ClusterNodeProps {
  node: NodeState;
  selected: boolean;
  onSelect: (id: string) => void;
  className?: string;
}

const HEALTH_TONE: Record<NodeState['status'], StatusTone> = {
  active: 'sync',
  synced: 'sync',
  lagging: 'warn',
  unreachable: 'critical',
};

const HEALTH_LABEL: Record<NodeState['status'], string> = {
  active: 'ACTIVE',
  synced: 'SYNCED',
  lagging: 'LAGGING',
  unreachable: 'UNREACHABLE',
};

/**
 * One node in the topology: identity, role and the full Raft readout a
 * follower vs. leader needs, always visible (this is a console, not a
 * teaser). Clicking or pressing Enter/Space selects it for the inspector
 * panel below; the same selection state is exposed to keyboard focus and to
 * assistive tech via aria-pressed, never through color alone.
 */
export function ClusterNode({ node, selected, onSelect, className = '' }: ClusterNodeProps) {
  const isLeader = node.role === 'leader';
  const tone = HEALTH_TONE[node.status];

  return (
    <button
      type="button"
      className={`cluster-node cluster-node--${node.role} cluster-node--${tone} ${selected ? 'cluster-node--selected' : ''} ${className}`}
      data-sc-tilt="4"
      data-sc-spotlight
      aria-pressed={selected}
      aria-label={`${node.id}, ${node.role}, ${HEALTH_LABEL[node.status]}. Select to inspect.`}
      onClick={() => onSelect(node.id)}
    >
      <div className="cluster-node__head">
        <StatusIndicator tone={tone} label={isLeader ? 'LEADER' : 'FOLLOWER'} pulse={isLeader} size="sm" />
        <span className="cluster-node__id mono">{node.id}</span>
      </div>

      <dl className="cluster-node__stats">
        <div className="cluster-node__stat">
          <dt>Term</dt>
          <dd className="mono">{node.term}</dd>
        </div>
        <div className="cluster-node__stat">
          <dt>Commit</dt>
          <dd className="mono">{node.commitIndex}</dd>
        </div>
        <div className="cluster-node__stat">
          <dt>Applied</dt>
          <dd className="mono">{node.lastApplied}</dd>
        </div>
        <div className="cluster-node__stat">
          <dt>Log</dt>
          <dd className="mono">{node.lastLogIndex}</dd>
        </div>
        {!isLeader ? (
          <>
            <div className="cluster-node__stat">
              <dt>Match</dt>
              <dd className="mono">{node.matchIndex}</dd>
            </div>
            <div className="cluster-node__stat">
              <dt>Lag</dt>
              <dd className="mono">{node.lag}</dd>
            </div>
          </>
        ) : null}
      </dl>

      <div className="cluster-node__footer">
        <span className="cluster-node__health mono">{HEALTH_LABEL[node.status]}</span>
        <span className="cluster-node__hint mono">{selected ? 'INSPECTING' : 'SELECT TO INSPECT'}</span>
      </div>
    </button>
  );
}
