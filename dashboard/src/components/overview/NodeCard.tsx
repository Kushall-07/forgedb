import type { NodeState } from '../../types/cluster';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import './NodeCard.css';

interface NodeCardProps {
  node: NodeState;
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
 * A cluster node. Always shows its id, role and health; the full technical
 * readout (commit/apply/match indices) is revealed on hover AND on keyboard
 * focus, so the detail is not mouse-only.
 */
export function NodeCard({ node, className = '' }: NodeCardProps) {
  const isLeader = node.role === 'leader';
  const tone = HEALTH_TONE[node.status];

  return (
    <div
      className={`node-card node-card--${node.role} node-card--${tone} ${className}`}
      data-sc-tilt="5"
      data-sc-spotlight
      tabIndex={0}
      role="group"
      aria-label={`${node.id}, ${node.role}, ${HEALTH_LABEL[node.status]}`}
    >
      <div className="node-card__summary">
        <StatusIndicator tone={tone} label={isLeader ? 'LEADER' : 'FOLLOWER'} pulse={isLeader} size="sm" />
        <div className="node-card__id mono">{node.id}</div>
        <div className="node-card__meta mono">
          {isLeader ? `TERM ${node.term} · ACTIVE` : `LAG ${node.lag} · ${HEALTH_LABEL[node.status]}`}
        </div>
      </div>

      <dl className="node-card__detail">
        <div className="node-card__detail-row">
          <dt>Node ID</dt>
          <dd className="mono">{node.id}</dd>
        </div>
        <div className="node-card__detail-row">
          <dt>Role</dt>
          <dd className="mono">{node.role}</dd>
        </div>
        <div className="node-card__detail-row">
          <dt>Term</dt>
          <dd className="mono">{node.term}</dd>
        </div>
        <div className="node-card__detail-row">
          <dt>Commit Index</dt>
          <dd className="mono">{node.commitIndex}</dd>
        </div>
        <div className="node-card__detail-row">
          <dt>Last Applied</dt>
          <dd className="mono">{node.lastApplied}</dd>
        </div>
        <div className="node-card__detail-row">
          <dt>Match Index</dt>
          <dd className="mono">{node.matchIndex}</dd>
        </div>
        <div className="node-card__detail-row">
          <dt>Lag</dt>
          <dd className="mono">{node.lag}</dd>
        </div>
        <div className="node-card__detail-row">
          <dt>Health</dt>
          <dd className="mono">{HEALTH_LABEL[node.status]}</dd>
        </div>
      </dl>
    </div>
  );
}
