import { raftNodes } from '../../data/mockRaft';
import { getRaftNode } from '../../data/raftSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import './RaftNodeSelector.css';

interface RaftNodeSelectorProps {
  selectedNodeId: string;
  onSelect: (nodeId: string) => void;
}

const ROLE_TONE: Record<string, StatusTone> = {
  LEADER: 'identity',
  FOLLOWER: 'sync',
  CANDIDATE: 'warn',
};

/**
 * Inspection only: selecting a node re-reads its own Raft fields below, the
 * same way the real node would report them. No action here can change what
 * it reports.
 */
export function RaftNodeSelector({ selectedNodeId, onSelect }: RaftNodeSelectorProps) {
  const selected = getRaftNode(selectedNodeId);

  return (
    <section className="raft-node-selector" aria-label="Node inspector">
      <SectionHeader
        eyebrow="Inspection"
        title="Node Inspector"
        description="Select any node to read its Raft state exactly as that node reports it."
      />

      <div className="raft-node-selector__tabs" role="tablist" aria-label="Select a Raft node" data-sc-in>
        {raftNodes.map((node) => (
          <button
            key={node.nodeId}
            type="button"
            role="tab"
            aria-selected={selectedNodeId === node.nodeId}
            className={`raft-node-selector__tab ${
              selectedNodeId === node.nodeId ? 'raft-node-selector__tab--selected' : ''
            }`}
            onClick={() => onSelect(node.nodeId)}
          >
            <StatusIndicator tone={ROLE_TONE[node.role]} label={node.role} size="sm" />
            <span className="mono">{node.nodeId}</span>
          </button>
        ))}
      </div>

      {selected ? (
        <div className="raft-node-selector__panel panel" data-sc-in>
          <dl className="raft-node-selector__rows">
            <div className="raft-node-selector__row">
              <dt>Role</dt>
              <dd className="mono">{selected.role}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Term</dt>
              <dd className="mono">{selected.term}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Voted For</dt>
              <dd className="mono">{selected.votedFor ?? '—'}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Last Log Index</dt>
              <dd className="mono">{selected.lastLogIndex}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Last Log Term</dt>
              <dd className="mono">{selected.lastLogTerm}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Commit Index</dt>
              <dd className="mono">{selected.commitIndex}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Last Applied</dt>
              <dd className="mono">{selected.lastApplied}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Next Index</dt>
              <dd className="mono">{selected.nextIndex ?? 'N/A'}</dd>
            </div>
            <div className="raft-node-selector__row">
              <dt>Match Index</dt>
              <dd className="mono">{selected.matchIndex ?? 'N/A'}</dd>
            </div>
          </dl>
        </div>
      ) : null}
    </section>
  );
}
