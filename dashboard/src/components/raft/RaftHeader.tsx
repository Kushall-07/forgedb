import { GitBranch } from 'lucide-react';
import { raftConsensus } from '../../data/raftSelectors';
import { StatusIndicator } from '../ui/StatusIndicator';
import './RaftHeader.css';

/** Compact page header, not a hero: this console is read many times a day. */
export function RaftHeader() {
  return (
    <section className="raft-header" aria-label="Raft header" data-sc-in>
      <div className="raft-header__identity">
        <span className="raft-header__icon" aria-hidden="true">
          <GitBranch size={20} strokeWidth={1.75} />
        </span>
        <div>
          <span className="raft-header__eyebrow mono">RAFT / CONSENSUS CONTROL PLANE</span>
          <h1 className="raft-header__title">Raft Consensus</h1>
          <p className="raft-header__subtitle">
            Inspect leader state, replicated log progress, quorum commitment, and state-machine application.
          </p>
        </div>
      </div>

      <div className="raft-header__context">
        <div className="raft-header__field">
          <span className="raft-header__field-label mono">CLUSTER</span>
          <span className="raft-header__field-value mono">{raftConsensus.cluster}</span>
        </div>
        <div className="raft-header__field">
          <span className="raft-header__field-label mono">LEADER</span>
          <span className="raft-header__field-value raft-header__field-value--identity mono">
            {raftConsensus.leader}
          </span>
        </div>
        <div className="raft-header__field">
          <span className="raft-header__field-label mono">TERM</span>
          <span className="raft-header__field-value mono">{raftConsensus.term}</span>
        </div>
        <div className="raft-header__field">
          <span className="raft-header__field-label mono">STATE</span>
          <StatusIndicator tone="sync" label={raftConsensus.electionState} size="sm" pulse />
        </div>
      </div>
    </section>
  );
}
