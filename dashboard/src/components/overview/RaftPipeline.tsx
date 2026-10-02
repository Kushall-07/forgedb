import type { CSSProperties } from 'react';
import { SectionHeader } from '../ui/SectionHeader';
import './RaftPipeline.css';

const STAGES = [
  { key: 'propose', label: 'Propose' },
  { key: 'replicate', label: 'Replicate' },
  { key: 'majority', label: 'Majority' },
  { key: 'commit', label: 'Commit' },
  { key: 'apply', label: 'Apply' },
] as const;

/**
 * The conceptual Raft pipeline every write passes through. A travelling
 * highlight loops across the stages to signal that this is a continuously
 * running process, not a one-time diagram; it carries no state of its own.
 */
export function RaftPipeline() {
  return (
    <section className="raft-pipeline" aria-label="Raft write pipeline">
      <SectionHeader
        eyebrow="Consensus"
        title="Raft State Flow"
        description="Every write moves through the replicated log the same way: proposed by the leader, replicated to followers, committed once a majority acknowledges, then applied to the state machine."
      />

      <ol className="raft-pipeline__track" data-sc-in data-sc-stagger="60">
        {STAGES.map((stage, i) => (
          <li className="raft-pipeline__stage" key={stage.key} style={{ '--i': i } as CSSProperties}>
            <span className="raft-pipeline__dot" aria-hidden="true" />
            <span className="raft-pipeline__label mono">{stage.label}</span>
            {i < STAGES.length - 1 ? <span className="raft-pipeline__arrow" aria-hidden="true">&rarr;</span> : null}
          </li>
        ))}
      </ol>
    </section>
  );
}
