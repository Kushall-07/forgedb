import { raftLog } from '../../data/mockRaft';
import { raftConsensus } from '../../data/raftSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './CommitApplyBoundary.css';

/**
 * A log-index number line with the commit index and last-applied index
 * marked independently, even though they currently coincide at 4: the two
 * are distinct events (commitment vs. state-machine application), not one.
 */
export function CommitApplyBoundary() {
  const indices = raftLog.map((e) => e.index);

  return (
    <section className="commit-apply-boundary" aria-label="Commit and apply boundary">
      <SectionHeader
        eyebrow="Consensus"
        title="Commit / Apply Boundary"
        description="Raft commit establishes the replicated ordering. State-machine application follows committed entries."
      />

      <div className="commit-apply-boundary__panel panel" data-sc-in>
        <TechnicalLabel as="div">Log Index</TechnicalLabel>

        <p className="sr-only">
          Log indices 1 through {indices.length}. Commit index {raftConsensus.commitIndex}. Last applied{' '}
          {raftConsensus.lastApplied}.
        </p>

        <div className="commit-apply-boundary__scroll">
          <div
            className="commit-apply-boundary__grid"
            style={{ gridTemplateColumns: `repeat(${indices.length}, minmax(96px, 1fr))` }}
            aria-hidden="true"
          >
            {indices.map((index) => (
              <div
                key={`commit-${index}`}
                className={`commit-apply-boundary__marker-cell ${
                  index === raftConsensus.commitIndex ? 'commit-apply-boundary__marker-cell--active' : ''
                }`}
              >
                {index === raftConsensus.commitIndex ? (
                  <>
                    <span className="commit-apply-boundary__marker-label commit-apply-boundary__marker-label--commit mono">
                      COMMIT INDEX = {index}
                    </span>
                    <span className="commit-apply-boundary__caret commit-apply-boundary__caret--down" />
                  </>
                ) : null}
              </div>
            ))}

            {indices.map((index) => (
              <div key={`idx-${index}`} className="commit-apply-boundary__index-cell mono">
                {index}
              </div>
            ))}

            {indices.map((index) => (
              <div
                key={`applied-${index}`}
                className={`commit-apply-boundary__marker-cell ${
                  index === raftConsensus.lastApplied ? 'commit-apply-boundary__marker-cell--active' : ''
                }`}
              >
                {index === raftConsensus.lastApplied ? (
                  <>
                    <span className="commit-apply-boundary__caret commit-apply-boundary__caret--up" />
                    <span className="commit-apply-boundary__marker-label commit-apply-boundary__marker-label--applied mono">
                      LAST APPLIED = {index}
                    </span>
                  </>
                ) : null}
              </div>
            ))}
          </div>
        </div>

        <p className="commit-apply-boundary__note">
          Committed entries have crossed the Raft majority boundary. Applied entries have been delivered to the
          state machine.
        </p>
      </div>
    </section>
  );
}
