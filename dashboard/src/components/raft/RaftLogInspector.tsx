import { Fragment, type KeyboardEvent } from 'react';
import { raftLog } from '../../data/mockRaft';
import { raftConsensus } from '../../data/raftSelectors';
import { SectionHeader } from '../ui/SectionHeader';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './RaftLogInspector.css';

interface RaftLogInspectorProps {
  selectedIndex: number | null;
  onSelect: (index: number) => void;
}

/**
 * The central table for inspecting the replicated log. Rows are keyboard
 * selectable (tabIndex + Enter/Space + aria-selected) rather than buttons, so
 * the table keeps its native row/cell semantics for assistive tech.
 */
export function RaftLogInspector({ selectedIndex, onSelect }: RaftLogInspectorProps) {
  const selected = raftLog.find((e) => e.index === selectedIndex) ?? null;

  function handleKeyDown(e: KeyboardEvent<HTMLTableRowElement>, index: number) {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      onSelect(index);
    }
  }

  return (
    <section className="raft-log-inspector" aria-label="Raft log inspector">
      <SectionHeader
        eyebrow="Replicated Log"
        title="Raft Log Inspector"
        description="Every entry the leader has proposed, in index order. Select a row for its full detail."
      />

      <div className="raft-log-inspector__layout">
        <div className="raft-log-inspector__table-wrap panel" data-sc-in>
          <table className="raft-log-inspector__table">
            <thead>
              <tr>
                <th scope="col">Index</th>
                <th scope="col">Term</th>
                <th scope="col">Command</th>
                <th scope="col">Status</th>
                <th scope="col">Committed</th>
                <th scope="col">Applied</th>
              </tr>
            </thead>
            <tbody>
              {raftLog.map((entry) => (
                <Fragment key={entry.index}>
                  <tr
                    className={`raft-log-inspector__row ${
                      selectedIndex === entry.index ? 'raft-log-inspector__row--selected' : ''
                    }`}
                    tabIndex={0}
                    aria-selected={selectedIndex === entry.index}
                    onClick={() => onSelect(entry.index)}
                    onKeyDown={(e) => handleKeyDown(e, entry.index)}
                  >
                    <td className="mono">{entry.index}</td>
                    <td className="mono">{entry.term}</td>
                    <td className="mono">
                      {entry.commandType} {entry.key}
                    </td>
                    <td className="mono">{entry.committed ? 'COMMITTED' : 'PENDING'}</td>
                    <td className="mono">{entry.committed ? 'YES' : 'NO'}</td>
                    <td className="mono">{entry.applied ? 'YES' : 'NO'}</td>
                  </tr>
                  {entry.index === raftConsensus.commitIndex ? (
                    <tr className="raft-log-inspector__boundary">
                      <td colSpan={6} className="mono raft-log-inspector__boundary-cell">
                        COMMITTED THROUGH INDEX {raftConsensus.commitIndex}
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>

        <div className="raft-log-inspector__detail panel" data-sc-in aria-live="polite">
          {selected ? (
            <>
              <TechnicalLabel as="div">Entry Detail</TechnicalLabel>
              <dl className="raft-log-inspector__detail-rows">
                <div className="raft-log-inspector__detail-row">
                  <dt>Entry</dt>
                  <dd className="mono">#{selected.index}</dd>
                </div>
                <div className="raft-log-inspector__detail-row">
                  <dt>Term</dt>
                  <dd className="mono">{selected.term}</dd>
                </div>
                <div className="raft-log-inspector__detail-row">
                  <dt>Command</dt>
                  <dd className="mono">{selected.commandType}</dd>
                </div>
                <div className="raft-log-inspector__detail-row">
                  <dt>Key</dt>
                  <dd className="mono">{selected.key}</dd>
                </div>
                <div className="raft-log-inspector__detail-row">
                  <dt>Status</dt>
                  <dd className="mono">{selected.committed ? 'COMMITTED' : 'PENDING'}</dd>
                </div>
                <div className="raft-log-inspector__detail-row">
                  <dt>Commit Index</dt>
                  <dd className="mono">{raftConsensus.commitIndex}</dd>
                </div>
                <div className="raft-log-inspector__detail-row">
                  <dt>Applied</dt>
                  <dd className="mono">{selected.applied ? 'YES' : 'NO'}</dd>
                </div>
              </dl>

              {selected.value !== undefined ? (
                <div className="raft-log-inspector__value">
                  <TechnicalLabel as="div">Value</TechnicalLabel>
                  <pre className="mono raft-log-inspector__value-block">
                    {JSON.stringify(selected.value, null, 2)}
                  </pre>
                </div>
              ) : null}
            </>
          ) : (
            <p className="raft-log-inspector__empty">Select a log entry to inspect its full detail.</p>
          )}
        </div>
      </div>
    </section>
  );
}
