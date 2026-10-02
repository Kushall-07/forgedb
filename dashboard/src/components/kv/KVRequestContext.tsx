import type { CSSProperties } from 'react';
import { nodes } from '../../data/mockCluster';
import { getKvRaftState } from '../../data/mockKV';
import { SectionHeader } from '../ui/SectionHeader';
import './KVRequestContext.css';

/**
 * A mocked illustration of the write path, not a live trace: Client never
 * talks to storage directly, and nothing here is measured from a real Raft
 * run. The real contract is Client -> API -> Raft leader -> replicated log
 * -> majority commit -> state machine -> storage, and this never implies
 * otherwise.
 */
export function KVRequestContext() {
  const { leader, commitIndex, appliedIndex } = getKvRaftState();
  const stages = [
    { label: 'Client', detail: 'dashboard-client' },
    { label: 'Leader', detail: leader },
    { label: 'Raft Log', detail: `index ${commitIndex}` },
    { label: 'Majority Commit', detail: `2 / ${nodes.length}` },
    { label: 'State Machine', detail: `applied ${appliedIndex}` },
    { label: 'Storage', detail: 'WAL → MemTable' },
  ];

  return (
    <section className="kv-request-context" aria-label="Distributed request context">
      <SectionHeader
        eyebrow="Request Path"
        title="Distributed Request Context"
        description="MOCKED DISTRIBUTED FLOW — how a write conceptually moves through ForgeDB, illustrated, not measured live."
      />

      <ol className="kv-request-context__flow panel" data-sc-in data-sc-stagger="55">
        {stages.map((stage, i) => (
          <li className="kv-request-context__stage" key={stage.label} style={{ '--i': i } as CSSProperties}>
            <div className="kv-request-context__node">
              <span className="kv-request-context__label mono">{stage.label}</span>
              <span className="kv-request-context__detail mono">{stage.detail}</span>
            </div>
            {i < stages.length - 1 ? (
              <span className="kv-request-context__arrow" aria-hidden="true">
                &rarr;
              </span>
            ) : null}
          </li>
        ))}
      </ol>
    </section>
  );
}
