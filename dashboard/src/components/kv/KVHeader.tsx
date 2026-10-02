import { Terminal } from 'lucide-react';
import { nodes } from '../../data/mockCluster';
import { getKvRaftState } from '../../data/mockKV';
import { StatusIndicator } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './KVHeader.css';

/** Compact page header matching the Cluster header's grammar, not a hero. */
export function KVHeader() {
  const raft = getKvRaftState();

  return (
    <section className="kv-header" aria-label="KV console header" data-sc-in>
      <div className="kv-header__identity">
        <span className="kv-header__icon" aria-hidden="true">
          <Terminal size={20} strokeWidth={1.75} />
        </span>
        <div>
          <span className="kv-header__eyebrow mono">KV / DATABASE CONSOLE</span>
          <h1 className="kv-header__title">Key-Value Operations</h1>
          <p className="kv-header__subtitle">Inspect and interact with ForgeDB&apos;s replicated key-value state.</p>
        </div>
      </div>

      <div className="kv-header__context">
        <div className="kv-header__field">
          <TechnicalLabel>Cluster</TechnicalLabel>
          <span className="mono kv-header__value">forgedb-cluster</span>
        </div>
        <div className="kv-header__field">
          <TechnicalLabel>Nodes</TechnicalLabel>
          <span className="mono kv-header__value">{nodes.length}</span>
        </div>
        <div className="kv-header__field">
          <TechnicalLabel>Leader</TechnicalLabel>
          <span className="mono kv-header__value kv-header__value--identity">{raft.leader}</span>
        </div>
        <div className="kv-header__field">
          <TechnicalLabel>Term</TechnicalLabel>
          <span className="mono kv-header__value">{raft.term}</span>
        </div>
        <StatusIndicator tone="sync" label="Healthy" pulse />
      </div>
    </section>
  );
}
