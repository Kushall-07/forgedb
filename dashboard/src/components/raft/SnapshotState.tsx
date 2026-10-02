import { raftSnapshot } from '../../data/mockRaft';
import { MetricValue } from '../ui/MetricValue';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import './SnapshotState.css';

/** Current compaction state only — no snapshot is invented to make this section more interesting. */
export function SnapshotState() {
  const hasSnapshot = raftSnapshot.state === 'AVAILABLE';

  return (
    <section className="snapshot-state" aria-label="Snapshot and compaction state">
      <SectionHeader
        eyebrow="Compaction"
        title="Snapshot / Compaction State"
        description="Raft's log-compaction mechanism for bounding replicated-log growth."
      />

      <div className="snapshot-state__metrics" data-sc-in data-sc-stagger="60">
        <MetricValue label="Snapshot Index" value={raftSnapshot.snapshotIndex} />
        <MetricValue label="Snapshot Term" value={raftSnapshot.snapshotTerm} />
        <MetricValue label="Log Start Index" value={raftSnapshot.logStartIndex} />
        <MetricValue label="Compacted Entries" value={raftSnapshot.compactedEntries} />
        <MetricValue
          label="Snapshot State"
          value={
            <StatusIndicator
              tone={hasSnapshot ? 'sync' : 'neutral'}
              label={hasSnapshot ? 'AVAILABLE' : 'NO SNAPSHOT'}
              size="sm"
            />
          }
        />
      </div>

      <p className="snapshot-state__note">
        {hasSnapshot
          ? 'A snapshot has been installed; log entries before the snapshot index have been compacted away.'
          : 'No snapshot has been installed for the current mock state. The full replicated log is retained from index 1.'}
      </p>
    </section>
  );
}
