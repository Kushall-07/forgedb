import { storageTelemetry } from '../../data/mockObservability';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './StorageTelemetryPanel.css';

function formatBytes(bytes: number): string {
  if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(2)} GB`;
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
  if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(1)} KB`;
  return `${bytes} B`;
}

/** WAL, memtable, SSTable, and compaction activity: the LSM engine's write and compaction path, not a generic storage analytics page. */
export function StorageTelemetryPanel() {
  const { wal, memtable, sstable, compaction } = storageTelemetry;

  return (
    <section className="observability-storage" aria-label="Storage telemetry">
      <SectionHeader
        eyebrow="Durability"
        title="Storage Activity"
        description="WAL, memtable, SSTable, and compaction activity on the LSM write path."
      />

      <div className="observability-storage__grid" data-sc-in data-sc-stagger="50">
        <div className="observability-storage__panel panel">
          <TechnicalLabel as="div">WAL</TechnicalLabel>
          <div className="observability-storage__rows">
            <div className="observability-storage__row">
              <span>Write Rate</span>
              <span className="mono">{formatBytes(wal.writeRateBytesPerSec)}/s</span>
            </div>
            <div className="observability-storage__row">
              <span>Bytes Written</span>
              <span className="mono">{formatBytes(wal.bytesWrittenTotal)}</span>
            </div>
            <div className="observability-storage__row">
              <span>Recovery</span>
              <StatusIndicator
                tone={wal.replayStatus === 'idle' ? 'sync' : 'warn'}
                label={wal.replayStatus.toUpperCase()}
                size="sm"
              />
            </div>
          </div>
        </div>

        <div className="observability-storage__panel panel">
          <TechnicalLabel as="div">Memtable</TechnicalLabel>
          <div className="observability-storage__rows">
            <div className="observability-storage__row">
              <span>Active Size</span>
              <span className="mono">{formatBytes(memtable.activeSizeBytes)}</span>
            </div>
            <div className="observability-storage__row">
              <span>Immutable</span>
              <span className="mono">{memtable.immutableCount}</span>
            </div>
          </div>
        </div>

        <div className="observability-storage__panel panel">
          <TechnicalLabel as="div">SSTable</TechnicalLabel>
          <div className="observability-storage__rows">
            <div className="observability-storage__row">
              <span>File Count</span>
              <span className="mono">{sstable.count}</span>
            </div>
            <div className="observability-storage__row">
              <span>Read Rate</span>
              <span className="mono">{sstable.readRatePerSec}/s</span>
            </div>
            <div className="observability-storage__row">
              <span>Write Rate</span>
              <span className="mono">{sstable.writeRatePerSec}/s</span>
            </div>
          </div>
        </div>

        <div className="observability-storage__panel panel">
          <TechnicalLabel as="div">Compaction</TechnicalLabel>
          <div className="observability-storage__rows">
            <div className="observability-storage__row">
              <span>Pending</span>
              <span className={`mono ${compaction.pending > 0 ? 'observability-storage__value--warn' : ''}`}>
                {compaction.pending}
              </span>
            </div>
            <div className="observability-storage__row">
              <span>Active</span>
              <span className="mono">{compaction.active}</span>
            </div>
            <div className="observability-storage__row">
              <span>Completed</span>
              <span className="mono">{compaction.completedTotal}</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
