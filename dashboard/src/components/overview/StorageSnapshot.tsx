import { storage } from '../../data/mockCluster';
import { SectionHeader } from '../ui/SectionHeader';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './StorageSnapshot.css';

export function StorageSnapshot() {
  return (
    <section className="storage-snapshot" aria-label="Storage snapshot">
      <SectionHeader
        eyebrow="Durability"
        title="Storage Snapshot"
        description="What the state machine is holding right now, separate from infrastructure that exists but is not currently on the live write path."
      />

      <div className="storage-snapshot__groups">
        <div className="storage-snapshot__group" data-sc-in>
          <TechnicalLabel as="div">Live State</TechnicalLabel>
          <div className="storage-snapshot__cards">
            <div className="storage-snapshot__card panel">
              <TechnicalLabel>MemTable</TechnicalLabel>
              <div className="storage-snapshot__value mono">{storage.memtableEntries} entries</div>
              <div className="storage-snapshot__sub mono">{storage.memtableBytes} B</div>
            </div>
            <div className="storage-snapshot__card panel">
              <TechnicalLabel>WAL</TechnicalLabel>
              <div className="storage-snapshot__value mono storage-snapshot__value--sync">
                {storage.wal.toUpperCase()}
              </div>
            </div>
          </div>
        </div>

        <div className="storage-snapshot__group" data-sc-in>
          <TechnicalLabel as="div">Infrastructure Available</TechnicalLabel>
          <div className="storage-snapshot__cards">
            <div className="storage-snapshot__card panel">
              <TechnicalLabel>SSTables</TechnicalLabel>
              <div className="storage-snapshot__value mono">{storage.sstables} ACTIVE</div>
            </div>
            <div className="storage-snapshot__card panel">
              <TechnicalLabel>Snapshot</TechnicalLabel>
              <div className="storage-snapshot__value mono">INDEX {storage.snapshotIndex}</div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
