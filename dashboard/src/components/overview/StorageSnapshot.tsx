import { SectionHeader } from '../ui/SectionHeader';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import type { StorageState } from '../../types/cluster';
import type { BackendStatus } from '../../hooks/useBackendCluster';
import './StorageSnapshot.css';

interface StorageSnapshotProps {
  storage: StorageState;
  status: BackendStatus;
}

/**
 * MemTable entries/bytes and the snapshot index come from the live node's
 * GET /cluster response when status is 'live' (see
 * hooks/useBackendCluster.ts); WAL status and SSTable count are never
 * reported by the backend at all, so they are always the mock values,
 * labeled as such, regardless of status.
 */
export function StorageSnapshot({ storage, status }: StorageSnapshotProps) {
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
              {status === 'live' ? <div className="storage-snapshot__sub mono">LIVE</div> : null}
            </div>
            <div className="storage-snapshot__card panel">
              <TechnicalLabel>WAL</TechnicalLabel>
              <div className="storage-snapshot__value mono storage-snapshot__value--sync">
                {storage.wal.toUpperCase()}
              </div>
              <div className="storage-snapshot__sub mono">NOT EXPOSED BY BACKEND -- MOCK</div>
            </div>
          </div>
        </div>

        <div className="storage-snapshot__group" data-sc-in>
          <TechnicalLabel as="div">Infrastructure Available</TechnicalLabel>
          <div className="storage-snapshot__cards">
            <div className="storage-snapshot__card panel">
              <TechnicalLabel>SSTables</TechnicalLabel>
              <div className="storage-snapshot__value mono">{storage.sstables} ACTIVE</div>
              <div className="storage-snapshot__sub mono">NOT EXPOSED BY BACKEND -- MOCK</div>
            </div>
            <div className="storage-snapshot__card panel">
              <TechnicalLabel>Snapshot</TechnicalLabel>
              <div className="storage-snapshot__value mono">INDEX {storage.snapshotIndex}</div>
              {status === 'live' ? <div className="storage-snapshot__sub mono">LIVE</div> : null}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
