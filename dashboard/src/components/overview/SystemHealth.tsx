import type { SystemHealthItem } from '../../types/cluster';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import './SystemHealth.css';

const ITEMS: SystemHealthItem[] = [
  { label: 'Raft', value: 'Healthy', status: 'healthy' },
  { label: 'Storage', value: 'Healthy', status: 'healthy' },
  { label: 'Replication', value: '3/3 Synchronized', status: 'healthy' },
  { label: 'WAL', value: 'Active', status: 'healthy' },
  { label: 'Snapshot', value: 'Available', status: 'healthy' },
];

const TONE = { healthy: 'sync', warning: 'warn', critical: 'critical' } as const;

export function SystemHealth() {
  return (
    <section className="system-health" aria-label="System health">
      <SectionHeader eyebrow="Status" title="System Health" />

      <div className="system-health__grid" data-sc-in data-sc-stagger="50">
        {ITEMS.map((item) => (
          <div className="system-health__item panel" key={item.label}>
            <span className="system-health__label mono">{item.label}</span>
            <StatusIndicator tone={TONE[item.status]} label={item.value} size="sm" />
          </div>
        ))}
      </div>
    </section>
  );
}
