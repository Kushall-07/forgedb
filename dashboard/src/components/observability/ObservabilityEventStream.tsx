import { useMemo, useState } from 'react';
import { observabilityEvents } from '../../data/mockObservability';
import type { ObservabilityEventSeverity } from '../../types/observability';
import { SectionHeader } from '../ui/SectionHeader';
import type { StatusTone } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './ObservabilityEventStream.css';

const SEVERITIES: Array<ObservabilityEventSeverity | 'ALL'> = ['ALL', 'INFO', 'WARN', 'ERROR'];

const SEVERITY_TONE: Record<ObservabilityEventSeverity, StatusTone> = {
  INFO: 'neutral',
  WARN: 'warn',
  ERROR: 'critical',
};

/** A mock event stream, newest first, filterable by severity. Not a claim of a live connection. */
export function ObservabilityEventStream() {
  const [filter, setFilter] = useState<ObservabilityEventSeverity | 'ALL'>('ALL');

  const visible = useMemo(
    () => (filter === 'ALL' ? observabilityEvents : observabilityEvents.filter((e) => e.severity === filter)),
    [filter],
  );

  return (
    <section className="observability-events" aria-label="Recent events">
      <SectionHeader
        eyebrow="Activity"
        title="Recent Events"
        description="Election, replication, compaction, WAL, and request activity across the cluster, newest first."
        trailing={<TechnicalLabel>Mock Event Stream</TechnicalLabel>}
      />

      <div className="observability-events__filters" role="group" aria-label="Filter by severity" data-sc-in>
        {SEVERITIES.map((s) => (
          <button
            key={s}
            type="button"
            className={`observability-events__filter ${filter === s ? 'observability-events__filter--active' : ''}`}
            aria-pressed={filter === s}
            onClick={() => setFilter(s)}
          >
            {s}
          </button>
        ))}
      </div>

      <ol className="observability-events__list panel" data-sc-in data-sc-stagger="40">
        {visible.map((event) => (
          <li className="observability-events__item" key={event.id}>
            <span className="observability-events__time mono">{event.timestamp}</span>
            <span
              className={`observability-events__severity mono observability-events__severity--${
                SEVERITY_TONE[event.severity]
              }`}
            >
              {event.severity}
            </span>
            <span className="observability-events__category mono">{event.category}</span>
            <span className="observability-events__text">
              <span className="observability-events__message">{event.message}</span>
              <span className="observability-events__source mono">{event.source}</span>
            </span>
          </li>
        ))}
        {visible.length === 0 ? <li className="observability-events__empty">No events at this severity.</li> : null}
      </ol>
    </section>
  );
}
