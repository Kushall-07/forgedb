import { useMemo, useState } from 'react';
import { raftEvents } from '../../data/mockRaft';
import type { RaftEventType } from '../../types/raft';
import { SectionHeader } from '../ui/SectionHeader';
import type { StatusTone } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './RaftEventTimeline.css';

const TYPES: Array<RaftEventType | 'ALL'> = ['ALL', 'ELECTION', 'REPLICATION', 'COMMIT', 'HEARTBEAT', 'SNAPSHOT'];

const TYPE_TONE: Record<RaftEventType, StatusTone> = {
  ELECTION: 'identity',
  REPLICATION: 'sync',
  COMMIT: 'sync',
  HEARTBEAT: 'neutral',
  SNAPSHOT: 'warn',
};

/** A mock event stream, newest first, filterable by category. Not a claim of a live connection. */
export function RaftEventTimeline() {
  const [filter, setFilter] = useState<RaftEventType | 'ALL'>('ALL');

  const visible = useMemo(
    () => (filter === 'ALL' ? raftEvents : raftEvents.filter((e) => e.type === filter)),
    [filter],
  );

  return (
    <section className="raft-event-timeline" aria-label="Recent Raft events">
      <SectionHeader
        eyebrow="Activity"
        title="Recent Raft Events"
        description="Election, replication, commit, heartbeat, and snapshot activity, newest first."
        trailing={<TechnicalLabel>Mock Raft Event Stream</TechnicalLabel>}
      />

      <div className="raft-event-timeline__filters" role="group" aria-label="Filter by event category" data-sc-in>
        {TYPES.map((type) => (
          <button
            key={type}
            type="button"
            className={`raft-event-timeline__filter ${
              filter === type ? 'raft-event-timeline__filter--active' : ''
            }`}
            aria-pressed={filter === type}
            onClick={() => setFilter(type)}
          >
            {type}
          </button>
        ))}
      </div>

      <ol className="raft-event-timeline__list panel" data-sc-in data-sc-stagger="55">
        {visible.map((event) => (
          <li className="raft-event-timeline__item" key={event.id}>
            <span className="raft-event-timeline__time mono">{event.timestamp}</span>
            <span className={`raft-event-timeline__type mono raft-event-timeline__type--${TYPE_TONE[event.type]}`}>
              {event.type}
            </span>
            <span className="raft-event-timeline__text">
              <span className="raft-event-timeline__title">{event.title}</span>
              <span className="raft-event-timeline__description mono">{event.description}</span>
            </span>
          </li>
        ))}
        {visible.length === 0 ? <li className="raft-event-timeline__empty">No events in this category.</li> : null}
      </ol>
    </section>
  );
}
