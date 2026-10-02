import { events } from '../../data/mockEvents';
import type { RaftEventKind } from '../../types/cluster';
import { SectionHeader } from '../ui/SectionHeader';
import type { StatusTone } from '../ui/StatusIndicator';
import './EventTimeline.css';

const KIND_TONE: Record<RaftEventKind, StatusTone> = {
  election: 'identity',
  append: 'sync',
  commit: 'sync',
  apply: 'sync',
  sync: 'sync',
};

export function EventTimeline() {
  return (
    <section className="event-timeline" aria-label="Recent Raft events">
      <SectionHeader eyebrow="Activity" title="Recent Raft Events" description="The latest consensus activity, oldest first." />

      <ol className="event-timeline__list" data-sc-in data-sc-stagger="60">
        {events.map((event) => (
          <li className="event-timeline__item" key={event.id}>
            <span className={`event-timeline__marker event-timeline__marker--${KIND_TONE[event.kind]}`} aria-hidden="true" />
            <span className="event-timeline__time mono">{event.timestamp}</span>
            <span className="event-timeline__message">{event.message}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}
