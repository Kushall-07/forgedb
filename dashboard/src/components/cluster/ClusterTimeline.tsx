import { events } from '../../data/mockEvents';
import type { RaftEventKind } from '../../types/cluster';
import { SectionHeader } from '../ui/SectionHeader';
import type { StatusTone } from '../ui/StatusIndicator';
import './ClusterTimeline.css';

const KIND_TONE: Record<RaftEventKind, StatusTone> = {
  election: 'identity',
  append: 'sync',
  commit: 'sync',
  apply: 'sync',
  sync: 'sync',
};

const KIND_LABEL: Record<RaftEventKind, string> = {
  election: 'ELECTION',
  append: 'APPEND',
  commit: 'COMMIT',
  apply: 'APPLY',
  sync: 'SYNC',
};

/** The same Raft activity feed as Overview, read here as a dense technical log rather than a summary strip. */
export function ClusterTimeline() {
  return (
    <section className="cluster-timeline" aria-label="Cluster event timeline">
      <SectionHeader
        eyebrow="Activity"
        title="Cluster Event Timeline"
        description="Consensus activity in the order it happened, oldest first."
      />

      <ol className="cluster-timeline__list panel" data-sc-in data-sc-stagger="55">
        {events.map((event) => (
          <li className="cluster-timeline__item" key={event.id}>
            <span className="cluster-timeline__time mono">{event.timestamp}</span>
            <span className={`cluster-timeline__kind mono cluster-timeline__kind--${KIND_TONE[event.kind]}`}>
              {KIND_LABEL[event.kind]}
            </span>
            <span className="cluster-timeline__message">{event.message}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}
