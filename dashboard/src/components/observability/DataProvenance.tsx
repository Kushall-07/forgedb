import { Info } from 'lucide-react';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './DataProvenance.css';

const ROWS: Array<{ tag: 'LIVE' | 'DERIVED' | 'MOCK'; text: string }> = [
  {
    tag: 'LIVE',
    text: 'Live Metrics (above): parsed directly from this node’s GET /metrics -- Raft state, consensus counters, storage/WAL counters, and per-route API request counts.',
  },
  {
    tag: 'DERIVED',
    text: 'Request rate, error rate, and ConsistentGet average latency: computed client-side from GET /metrics counter deltas across polls. Never a percentile, and never shown until a second poll has actually landed.',
  },
  {
    tag: 'MOCK',
    text: 'Key Metrics, Request & Latency Telemetry (p50/p95/p99), Storage Activity’s SSTable/compaction figures, Node Health’s CPU/memory, and the event stream / incidents below: ForgeDB does not currently expose these over HTTP, so they remain deterministic dashboard mock data, unchanged from pre-Phase-D.',
  },
];

/** The page-wide disclosure of exactly which sections are real, which are computed from real data, and which are still mock -- the one place this page states all three at once rather than a single blanket label. */
export function DataProvenance() {
  return (
    <section className="observability-provenance panel" aria-label="Data provenance" data-sc-in>
      <Info size={16} strokeWidth={2} className="observability-provenance__icon" aria-hidden="true" />
      <div>
        <TechnicalLabel as="div">Data Source — Live / Derived / Mock</TechnicalLabel>
        {ROWS.map((row) => (
          <p className="observability-provenance__text" key={row.tag}>
            <strong className="observability-provenance__tag">{row.tag}</strong> — {row.text}
          </p>
        ))}
      </div>
    </section>
  );
}
