import { Info } from 'lucide-react';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './DataProvenance.css';

/** Explicit disclosure that this page reads deterministic mock data, not a live metrics backend. */
export function DataProvenance() {
  return (
    <section className="observability-provenance panel" aria-label="Data provenance" data-sc-in>
      <Info size={16} strokeWidth={2} className="observability-provenance__icon" aria-hidden="true" />
      <div>
        <TechnicalLabel as="div">Data Source — Mock Telemetry</TechnicalLabel>
        <p className="observability-provenance__text">
          Observability data on this page is currently deterministic dashboard mock data. Backend metrics
          integration is reserved for a later phase.
        </p>
      </div>
    </section>
  );
}
