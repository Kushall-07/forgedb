import { Activity, RefreshCw } from 'lucide-react';
import type { ObservabilityTimeRange } from '../../types/observability';
import type { BackendStatus } from '../../hooks/useBackendCluster';
import { DataSourceBadge } from '../ui/DataSourceBadge';
import './ObservabilityHeader.css';

const RANGES: ObservabilityTimeRange[] = ['AUTO', '5M', '15M', '1H'];

interface ObservabilityHeaderProps {
  range: ObservabilityTimeRange;
  onRangeChange: (range: ObservabilityTimeRange) => void;
  /** Status of the Live Metrics panel's GET /metrics poll -- this badge is about that one live surface, not a claim that every panel on the page is live (most still are not; see DataProvenance). */
  backendStatus: BackendStatus;
}

/** Compact page header, not a hero: this console is read many times a day. */
export function ObservabilityHeader({ range, onRangeChange, backendStatus }: ObservabilityHeaderProps) {
  return (
    <section className="observability-header" aria-label="Observability header" data-sc-in>
      <div className="observability-header__identity">
        <span className="observability-header__icon" aria-hidden="true">
          <Activity size={20} strokeWidth={1.75} />
        </span>
        <div>
          <span className="observability-header__eyebrow mono">FORGEDB / OBSERVABILITY</span>
          <h1 className="observability-header__title">Observability</h1>
          <p className="observability-header__subtitle">ForgeDB operational telemetry and system diagnostics.</p>
        </div>
      </div>

      <div className="observability-header__controls">
        <div className="observability-header__range" role="group" aria-label="Time range">
          {RANGES.map((r) => (
            <button
              key={r}
              type="button"
              aria-pressed={range === r}
              className={`observability-header__range-btn ${
                range === r ? 'observability-header__range-btn--active' : ''
              }`}
              onClick={() => onRangeChange(r)}
            >
              {r}
            </button>
          ))}
        </div>

        <span className="observability-header__refresh mono">
          <RefreshCw size={13} strokeWidth={2} aria-hidden="true" />
          {backendStatus === 'live' ? 'Polling every 10s' : 'Static snapshot'}
        </span>

        <DataSourceBadge status={backendStatus} />
      </div>
    </section>
  );
}
