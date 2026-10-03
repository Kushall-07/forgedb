import { systemStatusSignals } from '../../data/mockObservability';
import type { ObservabilityStatus } from '../../types/observability';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './SystemHealthOverview.css';

const STATUS_TONE: Record<ObservabilityStatus, StatusTone> = {
  healthy: 'sync',
  warning: 'warn',
  critical: 'critical',
};

/**
 * The single answer to "is ForgeDB healthy" at the top of the page, followed
 * by the compact signals that back it up. No invented aggregate score: the
 * overall status is the worst status among the signals below it.
 */
export function SystemHealthOverview() {
  const overall: ObservabilityStatus = systemStatusSignals.some((s) => s.status === 'critical')
    ? 'critical'
    : systemStatusSignals.some((s) => s.status === 'warning')
      ? 'warning'
      : 'healthy';

  const overallLabel = overall === 'healthy' ? 'HEALTHY' : overall === 'warning' ? 'DEGRADED' : 'CRITICAL';

  return (
    <section className="observability-health" aria-label="System health" data-sc-in>
      <div className="observability-health__status panel">
        <TechnicalLabel>System Status</TechnicalLabel>
        <StatusIndicator tone={STATUS_TONE[overall]} label={overallLabel} pulse />
      </div>

      <div className="observability-health__signals" data-sc-in data-sc-stagger="50">
        {systemStatusSignals.map((signal) => (
          <div className="observability-health__signal panel" key={signal.label}>
            <span className="observability-health__signal-label mono">{signal.label}</span>
            <StatusIndicator tone={STATUS_TONE[signal.status]} label={signal.value} size="sm" />
          </div>
        ))}
      </div>
    </section>
  );
}
