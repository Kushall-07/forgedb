import { AlertTriangle } from 'lucide-react';
import { activeIncidents } from '../../data/mockObservability';
import type { IncidentSeverity, IncidentStatus } from '../../types/observability';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator, type StatusTone } from '../ui/StatusIndicator';
import './ActiveIncidents.css';

const SEVERITY_TONE: Record<IncidentSeverity, StatusTone> = {
  WARN: 'warn',
  ERROR: 'critical',
};

const STATUS_LABEL: Record<IncidentStatus, string> = {
  investigating: 'Investigating',
  monitoring: 'Monitoring',
  resolved: 'Resolved',
};

/** A few realistic operational conditions worth watching, not a claim the system is unhealthy. */
export function ActiveIncidents() {
  return (
    <section className="observability-incidents" aria-label="Active warnings and incidents">
      <SectionHeader
        eyebrow="Attention"
        title="Active Warnings"
        description="Conditions an operator should be aware of. A mostly healthy cluster can still have a few of these."
      />

      {activeIncidents.length === 0 ? (
        <div className="observability-incidents__empty panel" data-sc-in>
          No active warnings or incidents.
        </div>
      ) : (
        <div className="observability-incidents__list" data-sc-in data-sc-stagger="60">
          {activeIncidents.map((incident) => (
            <div className="observability-incidents__item panel" key={incident.id}>
              <span
                className={`observability-incidents__icon observability-incidents__icon--${SEVERITY_TONE[incident.severity]}`}
                aria-hidden="true"
              >
                <AlertTriangle size={16} strokeWidth={2} />
              </span>
              <div className="observability-incidents__body">
                <div className="observability-incidents__header">
                  <h3 className="observability-incidents__title">{incident.title}</h3>
                  <StatusIndicator tone={SEVERITY_TONE[incident.severity]} label={incident.severity} size="sm" />
                </div>
                <p className="observability-incidents__description">{incident.description}</p>
                <div className="observability-incidents__meta mono">
                  <span>{incident.source}</span>
                  <span aria-hidden="true">·</span>
                  <span>{incident.timestamp}</span>
                  <span aria-hidden="true">·</span>
                  <span>{STATUS_LABEL[incident.status]}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
