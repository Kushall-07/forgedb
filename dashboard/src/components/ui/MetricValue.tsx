import type { ReactNode } from 'react';
import { TechnicalLabel } from './TechnicalLabel';
import './MetricValue.css';

interface MetricValueProps {
  label: string;
  value: ReactNode;
  emphasis?: boolean;
  tone?: 'default' | 'sync' | 'identity' | 'warn' | 'critical';
  footnote?: string;
}

/**
 * A labelled technical value: small mono label on top, large mono value
 * below. `emphasis` gives one metric (the leader) a stronger treatment so a
 * row of metrics does not read as five identical cards.
 */
export function MetricValue({ label, value, emphasis = false, tone = 'default', footnote }: MetricValueProps) {
  return (
    <div className={`metric-value ${emphasis ? 'metric-value--emphasis' : ''} metric-value--${tone}`}>
      <TechnicalLabel>{label}</TechnicalLabel>
      <div className="metric-value__value mono">{value}</div>
      {footnote ? <div className="metric-value__footnote">{footnote}</div> : null}
    </div>
  );
}
