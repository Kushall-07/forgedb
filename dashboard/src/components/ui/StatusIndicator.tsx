import './StatusIndicator.css';

export type StatusTone = 'sync' | 'identity' | 'warn' | 'critical' | 'neutral';

interface StatusIndicatorProps {
  tone: StatusTone;
  label: string;
  pulse?: boolean;
  size?: 'sm' | 'md';
}

/**
 * A coloured dot plus a text label, never colour alone: the label carries
 * the state for anyone who cannot distinguish the hue.
 */
export function StatusIndicator({ tone, label, pulse = false, size = 'md' }: StatusIndicatorProps) {
  return (
    <span className={`status-indicator status-indicator--${size}`}>
      <span
        className={`status-indicator__dot status-indicator__dot--${tone} ${pulse ? 'status-indicator__dot--pulse' : ''}`}
        aria-hidden="true"
      />
      <span className="status-indicator__label">{label}</span>
    </span>
  );
}
