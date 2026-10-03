import { StatusIndicator, type StatusTone } from './StatusIndicator';
import type { BackendStatus } from '../../hooks/useBackendCluster';

const TONE: Record<BackendStatus, StatusTone> = {
  loading: 'neutral',
  live: 'sync',
  mock: 'warn',
  unavailable: 'critical',
};

const LABEL: Record<BackendStatus, string> = {
  loading: 'CONNECTING',
  live: 'LIVE BACKEND',
  mock: 'MOCK DATA',
  unavailable: 'BACKEND UNAVAILABLE',
};

/**
 * The dashboard's single truthful indicator of where the data on screen
 * came from this render -- never shown as LIVE when a request actually
 * failed and mock data is standing in for it (see
 * hooks/useBackendCluster.ts).
 */
export function DataSourceBadge({ status }: { status: BackendStatus }) {
  return <StatusIndicator tone={TONE[status]} label={LABEL[status]} pulse={status === 'live'} size="sm" />;
}
