import type { DerivedRateMetrics, LiveMetricsSnapshot } from '../../types/liveMetrics';
import type { BackendStatus } from '../../hooks/useBackendCluster';
import { SectionHeader } from '../ui/SectionHeader';
import { DataSourceBadge } from '../ui/DataSourceBadge';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './LiveMetricsPanel.css';

interface LiveMetricsPanelProps {
  status: BackendStatus;
  snapshot: LiveMetricsSnapshot | null;
  rates: DerivedRateMetrics;
  error: string | null;
}

function fmt(n: number | null | undefined, digits = 0): string {
  if (n === null || n === undefined) return 'COLLECTING';
  return n.toFixed(digits);
}

function formatBytes(bytes: number): string {
  if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(2)} GB`;
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
  if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(1)} KB`;
  return `${bytes} B`;
}

/**
 * The Observability page's one genuinely live surface: a direct render of
 * GET /metrics (via hooks/useObservabilityMetrics.ts), parsed but not
 * reinterpreted -- every LIVE row below is exactly one metric from
 * internal/metrics/catalog.go or internal/dbnode/metrics.go. The two rows
 * marked "(derived)" are counter deltas computed client-side across two
 * polls, never a percentile and never a fabricated history. Everything
 * this node does NOT expose (SSTable/compaction, CPU/memory, latency
 * percentiles, historical samples) stays in the mock panels below this
 * one, which this component does not touch -- see DataProvenance for the
 * page-wide disclosure of which is which.
 */
export function LiveMetricsPanel({ status, snapshot, rates, error }: LiveMetricsPanelProps) {
  const peerLagEntries = snapshot ? Object.entries(snapshot.raft.followerLagEntries) : [];
  const errorEntries = snapshot ? Object.entries(snapshot.system.errorsByComponent) : [];
  const requestKeyEntries = snapshot ? Object.entries(snapshot.api.requestsByKey) : [];

  return (
    <section className="observability-live-metrics" aria-label="Live backend metrics">
      <SectionHeader
        eyebrow="Real Backend"
        title="Live Metrics"
        description="Parsed directly from this node's GET /metrics every 10s. Node-local only -- see the panels below for the rest of this page's mock telemetry and DataProvenance for what each section actually is."
      />

      <div className="observability-live-metrics__panel panel" data-sc-in>
        <div className="observability-live-metrics__status">
          <DataSourceBadge status={status} />
          {status !== 'live' && error ? <span className="observability-live-metrics__error mono">{error}</span> : null}
        </div>

        {snapshot ? (
          <div className="observability-live-metrics__groups">
            <div className="observability-live-metrics__group">
              <TechnicalLabel as="div">Raft State</TechnicalLabel>
              <dl className="observability-live-metrics__rows">
                <div className="observability-live-metrics__row">
                  <dt>Role</dt>
                  <dd>{snapshot.raft.role.toUpperCase()}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Term</dt>
                  <dd>{snapshot.raft.term}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Commit Index</dt>
                  <dd>{snapshot.raft.commitIndex}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Last Applied</dt>
                  <dd>{snapshot.raft.lastApplied}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Log Entries</dt>
                  <dd>{snapshot.raft.logEntries}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Snapshot Index</dt>
                  <dd>{snapshot.raft.snapshotIndex}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Uptime</dt>
                  <dd>{snapshot.system.uptimeSeconds.toFixed(0)}s</dd>
                </div>
                {peerLagEntries.length > 0 ? (
                  peerLagEntries.map(([peer, lag]) => (
                    <div className="observability-live-metrics__row" key={peer}>
                      <dt>Lag ({peer})</dt>
                      <dd>{lag} entries</dd>
                    </div>
                  ))
                ) : (
                  <div className="observability-live-metrics__row">
                    <dt>Follower Lag</dt>
                    <dd>no peers configured</dd>
                  </div>
                )}
              </dl>
            </div>

            <div className="observability-live-metrics__group">
              <TechnicalLabel as="div">Consensus Activity</TechnicalLabel>
              <dl className="observability-live-metrics__rows">
                <div className="observability-live-metrics__row">
                  <dt>Elections (started/won/lost)</dt>
                  <dd>
                    {snapshot.raft.electionsStarted} / {snapshot.raft.electionsWon} / {snapshot.raft.electionsLost}
                  </dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Leader Changes</dt>
                  <dd>{snapshot.raft.leaderChanges}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>AppendEntries (sent/ok/failed)</dt>
                  <dd>
                    {snapshot.raft.appendEntriesSent} / {snapshot.raft.appendEntriesSuccess} / {snapshot.raft.appendEntriesFailed}
                  </dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Entries Replicated</dt>
                  <dd>{snapshot.raft.entriesReplicated}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Entries Committed</dt>
                  <dd>{snapshot.raft.entriesCommitted}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>ReadIndex (total/ok/fail)</dt>
                  <dd>
                    {snapshot.raft.readIndexTotal} / {snapshot.raft.readIndexSuccess} / {snapshot.raft.readIndexFailure}
                  </dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>ConsistentGet (total/ok/fail)</dt>
                  <dd>
                    {snapshot.raft.consistentGetTotal} / {snapshot.raft.consistentGetSuccess} / {snapshot.raft.consistentGetFailure}
                  </dd>
                </div>
                <div className="observability-live-metrics__row observability-live-metrics__row--derived">
                  <dt>ConsistentGet Avg Latency</dt>
                  <dd>{snapshot.raft.consistentGetAvgLatencyMs === null ? 'COLLECTING' : `${snapshot.raft.consistentGetAvgLatencyMs.toFixed(2)} ms`}</dd>
                </div>
              </dl>
            </div>

            <div className="observability-live-metrics__group">
              <TechnicalLabel as="div">Storage &amp; WAL</TechnicalLabel>
              <dl className="observability-live-metrics__rows">
                <div className="observability-live-metrics__row">
                  <dt>MemTable Entries</dt>
                  <dd>{snapshot.storage.memtableEntries}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>MemTable Bytes</dt>
                  <dd>{formatBytes(snapshot.storage.memtableBytes)}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Put / Get / Delete</dt>
                  <dd>
                    {snapshot.storage.putTotal} / {snapshot.storage.getTotal} / {snapshot.storage.deleteTotal}
                  </dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>Key Not Found</dt>
                  <dd>{snapshot.storage.keyNotFoundTotal}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>WAL Appends</dt>
                  <dd>{snapshot.storage.walAppendTotal}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>WAL Bytes Written</dt>
                  <dd>{formatBytes(snapshot.storage.walBytesWrittenTotal)}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>WAL Syncs</dt>
                  <dd>{snapshot.storage.walSyncTotal}</dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>WAL Recovery (passes/records)</dt>
                  <dd>
                    {snapshot.storage.walRecoveryTotal} / {snapshot.storage.walRecoveryRecordsTotal}
                  </dd>
                </div>
                <div className="observability-live-metrics__row">
                  <dt>WAL Corruption Errors</dt>
                  <dd>{snapshot.storage.walCorruptionErrorsTotal}</dd>
                </div>
              </dl>
            </div>

            <div className="observability-live-metrics__group">
              <TechnicalLabel as="div">API &amp; Errors</TechnicalLabel>
              <dl className="observability-live-metrics__rows">
                <div className="observability-live-metrics__row observability-live-metrics__row--derived">
                  <dt>Request Rate</dt>
                  <dd>{rates.requestsPerSec === null ? 'COLLECTING' : `${fmt(rates.requestsPerSec, 2)}/s`}</dd>
                </div>
                <div className="observability-live-metrics__row observability-live-metrics__row--derived">
                  <dt>Error Rate</dt>
                  <dd>{rates.errorsPerSec === null ? 'COLLECTING' : `${fmt(rates.errorsPerSec, 3)}/s`}</dd>
                </div>
                {requestKeyEntries.length > 0 ? (
                  requestKeyEntries.map(([key, count]) => (
                    <div className="observability-live-metrics__row" key={key}>
                      <dt>{key}</dt>
                      <dd>{count}</dd>
                    </div>
                  ))
                ) : (
                  <div className="observability-live-metrics__row">
                    <dt>Requests</dt>
                    <dd>none recorded yet</dd>
                  </div>
                )}
                {errorEntries.length > 0 ? (
                  errorEntries.map(([component, count]) => (
                    <div className="observability-live-metrics__row" key={component}>
                      <dt>Errors ({component})</dt>
                      <dd>{count}</dd>
                    </div>
                  ))
                ) : (
                  <div className="observability-live-metrics__row">
                    <dt>Errors</dt>
                    <dd>0</dd>
                  </div>
                )}
              </dl>
            </div>
          </div>
        ) : (
          <p className="observability-live-metrics__empty mono">
            {status === 'loading'
              ? 'Connecting to the ForgeDB backend...'
              : 'Backend unreachable -- showing the mock telemetry below only. Start the ForgeDB node and refresh to see live metrics here.'}
          </p>
        )}
      </div>
    </section>
  );
}
