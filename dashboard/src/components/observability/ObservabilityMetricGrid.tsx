import { Line, LineChart, ResponsiveContainer } from 'recharts';
import {
  latencySeries,
  pointsForRange,
  requestRateSeries,
  storageTelemetry,
  systemMetrics,
} from '../../data/mockObservability';
import type { ObservabilityTimeRange } from '../../types/observability';
import { SectionHeader } from '../ui/SectionHeader';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import { CHART_COLORS } from './chartColors';
import './ObservabilityMetricGrid.css';

interface Metric {
  label: string;
  value: string;
  unit?: string;
  delta?: string;
  tone?: 'default' | 'sync' | 'identity' | 'warn' | 'critical';
  trend?: number[];
}

function buildMetrics(trendPointCount: number): Metric[] {
  const requestTrend = requestRateSeries.slice(-trendPointCount).map((p) => p.reads + p.writes);
  const latencyTrend = latencySeries.slice(-trendPointCount).map((p) => p.p95);

  return [
    {
      label: 'Request Rate',
      value: (systemMetrics.requestsPerSec / 1000).toFixed(2),
      unit: 'k req/s',
      delta: `${systemMetrics.readsPerSec} read · ${systemMetrics.writesPerSec} write`,
      tone: 'sync',
      trend: requestTrend,
    },
    {
      label: 'P95 Latency',
      value: systemMetrics.latencyMsP95.toFixed(1),
      unit: 'ms',
      delta: `p50 ${systemMetrics.latencyMsP50.toFixed(1)}ms · p99 ${systemMetrics.latencyMsP99.toFixed(1)}ms`,
      trend: latencyTrend,
    },
    {
      label: 'Error Rate',
      value: systemMetrics.errorRatePct.toFixed(2),
      unit: '%',
      delta: 'within nominal bounds',
      tone: 'sync',
    },
    {
      label: 'Commit Rate',
      value: (systemMetrics.commitRatePerSec / 1000).toFixed(2),
      unit: 'k/s',
      delta: 'tracking request rate',
    },
    {
      label: 'Apply Rate',
      value: (systemMetrics.applyRatePerSec / 1000).toFixed(2),
      unit: 'k/s',
      delta: 'no apply backlog',
    },
    {
      label: 'Replication Lag',
      value: '0–2',
      unit: 'entries',
      delta: 'NODE-3 trailing by up to 2',
      tone: 'warn',
    },
    {
      label: 'Memtable',
      value: (storageTelemetry.memtable.activeSizeBytes / 1_000_000).toFixed(0),
      unit: 'MB',
      delta: `${storageTelemetry.memtable.immutableCount} immutable`,
    },
    {
      label: 'SSTables',
      value: String(storageTelemetry.sstable.count),
      unit: 'files',
      delta: `${storageTelemetry.compaction.pending} pending compaction`,
    },
  ];
}

interface ObservabilityMetricGridProps {
  range: ObservabilityTimeRange;
}

/** The eight numbers an operator checks first. Believable precision, not fake decimals everywhere. */
export function ObservabilityMetricGrid({ range }: ObservabilityMetricGridProps) {
  const metrics = buildMetrics(Math.min(20, pointsForRange(range)));

  return (
    <section className="observability-metrics" aria-label="Key performance metrics">
      <SectionHeader
        eyebrow="Telemetry"
        title="Key Metrics"
        description="Request, consensus, and storage throughput over the selected window."
      />

      <div className="observability-metrics__grid" data-sc-in data-sc-stagger="45">
        {metrics.map((metric) => (
          <div className={`observability-metric panel observability-metric--${metric.tone ?? 'default'}`} key={metric.label}>
            <TechnicalLabel>{metric.label}</TechnicalLabel>
            <div className="observability-metric__value-row">
              <span className="observability-metric__value mono">{metric.value}</span>
              {metric.unit ? <span className="observability-metric__unit mono">{metric.unit}</span> : null}
            </div>
            {metric.delta ? <div className="observability-metric__delta">{metric.delta}</div> : null}
            {metric.trend ? (
              <div className="observability-metric__trend" aria-hidden="true">
                <ResponsiveContainer width="100%" height={28}>
                  <LineChart data={metric.trend.map((value, i) => ({ i, value }))}>
                    <Line
                      type="monotone"
                      dataKey="value"
                      stroke={CHART_COLORS.sync}
                      strokeWidth={1.5}
                      dot={false}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : null}
          </div>
        ))}
      </div>
    </section>
  );
}
