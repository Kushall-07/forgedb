import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { latencySeries, pointsForRange, requestRateSeries } from '../../data/mockObservability';
import type { ObservabilityTimeRange } from '../../types/observability';
import { SectionHeader } from '../ui/SectionHeader';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import { CHART_COLORS } from './chartColors';
import './RequestLatencyPanel.css';

const TICK_STYLE = { fill: CHART_COLORS.inkSoft, fontSize: 11, fontFamily: 'var(--sc-font-mono)' };

const TOOLTIP_STYLE = {
  background: '#13151a',
  border: `1px solid ${CHART_COLORS.hairline}`,
  borderRadius: 8,
  fontSize: 12,
  fontFamily: 'var(--sc-font-mono)',
  color: '#f1f2f5',
};

interface RequestLatencyPanelProps {
  range: ObservabilityTimeRange;
}

/** Request throughput and latency distribution, sampled once per minute over the selected window. Engineering telemetry, not a financial chart. */
export function RequestLatencyPanel({ range }: RequestLatencyPanelProps) {
  const pointCount = pointsForRange(range);
  const visibleRequestRate = requestRateSeries.slice(-pointCount);
  const visibleLatency = latencySeries.slice(-pointCount);
  const tickInterval = Math.max(0, Math.floor(pointCount / 4) - 1);

  return (
    <section className="observability-request-latency" aria-label="Request and latency telemetry">
      <SectionHeader
        eyebrow="Traffic"
        title="Request & Latency Telemetry"
        description={`Read/write throughput and the p50/p95/p99 latency distribution over the last ${pointCount} minutes.`}
      />

      <div className="observability-request-latency__grid" data-sc-in data-sc-stagger="80">
        <div className="observability-request-latency__chart panel">
          <TechnicalLabel as="div">Request Throughput</TechnicalLabel>
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={visibleRequestRate} margin={{ top: 12, right: 8, left: -16, bottom: 0 }}>
              <defs>
                <linearGradient id="obsReads" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={CHART_COLORS.sync} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={CHART_COLORS.sync} stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="obsWrites" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={CHART_COLORS.identity} stopOpacity={0.3} />
                  <stop offset="100%" stopColor={CHART_COLORS.identity} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke={CHART_COLORS.hairline} vertical={false} />
              <XAxis dataKey="t" tick={TICK_STYLE} interval={tickInterval} axisLine={{ stroke: CHART_COLORS.hairline }} tickLine={false} />
              <YAxis tick={TICK_STYLE} axisLine={false} tickLine={false} width={44} />
              <Tooltip contentStyle={TOOLTIP_STYLE} labelStyle={{ color: CHART_COLORS.inkSoft }} />
              <Area
                type="monotone"
                dataKey="reads"
                name="Reads/s"
                stroke={CHART_COLORS.sync}
                fill="url(#obsReads)"
                strokeWidth={1.75}
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="writes"
                name="Writes/s"
                stroke={CHART_COLORS.identity}
                fill="url(#obsWrites)"
                strokeWidth={1.75}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
          <div className="observability-request-latency__legend">
            <span className="observability-request-latency__legend-item">
              <span className="observability-request-latency__swatch observability-request-latency__swatch--sync" />
              Reads/s
            </span>
            <span className="observability-request-latency__legend-item">
              <span className="observability-request-latency__swatch observability-request-latency__swatch--identity" />
              Writes/s
            </span>
          </div>
        </div>

        <div className="observability-request-latency__chart panel">
          <TechnicalLabel as="div">Latency Distribution</TechnicalLabel>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={visibleLatency} margin={{ top: 12, right: 8, left: -16, bottom: 0 }}>
              <CartesianGrid stroke={CHART_COLORS.hairline} vertical={false} />
              <XAxis dataKey="t" tick={TICK_STYLE} interval={tickInterval} axisLine={{ stroke: CHART_COLORS.hairline }} tickLine={false} />
              <YAxis tick={TICK_STYLE} axisLine={false} tickLine={false} width={36} unit="ms" />
              <Tooltip contentStyle={TOOLTIP_STYLE} labelStyle={{ color: CHART_COLORS.inkSoft }} />
              <Line type="monotone" dataKey="p50" name="p50" stroke={CHART_COLORS.sync} strokeWidth={1.5} dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="p95" name="p95" stroke={CHART_COLORS.warn} strokeWidth={1.5} dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="p99" name="p99" stroke={CHART_COLORS.critical} strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
          <div className="observability-request-latency__legend">
            <span className="observability-request-latency__legend-item">
              <span className="observability-request-latency__swatch observability-request-latency__swatch--sync" />
              p50
            </span>
            <span className="observability-request-latency__legend-item">
              <span className="observability-request-latency__swatch observability-request-latency__swatch--warn" />
              p95
            </span>
            <span className="observability-request-latency__legend-item">
              <span className="observability-request-latency__swatch observability-request-latency__swatch--critical" />
              p99
            </span>
          </div>
        </div>
      </div>
    </section>
  );
}
