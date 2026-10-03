import { ArrowRight } from 'lucide-react';
import { raftTelemetry } from '../../data/mockObservability';
import { MetricValue } from '../ui/MetricValue';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import './RaftTelemetryPanel.css';

/**
 * Raft telemetry, not another Raft inspector: rates and counts rather than
 * the full per-node log/index detail the Raft page already owns. The flow
 * strip exists to make replicate -> commit -> apply legible as a pipeline,
 * not to re-litigate per-index state.
 */
export function RaftTelemetryPanel() {
  return (
    <section className="observability-raft" aria-label="Raft telemetry">
      <SectionHeader
        eyebrow="Consensus"
        title="Raft Activity"
        description="Consensus throughput for the current term. See the Raft page for per-node log inspection."
      />

      <div className="observability-raft__flow panel" data-sc-in>
        <div className="observability-raft__stage">
          <span className="observability-raft__stage-label mono">Replicate</span>
          <span className="observability-raft__stage-value mono">{raftTelemetry.appendEntriesPerSec}/s</span>
          <span className="observability-raft__stage-sub">AppendEntries</span>
        </div>
        <ArrowRight className="observability-raft__arrow" size={18} strokeWidth={1.5} aria-hidden="true" />
        <div className="observability-raft__stage">
          <span className="observability-raft__stage-label mono">Commit</span>
          <span className="observability-raft__stage-value observability-raft__stage-value--sync mono">
            {raftTelemetry.commitIndex}
          </span>
          <span className="observability-raft__stage-sub">commit index</span>
        </div>
        <ArrowRight className="observability-raft__arrow" size={18} strokeWidth={1.5} aria-hidden="true" />
        <div className="observability-raft__stage">
          <span className="observability-raft__stage-label mono">Apply</span>
          <span className="observability-raft__stage-value observability-raft__stage-value--identity mono">
            {raftTelemetry.appliedIndex}
          </span>
          <span className="observability-raft__stage-sub">applied index</span>
        </div>
      </div>

      <div className="observability-raft__grid" data-sc-in data-sc-stagger="45">
        <MetricValue label="Current Term" value={raftTelemetry.currentTerm} />
        <MetricValue label="Leader" value={raftTelemetry.leader} tone="identity" />
        <MetricValue label="Log Length" value={raftTelemetry.logLength} />
        <MetricValue
          label="Replication Lag"
          value={`${raftTelemetry.replicationLagMinEntries}–${raftTelemetry.replicationLagMaxEntries}`}
          footnote="entries behind leader"
        />
        <MetricValue label="RequestVote Rate" value={`${raftTelemetry.requestVotePerSec}/s`} />
        <MetricValue label="Election Count" value={raftTelemetry.electionCount} footnote="since cluster start" />
        <div className="observability-raft__heartbeat panel">
          <span className="observability-raft__signal-label mono">Heartbeat</span>
          <StatusIndicator
            tone={raftTelemetry.heartbeatStatus === 'stable' ? 'sync' : 'warn'}
            label={raftTelemetry.heartbeatStatus === 'stable' ? 'Stable' : 'Degraded'}
            pulse
            size="sm"
          />
          <span className="observability-raft__heartbeat-sub mono">last {raftTelemetry.lastHeartbeatMs}ms ago</span>
        </div>
      </div>
    </section>
  );
}
