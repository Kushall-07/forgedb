import { useState } from 'react';
import { useScrollCraft } from '../lib/scrollcraft/useScrollCraft';
import type { ObservabilityTimeRange } from '../types/observability';
import { ObservabilityHeader } from '../components/observability/ObservabilityHeader';
import { SystemHealthOverview } from '../components/observability/SystemHealthOverview';
import { ObservabilityMetricGrid } from '../components/observability/ObservabilityMetricGrid';
import { RequestLatencyPanel } from '../components/observability/RequestLatencyPanel';
import { RaftTelemetryPanel } from '../components/observability/RaftTelemetryPanel';
import { StorageTelemetryPanel } from '../components/observability/StorageTelemetryPanel';
import { NodeHealthMatrix } from '../components/observability/NodeHealthMatrix';
import { ObservabilityEventStream } from '../components/observability/ObservabilityEventStream';
import { ActiveIncidents } from '../components/observability/ActiveIncidents';
import { DataProvenance } from '../components/observability/DataProvenance';

export default function Observability() {
  const scopeRef = useScrollCraft<HTMLDivElement>();
  const [range, setRange] = useState<ObservabilityTimeRange>('AUTO');

  return (
    <div ref={scopeRef}>
      <ObservabilityHeader range={range} onRangeChange={setRange} />
      <SystemHealthOverview />
      <ObservabilityMetricGrid range={range} />
      <RequestLatencyPanel range={range} />
      <RaftTelemetryPanel />
      <StorageTelemetryPanel />
      <NodeHealthMatrix />
      <ObservabilityEventStream />
      <ActiveIncidents />
      <DataProvenance />
    </div>
  );
}
