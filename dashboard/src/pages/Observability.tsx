import { Activity } from 'lucide-react';
import { ComingSoon } from './ComingSoon';

export default function Observability() {
  return (
    <ComingSoon
      icon={Activity}
      title="Observability"
      description="Metrics, structured logs, and diagnostics from the running cluster: compaction activity, request latency, and the deterministic chaos-test harness."
    />
  );
}
