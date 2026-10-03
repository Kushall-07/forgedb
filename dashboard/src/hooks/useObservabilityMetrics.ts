import { useEffect, useRef, useState } from 'react';
import { forgedbApi } from '../services/forgedbApi';
import { buildLiveMetricsSnapshot } from '../lib/metrics/buildLiveMetricsSnapshot';
import type { DerivedRateMetrics, LiveMetricsSnapshot } from '../types/liveMetrics';
import type { BackendStatus } from './useBackendCluster';

export interface UseObservabilityMetricsResult {
  status: BackendStatus;
  snapshot: LiveMetricsSnapshot | null;
  rates: DerivedRateMetrics;
  error: string | null;
  lastUpdated: Date | null;
}

const POLL_INTERVAL_MS = 10_000;

function sumRequests(byKey: Record<string, number>): number {
  return Object.values(byKey).reduce((total, count) => total + count, 0);
}

function sumErrors(byComponent: Record<string, number>): number {
  return Object.values(byComponent).reduce((total, count) => total + count, 0);
}

/**
 * Polls the real GET /metrics endpoint (through forgedbApi.getLiveMetricsText,
 * same /api/* dev proxy every other live hook uses) and parses it into a
 * LiveMetricsSnapshot, falling back to an explicit 'mock'/'unavailable'
 * status -- never a silently-stale snapshot -- on any failure. This is the
 * Observability page's one live-polling hook; the page's remaining mock
 * panels (latency time series, SSTable/compaction, node CPU/memory, event
 * history) are unaffected and keep reading src/data/mockObservability.ts
 * directly, same as Phase B left Cluster's mock topology untouched.
 *
 * rates is request/error throughput derived from two consecutive polls'
 * counter deltas -- never shown until a second sample actually exists
 * (both fields are null before then, rendered as "COLLECTING" by callers,
 * never a fabricated number).
 */
export function useObservabilityMetrics(): UseObservabilityMetricsResult {
  const [state, setState] = useState<UseObservabilityMetricsResult>({
    status: 'loading',
    snapshot: null,
    rates: { requestsPerSec: null, errorsPerSec: null, windowSeconds: null },
    error: null,
    lastUpdated: null,
  });

  const lastUpdatedRef = useRef<Date | null>(null);
  const previousRef = useRef<{ requests: number; errors: number; at: number } | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      let text: string;
      try {
        text = await forgedbApi.getLiveMetricsText();
      } catch (err) {
        if (cancelled) return;
        setState((prev) => ({
          status: 'mock',
          snapshot: null,
          rates: prev.rates,
          error: err instanceof Error ? err.message : String(err),
          lastUpdated: lastUpdatedRef.current,
        }));
        return;
      }
      if (cancelled) return;

      const snapshot = buildLiveMetricsSnapshot(text);
      const now = Date.now();
      const totalRequests = sumRequests(snapshot.api.requestsByKey);
      const totalErrors = sumErrors(snapshot.system.errorsByComponent);

      let rates: DerivedRateMetrics = { requestsPerSec: null, errorsPerSec: null, windowSeconds: null };
      const previous = previousRef.current;
      if (previous) {
        const windowSeconds = (now - previous.at) / 1000;
        if (windowSeconds > 0) {
          rates = {
            requestsPerSec: Math.max(0, totalRequests - previous.requests) / windowSeconds,
            errorsPerSec: Math.max(0, totalErrors - previous.errors) / windowSeconds,
            windowSeconds,
          };
        }
      }
      previousRef.current = { requests: totalRequests, errors: totalErrors, at: now };

      lastUpdatedRef.current = new Date();
      setState({ status: 'live', snapshot, rates, error: null, lastUpdated: lastUpdatedRef.current });
    }

    poll();
    const id = window.setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  return state;
}
