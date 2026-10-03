import { useEffect, useRef, useState } from 'react';
import { forgedbApi } from '../services/forgedbApi';
import { cluster as mockCluster, storage as mockStorage } from '../data/mockCluster';
import type { ClusterState, StorageState } from '../types/cluster';
import type { LiveNodeSnapshot } from '../types/backend';

/**
 * The dashboard's one source-of-truth label for where its cluster/health
 * data came from this render -- see Phase B's "do not silently fall back
 * from a failed live request to mock data while still displaying a LIVE
 * indicator" rule. 'loading' only ever applies to the very first fetch;
 * every poll after that resolves to 'live' or 'mock'.
 */
export type BackendStatus = 'loading' | 'live' | 'mock' | 'unavailable';

export interface UseBackendClusterResult {
  status: BackendStatus;
  /** Live GET /cluster view when status is 'live', otherwise the mock ClusterState unchanged. */
  cluster: ClusterState;
  /**
   * memtableEntries/memtableBytes/snapshotIndex come from the live node
   * when status is 'live' (GET /cluster exposes exactly those storage
   * fields -- see internal/dbnode.StorageStatus); wal and sstables are
   * never reported by the backend at all and so are always the mock
   * values, in both states.
   */
  storage: StorageState;
  /** The queried node's own raw Raft/storage snapshot, only present when status is 'live'. */
  liveNode: LiveNodeSnapshot | null;
  error: string | null;
  lastUpdated: Date | null;
}

const POLL_INTERVAL_MS = 10_000;

/**
 * Polls the real ForgeDB /health and /cluster endpoints (through
 * forgedbApi.getLiveSnapshot) and exposes the result alongside a status
 * flag, falling back to src/data/mockCluster.ts's static values -- clearly
 * labeled 'mock', never silently relabeled 'live' -- whenever the backend
 * request fails for any reason (network error, non-2xx, malformed body).
 * This is the dashboard's only live-polling cluster hook; components never
 * fetch on their own (see services/forgedbApi.ts's doc comment).
 */
export function useBackendCluster(): UseBackendClusterResult {
  const [state, setState] = useState<UseBackendClusterResult>({
    status: 'loading',
    cluster: mockCluster,
    storage: mockStorage,
    liveNode: null,
    error: null,
    lastUpdated: null,
  });
  const lastUpdatedRef = useRef<Date | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      const snapshot = await forgedbApi.getLiveSnapshot();
      if (cancelled) return;

      if (snapshot.source === 'live') {
        lastUpdatedRef.current = new Date();
        setState({
          status: 'live',
          cluster: snapshot.cluster,
          storage: {
            ...mockStorage,
            memtableEntries: snapshot.node.memtableEntries,
            memtableBytes: snapshot.node.memtableBytes,
            snapshotIndex: snapshot.node.snapshotIndex,
          },
          liveNode: snapshot.node,
          error: null,
          lastUpdated: lastUpdatedRef.current,
        });
      } else {
        setState({
          status: 'mock',
          cluster: mockCluster,
          storage: mockStorage,
          liveNode: null,
          error: snapshot.error,
          lastUpdated: lastUpdatedRef.current,
        });
      }
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
