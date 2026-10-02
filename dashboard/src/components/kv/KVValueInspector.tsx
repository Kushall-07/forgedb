import type { KVEntry } from '../../types/kv';
import { SectionHeader } from '../ui/SectionHeader';
import { StatusIndicator } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './KVValueInspector.css';

interface KVValueInspectorProps {
  entryKey: string;
  entry: KVEntry | null;
}

/** The KV state viewer: what the store currently holds for the key the last request touched. */
export function KVValueInspector({ entryKey, entry }: KVValueInspectorProps) {
  const isTombstone = entry?.state === 'TOMBSTONE';

  return (
    <section className="kv-value-inspector" aria-label="Key state viewer">
      <SectionHeader eyebrow="State" title="Key State / Value Viewer" description="The stored entry for the key the last request touched." />

      <div className="kv-value-inspector__panel panel" data-sc-in>
        <div className="kv-value-inspector__head">
          <div className="kv-value-inspector__field">
            <TechnicalLabel>Key</TechnicalLabel>
            <span className="mono kv-value-inspector__key">{entryKey}</span>
          </div>
          <StatusIndicator
            tone={!entry ? 'neutral' : isTombstone ? 'critical' : 'sync'}
            label={!entry ? 'NEVER WRITTEN' : isTombstone ? 'DELETED / TOMBSTONE' : 'PRESENT'}
          />
        </div>

        {entry ? (
          <>
            <div className="kv-value-inspector__meta">
              <div className="kv-value-inspector__field">
                <TechnicalLabel>Value Type</TechnicalLabel>
                <span className="mono">{entry.valueType}</span>
              </div>
              <div className="kv-value-inspector__field">
                <TechnicalLabel>Size</TechnicalLabel>
                <span className="mono">{entry.sizeBytes} B</span>
              </div>
              <div className="kv-value-inspector__field">
                <TechnicalLabel>Raft Log Index</TechnicalLabel>
                <span className="mono">{entry.raftLogIndex}</span>
              </div>
              <div className="kv-value-inspector__field">
                <TechnicalLabel>Last Updated</TechnicalLabel>
                <span className="mono">{entry.updatedAt}</span>
              </div>
            </div>

            <div className="kv-value-inspector__value">
              <TechnicalLabel as="div">Value</TechnicalLabel>
              {isTombstone ? (
                <div className="kv-value-inspector__tombstone mono">TOMBSTONE — value cleared</div>
              ) : (
                <pre className="kv-value-inspector__value-block mono">{JSON.stringify(entry.value, null, 2)}</pre>
              )}
            </div>
          </>
        ) : (
          <p className="kv-value-inspector__empty">This key has no stored entry in the mock store yet.</p>
        )}
      </div>
    </section>
  );
}
