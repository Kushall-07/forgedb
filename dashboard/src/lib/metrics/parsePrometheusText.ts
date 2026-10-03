/**
 * A small, dashboard-side parser for the Prometheus text exposition
 * format ForgeDB's GET /metrics returns (see internal/metrics/render.go).
 * It only implements what that one endpoint actually produces: HELP/TYPE
 * comment lines (skipped -- this layer has no use for metric descriptions
 * or declared type, only values), counters, gauges, and the per-bucket/
 * _sum/_count lines a histogram renders (each of those is just another
 * sample under its own name, so no special-case handling is needed for
 * them). This is not a general Prometheus client; it does not handle
 * summaries, exemplars, or timestamps, because ForgeDB's Render() never
 * emits any of those.
 */

export interface PrometheusSample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

const SAMPLE_LINE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)$/;
const LABEL_PAIR = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;

function unescapeLabelValue(raw: string): string {
  return raw.replace(/\\"/g, '"').replace(/\\n/g, '\n').replace(/\\\\/g, '\\');
}

/** Parses one Prometheus text-exposition document into a flat list of samples, skipping comments, HELP/TYPE lines, and any line that does not match a valid sample. */
export function parsePrometheusText(text: string): PrometheusSample[] {
  const samples: PrometheusSample[] = [];

  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;

    const match = SAMPLE_LINE.exec(line);
    if (!match) continue;

    const [, name, , labelPart, valueStr] = match;
    const value = Number(valueStr);
    if (!Number.isFinite(value)) continue;

    const labels: Record<string, string> = {};
    if (labelPart) {
      let labelMatch: RegExpExecArray | null;
      LABEL_PAIR.lastIndex = 0;
      while ((labelMatch = LABEL_PAIR.exec(labelPart))) {
        labels[labelMatch[1]] = unescapeLabelValue(labelMatch[2]);
      }
    }

    samples.push({ name, labels, value });
  }

  return samples;
}

/** Groups samples by metric name for O(1) lookup -- every metric this dashboard reads is looked up by name at least once per poll. */
export function indexSamples(samples: PrometheusSample[]): Map<string, PrometheusSample[]> {
  const index = new Map<string, PrometheusSample[]>();
  for (const sample of samples) {
    const bucket = index.get(sample.name);
    if (bucket) bucket.push(sample);
    else index.set(sample.name, [sample]);
  }
  return index;
}

/** The value of an unlabeled counter/gauge, or undefined if that metric is absent from this scrape (never 0 by assumption). */
export function scalarValue(index: Map<string, PrometheusSample[]>, name: string): number | undefined {
  return index.get(name)?.[0]?.value;
}

/** A labeled metric's samples as a plain {labelValue: value} map, keyed by one label (e.g. peer_id, component, role). */
export function labeledValues(index: Map<string, PrometheusSample[]>, name: string, labelKey: string): Record<string, number> {
  const out: Record<string, number> = {};
  for (const sample of index.get(name) ?? []) {
    out[sample.labels[labelKey] ?? ''] = sample.value;
  }
  return out;
}

/** For a GaugeVec used as a 1-of-N indicator (e.g. forgedb_raft_role), returns the label value whose sample is 1. */
export function activeLabel(index: Map<string, PrometheusSample[]>, name: string, labelKey: string): string | undefined {
  for (const sample of index.get(name) ?? []) {
    if (sample.value === 1) return sample.labels[labelKey];
  }
  return undefined;
}
