package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// typeName returns the Prometheus exposition-format TYPE token for kind.
func (k metricKind) typeName() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindHistogram:
		return "histogram"
	default:
		return "gauge"
	}
}

// formatValue renders v the way Prometheus text exposition expects:
// plain decimal, using Go's shortest round-trippable representation.
func formatValue(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Render returns every metric in r, serialized in Prometheus text
// exposition format (the format /metrics is documented to return -- see
// docs/observability/phase12-observability.md). Metrics are rendered in
// registration order within each HELP/TYPE block; label values are never
// escaped beyond Prometheus's own quoting rules (ForgeDB never puts a
// quote or backslash in a label value -- every label this package emits
// is a peer ID, HTTP route, role name, or component name it controls
// itself).
func (r *Registry) Render() string {
	r.mu.Lock()
	names := append([]string(nil), r.order...)
	entries := make(map[string]*entry, len(r.entries))
	for k, v := range r.entries {
		entries[k] = v
	}
	r.mu.Unlock()

	var b strings.Builder
	for _, name := range names {
		e := entries[name]
		fmt.Fprintf(&b, "# HELP %s %s\n", e.name, e.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", e.name, e.kind.typeName())
		renderEntry(&b, e)
	}
	return b.String()
}

func renderEntry(b *strings.Builder, e *entry) {
	switch {
	case e.collector != nil:
		values := e.collector()
		keys := make([]string, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			writeSample(b, e.name, e.labelNames, k, formatValue(values[k]))
		}

	case e.counter != nil:
		writeSample(b, e.name, nil, "", formatValue(float64(e.counter.Value())))

	case e.gauge != nil:
		writeSample(b, e.name, nil, "", formatValue(e.gauge.Value()))

	case e.histogram != nil:
		writeHistogram(b, e.name, nil, "", e.histogram)

	case e.counterVec != nil:
		e.vecMu.Lock()
		labels := make([]string, 0, len(e.counterVec))
		for k := range e.counterVec {
			labels = append(labels, k)
		}
		sort.Strings(labels)
		for _, l := range labels {
			writeSample(b, e.name, e.labelNames, l, formatValue(float64(e.counterVec[l].Value())))
		}
		e.vecMu.Unlock()

	case e.gaugeVec != nil:
		e.vecMu.Lock()
		labels := make([]string, 0, len(e.gaugeVec))
		for k := range e.gaugeVec {
			labels = append(labels, k)
		}
		sort.Strings(labels)
		for _, l := range labels {
			writeSample(b, e.name, e.labelNames, l, formatValue(e.gaugeVec[l].Value()))
		}
		e.vecMu.Unlock()

	case e.histVec != nil:
		e.vecMu.Lock()
		labels := make([]string, 0, len(e.histVec))
		for k := range e.histVec {
			labels = append(labels, k)
		}
		sort.Strings(labels)
		for _, l := range labels {
			writeHistogram(b, e.name, e.labelNames, l, e.histVec[l])
		}
		e.vecMu.Unlock()
	}
}

// writeSample writes one `name{label="value"} value` line. labelNames/
// labelValue are empty for an unlabeled metric.
func writeSample(b *strings.Builder, name string, labelNames []string, labelValue, value string) {
	if len(labelNames) == 0 {
		fmt.Fprintf(b, "%s %s\n", name, value)
		return
	}
	fmt.Fprintf(b, "%s{%s=%q} %s\n", name, labelNames[0], labelValue, value)
}

// writeHistogram writes a full histogram block: one cumulative `_bucket`
// line per bucket boundary (plus the implicit +Inf bucket), then `_sum`
// and `_count`, following Prometheus's histogram exposition convention.
func writeHistogram(b *strings.Builder, name string, labelNames []string, labelValue string, h *Histogram) {
	cumulative, sum, count := h.Snapshot()
	for i, upper := range h.buckets {
		writeLabeledBucket(b, name, labelNames, labelValue, formatValue(upper), cumulative[i])
	}
	writeLabeledBucket(b, name, labelNames, labelValue, "+Inf", cumulative[len(cumulative)-1])

	sumName := name + "_sum"
	countName := name + "_count"
	writeSample(b, sumName, labelNames, labelValue, formatValue(sum))
	writeSample(b, countName, labelNames, labelValue, strconv.FormatUint(count, 10))
}

func writeLabeledBucket(b *strings.Builder, name string, labelNames []string, labelValue, le string, count uint64) {
	bucketName := name + "_bucket"
	if len(labelNames) == 0 {
		fmt.Fprintf(b, "%s{le=%q} %d\n", bucketName, le, count)
		return
	}
	fmt.Fprintf(b, "%s{%s=%q,le=%q} %d\n", bucketName, labelNames[0], labelValue, le, count)
}
