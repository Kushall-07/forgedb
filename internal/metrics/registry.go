// Package metrics is ForgeDB's Phase 12 metrics subsystem: a small,
// dependency-free registry of counters, gauges, and histograms, plus a
// Prometheus text-exposition renderer. It knows nothing about Raft,
// storage, or HTTP -- other packages import it to record what happened
// (push metrics, e.g. a counter incremented at the moment an election
// starts) or to publish what the current state is (pull metrics, e.g. a
// gauge function that reads the current Raft term on demand when /metrics
// is scraped). See docs/observability/phase12-observability.md.
//
// Every metric type here is safe for concurrent use and built entirely on
// atomics: no metric operation ever blocks on, or is blocked by, a lock
// held somewhere else in the system (see the phase doc's "avoid lock
// contention" rule). Labels are always a bounded, caller-supplied set
// (e.g. one entry per peer in a fixed-size cluster) -- this package has no
// mechanism for, and nothing in ForgeDB uses it to create, unbounded or
// high-cardinality labels such as a request ID or a raw key.
package metrics

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// Default is the process-wide registry every ForgeDB package records
// into and that the diagnostic /metrics endpoint renders. A single global
// registry (mirroring the pattern internal/metrics already used for
// logging before Phase 12, and the way most Prometheus client libraries
// register collectors) keeps every package's instrumentation call sites a
// one-line, dependency-free call -- callers never thread a *Registry
// through every constructor just to record a metric.
var Default = NewRegistry()

// Counter is a monotonically increasing count of events (e.g. elections
// started, WAL syncs performed). It never decreases.
type Counter struct {
	v atomic.Uint64
}

// Inc increments the counter by 1.
func (c *Counter) Inc() { c.v.Add(1) }

// Add increments the counter by n. n must not be negative -- a Counter can
// never decrease.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value returns the counter's current value.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a value that can go up or down, reflecting some current state
// (e.g. the current Raft term, or the number of live MemTable entries).
type Gauge struct {
	bits atomic.Uint64 // math.Float64bits of the current value
}

// Set sets the gauge to v.
func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }

// Inc increments the gauge by 1.
func (g *Gauge) Inc() { g.Add(1) }

// Dec decrements the gauge by 1.
func (g *Gauge) Dec() { g.Add(-1) }

// Add adds delta to the gauge's current value.
func (g *Gauge) Add(delta float64) {
	for {
		old := g.bits.Load()
		newV := math.Float64bits(math.Float64frombits(old) + delta)
		if g.bits.CompareAndSwap(old, newV) {
			return
		}
	}
}

// Value returns the gauge's current value.
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// DefaultLatencyBuckets are the histogram bucket boundaries (in seconds)
// used for every latency histogram in this package's catalog, unless a
// metric specifically needs a different range. They follow the same
// shape Prometheus client libraries default to: fine-grained at
// sub-millisecond to low-millisecond scale, coarser toward multi-second
// outliers.
var DefaultLatencyBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// Histogram tracks the distribution of observed values (e.g. request
// latency) across a fixed, caller-supplied set of bucket boundaries. It
// never stores individual observations -- only a running count per
// bucket, a total count, and a running sum -- so its memory footprint is
// fixed regardless of how many observations it has recorded (see the
// phase doc's "no unbounded observability memory" rule).
type Histogram struct {
	buckets []float64 // ascending upper bounds; a final +Inf bucket is implicit
	counts  []atomic.Uint64
	sumBits atomic.Uint64
	total   atomic.Uint64
}

func newHistogram(buckets []float64) *Histogram {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	return &Histogram{buckets: b, counts: make([]atomic.Uint64, len(b)+1)}
}

// Observe records a single value.
func (h *Histogram) Observe(v float64) {
	idx := sort.SearchFloat64s(h.buckets, v)
	// sort.SearchFloat64s returns the first index whose bucket bound is >=
	// v; a value exactly equal to a bound belongs in that bucket, matching
	// Prometheus's own "le" (less-than-or-equal) bucket semantics.
	if idx < len(h.buckets) && h.buckets[idx] < v {
		idx++
	}
	h.counts[idx].Add(1)
	h.total.Add(1)
	for {
		old := h.sumBits.Load()
		newV := math.Float64bits(math.Float64frombits(old) + v)
		if h.sumBits.CompareAndSwap(old, newV) {
			return
		}
	}
}

// Snapshot returns the histogram's current cumulative bucket counts
// (indexed the same as the buckets this Histogram was created with, plus
// one trailing +Inf bucket), total observation count, and sum.
func (h *Histogram) Snapshot() (cumulative []uint64, sum float64, count uint64) {
	cumulative = make([]uint64, len(h.counts))
	var running uint64
	for i := range h.counts {
		running += h.counts[i].Load()
		cumulative[i] = running
	}
	return cumulative, math.Float64frombits(h.sumBits.Load()), h.total.Load()
}

// metricKind identifies a metric's Prometheus exposition type.
type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

// entry is one named metric the Registry knows how to render: either a
// concrete Counter/Gauge/Histogram this package owns, or a pull-based
// collector function supplied by another package (e.g. dbnode, wiring in
// a live read of raft.Node's current term). labelNames declares which
// label a vector/collector metric is keyed by (empty for an unlabeled
// scalar metric).
type entry struct {
	name       string
	help       string
	kind       metricKind
	labelNames []string

	counter   *Counter
	gauge     *Gauge
	histogram *Histogram

	// counterVec/gaugeVec hold one Counter/Gauge per distinct label value,
	// for a bounded set of labels (e.g. one per peer, one per API route).
	vecMu       sync.Mutex
	counterVec  map[string]*Counter
	gaugeVec    map[string]*Gauge
	histVec     map[string]*Histogram
	histBuckets []float64

	// collector, if non-nil, is called at render time to produce the
	// current label->value pairs for a pull-based gauge (see
	// NewGaugeFunc/NewGaugeVecFunc). It is never called while any
	// Registry-internal lock is held for longer than appending to a
	// slice, and it must not itself block on anything slow.
	collector func() map[string]float64
}

// Registry holds every metric ForgeDB has registered, by name. A
// Registry is safe for concurrent use.
type Registry struct {
	mu      sync.Mutex
	entries map[string]*entry
	order   []string
}

// NewRegistry returns an empty Registry. Production code almost always
// uses Default instead; NewRegistry exists mainly for tests that want an
// isolated registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]*entry)}
}

func (r *Registry) register(e *entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[e.name]; exists {
		panic(fmt.Sprintf("metrics: %q already registered", e.name))
	}
	r.entries[e.name] = e
	r.order = append(r.order, e.name)
}

// NewCounter registers and returns a new unlabeled Counter.
func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{}
	r.register(&entry{name: name, help: help, kind: kindCounter, counter: c})
	return c
}

// NewGauge registers and returns a new unlabeled, push-based Gauge.
func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &Gauge{}
	r.register(&entry{name: name, help: help, kind: kindGauge, gauge: g})
	return g
}

// NewHistogram registers and returns a new unlabeled Histogram with the
// given bucket boundaries.
func (r *Registry) NewHistogram(name, help string, buckets []float64) *Histogram {
	h := newHistogram(buckets)
	r.register(&entry{name: name, help: help, kind: kindHistogram, histogram: h})
	return h
}

// CounterVec is a set of Counters sharing one metric name, distinguished
// by a single label's value (e.g. peer_id or route). The set of distinct
// label values actually used is expected to stay small and bounded --
// CounterVec itself does not enforce a limit (that discipline belongs to
// the caller; see the phase doc's cardinality policy), but every
// CounterVec ForgeDB itself registers is keyed by a value drawn from a
// fixed, small set (peer IDs, HTTP routes, or component names).
type CounterVec struct {
	e *entry
}

// WithLabelValues returns the Counter for label, creating it on first use.
func (v *CounterVec) WithLabelValues(label string) *Counter {
	v.e.vecMu.Lock()
	defer v.e.vecMu.Unlock()
	if c, ok := v.e.counterVec[label]; ok {
		return c
	}
	c := &Counter{}
	v.e.counterVec[label] = c
	return c
}

// NewCounterVec registers and returns a new CounterVec keyed by labelName.
func (r *Registry) NewCounterVec(name, help, labelName string) *CounterVec {
	e := &entry{name: name, help: help, kind: kindCounter, labelNames: []string{labelName}, counterVec: make(map[string]*Counter)}
	r.register(e)
	return &CounterVec{e: e}
}

// GaugeVec is a set of Gauges sharing one metric name, distinguished by a
// single label's value. See CounterVec's cardinality note.
type GaugeVec struct {
	e *entry
}

// WithLabelValues returns the Gauge for label, creating it on first use.
func (v *GaugeVec) WithLabelValues(label string) *Gauge {
	v.e.vecMu.Lock()
	defer v.e.vecMu.Unlock()
	if g, ok := v.e.gaugeVec[label]; ok {
		return g
	}
	g := &Gauge{}
	v.e.gaugeVec[label] = g
	return g
}

// NewGaugeVec registers and returns a new GaugeVec keyed by labelName.
func (r *Registry) NewGaugeVec(name, help, labelName string) *GaugeVec {
	e := &entry{name: name, help: help, kind: kindGauge, labelNames: []string{labelName}, gaugeVec: make(map[string]*Gauge)}
	r.register(e)
	return &GaugeVec{e: e}
}

// HistogramVec is a set of Histograms sharing one metric name,
// distinguished by a single label's value (e.g. API route).
type HistogramVec struct {
	e *entry
}

// WithLabelValues returns the Histogram for label, creating it on first use.
func (v *HistogramVec) WithLabelValues(label string) *Histogram {
	v.e.vecMu.Lock()
	defer v.e.vecMu.Unlock()
	if h, ok := v.e.histVec[label]; ok {
		return h
	}
	h := newHistogram(v.e.histBuckets)
	v.e.histVec[label] = h
	return h
}

// NewHistogramVec registers and returns a new HistogramVec keyed by
// labelName, with every member Histogram using buckets.
func (r *Registry) NewHistogramVec(name, help, labelName string, buckets []float64) *HistogramVec {
	e := &entry{name: name, help: help, kind: kindHistogram, labelNames: []string{labelName}, histVec: make(map[string]*Histogram), histBuckets: buckets}
	r.register(e)
	return &HistogramVec{e: e}
}

// NewGaugeFunc registers a pull-based, unlabeled gauge: fn is called at
// render time to produce its current value. fn must return quickly and
// must not block on I/O or on a lock any hot path might also need -- it
// is expected to do nothing more than read already-available state
// through a cheap, already-synchronized accessor (e.g.
// (*raft.Node).State()).
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.register(&entry{name: name, help: help, kind: kindGauge, collector: func() map[string]float64 {
		return map[string]float64{"": fn()}
	}})
}

// NewGaugeVecFunc registers a pull-based gauge keyed by labelName: fn is
// called at render time and must return the current value for every
// label value that currently exists (e.g. one entry per peer). See
// NewGaugeFunc's performance note.
func (r *Registry) NewGaugeVecFunc(name, help, labelName string, fn func() map[string]float64) {
	r.register(&entry{name: name, help: help, kind: kindGauge, labelNames: []string{labelName}, collector: fn})
}

// RecordError increments the shared forgedb_errors_total{component}
// aggregate for component. See catalog.go's ErrorsTotal: this exists so
// ForgeDB has one bounded, low-cardinality place to look for "is anything
// failing anywhere" instead of dozens of near-identical per-component
// error counters (see the phase doc's cardinality and "avoid near
// duplicate metrics" rules).
func RecordError(component string) {
	ErrorsTotal.WithLabelValues(component).Inc()
}
