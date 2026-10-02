package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestCounter_IncAndAdd(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("c", "help")
	c.Inc()
	c.Add(5)
	if got := c.Value(); got != 6 {
		t.Fatalf("Value() = %d, want 6", got)
	}
}

func TestGauge_SetIncDecAdd(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("g", "help")
	g.Set(10)
	g.Inc()
	g.Dec()
	g.Dec()
	g.Add(2.5)
	if got := g.Value(); got != 11.5 {
		t.Fatalf("Value() = %v, want 11.5", got)
	}
}

func TestHistogram_Observe(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("h", "help", []float64{1, 5, 10})
	h.Observe(0.5)
	h.Observe(1) // exactly on a bucket bound: belongs in that bucket (le semantics)
	h.Observe(3)
	h.Observe(100) // beyond every bound: only the +Inf bucket

	cumulative, sum, count := h.Snapshot()
	if count != 4 {
		t.Fatalf("count = %d, want 4", count)
	}
	if sum != 0.5+1+3+100 {
		t.Fatalf("sum = %v, want %v", sum, 0.5+1+3+100)
	}
	// buckets: [<=1, <=5, <=10, +Inf]
	want := []uint64{2, 3, 3, 4}
	if len(cumulative) != len(want) {
		t.Fatalf("len(cumulative) = %d, want %d", len(cumulative), len(want))
	}
	for i := range want {
		if cumulative[i] != want[i] {
			t.Fatalf("cumulative[%d] = %d, want %d", i, cumulative[i], want[i])
		}
	}
}

func TestCounterVec_WithLabelValues(t *testing.T) {
	r := NewRegistry()
	v := r.NewCounterVec("cv", "help", "peer_id")
	v.WithLabelValues("peer-1").Inc()
	v.WithLabelValues("peer-1").Inc()
	v.WithLabelValues("peer-2").Inc()

	if got := v.WithLabelValues("peer-1").Value(); got != 2 {
		t.Fatalf("peer-1 = %d, want 2", got)
	}
	if got := v.WithLabelValues("peer-2").Value(); got != 1 {
		t.Fatalf("peer-2 = %d, want 1", got)
	}
}

func TestGaugeFunc_PullsCurrentValue(t *testing.T) {
	r := NewRegistry()
	current := 42.0
	r.NewGaugeFunc("gf", "help", func() float64 { return current })

	out := r.Render()
	if !strings.Contains(out, "gf 42\n") {
		t.Fatalf("Render() missing gf 42, got:\n%s", out)
	}

	current = 99
	out = r.Render()
	if !strings.Contains(out, "gf 99\n") {
		t.Fatalf("Render() did not reflect updated value, got:\n%s", out)
	}
}

func TestGaugeVecFunc_MultipleLabels(t *testing.T) {
	r := NewRegistry()
	r.NewGaugeVecFunc("lag", "help", "peer_id", func() map[string]float64 {
		return map[string]float64{"a": 1, "b": 2}
	})
	out := r.Render()
	if !strings.Contains(out, `lag{peer_id="a"} 1`) || !strings.Contains(out, `lag{peer_id="b"} 2`) {
		t.Fatalf("Render() missing expected label rows, got:\n%s", out)
	}
}

func TestRender_CounterGaugeAndHistogram(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("forgedb_test_counter", "a counter")
	c.Add(3)
	g := r.NewGauge("forgedb_test_gauge", "a gauge")
	g.Set(7)
	h := r.NewHistogram("forgedb_test_hist", "a histogram", []float64{1, 2})
	h.Observe(1.5)

	out := r.Render()
	for _, want := range []string{
		"# HELP forgedb_test_counter a counter",
		"# TYPE forgedb_test_counter counter",
		"forgedb_test_counter 3",
		"# TYPE forgedb_test_gauge gauge",
		"forgedb_test_gauge 7",
		"# TYPE forgedb_test_hist histogram",
		`forgedb_test_hist_bucket{le="1"} 0`,
		`forgedb_test_hist_bucket{le="2"} 1`,
		`forgedb_test_hist_bucket{le="+Inf"} 1`,
		"forgedb_test_hist_sum 1.5",
		"forgedb_test_hist_count 1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("Render() missing %q, got:\n%s", want, out)
		}
	}
}

func TestRegistry_DuplicateNamePanics(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("dup", "help")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic registering a duplicate metric name")
		}
	}()
	r.NewCounter("dup", "help")
}

func TestCounter_ConcurrentIncrements(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("concurrent", "help")
	v := r.NewCounterVec("concurrent_vec", "help", "k")

	var wg sync.WaitGroup
	const goroutines = 50
	const perGoroutine = 200
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				c.Inc()
				v.WithLabelValues("x").Inc()
			}
		}()
	}
	wg.Wait()

	if got, want := c.Value(), uint64(goroutines*perGoroutine); got != want {
		t.Fatalf("Value() = %d, want %d", got, want)
	}
	if got, want := v.WithLabelValues("x").Value(), uint64(goroutines*perGoroutine); got != want {
		t.Fatalf("vec Value() = %d, want %d", got, want)
	}
}

func TestRecordError_IncrementsSharedAggregate(t *testing.T) {
	before := ErrorsTotal.WithLabelValues("test-component").Value()
	RecordError("test-component")
	after := ErrorsTotal.WithLabelValues("test-component").Value()
	if after != before+1 {
		t.Fatalf("ErrorsTotal[test-component] = %d, want %d", after, before+1)
	}
}
