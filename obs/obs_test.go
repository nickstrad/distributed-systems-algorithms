// Story: In-memory metrics, log-linear histograms, and snapshots stay deterministic, allocation-free, and accurate.
//
// What these tests prove: Counter/Gauge semantics, exact histogram buckets for 0..255 and <1% quantile error, zero-alloc hot-path updates, deterministic Collector snapshot ordering/cloning, and adapter error isolation.
// How they probe: unit boundary checks, testing.AllocsPerRun assertions on Counter.Inc/Gauge.Add/Histogram.Record, fault-injecting adapters, and rapid quantile/monotonicity checks.
// What they do not prove: disk, Prometheus text, or structured log-line encoding formats (tested in obs/csvflush, obs/promtext, and obs/logline).
package obs_test

import (
	"errors"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/nickstrad/distributed-systems-algorithms/obs"
)

func TestKindString(t *testing.T) {
	if got := obs.KindCounter.String(); got != "counter" {
		t.Fatalf("KindCounter.String() = %q, want %q", got, "counter")
	}
	if got := obs.KindGauge.String(); got != "gauge" {
		t.Fatalf("KindGauge.String() = %q, want %q", got, "gauge")
	}
	if got := obs.Kind(99).String(); got != "unknown" {
		t.Fatalf("Kind(99).String() = %q, want %q", got, "unknown")
	}
}

func TestCounter(t *testing.T) {
	var c obs.Counter
	if c.Value() != 0 {
		t.Fatalf("initial Counter.Value() = %d, want 0", c.Value())
	}
	c.Inc()
	if c.Value() != 1 {
		t.Fatalf("after Inc, Counter.Value() = %d, want 1", c.Value())
	}
	c.Add(5)
	if c.Value() != 6 {
		t.Fatalf("after Add(5), Counter.Value() = %d, want 6", c.Value())
	}
	c.Add(0)
	c.Add(-10)
	if c.Value() != 6 {
		t.Fatalf("after Add(0) and Add(-10), Counter.Value() = %d, want 6", c.Value())
	}
}

func TestGauge(t *testing.T) {
	var g obs.Gauge
	if g.Value() != 0 || g.Peak() != 0 {
		t.Fatalf("initial Gauge = (%d, peak=%d), want (0, 0)", g.Value(), g.Peak())
	}
	g.Inc()
	g.Add(4)
	if g.Value() != 5 || g.Peak() != 5 {
		t.Fatalf("after Inc+Add(4), Gauge = (%d, peak=%d), want (5, 5)", g.Value(), g.Peak())
	}
	g.Dec()
	g.Add(-6)
	if g.Value() != -2 || g.Peak() != 5 {
		t.Fatalf("after Dec+Add(-6), Gauge = (%d, peak=%d), want (-2, 5)", g.Value(), g.Peak())
	}
	g.Set(3)
	if g.Value() != 3 || g.Peak() != 5 {
		t.Fatalf("after Set(3), Gauge = (%d, peak=%d), want (3, 5)", g.Value(), g.Peak())
	}
	g.Set(12)
	if g.Value() != 12 || g.Peak() != 12 {
		t.Fatalf("after Set(12), Gauge = (%d, peak=%d), want (12, 12)", g.Value(), g.Peak())
	}
}

func TestHistogramEmptyAndConstructors(t *testing.T) {
	dur := obs.NewDurationHistogram()
	if dur.Count() != 0 || dur.ClampedLow() != 0 || dur.ClampedHigh() != 0 {
		t.Fatalf("empty duration hist stats = (%d, %d, %d), want all 0",
			dur.Count(), dur.ClampedLow(), dur.ClampedHigh())
	}
	if got := dur.Quantile(5000); got != 0 {
		t.Fatalf("empty duration Quantile(5000) = %d, want 0", got)
	}
	if got := dur.Buckets(); got != nil {
		t.Fatalf("empty duration Buckets() = %v, want nil", got)
	}

	dur.Record(999)
	dur.Record(1_000)
	dur.Record(60_000_000_000)
	dur.Record(60_000_000_001)
	if dur.Count() != 4 {
		t.Fatalf("dur.Count() = %d, want 4", dur.Count())
	}
	if dur.ClampedLow() != 1 {
		t.Fatalf("dur.ClampedLow() = %d, want 1", dur.ClampedLow())
	}
	if dur.ClampedHigh() != 1 {
		t.Fatalf("dur.ClampedHigh() = %d, want 1", dur.ClampedHigh())
	}

	cnt := obs.NewCountHistogram()
	if cnt.Quantile(0) != 0 || cnt.Quantile(9900) != 0 {
		t.Fatalf("empty count histogram Quantile should be 0")
	}
	cnt.Record(-1)
	cnt.Record(1_000_001)
	if cnt.ClampedLow() != 1 || cnt.ClampedHigh() != 1 || cnt.Count() != 2 {
		t.Fatalf("count clamp stats = (count=%d, low=%d, high=%d), want (2, 1, 1)",
			cnt.Count(), cnt.ClampedLow(), cnt.ClampedHigh())
	}

	// Normalized bounds when lo < 0 or hi < lo.
	h := obs.NewHistogram(-10, -5)
	h.Record(0)
	h.Record(5)
	if h.Count() != 2 || h.ClampedHigh() != 1 || h.ClampedLow() != 0 {
		t.Fatalf("degenerate histogram stats = (count=%d, low=%d, high=%d), want (2, 0, 1)",
			h.Count(), h.ClampedLow(), h.ClampedHigh())
	}
	if q := h.Quantile(5000); q != 0 {
		t.Fatalf("degenerate histogram Quantile(5000) = %d, want 0", q)
	}
}

func TestHistogramExactBuckets0To255(t *testing.T) {
	h := obs.NewCountHistogram()
	for v := int64(0); v < 256; v++ {
		h.Record(v)
	}
	if h.Count() != 256 || h.ClampedLow() != 0 || h.ClampedHigh() != 0 {
		t.Fatalf("unexpected counts: count=%d low=%d high=%d", h.Count(), h.ClampedLow(), h.ClampedHigh())
	}
	buckets := h.Buckets()
	if len(buckets) != 256 {
		t.Fatalf("len(Buckets()) = %d, want 256", len(buckets))
	}
	for i, b := range buckets {
		want := obs.Bucket{Lo: int64(i), Hi: int64(i), Count: 1}
		if b != want {
			t.Fatalf("bucket[%d] = %+v, want %+v", i, b, want)
		}
	}
	// Spot-check quantiles on uniform 0..255.
	if q := h.Quantile(0); q != 0 {
		t.Fatalf("Quantile(0) = %d, want 0", q)
	}
	if q := h.Quantile(5000); q != 127 {
		t.Fatalf("Quantile(5000) = %d, want 127", q)
	}
	if q := h.Quantile(10_000); q != 255 {
		t.Fatalf("Quantile(10000) = %d, want 255", q)
	}
	if q := h.Quantile(20_000); q != 255 {
		t.Fatalf("Quantile(20000) = %d, want 255", q)
	}
}

type recordingAdapter struct {
	name      string
	errOnCall map[int]error
	calls     int
	snapshots []obs.Snapshot
	mutate    bool
}

func (a *recordingAdapter) Name() string { return a.name }

func (a *recordingAdapter) Export(s obs.Snapshot) error {
	a.calls++
	if err := a.errOnCall[a.calls]; err != nil {
		return err
	}
	a.snapshots = append(a.snapshots, s)
	if a.mutate {
		if len(s.Points) > 0 {
			s.Points[0].Value = -999
			if len(s.Points[0].Labels) > 0 {
				s.Points[0].Labels[0].Value = "mutated"
			}
		}
		if len(s.Hists) > 0 {
			s.Hists[0].Count = -999
			if len(s.Hists[0].Buckets) > 0 {
				s.Hists[0].Buckets[0].Count = -999
			}
			if len(s.Hists[0].Labels) > 0 {
				s.Hists[0].Labels[0].Value = "mutated"
			}
		}
	}
	return nil
}

func TestCollectorOrderingIsolationAndAdapterErrors(t *testing.T) {
	c := obs.NewCollector("node-1")
	if _, ok := c.Last(); ok {
		t.Fatal("Last() before Publish returned ok=true, want false")
	}

	h := obs.NewCountHistogram()
	h.Record(42)

	var emitOrder []string

	// Register sources out of alphabetical order and emit unsorted metrics/labels.
	c.Register(nil)
	c.Register(obs.SourceFunc{
		Name: "zeta",
		Fn: func(e *obs.Emitter) {
			emitOrder = append(emitOrder, "zeta")
			e.Counter("z_metric", "count", "help z", -5, obs.Label{Key: "b", Value: "2"}, obs.Label{Key: "a", Value: "1"})
			e.Counter("a_metric", "count", "help a", 7, obs.Label{Key: "b", Value: "2"}, obs.Label{Key: "a", Value: "1"})
			e.Counter("a_metric", "count", "help a", 9, obs.Label{Key: "a", Value: "2"})
			e.Histogram("h_b", "count", "hist b", h, obs.Label{Key: "k", Value: "2"})
			e.Histogram("h_a", "count", "hist a", h, obs.Label{Key: "z", Value: "1"}, obs.Label{Key: "a", Value: "1"})
			e.Histogram("ignored_nil", "count", "nil hist", nil)
		},
	})
	c.Register(obs.SourceFunc{
		Name: "alpha",
		Fn: func(e *obs.Emitter) {
			emitOrder = append(emitOrder, "alpha")
			e.Gauge("queue_depth", "count", "depth", 11, obs.Label{Key: "tier", Value: "edge"})
		},
	})

	failing := &recordingAdapter{
		name:      "flaky",
		errOnCall: map[int]error{1: errors.New("boom-1"), 2: errors.New("boom-2")},
	}
	mutating := &recordingAdapter{name: "mutator", mutate: true}
	healthy := &recordingAdapter{name: "healthy"}

	c.AddAdapter(nil)
	c.AddAdapter(failing)
	c.AddAdapter(mutating)
	c.AddAdapter(healthy)

	snap0 := c.Publish(1000)
	if !slices.Equal(emitOrder, []string{"alpha", "zeta"}) {
		t.Fatalf("source emit order = %v, want [alpha zeta]", emitOrder)
	}
	if snap0.Seq != 0 || snap0.TimeNs != 1000 || snap0.Instance != "node-1" {
		t.Fatalf("snap0 header = (seq=%d, t=%d, inst=%q), want (0, 1000, node-1)",
			snap0.Seq, snap0.TimeNs, snap0.Instance)
	}

	// Verify deterministic sorted points.
	wantPoints := []struct {
		comp, metric string
		kind         obs.Kind
		val          int64
		labels       []obs.Label
	}{
		{"alpha", "queue_depth", obs.KindGauge, 11, []obs.Label{{Key: "tier", Value: "edge"}}},
		{"zeta", "a_metric", obs.KindCounter, 7, []obs.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}},
		{"zeta", "a_metric", obs.KindCounter, 9, []obs.Label{{Key: "a", Value: "2"}}},
		{"zeta", "z_metric", obs.KindCounter, 0, []obs.Label{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}},
	}
	if len(snap0.Points) != len(wantPoints) {
		t.Fatalf("len(Points) = %d, want %d", len(snap0.Points), len(wantPoints))
	}
	for i, wp := range wantPoints {
		got := snap0.Points[i]
		if got.Component != wp.comp || got.Metric != wp.metric || got.Kind != wp.kind || got.Value != wp.val || !slices.Equal(got.Labels, wp.labels) {
			t.Fatalf("Points[%d] = %+v, want comp=%s metric=%s kind=%v val=%d labels=%+v",
				i, got, wp.comp, wp.metric, wp.kind, wp.val, wp.labels)
		}
	}

	// Verify deterministic sorted histograms and sorted labels.
	if len(snap0.Hists) != 2 {
		t.Fatalf("len(Hists) = %d, want 2", len(snap0.Hists))
	}
	if snap0.Hists[0].Metric != "h_a" || snap0.Hists[1].Metric != "h_b" {
		t.Fatalf("Hists order = [%s, %s], want [h_a, h_b]", snap0.Hists[0].Metric, snap0.Hists[1].Metric)
	}
	if !slices.Equal(snap0.Hists[0].Labels, []obs.Label{{Key: "a", Value: "1"}, {Key: "z", Value: "1"}}) {
		t.Fatalf("Hists[0].Labels = %+v, want sorted", snap0.Hists[0].Labels)
	}

	// Verify healthy adapter was unaffected by mutator or flaky adapter errors.
	if len(healthy.snapshots) != 1 {
		t.Fatalf("healthy snapshots = %d, want 1", len(healthy.snapshots))
	}
	if healthy.snapshots[0].Points[0].Value != 11 {
		t.Fatalf("healthy adapter saw mutated point value %d, want 11", healthy.snapshots[0].Points[0].Value)
	}
	if healthy.snapshots[0].Hists[0].Count != 1 || healthy.snapshots[0].Hists[0].Buckets[0].Count != 1 {
		t.Fatalf("healthy adapter saw mutated hist: %+v", healthy.snapshots[0].Hists[0])
	}

	// Verify Last() deep-copy isolation.
	last1, ok := c.Last()
	if !ok {
		t.Fatal("Last() after Publish returned ok=false")
	}
	last1.Points[0].Value = 777
	last1.Points[0].Labels[0].Value = "corrupted"
	last1.Hists[0].Labels[0].Value = "corrupted"
	last1.Hists[0].Buckets[0].Count = 777

	last2, ok := c.Last()
	if !ok {
		t.Fatal("second Last() returned ok=false")
	}
	if last2.Points[0].Value != 11 || last2.Points[0].Labels[0].Value != "edge" {
		t.Fatalf("Last() leaked point mutation: %+v", last2.Points[0])
	}
	if last2.Hists[0].Labels[0].Value != "1" || last2.Hists[0].Buckets[0].Count != 1 {
		t.Fatalf("Last() leaked hist mutation: %+v", last2.Hists[0])
	}

	// Second publish fails again on flaky, third succeeds; cumulative error count remains 2.
	snap1 := c.Publish(2000)
	snap2 := c.Publish(3000)
	if snap1.Seq != 1 || snap2.Seq != 2 {
		t.Fatalf("Seq progression = (%d, %d), want (1, 2)", snap1.Seq, snap2.Seq)
	}
	if got := c.AdapterErrors("flaky"); got != 2 {
		t.Fatalf("AdapterErrors(flaky) = %d, want 2", got)
	}
	if got := c.AdapterErrors("healthy"); got != 0 {
		t.Fatalf("AdapterErrors(healthy) = %d, want 0", got)
	}
	if len(failing.snapshots) != 1 || failing.snapshots[0].Seq != 2 {
		t.Fatalf("failing adapter recovery snapshots = %+v, want 1 snapshot with Seq=2", failing.snapshots)
	}
	if len(healthy.snapshots) != 3 {
		t.Fatalf("healthy adapter received %d snapshots, want 3", len(healthy.snapshots))
	}
}

func TestZeroAllocsHotPath(t *testing.T) {
	var c obs.Counter
	var g obs.Gauge
	h := obs.NewDurationHistogram()

	if allocs := testing.AllocsPerRun(1000, func() {
		c.Inc()
	}); allocs != 0 {
		t.Fatalf("Counter.Inc allocs = %v, want 0", allocs)
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		g.Add(1)
	}); allocs != 0 {
		t.Fatalf("Gauge.Add(1) allocs = %v, want 0", allocs)
	}

	if allocs := testing.AllocsPerRun(1000, func() {
		h.Record(500_000)
	}); allocs != 0 {
		t.Fatalf("Histogram.Record(500_000) allocs = %v, want 0", allocs)
	}
}

func checkQuantileAccuracy(t *rapid.T, v, q int64) {
	t.Helper()
	if v > q {
		t.Fatalf("value %d exceeds reported quantile upper bound %d", v, q)
	}
	if v < 256 {
		if q != v {
			t.Fatalf("exact range value %d got quantile %d, want %d", v, q, v)
		}
		return
	}
	if (q-v)*128 > v {
		t.Fatalf("quantile error too large for v=%d, q=%d: (q-v)*128=%d > v", v, q, (q-v)*128)
	}
}

func TestPropHistogramQuantileAccuracy(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		v := rapid.Int64Range(0, 1_000_000).Draw(t, "v")
		bp := rapid.Int64Range(0, 10_000).Draw(t, "bp")

		single := obs.NewCountHistogram()
		single.Record(v)
		q := single.Quantile(bp)
		checkQuantileAccuracy(t, v, q)

		// Also verify multi-sample accuracy and monotonic non-decreasing Quantile(bp).
		vals := rapid.SliceOfN(rapid.Int64Range(0, 1_000_000), 1, 128).Draw(t, "vals")
		h := obs.NewCountHistogram()
		for _, x := range vals {
			h.Record(x)
		}
		if h.Count() != int64(len(vals)) || h.ClampedLow() != 0 || h.ClampedHigh() != 0 {
			t.Fatalf("unexpected counts: count=%d low=%d high=%d", h.Count(), h.ClampedLow(), h.ClampedHigh())
		}

		sorted := slices.Clone(vals)
		slices.Sort(sorted)

		bps := rapid.SliceOfN(rapid.Int64Range(0, 10_000), 2, 32).Draw(t, "bps")
		bps = append(bps, 0, 5000, 9900, 10_000)
		slices.Sort(bps)

		prev := int64(-1)
		n := int64(len(sorted))
		for _, curBP := range bps {
			curQ := h.Quantile(curBP)
			if curQ < prev {
				t.Fatalf("Quantile not monotonic: bp=%d gave %d < prev %d", curBP, curQ, prev)
			}
			prev = curQ

			var exact int64
			if curBP == 0 {
				exact = sorted[0]
			} else {
				rank := (n*curBP + 9999) / 10_000
				if rank < 1 {
					rank = 1
				}
				exact = sorted[rank-1]
			}
			checkQuantileAccuracy(t, exact, curQ)
		}
	})
}
