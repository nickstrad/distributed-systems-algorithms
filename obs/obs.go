// Package obs provides a zero-dependency, deterministic observability spine.
//
// Protocol components maintain plain integer Counters, Gauges, and Histograms
// and expose a Snapshot through the Source interface. A Collector gathers
// sources in deterministic order and fans snapshots out to pluggable Adapters
// (such as CSV files for offline/DuckDB analysis, Prometheus text exposition,
// or compact structured log lines).
//
// Core obs has no locks, no floats, no wall clock, and no third-party imports,
// so it can be used safely inside deterministic simulations as well as real
// production nodes.
package obs

import (
	"cmp"
	"slices"
)

// Kind distinguishes cumulative monotonic counters from point-in-time gauges.
type Kind uint8

const (
	KindCounter Kind = iota
	KindGauge
)

// String returns the schema name for k ("counter" or "gauge").
func (k Kind) String() string {
	switch k {
	case KindCounter:
		return "counter"
	case KindGauge:
		return "gauge"
	default:
		return "unknown"
	}
}

// Label is a bounded, low-cardinality key-value dimension attached to a metric.
type Label struct {
	Key   string
	Value string
}

// Counter is a non-negative cumulative integer counter with zero-allocation updates.
type Counter struct {
	v int64
}

// Inc increments the counter by 1.
func (c *Counter) Inc() { c.v++ }

// Add increments the counter by n (ignoring negative deltas).
func (c *Counter) Add(n int64) {
	if n > 0 {
		c.v += n
	}
}

// Value returns the current counter value.
func (c *Counter) Value() int64 { return c.v }

// Gauge tracks a signed integer level along with its high-water mark Peak.
type Gauge struct {
	v    int64
	peak int64
}

// Set sets the gauge value and updates Peak if v exceeds it.
func (g *Gauge) Set(v int64) {
	g.v = v
	if v > g.peak {
		g.peak = v
	}
}

// Add adjusts the gauge by delta and updates Peak.
func (g *Gauge) Add(delta int64) {
	g.Set(g.v + delta)
}

// Inc increments the gauge by 1.
func (g *Gauge) Inc() { g.Add(1) }

// Dec decrements the gauge by 1.
func (g *Gauge) Dec() { g.Add(-1) }

// Value returns the current gauge level.
func (g *Gauge) Value() int64 { return g.v }

// Peak returns the highest value ever recorded on this gauge.
func (g *Gauge) Peak() int64 { return g.peak }

// Point is one scalar counter or gauge observation inside a Snapshot.
type Point struct {
	Component string
	Metric    string
	Kind      Kind
	Unit      string // e.g. "count", "bytes", "ns"
	Help      string
	Labels    []Label
	Value     int64
}

// HistPoint is one bucketed integer distribution observation inside a Snapshot.
type HistPoint struct {
	Component   string
	Metric      string
	Unit        string // e.g. "ns", "bytes", "count"
	Help        string
	Labels      []Label
	Buckets     []Bucket
	Count       int64
	ClampedLow  int64
	ClampedHigh int64
}

// Snapshot is a deterministic, sorted, immutable point-in-time capture of all
// registered sources for one instance.
type Snapshot struct {
	Seq      int64
	TimeNs   int64
	Instance string
	Points   []Point
	Hists    []HistPoint
}

// Clone returns a deep copy of s so adapters and readers cannot alias state.
func (s Snapshot) Clone() Snapshot {
	out := Snapshot{
		Seq:      s.Seq,
		TimeNs:   s.TimeNs,
		Instance: s.Instance,
		Points:   make([]Point, len(s.Points)),
		Hists:    make([]HistPoint, len(s.Hists)),
	}
	for i, p := range s.Points {
		p.Labels = slices.Clone(p.Labels)
		out.Points[i] = p
	}
	for i, h := range s.Hists {
		h.Labels = slices.Clone(h.Labels)
		h.Buckets = slices.Clone(h.Buckets)
		out.Hists[i] = h
	}
	return out
}

func sortLabels(labels []Label) []Label {
	if len(labels) == 0 {
		return nil
	}
	out := slices.Clone(labels)
	slices.SortFunc(out, func(a, b Label) int {
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return cmp.Compare(a.Value, b.Value)
	})
	return out
}

func compareLabels(a, b []Label) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if c := cmp.Compare(a[i].Key, b[i].Key); c != 0 {
			return c
		}
		if c := cmp.Compare(a[i].Value, b[i].Value); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(a), len(b))
}

func comparePoints(a, b Point) int {
	if c := cmp.Compare(a.Component, b.Component); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Metric, b.Metric); c != 0 {
		return c
	}
	return compareLabels(a.Labels, b.Labels)
}

func compareHists(a, b HistPoint) int {
	if c := cmp.Compare(a.Component, b.Component); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Metric, b.Metric); c != 0 {
		return c
	}
	return compareLabels(a.Labels, b.Labels)
}
