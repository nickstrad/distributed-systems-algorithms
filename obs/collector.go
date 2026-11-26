package obs

import (
	"cmp"
	"slices"
)

// Emitter collects points and histograms for a single named component during Collect.
type Emitter struct {
	component string
	points    *[]Point
	hists     *[]HistPoint
}

// Counter emits a cumulative non-negative counter point.
func (e *Emitter) Counter(metric, unit, help string, value int64, labels ...Label) {
	if value < 0 {
		value = 0
	}
	*e.points = append(*e.points, Point{
		Component: e.component,
		Metric:    metric,
		Kind:      KindCounter,
		Unit:      unit,
		Help:      help,
		Labels:    sortLabels(labels),
		Value:     value,
	})
}

// Gauge emits a point-in-time gauge point.
func (e *Emitter) Gauge(metric, unit, help string, value int64, labels ...Label) {
	*e.points = append(*e.points, Point{
		Component: e.component,
		Metric:    metric,
		Kind:      KindGauge,
		Unit:      unit,
		Help:      help,
		Labels:    sortLabels(labels),
		Value:     value,
	})
}

// Histogram emits a bucketed distribution snapshot along with its clamp counts.
func (e *Emitter) Histogram(metric, unit, help string, h *Histogram, labels ...Label) {
	if h == nil {
		return
	}
	*e.hists = append(*e.hists, HistPoint{
		Component:   e.component,
		Metric:      metric,
		Unit:        unit,
		Help:        help,
		Labels:      sortLabels(labels),
		Buckets:     h.Buckets(),
		Count:       h.Count(),
		ClampedLow:  h.ClampedLow(),
		ClampedHigh: h.ClampedHigh(),
	})
}

// Source is implemented by any component that exports runtime metrics.
type Source interface {
	Component() string
	Emit(e *Emitter)
}

// SourceFunc adapts a plain function into a Source.
type SourceFunc struct {
	Name string
	Fn   func(e *Emitter)
}

func (s SourceFunc) Component() string { return s.Name }
func (s SourceFunc) Emit(e *Emitter) {
	if s.Fn != nil {
		s.Fn(e)
	}
}

// Adapter consumes immutable published Snapshots (CSV, Prometheus, log lines, etc.).
type Adapter interface {
	Name() string
	Export(s Snapshot) error
}

type adapterSlot struct {
	adapter Adapter
	errors  int64
}

// Collector gathers registered Sources in deterministic sorted component order
// and fans immutable Snapshots out to attached Adapters. Adapter failures are
// isolated and counted without interrupting execution.
type Collector struct {
	instance string
	sources  []Source
	adapters []adapterSlot
	seq      int64
	last     Snapshot
	hasLast  bool
}

// NewCollector creates a Collector stamped with instance (e.g. node ID or "sim").
func NewCollector(instance string) *Collector {
	return &Collector{instance: instance}
}

// Register adds a Source; sources are kept sorted by Component name.
func (c *Collector) Register(src Source) {
	if src == nil {
		return
	}
	c.sources = append(c.sources, src)
	slices.SortStableFunc(c.sources, func(a, b Source) int {
		return cmp.Compare(a.Component(), b.Component())
	})
}

// AddAdapter attaches an output Adapter to receive published snapshots.
func (c *Collector) AddAdapter(a Adapter) {
	if a == nil {
		return
	}
	c.adapters = append(c.adapters, adapterSlot{adapter: a})
}

// Collect gathers all registered sources into a sorted Snapshot without exporting.
func (c *Collector) Collect(timeNs int64) Snapshot {
	var points []Point
	var hists []HistPoint
	for _, src := range c.sources {
		em := &Emitter{
			component: src.Component(),
			points:    &points,
			hists:     &hists,
		}
		src.Emit(em)
	}
	slices.SortFunc(points, comparePoints)
	slices.SortFunc(hists, compareHists)
	return Snapshot{
		Seq:      c.seq,
		TimeNs:   timeNs,
		Instance: c.instance,
		Points:   points,
		Hists:    hists,
	}
}

// Publish collects a Snapshot at timeNs, caches it for Last(), and exports a
// defensive copy to every attached Adapter. Adapter errors are counted and never
// prevent subsequent adapters from running.
func (c *Collector) Publish(timeNs int64) Snapshot {
	snap := c.Collect(timeNs)
	c.seq++
	c.last = snap.Clone()
	c.hasLast = true
	for i := range c.adapters {
		slot := &c.adapters[i]
		if err := slot.adapter.Export(snap.Clone()); err != nil {
			slot.errors++
		}
	}
	return snap
}

// Last returns a deep copy of the most recently published Snapshot.
func (c *Collector) Last() (Snapshot, bool) {
	if !c.hasLast {
		return Snapshot{}, false
	}
	return c.last.Clone(), true
}

// AdapterErrors returns the cumulative export error count for the named adapter.
func (c *Collector) AdapterErrors(name string) int64 {
	var total int64
	for _, slot := range c.adapters {
		if slot.adapter.Name() == name {
			total += slot.errors
		}
	}
	return total
}
