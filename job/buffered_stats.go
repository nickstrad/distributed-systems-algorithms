package job

import "github.com/nickstrad/distributed-systems-algorithms/obs"

// bufferedStats belongs to the dispatcher that owns the transformer.
type bufferedStats struct {
	submitted obs.Counter
	confirmed obs.Counter
	errored   obs.Counter
	forwarded obs.Counter
	buffered  obs.Gauge
	inFlight  obs.Gauge
}

// MetricsSource returns an obs.Source exporting this transformer's runtime metrics.
func (t *BufferedTransformer) MetricsSource(component string) obs.Source {
	if component == "" {
		component = "job_buffered"
	}
	return obs.SourceFunc{
		Name: component,
		Fn:   t.stats.emit,
	}
}

func (s *bufferedStats) emit(e *obs.Emitter) {
	e.Counter("submitted", "count", "Total jobs submitted", s.submitted.Value())
	e.Counter("confirmed", "count", "Total accepted jobs confirmed", s.confirmed.Value())
	e.Counter("errored", "count", "Total jobs rejected due to full buffer", s.errored.Value())
	e.Counter("forwarded", "count", "Total jobs forwarded to underlying handler", s.forwarded.Value())
	e.Gauge("buffered", "count", "Current buffered jobs", s.buffered.Value())
	e.Gauge("buffered_peak", "count", "Peak buffered jobs", s.buffered.Peak())
	e.Gauge("in_flight", "count", "Current in-flight jobs in underlying handler", s.inFlight.Value())
}
