package job

import "github.com/nickstrad/distributed-systems-algorithms/obs"

// handlerStats is updated from the handler's owning execution loop.
type handlerStats struct {
	submitted obs.Counter
	processed obs.Counter
	confirmed obs.Counter
}

func (s *handlerStats) emit(e *obs.Emitter) {
	e.Counter("submitted", "count", "Total jobs submitted", s.submitted.Value())
	e.Counter("processed", "count", "Total jobs processed", s.processed.Value())
	e.Counter("confirmed", "count", "Total job confirmations emitted", s.confirmed.Value())
}

type asyncStats struct {
	handlerStats
	buffered obs.Gauge
}

func (s *asyncStats) emit(e *obs.Emitter) {
	s.handlerStats.emit(e)
	e.Gauge("buffered", "count", "Current buffered jobs awaiting drain", s.buffered.Value())
	e.Gauge("buffered_peak", "count", "Peak buffered jobs awaiting drain", s.buffered.Peak())
}

// MetricsSource returns an obs.Source exporting this handler's runtime counters.
func (h *SyncHandler) MetricsSource(component string) obs.Source {
	if component == "" {
		component = "job_sync"
	}
	return obs.SourceFunc{
		Name: component,
		Fn:   h.stats.emit,
	}
}

// MetricsSource returns an obs.Source exporting this handler's runtime metrics.
func (h *AsyncHandler) MetricsSource(component string) obs.Source {
	if component == "" {
		component = "job_async"
	}
	return obs.SourceFunc{
		Name: component,
		Fn:   h.stats.emit,
	}
}
