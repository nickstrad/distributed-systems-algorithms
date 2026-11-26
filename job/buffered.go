package job

import (
	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
)

// Transformer extends Handler with an error callback for rejected jobs
// (for example, due to bounded buffer backpressure).
type Transformer interface {
	Submit(j Job)
	OnConfirm(fn func(Job))
	OnError(fn func(Job))
}

// BufferedTransformer wraps an underlying Handler with a bounded circular
// buffer of fixed capacity and forwards at most one in-flight job at a time.
type BufferedTransformer struct {
	dispatch         env.Dispatcher
	handler          Handler
	capacity         int
	buf              []Job
	top              int
	bottom           int
	handling         bool
	confirmListeners []func(Job)
	errorListeners   []func(Job)
	submitted        obs.Counter
	confirmed        obs.Counter
	errored          obs.Counter
	forwarded        obs.Counter
	buffered         obs.Gauge
}

var (
	_ Transformer = (*BufferedTransformer)(nil)
	_ Handler     = (*BufferedTransformer)(nil)
)

// NewBufferedTransformer constructs a BufferedTransformer with a circular
// buffer of size capacity on top of handler. It panics if capacity <= 0 or if
// dispatch or handler is nil.
func NewBufferedTransformer(dispatch env.Dispatcher, handler Handler, capacity int) *BufferedTransformer {
	if dispatch == nil {
		panic("job: nil dispatcher")
	}
	if handler == nil {
		panic("job: nil handler")
	}
	if capacity <= 0 {
		panic("job: capacity must be positive")
	}
	t := &BufferedTransformer{
		dispatch: dispatch,
		handler:  handler,
		capacity: capacity,
		buf:      make([]Job, capacity),
	}
	handler.OnConfirm(func(Job) {
		t.handling = false
		t.step()
	})
	return t
}

// OnConfirm registers a callback invoked asynchronously on dispatch when a job
// is accepted into the bounded buffer.
func (t *BufferedTransformer) OnConfirm(fn func(Job)) {
	if fn == nil {
		return
	}
	t.confirmListeners = append(t.confirmListeners, fn)
}

// OnError registers a callback invoked asynchronously on dispatch when a job is
// rejected because the bounded buffer is full.
func (t *BufferedTransformer) OnError(fn func(Job)) {
	if fn == nil {
		return
	}
	t.errorListeners = append(t.errorListeners, fn)
}

// Submit accepts j into the bounded circular buffer and emits OnConfirm via
// dispatch if space is available, or rejects j and emits OnError via dispatch
// when full.
func (t *BufferedTransformer) Submit(j Job) {
	t.submitted.Inc()
	if t.bottom+t.capacity == t.top {
		t.dispatch.Enqueue(func() {
			t.errored.Inc()
			emitJob(t.errorListeners, j)
		})
		return
	}
	t.buf[t.top%t.capacity] = j
	t.top++
	t.buffered.Set(int64(t.top - t.bottom))
	t.dispatch.Enqueue(func() {
		t.confirmed.Inc()
		emitJob(t.confirmListeners, j)
	})
	t.step()
}

func (t *BufferedTransformer) step() {
	for t.bottom < t.top && !t.handling {
		idx := t.bottom % t.capacity
		j := t.buf[idx]
		t.buf[idx] = Job{}
		t.bottom++
		t.buffered.Set(int64(t.top - t.bottom))
		t.handling = true
		t.forwarded.Inc()
		t.handler.Submit(j)
	}
}

// MetricsSource returns an obs.Source exporting this transformer's runtime metrics.
func (t *BufferedTransformer) MetricsSource(component string) obs.Source {
	if component == "" {
		component = "job_buffered"
	}
	return obs.SourceFunc{
		Name: component,
		Fn: func(e *obs.Emitter) {
			var inFlight int64
			if t.handling {
				inFlight = 1
			}
			e.Counter("submitted", "count", "Total jobs submitted", t.submitted.Value())
			e.Counter("confirmed", "count", "Total accepted jobs confirmed", t.confirmed.Value())
			e.Counter("errored", "count", "Total jobs rejected due to full buffer", t.errored.Value())
			e.Counter("forwarded", "count", "Total jobs forwarded to underlying handler", t.forwarded.Value())
			e.Gauge("buffered", "count", "Current buffered jobs", t.buffered.Value())
			e.Gauge("buffered_peak", "count", "Peak buffered jobs", t.buffered.Peak())
			e.Gauge("in_flight", "count", "Current in-flight jobs in underlying handler", inFlight)
		},
	}
}
