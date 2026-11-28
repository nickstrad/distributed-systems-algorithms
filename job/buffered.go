package job

import (
	"fmt"

	"github.com/nickstrad/distributed-systems-algorithms/env"
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
// The underlying handler must emit exactly one confirmation for each forwarded
// job and must be dedicated to this transformer.
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
	stats            bufferedStats
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
		panic(fmt.Sprintf("job: capacity must be positive, got %d", capacity))
	}
	t := &BufferedTransformer{
		dispatch: dispatch,
		handler:  handler,
		capacity: capacity,
		buf:      make([]Job, capacity),
	}
	handler.OnConfirm(func(Job) {
		t.handling = false
		t.stats.inFlight.Set(0)
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
	depth := t.checkedDepth()
	t.stats.submitted.Inc()
	if depth == t.capacity {
		t.dispatch.Enqueue(func() {
			t.stats.errored.Inc()
			emitJob(t.errorListeners, j)
		})
		return
	}
	t.buf[t.top%t.capacity] = j
	t.top++
	t.recordDepth()
	t.dispatch.Enqueue(func() {
		t.stats.confirmed.Inc()
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
		t.recordDepth()
		t.handling = true
		t.stats.inFlight.Set(1)
		t.stats.forwarded.Inc()
		t.handler.Submit(j)
	}
}

// recordDepth follows each ring mutation, before control passes to another module.
//
//	post: the published depth equals the number of occupied slots
func (t *BufferedTransformer) recordDepth() {
	t.stats.buffered.Set(int64(t.checkedDepth()))
}

// checkedDepth checks the ring occupancy at a stable boundary.
//
//	Invariant: 0 <= top - bottom <= capacity
func (t *BufferedTransformer) checkedDepth() int {
	depth := t.top - t.bottom
	if depth < 0 || depth > t.capacity {
		panic(fmt.Sprintf("job: ring depth %d outside [0, %d] (top=%d bottom=%d)", depth, t.capacity, t.top, t.bottom))
	}
	return depth
}
