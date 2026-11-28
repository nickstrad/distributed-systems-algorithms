// Package job provides composable synchronous, asynchronous, and bounded
// buffered job handlers built on env.Dispatcher.
package job

import "github.com/nickstrad/distributed-systems-algorithms/env"

// Job represents a unit of work submitted to a Handler or Transformer.
type Job struct {
	ID      string
	Payload []byte
}

// Handler accepts jobs for processing and emits confirmation callbacks when
// submission has been accepted or completed, as documented by the implementation.
// The built-in handlers enqueue indications on the owning dispatcher. Callers
// register listeners and submit jobs from the same execution loop.
type Handler interface {
	Submit(j Job)
	OnConfirm(fn func(Job))
}

// SyncHandler executes a job's process function synchronously inside Submit
// and schedules confirmation callbacks via an env.Dispatcher so confirmations
// never run re-entrantly before Submit returns.
type SyncHandler struct {
	dispatch  env.Dispatcher
	process   func(Job)
	listeners []func(Job)
	stats     handlerStats
}

var _ Handler = (*SyncHandler)(nil)

// NewSyncHandler constructs a SyncHandler that dispatches confirmations on
// dispatch and invokes process (if non-nil) for each submitted job.
func NewSyncHandler(dispatch env.Dispatcher, process func(Job)) *SyncHandler {
	if dispatch == nil {
		panic("job: nil dispatcher")
	}
	return &SyncHandler{
		dispatch: dispatch,
		process:  process,
	}
}

// OnConfirm registers a callback invoked asynchronously on dispatch after a
// submitted job has been processed.
func (h *SyncHandler) OnConfirm(fn func(Job)) {
	if fn == nil {
		return
	}
	h.listeners = append(h.listeners, fn)
}

// Submit processes j synchronously (if process is non-nil) and enqueues
// registered OnConfirm callbacks on dispatch.
func (h *SyncHandler) Submit(j Job) {
	h.stats.submitted.Inc()
	if h.process != nil {
		h.process(j)
	}
	h.stats.processed.Inc()
	h.dispatch.Enqueue(func() {
		h.stats.confirmed.Inc()
		emitJob(h.listeners, j)
	})
}

// AsyncHandler buffers submitted jobs in an unbounded FIFO queue, immediately
// schedules confirmation via dispatch, and drains one buffered job per
// scheduled local turn.
type AsyncHandler struct {
	dispatch  env.Dispatcher
	process   func(Job)
	listeners []func(Job)
	buf       *env.SliceFIFO[Job]
	scheduled bool
	epoch     uint64
	stats     asyncStats
}

var _ Handler = (*AsyncHandler)(nil)

// NewAsyncHandler constructs an AsyncHandler that schedules confirmations and
// background drain steps on dispatch.
func NewAsyncHandler(dispatch env.Dispatcher, process func(Job)) *AsyncHandler {
	if dispatch == nil {
		panic("job: nil dispatcher")
	}
	return &AsyncHandler{
		dispatch: dispatch,
		process:  process,
		buf:      env.NewSliceFIFO[Job](),
	}
}

// Init resets the handler's pending job buffer and drain state while preserving
// registered OnConfirm listeners, queued confirmations, and cumulative metrics.
// AsyncHandler is safe to use without calling Init.
func (h *AsyncHandler) Init() {
	if h.buf == nil {
		h.buf = env.NewSliceFIFO[Job]()
	} else {
		h.buf.Clear()
	}
	h.stats.buffered.Set(0)
	h.scheduled = false
	h.epoch++
}

// OnConfirm registers a callback invoked on dispatch when a job is submitted.
func (h *AsyncHandler) OnConfirm(fn func(Job)) {
	if fn == nil {
		return
	}
	h.listeners = append(h.listeners, fn)
}

// Submit appends j to the internal FIFO buffer, enqueues OnConfirm(j)
// immediately on dispatch, and schedules a drain step if one is not already
// pending.
func (h *AsyncHandler) Submit(j Job) {
	if h.buf == nil {
		h.buf = env.NewSliceFIFO[Job]()
	}
	h.stats.submitted.Inc()
	h.buf.Push(j)
	h.stats.buffered.Set(int64(h.buf.Len()))
	h.dispatch.Enqueue(func() {
		h.stats.confirmed.Inc()
		emitJob(h.listeners, j)
	})
	if !h.scheduled {
		h.scheduleDrain()
	}
}

func (h *AsyncHandler) scheduleDrain() {
	h.scheduled = true
	epoch := h.epoch
	h.dispatch.Enqueue(func() {
		h.drain(epoch)
	})
}

// drain processes at most one FIFO entry for the scheduled epoch. The process
// callback may reset the handler, so the epoch is checked again afterward.
//
//	post: for the current epoch, pending work has a scheduled drain
func (h *AsyncHandler) drain(epoch uint64) {
	if epoch != h.epoch {
		return
	}
	j, ok := h.buf.Pop()
	if !ok {
		h.scheduled = false
		return
	}
	h.stats.buffered.Set(int64(h.buf.Len()))
	if h.process != nil {
		h.process(j)
	}
	h.stats.processed.Inc()
	if epoch != h.epoch {
		return
	}
	if h.buf.Len() > 0 {
		h.scheduleDrain()
	} else {
		h.scheduled = false
	}
}

func emitJob(listeners []func(Job), j Job) {
	for _, fn := range listeners {
		fn(j)
	}
}
