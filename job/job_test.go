// Story: Synchronous, asynchronous, and bounded-buffer job handlers process and confirm work deterministically.
//
// What these tests prove: SyncHandler inline execution vs AsyncHandler FIFO queueing and Init reset, BufferedTransformer capacity panics and FIFO order over both handlers, and identical results across SliceFIFO/ChanFIFO and seed replays.
// How they probe: a standalone fifoDispatcher for handler lifecycle tests and sim-driven multi-burst workloads comparing Digest, Steps, and processed/confirmed/errored job sequences.
// What they do not prove: randomized job conservation across arbitrary burst schedules (covered in job/prop_test.go) or node crash recovery mid-flight.
package job_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/job"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

type fifoDispatcher struct {
	q env.FIFO[func()]
}

func newFIFODispatcher() *fifoDispatcher {
	return &fifoDispatcher{q: env.NewSliceFIFO[func()]()}
}

func (d *fifoDispatcher) Enqueue(fn func()) {
	d.q.Push(fn)
}

func (d *fifoDispatcher) step() bool {
	fn, ok := d.q.Pop()
	if !ok {
		return false
	}
	fn()
	return true
}

func (d *fifoDispatcher) drain() int {
	n := 0
	for d.step() {
		n++
	}
	return n
}

func TestSyncHandler(t *testing.T) {
	d := newFIFODispatcher()
	var processed []string
	var confirmed []string

	h := job.NewSyncHandler(d, func(j job.Job) {
		processed = append(processed, j.ID)
		if len(confirmed) != 0 {
			t.Fatalf("OnConfirm ran before Submit completed: confirmed=%v", confirmed)
		}
	})
	h.OnConfirm(func(j job.Job) {
		confirmed = append(confirmed, j.ID)
	})

	want := []string{"j1", "j2", "j3", "j4"}
	for _, id := range want {
		beforeConfirms := len(confirmed)
		h.Submit(job.Job{ID: id, Payload: []byte(id)})
		// Strict non-reentrancy: OnConfirm must not run synchronously inside Submit.
		if len(confirmed) != beforeConfirms {
			t.Fatalf("OnConfirm executed re-entrantly inside Submit(%s): got %d confirms, want %d",
				id, len(confirmed), beforeConfirms)
		}
	}

	if !slices.Equal(processed, want) {
		t.Fatalf("processed = %v, want %v", processed, want)
	}
	if len(confirmed) != 0 {
		t.Fatalf("confirmed before dispatch drain = %v, want empty", confirmed)
	}

	if steps := d.drain(); steps != len(want) {
		t.Fatalf("drain steps = %d, want %d", steps, len(want))
	}
	if !slices.Equal(confirmed, want) {
		t.Fatalf("confirmed = %v, want %v", confirmed, want)
	}
}

func TestSyncHandlerNilProcessAndNilListener(t *testing.T) {
	d := newFIFODispatcher()
	var confirmed []string

	h := job.NewSyncHandler(d, nil)
	h.OnConfirm(nil)
	h.OnConfirm(func(j job.Job) {
		confirmed = append(confirmed, j.ID)
	})

	h.Submit(job.Job{ID: "only"})
	if len(confirmed) != 0 {
		t.Fatalf("confirmed before drain = %v, want empty", confirmed)
	}
	d.drain()
	if !slices.Equal(confirmed, []string{"only"}) {
		t.Fatalf("confirmed = %v, want [only]", confirmed)
	}
}

func TestAsyncHandler(t *testing.T) {
	d := newFIFODispatcher()
	var events []string
	var confirmed []string
	var processed []string

	h := job.NewAsyncHandler(d, func(j job.Job) {
		processed = append(processed, j.ID)
		events = append(events, "process:"+j.ID)
	})
	h.OnConfirm(func(j job.Job) {
		confirmed = append(confirmed, j.ID)
		events = append(events, "confirm:"+j.ID)
	})

	want := []string{"a1", "a2", "a3"}
	for _, id := range want {
		h.Submit(job.Job{ID: id})
	}
	if len(confirmed) != 0 || len(processed) != 0 {
		t.Fatalf("expected no synchronous callbacks; confirmed=%v processed=%v", confirmed, processed)
	}

	// First FIFO turn must confirm a1 immediately before processing a1.
	if !d.step() {
		t.Fatal("expected queued confirm turn")
	}
	if !slices.Equal(confirmed, []string{"a1"}) {
		t.Fatalf("after first FIFO turn confirmed = %v, want [a1]", confirmed)
	}
	if len(processed) != 0 {
		t.Fatalf("after first FIFO turn processed = %v, want empty", processed)
	}

	d.drain()
	if !slices.Equal(confirmed, want) {
		t.Fatalf("confirmed = %v, want %v", confirmed, want)
	}
	if !slices.Equal(processed, want) {
		t.Fatalf("processed = %v, want %v", processed, want)
	}
	// All confirms were scheduled immediately upon Submit before the first drain step,
	// so every confirm precedes drain-driven processing when submitted in the same turn.
	wantEvents := []string{
		"confirm:a1", "process:a1",
		"confirm:a2", "confirm:a3",
		"process:a2", "process:a3",
	}
	if !slices.Equal(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}

func TestAsyncHandlerInitReset(t *testing.T) {
	d := newFIFODispatcher()
	var confirmed []string
	var processed []string

	h := job.NewAsyncHandler(d, func(j job.Job) {
		processed = append(processed, j.ID)
	})
	h.OnConfirm(func(j job.Job) {
		confirmed = append(confirmed, j.ID)
	})

	c := obs.NewCollector("node")
	c.Register(h.MetricsSource(""))

	h.Submit(job.Job{ID: "stale-1"})
	h.Submit(job.Job{ID: "stale-2"})
	h.Init()
	checkMetrics(t, c, "job_async", map[string]int64{
		"submitted": 2, "processed": 0, "confirmed": 0, "buffered": 0, "buffered_peak": 2,
	})

	h.Submit(job.Job{ID: "fresh-1"})
	h.Submit(job.Job{ID: "fresh-2"})
	d.drain()

	if !slices.Equal(processed, []string{"fresh-1", "fresh-2"}) {
		t.Fatalf("processed after Init = %v, want [fresh-1 fresh-2]", processed)
	}
	// Confirmations for stale-1/stale-2 were already queued on d, while fresh-1/fresh-2 also confirmed.
	if !slices.Equal(confirmed, []string{"stale-1", "stale-2", "fresh-1", "fresh-2"}) {
		t.Fatalf("confirmed = %v", confirmed)
	}
	checkMetrics(t, c, "job_async", map[string]int64{
		"submitted": 4, "processed": 2, "confirmed": 4, "buffered": 0, "buffered_peak": 2,
	})
}

func TestBufferedTransformerInvalidCapacityPanics(t *testing.T) {
	d := newFIFODispatcher()
	h := job.NewSyncHandler(d, nil)
	for _, capVal := range []int{0, -1, -10} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for capacity %d", capVal)
				}
			}()
			job.NewBufferedTransformer(d, h, capVal)
		}()
	}
}

type spyHandler struct {
	inner     job.Handler
	inFlight  int
	maxFlight int
}

func newSpyHandler(inner job.Handler) *spyHandler {
	s := &spyHandler{inner: inner}
	inner.OnConfirm(func(job.Job) {
		s.inFlight--
	})
	return s
}

func (s *spyHandler) Submit(j job.Job) {
	s.inFlight++
	if s.inFlight > s.maxFlight {
		s.maxFlight = s.inFlight
	}
	s.inner.Submit(j)
}

func (s *spyHandler) OnConfirm(fn func(job.Job)) {
	s.inner.OnConfirm(fn)
}

type burstWorker struct {
	env      env.Env
	sim      *sim.Sim
	useAsync bool
	capacity int
	rounds   int
	result   *workloadResult
	spy      *spyHandler
	bt       *job.BufferedTransformer
	seq      int
}

type workloadResult struct {
	submittedOrder []string
	confirmed      map[string]bool
	errored        map[string]bool
	confirmedOrder []string
	erroredOrder   []string
	processedOrder []string
	processedCount map[string]int
}

func (w *burstWorker) Init() {
	w.scheduleRound(0)
}

func (w *burstWorker) Recover()                     {}
func (w *burstWorker) OnMessage(env.NodeID, []byte) {}

func (w *burstWorker) scheduleRound(round int) {
	if round >= w.rounds {
		return
	}
	delay := time.Duration(1+w.env.Rand.IntN(5)) * time.Millisecond
	w.env.Clock.AfterFunc(delay, func() {
		// Ensure bursts regularly exceed buffer capacity so both Confirm and Error paths are exercised.
		burstSize := 1 + w.env.Rand.IntN(w.capacity*3+2)
		if round == 0 && burstSize < w.capacity+3 {
			burstSize = w.capacity + 3
		}
		for i := 0; i < burstSize; i++ {
			w.seq++
			id := fmt.Sprintf("job-%03d", w.seq)
			w.result.submittedOrder = append(w.result.submittedOrder, id)
			w.sim.Trace("job.submit", id)
			w.bt.Submit(job.Job{ID: id, Payload: []byte(id)})
		}
		w.scheduleRound(round + 1)
	})
}

func runBufferedWorkload(
	t *testing.T,
	seed int64,
	useAsync bool,
	capacity int,
	newFIFO func() env.FIFO[func()],
) (*sim.Sim, *workloadResult, *spyHandler) {
	t.Helper()

	s := sim.New(sim.Config{
		Seed:    seed,
		NewFIFO: newFIFO,
	})

	res := &workloadResult{
		confirmed:      make(map[string]bool),
		errored:        make(map[string]bool),
		processedCount: make(map[string]int),
	}

	var spyRef *spyHandler

	err := s.AddNode("worker", []env.NodeID{"worker"}, func(e env.Env) env.Process {
		process := func(j job.Job) {
			res.processedOrder = append(res.processedOrder, j.ID)
			res.processedCount[j.ID]++
			s.Trace("job.process", j.ID)
		}

		var base job.Handler
		if useAsync {
			ah := job.NewAsyncHandler(e.Dispatch, process)
			ah.Init()
			base = ah
		} else {
			base = job.NewSyncHandler(e.Dispatch, process)
		}

		spy := newSpyHandler(base)
		spyRef = spy

		bt := job.NewBufferedTransformer(e.Dispatch, spy, capacity)
		bt.OnConfirm(func(j job.Job) {
			res.confirmed[j.ID] = true
			res.confirmedOrder = append(res.confirmedOrder, j.ID)
			s.Trace("job.confirm", j.ID)
		})
		bt.OnError(func(j job.Job) {
			res.errored[j.ID] = true
			res.erroredOrder = append(res.erroredOrder, j.ID)
			s.Trace("job.error", j.ID)
		})

		return &burstWorker{
			env:      e,
			sim:      s,
			useAsync: useAsync,
			capacity: capacity,
			rounds:   12,
			result:   res,
			spy:      spy,
			bt:       bt,
		}
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	s.AddInvariant("buffered-safety", func() error {
		if spyRef.inFlight < 0 || spyRef.inFlight > 1 {
			return fmt.Errorf("in-flight jobs = %d, must be in [0, 1]", spyRef.inFlight)
		}
		if spyRef.maxFlight > 1 {
			return fmt.Errorf("max in-flight jobs = %d, must be <= 1", spyRef.maxFlight)
		}
		for _, id := range res.submittedOrder {
			if res.confirmed[id] && res.errored[id] {
				return fmt.Errorf("job %s received both confirm and error", id)
			}
			if res.errored[id] && res.processedCount[id] > 0 {
				return fmt.Errorf("errored job %s was processed %d times", id, res.processedCount[id])
			}
			if res.processedCount[id] > 1 {
				return fmt.Errorf("job %s was processed %d times", id, res.processedCount[id])
			}
		}
		return nil
	})

	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet(seed=%d async=%v): %v", seed, useAsync, err)
	}

	verifyWorkloadResult(t, res, spyRef)
	return s, res, spyRef
}

func verifyWorkloadResult(t *testing.T, res *workloadResult, spy *spyHandler) {
	t.Helper()

	if len(res.submittedOrder) == 0 {
		t.Fatal("expected at least one submitted job")
	}
	if len(res.confirmedOrder) == 0 || len(res.erroredOrder) == 0 {
		t.Fatalf("expected both confirmations and errors; confirmed=%d errored=%d",
			len(res.confirmedOrder), len(res.erroredOrder))
	}
	if spy.inFlight != 0 {
		t.Fatalf("expected 0 in-flight jobs at quiet state, got %d", spy.inFlight)
	}
	if spy.maxFlight != 1 {
		t.Fatalf("expected maxFlight == 1, got %d", spy.maxFlight)
	}

	for _, id := range res.submittedOrder {
		c := res.confirmed[id]
		e := res.errored[id]
		if c == e {
			t.Fatalf("job %s must receive exactly one of confirm or error (confirm=%v, error=%v)", id, c, e)
		}
		if e && res.processedCount[id] != 0 {
			t.Fatalf("errored job %s processed %d times, want 0", id, res.processedCount[id])
		}
		if c && res.processedCount[id] != 1 {
			t.Fatalf("confirmed job %s processed %d times, want 1", id, res.processedCount[id])
		}
	}

	if !slices.Equal(res.processedOrder, res.confirmedOrder) {
		t.Fatalf("processed order %v != confirmed order %v", res.processedOrder, res.confirmedOrder)
	}
}

func TestBufferedTransformerOverSyncAndAsync(t *testing.T) {
	for _, useAsync := range []bool{false, true} {
		name := "sync"
		if useAsync {
			name = "async"
		}
		for _, capVal := range []int{1, 3, 5} {
			t.Run(fmt.Sprintf("%s/cap=%d", name, capVal), func(t *testing.T) {
				runBufferedWorkload(t, 42+int64(capVal), useAsync, capVal, nil)
			})
		}
	}
}

func TestBufferedTransformerSwappableFIFO(t *testing.T) {
	for _, useAsync := range []bool{false, true} {
		name := "sync"
		if useAsync {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			const seed int64 = 99
			const capacity = 3

			s1, r1, _ := runBufferedWorkload(t, seed, useAsync, capacity, func() env.FIFO[func()] {
				return env.NewSliceFIFO[func()]()
			})
			s2, r2, _ := runBufferedWorkload(t, seed, useAsync, capacity, func() env.FIFO[func()] {
				return env.NewChanFIFO[func()](256)
			})

			if s1.Digest() != s2.Digest() {
				t.Fatalf("digest mismatch between SliceFIFO and ChanFIFO: %s != %s", s1.Digest(), s2.Digest())
			}
			if s1.Steps() != s2.Steps() {
				t.Fatalf("steps mismatch between SliceFIFO and ChanFIFO: %d != %d", s1.Steps(), s2.Steps())
			}
			if !slices.Equal(r1.processedOrder, r2.processedOrder) {
				t.Fatalf("processedOrder mismatch: %v != %v", r1.processedOrder, r2.processedOrder)
			}
			if !slices.Equal(r1.confirmedOrder, r2.confirmedOrder) {
				t.Fatalf("confirmedOrder mismatch: %v != %v", r1.confirmedOrder, r2.confirmedOrder)
			}
			if !slices.Equal(r1.erroredOrder, r2.erroredOrder) {
				t.Fatalf("erroredOrder mismatch: %v != %v", r1.erroredOrder, r2.erroredOrder)
			}
		})
	}
}

func TestBufferedTransformerDeterministicReplay(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		for _, useAsync := range []bool{false, true} {
			s1, r1, _ := runBufferedWorkload(t, seed, useAsync, 3, nil)
			s2, r2, _ := runBufferedWorkload(t, seed, useAsync, 3, nil)

			if s1.Digest() != s2.Digest() {
				t.Fatalf("seed=%d async=%v digest mismatch: %s != %s",
					seed, useAsync, s1.Digest(), s2.Digest())
			}
			if s1.Steps() != s2.Steps() {
				t.Fatalf("seed=%d async=%v steps mismatch: %d != %d",
					seed, useAsync, s1.Steps(), s2.Steps())
			}
			if !slices.Equal(r1.processedOrder, r2.processedOrder) {
				t.Fatalf("seed=%d async=%v processedOrder mismatch", seed, useAsync)
			}
		}
	}
}

func TestBufferedMetricsFollowDispatch(t *testing.T) {
	d := newFIFODispatcher()
	inner := job.NewSyncHandler(d, nil)
	bt := job.NewBufferedTransformer(d, inner, 1)
	c := obs.NewCollector("node")
	c.Register(bt.MetricsSource(""))
	c.Register(inner.MetricsSource(""))

	bt.Submit(job.Job{ID: "first"})
	bt.Submit(job.Job{ID: "buffered"})
	bt.Submit(job.Job{ID: "rejected"})
	checkMetrics(t, c, "job_buffered", map[string]int64{
		"submitted": 3, "confirmed": 0, "errored": 0, "forwarded": 1,
		"buffered": 1, "buffered_peak": 1, "in_flight": 1,
	})
	checkMetrics(t, c, "job_sync", map[string]int64{"submitted": 1, "processed": 1, "confirmed": 0})

	d.drain()
	checkMetrics(t, c, "job_buffered", map[string]int64{
		"submitted": 3, "confirmed": 2, "errored": 1, "forwarded": 2,
		"buffered": 0, "buffered_peak": 1, "in_flight": 0,
	})
	checkMetrics(t, c, "job_sync", map[string]int64{"submitted": 2, "processed": 2, "confirmed": 2})
}

func checkMetrics(t *testing.T, c *obs.Collector, component string, want map[string]int64) {
	t.Helper()
	got := make(map[string]int64)
	for _, p := range c.Collect(0).Points {
		if p.Component == component {
			got[p.Metric] = p.Value
		}
	}
	if !maps.Equal(got, want) {
		t.Fatalf("%s metrics = %v, want %v", component, got, want)
	}
}
