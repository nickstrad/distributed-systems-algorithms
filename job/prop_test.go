// Story: BufferedTransformer conserves every submitted job with zero silent drops or duplicate executions.
//
// What these tests prove: submitted == confirmed + errored, exclusive confirmed/errored outcomes, FIFO processing order, zero buffered/in_flight residue at quiet, and identical obs.Snapshot histories across multi-seed sweeps.
// How they probe: rapid-generated burst schedules across sync/async handlers and SliceFIFO/ChanFIFO, plus simtest.Sweep with simtest.RequireDeterministic comparing obs.Collector snapshots.
// What they do not prove: persistence across node crashes/restarts or network transport of jobs across multiple nodes.
package job_test

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/job"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
	"github.com/nickstrad/distributed-systems-algorithms/sim/simtest"
)

func metricPointMap(snap obs.Snapshot, component string) map[string]int64 {
	out := make(map[string]int64)
	for _, p := range snap.Points {
		if p.Component == component {
			out[p.Metric] = p.Value
		}
	}
	return out
}

func TestPropBufferedTransformerConservationAndSoundness(t *testing.T) {
	type burstSpec struct {
		delayMs int
		size    int
	}

	rapid.Check(t, func(t *rapid.T) {
		seed := rapid.Int64().Draw(t, "seed")
		capacity := rapid.IntRange(1, 16).Draw(t, "capacity")
		useAsync := rapid.Bool().Draw(t, "useAsync")
		useChanFIFO := rapid.Bool().Draw(t, "useChanFIFO")
		numBursts := rapid.IntRange(1, 14).Draw(t, "numBursts")

		bursts := make([]burstSpec, numBursts)
		for i := range bursts {
			bursts[i] = burstSpec{
				delayMs: rapid.IntRange(0, 10).Draw(t, fmt.Sprintf("burst%d.delayMs", i)),
				size:    rapid.IntRange(1, 32).Draw(t, fmt.Sprintf("burst%d.size", i)),
			}
		}

		newFIFO := func() env.FIFO[func()] {
			if useChanFIFO {
				return env.NewChanFIFO[func()](4096)
			}
			return env.NewSliceFIFO[func()]()
		}

		s := sim.New(sim.Config{
			Seed:    seed,
			NewFIFO: newFIFO,
		})

		var (
			submittedIDs   []string
			confirmed      = make(map[string]bool)
			errored        = make(map[string]bool)
			confirmedOrder []string
			erroredOrder   []string
			processedOrder []string
			processedCount = make(map[string]int)
			btRef          *job.BufferedTransformer
			handlerSrc     obs.Source
		)

		if err := s.AddNode("worker", []env.NodeID{"worker"}, func(e env.Env) env.Process {
			process := func(j job.Job) {
				processedOrder = append(processedOrder, j.ID)
				processedCount[j.ID]++
			}

			var base job.Handler
			if useAsync {
				ah := job.NewAsyncHandler(e.Dispatch, process)
				ah.Init()
				base = ah
				handlerSrc = ah.MetricsSource("handler")
			} else {
				sh := job.NewSyncHandler(e.Dispatch, process)
				base = sh
				handlerSrc = sh.MetricsSource("handler")
			}

			bt := job.NewBufferedTransformer(e.Dispatch, base, capacity)
			btRef = bt
			bt.OnConfirm(func(j job.Job) {
				confirmed[j.ID] = true
				confirmedOrder = append(confirmedOrder, j.ID)
			})
			bt.OnError(func(j job.Job) {
				errored[j.ID] = true
				erroredOrder = append(erroredOrder, j.ID)
			})

			return &hookJobProcess{
				onInit: func() {
					cursor := e.Clock.Now()
					seq := 0
					for _, b := range bursts {
						b := b
						cursor = cursor.Add(time.Duration(b.delayMs) * time.Millisecond)
						delay := cursor.Sub(e.Clock.Now())
						e.Clock.AfterFunc(delay, func() {
							for range b.size {
								seq++
								id := fmt.Sprintf("j-%04d", seq)
								submittedIDs = append(submittedIDs, id)
								bt.Submit(job.Job{ID: id, Payload: []byte(id)})
							}
						})
					}
				},
			}
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}

		s.AddInvariant("soundness", func() error {
			for _, id := range submittedIDs {
				if confirmed[id] && errored[id] {
					return fmt.Errorf("job %s both confirmed and errored", id)
				}
				if errored[id] && processedCount[id] > 0 {
					return fmt.Errorf("errored job %s processed %d times", id, processedCount[id])
				}
				if processedCount[id] > 1 {
					return fmt.Errorf("job %s processed %d times", id, processedCount[id])
				}
			}
			return nil
		})

		if err := s.RunUntilQuiet(); err != nil {
			t.Fatalf("RunUntilQuiet: %v", err)
		}

		submittedN := int64(len(submittedIDs))
		confirmedN := int64(len(confirmedOrder))
		erroredN := int64(len(erroredOrder))
		processedN := int64(len(processedOrder))

		if submittedN != confirmedN+erroredN {
			t.Fatalf("conservation violated: submitted=%d != confirmed(%d) + errored(%d)",
				submittedN, confirmedN, erroredN)
		}
		if confirmedN != processedN {
			t.Fatalf("completion mismatch: confirmed=%d != processed=%d", confirmedN, processedN)
		}
		if !slices.Equal(confirmedOrder, processedOrder) {
			t.Fatalf("order mismatch: confirmed=%v != processed=%v", confirmedOrder, processedOrder)
		}
		for _, id := range submittedIDs {
			c := confirmed[id]
			e := errored[id]
			if c == e {
				t.Fatalf("job %s must be exclusively confirmed or errored (c=%v e=%v)", id, c, e)
			}
			if e && processedCount[id] != 0 {
				t.Fatalf("errored job %s processed %d times, want 0", id, processedCount[id])
			}
			if c && processedCount[id] != 1 {
				t.Fatalf("confirmed job %s processed %d times, want 1", id, processedCount[id])
			}
		}

		col := obs.NewCollector("job-prop")
		col.Register(btRef.MetricsSource("bt"))
		col.Register(handlerSrc)
		snap := col.Collect(int64(s.Now()))

		btMetrics := metricPointMap(snap, "bt")
		if got := btMetrics["submitted"]; got != submittedN {
			t.Fatalf("bt.submitted = %d, want %d", got, submittedN)
		}
		if got := btMetrics["confirmed"]; got != confirmedN {
			t.Fatalf("bt.confirmed = %d, want %d", got, confirmedN)
		}
		if got := btMetrics["errored"]; got != erroredN {
			t.Fatalf("bt.errored = %d, want %d", got, erroredN)
		}
		if got := btMetrics["forwarded"]; got != confirmedN {
			t.Fatalf("bt.forwarded = %d, want %d", got, confirmedN)
		}
		if got := btMetrics["buffered"]; got != 0 {
			t.Fatalf("bt.buffered = %d at quiet state, want 0", got)
		}
		if got := btMetrics["in_flight"]; got != 0 {
			t.Fatalf("bt.in_flight = %d at quiet state, want 0", got)
		}

		hMetrics := metricPointMap(snap, "handler")
		if got := hMetrics["submitted"]; got != processedN {
			t.Fatalf("handler.submitted = %d, want %d", got, processedN)
		}
		if got := hMetrics["confirmed"]; got != processedN {
			t.Fatalf("handler.confirmed = %d, want %d", got, processedN)
		}
		if got := hMetrics["processed"]; got != processedN {
			t.Fatalf("handler.processed = %d, want %d", got, processedN)
		}
	})
}

type hookJobProcess struct {
	env.BaseProcess
	onInit func()
}

func (p *hookJobProcess) Init() {
	if p.onInit != nil {
		p.onInit()
	}
}

func TestBufferedTransformerSweep(t *testing.T) {
	simtest.Sweep(t, func(t *testing.T, seed int64) {
		for _, useAsync := range []bool{false, true} {
			mode := "sync"
			if useAsync {
				mode = "async"
			}
			t.Run(mode, func(t *testing.T) {
				var runSnapshots [][]obs.Snapshot

				simtest.RequireDeterministic(t, seed, func(seed int64) (*sim.Sim, error) {
					s := sim.New(sim.Config{
						Seed:       seed,
						LogEnabled: true,
						LogLevel:   env.LevelInfo,
					})
					col := obs.NewCollector(fmt.Sprintf("worker-%s", mode))
					col.Register(s.MetricsSource())

					var snaps []obs.Snapshot
					var submittedCount, confirmedCount, erroredCount, processedCount int64

					const capacity = 4
					const rounds = 10

					if err := s.AddNode("worker", []env.NodeID{"worker"}, func(e env.Env) env.Process {
						process := func(j job.Job) {
							processedCount++
							e.Log.Info("processed", env.Str("id", j.ID))
						}
						var base job.Handler
						if useAsync {
							ah := job.NewAsyncHandler(e.Dispatch, process)
							ah.Init()
							base = ah
							col.Register(ah.MetricsSource("handler"))
						} else {
							sh := job.NewSyncHandler(e.Dispatch, process)
							base = sh
							col.Register(sh.MetricsSource("handler"))
						}

						bt := job.NewBufferedTransformer(e.Dispatch, base, capacity)
						col.Register(bt.MetricsSource("bt"))
						bt.OnConfirm(func(j job.Job) {
							confirmedCount++
							e.Log.Info("confirmed", env.Str("id", j.ID))
						})
						bt.OnError(func(j job.Job) {
							erroredCount++
							e.Log.Warn("errored", env.Str("id", j.ID))
						})

						var seq int
						var scheduleRound func(round int)
						scheduleRound = func(round int) {
							if round >= rounds {
								return
							}
							delay := time.Duration(1+e.Rand.IntN(5)) * time.Millisecond
							e.Clock.AfterFunc(delay, func() {
								burst := 1 + e.Rand.IntN(capacity*3+2)
								for range burst {
									seq++
									id := fmt.Sprintf("job-%03d", seq)
									submittedCount++
									bt.Submit(job.Job{ID: id, Payload: []byte(id)})
								}
								snaps = append(snaps, col.Publish(int64(s.Now())))
								scheduleRound(round + 1)
							})
						}

						return &hookJobProcess{
							onInit: func() {
								scheduleRound(0)
							},
						}
					}); err != nil {
						return s, err
					}

					if err := s.RunUntilQuiet(); err != nil {
						return s, err
					}
					if submittedCount != confirmedCount+erroredCount {
						return s, fmt.Errorf("conservation failed: %d != %d + %d",
							submittedCount, confirmedCount, erroredCount)
					}
					if confirmedCount != processedCount {
						return s, fmt.Errorf("processed mismatch: %d != %d", confirmedCount, processedCount)
					}

					snaps = append(snaps, col.Publish(int64(s.Now())))
					runSnapshots = append(runSnapshots, snaps)
					return s, nil
				})

				if len(runSnapshots) != 2 {
					t.Fatalf("expected 2 replay snapshot slices, got %d", len(runSnapshots))
				}
				if !reflect.DeepEqual(runSnapshots[0], runSnapshots[1]) {
					t.Fatalf("obs.Collector snapshots diverged across deterministic replay:\nrun1=%+v\nrun2=%+v",
						runSnapshots[0], runSnapshots[1])
				}
			})
		}
	})
}
