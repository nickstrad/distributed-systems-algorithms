// Story: CSV metric snapshots round-trip losslessly, fail closed when unclosed, and match byte-for-byte on replay.
//
// What these tests prove: lossless CSV serialization of run info, scalar points, escaped labels, and histograms; ErrIncomplete on unclosed or reopened run directories; and byte-identical CSV output across identical sim+job seeds.
// How they probe: t.TempDir() round-trip and incomplete-marker tests plus two independent end-to-end sim+job.BufferedTransformer runs compared across metrics.csv, histograms.csv, and run.csv.
// What they do not prove: concurrent multi-writer access to the same directory or recovery from partially corrupted CSV rows on disk.
package csvflush_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/job"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/obs/csvflush"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

func TestRoundtripAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	runInfo := csvflush.RunInfo{
		RunID:    "run-42",
		Scenario: "roundtrip",
		Seed:     99,
		Instance: "node-a",
	}

	w, err := csvflush.New(dir, runInfo)
	if err != nil {
		t.Fatalf("csvflush.New: %v", err)
	}
	if w.Name() != csvflush.AdapterName {
		t.Fatalf("w.Name() = %q, want %q", w.Name(), csvflush.AdapterName)
	}

	// Read before Close must fail closed with ErrIncomplete.
	if _, err := csvflush.Read(dir); !errors.Is(err, csvflush.ErrIncomplete) {
		t.Fatalf("Read before Close err = %v, want ErrIncomplete", err)
	}

	var cnt obs.Counter
	cnt.Add(7)
	var gauge obs.Gauge
	gauge.Set(12)
	gauge.Set(5)
	hist := obs.NewCountHistogram()
	hist.Record(3)
	hist.Record(3)
	hist.Record(500)

	c := obs.NewCollector(runInfo.Instance)
	c.AddAdapter(w)
	c.Register(obs.SourceFunc{
		Name: "worker",
		Fn: func(e *obs.Emitter) {
			e.Counter("jobs_done", "count", "jobs completed", cnt.Value(),
				obs.Label{Key: "note", Value: "a,b\"c\nd\\e"},
				obs.Label{Key: "region", Value: "us-east=1"},
			)
			e.Gauge("queue_depth", "count", "current queue depth", gauge.Value())
			e.Histogram("batch_size", "count", "batch size", hist,
				obs.Label{Key: "kind", Value: "λ-batch"},
			)
		},
	})

	c.Publish(1_000_000)
	cnt.Add(3)
	gauge.Set(15)
	c.Publish(2_500_000)

	if err := c.AdapterErrors(csvflush.AdapterName); err != 0 {
		t.Fatalf("AdapterErrors = %d, want 0", err)
	}

	// Still incomplete until Close().
	if _, err := csvflush.Read(dir); !errors.Is(err, csvflush.ErrIncomplete) {
		t.Fatalf("Read after Publish before Close err = %v, want ErrIncomplete", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}
	// Second Close is a no-op.
	if err := w.Close(); err != nil {
		t.Fatalf("second w.Close: %v", err)
	}

	// Export after Close must return ErrClosed.
	if err := w.Export(obs.Snapshot{}); !errors.Is(err, csvflush.ErrClosed) {
		t.Fatalf("Export after Close err = %v, want ErrClosed", err)
	}

	data, err := csvflush.Read(dir)
	if err != nil {
		t.Fatalf("csvflush.Read: %v", err)
	}
	if data.Run != runInfo {
		t.Fatalf("data.Run = %+v, want %+v", data.Run, runInfo)
	}
	if data.Flushes != 2 {
		t.Fatalf("data.Flushes = %d, want 2", data.Flushes)
	}
	if data.DurationNs != 2_500_000 {
		t.Fatalf("data.DurationNs = %d, want 2500000", data.DurationNs)
	}

	// 2 flushes * 2 scalar metrics = 4 rows.
	if len(data.Metrics) != 4 {
		t.Fatalf("len(data.Metrics) = %d, want 4", len(data.Metrics))
	}
	wantSpecialLabels := map[string]string{
		"note":   "a,b\"c\nd\\e",
		"region": "us-east=1",
	}
	firstJobs := data.Metrics[0]
	if firstJobs.RunID != "run-42" || firstJobs.FlushSeq != 0 || firstJobs.TimeNs != 1_000_000 ||
		firstJobs.Instance != "node-a" || firstJobs.Component != "worker" ||
		firstJobs.Metric != "jobs_done" || firstJobs.Kind != "counter" ||
		firstJobs.Unit != "count" || firstJobs.Value != 7 ||
		!reflect.DeepEqual(firstJobs.Labels, wantSpecialLabels) {
		t.Fatalf("Metrics[0] = %+v, want jobs_done=7 with labels %v", firstJobs, wantSpecialLabels)
	}
	lastQueue := data.Metrics[3]
	if lastQueue.FlushSeq != 1 || lastQueue.TimeNs != 2_500_000 ||
		lastQueue.Metric != "queue_depth" || lastQueue.Kind != "gauge" ||
		lastQueue.Value != 15 || lastQueue.Labels != nil {
		t.Fatalf("Metrics[3] = %+v, want queue_depth=15 at flush 1 with nil labels", lastQueue)
	}

	// 2 flushes * 2 non-empty histogram buckets (3 and 500's bucket) = 4 rows.
	if len(data.Histograms) != 4 {
		t.Fatalf("len(data.Histograms) = %d, want 4", len(data.Histograms))
	}
	wantHistLabels := map[string]string{"kind": "λ-batch"}
	h0 := data.Histograms[0]
	if h0.RunID != "run-42" || h0.FlushSeq != 0 || h0.TimeNs != 1_000_000 ||
		h0.Component != "worker" || h0.Metric != "batch_size" || h0.Unit != "count" ||
		h0.BucketLo != 3 || h0.BucketHi != 3 || h0.Count != 2 ||
		!reflect.DeepEqual(h0.Labels, wantHistLabels) {
		t.Fatalf("Histograms[0] = %+v, want bucket [3,3]=2 with labels %v", h0, wantHistLabels)
	}
	h1 := data.Histograms[1]
	if h1.BucketLo > 500 || h1.BucketHi < 500 || h1.Count != 1 {
		t.Fatalf("Histograms[1] = %+v, want bucket covering 500 with count 1", h1)
	}
}

func TestNewRemovesPriorRunMarker(t *testing.T) {
	dir := t.TempDir()
	info := csvflush.RunInfo{RunID: "r1", Scenario: "s", Seed: 1, Instance: "i"}
	w1, err := csvflush.New(dir, info)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if _, err := csvflush.Read(dir); err != nil {
		t.Fatalf("Read after first Close: %v", err)
	}

	// Reopening same directory without closing must fail closed on Read.
	w2, err := csvflush.New(dir, info)
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	defer w2.Close()
	if _, err := csvflush.Read(dir); !errors.Is(err, csvflush.ErrIncomplete) {
		t.Fatalf("Read while second writer open err = %v, want ErrIncomplete", err)
	}
}

type simJobNode struct {
	env         env.Env
	transformer *job.BufferedTransformer
}

func (n *simJobNode) Init() {
	if n.env.Self != "n1" || n.transformer == nil {
		return
	}
	for i := range 6 {
		n.transformer.Submit(job.Job{
			ID:      fmt.Sprintf("job-%d", i),
			Payload: []byte("work"),
		})
	}
	n.env.Clock.AfterFunc(4*time.Millisecond, func() {
		for i := 6; i < 9; i++ {
			n.transformer.Submit(job.Job{
				ID:      fmt.Sprintf("job-%d", i),
				Payload: []byte("later"),
			})
		}
	})
}

func (n *simJobNode) Recover() {}

func (n *simJobNode) OnMessage(from env.NodeID, data []byte) {
	if n.env.Self == "n2" {
		_ = n.env.Net.Send(from, append([]byte("ack:"), data...))
	}
}

func runDeterministicWorkload(t *testing.T, dir string, seed int64) csvflush.RunData {
	t.Helper()

	s := sim.New(sim.Config{
		Seed: seed,
		Net: sim.NetConfig{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 5 * time.Millisecond,
			DropPPM:    100_000,
			DupPPM:     50_000,
		},
	})

	var transformer *job.BufferedTransformer
	peers := []env.NodeID{"n1", "n2"}
	if err := s.AddCluster(peers, func(e env.Env) env.Process {
		n := &simJobNode{env: e}
		if e.Self == "n1" {
			async := job.NewAsyncHandler(e.Dispatch, func(j job.Job) {
				_ = e.Net.Send("n2", []byte(j.ID))
			})
			transformer = job.NewBufferedTransformer(e.Dispatch, async, 2)
			n.transformer = transformer
		}
		return n
	}); err != nil {
		t.Fatalf("AddCluster: %v", err)
	}

	w, err := csvflush.New(dir, csvflush.RunInfo{
		RunID:    "det-run",
		Scenario: "sim-job-e2e",
		Seed:     seed,
		Instance: "sim",
	})
	if err != nil {
		t.Fatalf("csvflush.New: %v", err)
	}

	c := obs.NewCollector("sim")
	c.Register(s.MetricsSource())
	c.Register(transformer.MetricsSource("job"))
	c.AddAdapter(w)

	for _, at := range []time.Duration{2 * time.Millisecond, 6 * time.Millisecond} {
		s.ScheduleAfter(at, func() {
			c.Publish(int64(s.Now()))
		})
	}

	if err := s.RunUntilQuiet(); err != nil {
		t.Fatalf("RunUntilQuiet: %v", err)
	}
	c.Publish(int64(s.Now()))

	if err := c.AdapterErrors(csvflush.AdapterName); err != 0 {
		t.Fatalf("AdapterErrors = %d, want 0", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}
	data, err := csvflush.Read(dir)
	if err != nil {
		t.Fatalf("csvflush.Read: %v", err)
	}
	return data
}

func TestEndToEndSimAndJobDeterminism(t *testing.T) {
	const seed int64 = 20261126
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	data1 := runDeterministicWorkload(t, dir1, seed)
	data2 := runDeterministicWorkload(t, dir2, seed)

	if data1.Flushes != 3 || len(data1.Metrics) == 0 || len(data1.Histograms) == 0 {
		t.Fatalf("unexpected run1 summary: flushes=%d metrics=%d hists=%d",
			data1.Flushes, len(data1.Metrics), len(data1.Histograms))
	}
	if !reflect.DeepEqual(data1, data2) {
		t.Fatalf("RunData mismatch between identical seeds")
	}

	for _, name := range []string{"metrics.csv", "histograms.csv", "run.csv"} {
		b1, err := os.ReadFile(filepath.Join(dir1, name))
		if err != nil {
			t.Fatalf("read dir1/%s: %v", name, err)
		}
		b2, err := os.ReadFile(filepath.Join(dir2, name))
		if err != nil {
			t.Fatalf("read dir2/%s: %v", name, err)
		}
		if !bytes.Equal(b1, b2) {
			t.Fatalf("%s differs between identical runs:\n--- run1 ---\n%s\n--- run2 ---\n%s", name, b1, b2)
		}
	}
}
