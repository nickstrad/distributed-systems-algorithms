// Story: Logline renders compact, single-line metric summaries bounded by MaxLineBytes.
//
// What these tests prove: deterministic single-line obs.publish formatting, omission of labelled scalars and empty instance, instance quoting, TruncatedMark clamping within MaxLineBytes, defensive Last() copies, and writer error handling.
// How they probe: exact string assertions on Render output, an 80-point overflow test for MaxLineBytes, slice-mutation checks on Last(), and nil/failing io.Writer adapters.
// What they do not prove: ingestion by external log aggregators or preservation of labelled scalar points and histogram buckets (intentionally omitted from the summary line).
package logline_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/obs/logline"
)

func TestRenderSingleLineAndLabelFiltering(t *testing.T) {
	h := obs.NewCountHistogram()
	h.Record(5)

	snap := obs.Snapshot{
		Seq:      7,
		TimeNs:   42_000,
		Instance: "node-1",
		Points: []obs.Point{
			{Component: "sim", Metric: "steps", Kind: obs.KindCounter, Value: 100},
			{Component: "sim", Metric: "peer_msgs", Kind: obs.KindCounter, Value: 55, Labels: []obs.Label{{Key: "peer", Value: "n2"}}},
			{Component: "job", Metric: "buffered", Kind: obs.KindGauge, Value: 3},
		},
		Hists: []obs.HistPoint{
			{Component: "sim", Metric: "latency", Count: 1, Buckets: h.Buckets()},
		},
	}

	line := string(logline.Render(snap))
	if !strings.HasSuffix(line, "\n") {
		t.Fatalf("line does not end with newline: %q", line)
	}
	if strings.Count(line, "\n") != 1 {
		t.Fatalf("expected single line, got %q", line)
	}

	want := "obs.publish seq=7 t_ns=42000 instance=node-1 points=3 histograms=1 sim.steps=100 job.buffered=3\n"
	if line != want {
		t.Fatalf("Render =\n  %q\nwant:\n  %q", line, want)
	}
	if strings.Contains(line, "peer_msgs") {
		t.Fatalf("labelled scalar point should have been omitted: %q", line)
	}

	// Empty instance is omitted; instance with spaces is quoted.
	noInst := string(logline.Render(obs.Snapshot{Seq: 1, TimeNs: 2}))
	if strings.Contains(noInst, "instance=") {
		t.Fatalf("empty instance should be omitted: %q", noInst)
	}
	spacedInst := string(logline.Render(obs.Snapshot{Seq: 1, TimeNs: 2, Instance: "node 1"}))
	if !strings.Contains(spacedInst, `instance="node 1"`) {
		t.Fatalf("instance with space should be quoted: %q", spacedInst)
	}
}

func TestRenderMaxLineBytesTruncation(t *testing.T) {
	points := make([]obs.Point, 80)
	for i := range points {
		points[i] = obs.Point{
			Component: "comp",
			Metric:    fmt.Sprintf("long_metric_name_%02d", i),
			Kind:      obs.KindCounter,
			Value:     int64(i * 1000),
		}
	}
	snap := obs.Snapshot{
		Seq:      9,
		TimeNs:   999_999,
		Instance: "node-alpha",
		Points:   points,
	}

	raw := logline.Render(snap)
	if len(raw) > logline.MaxLineBytes {
		t.Fatalf("line length %d exceeds MaxLineBytes (%d)", len(raw), logline.MaxLineBytes)
	}
	line := string(raw)
	if strings.Count(line, "\n") != 1 || !strings.HasSuffix(line, logline.TruncatedMark+"\n") {
		t.Fatalf("truncated line missing %q suffix or extra newlines: %q", logline.TruncatedMark+"\n", line)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestAdapterExportAndLastDefensiveCopy(t *testing.T) {
	var buf bytes.Buffer
	a := logline.New(&buf)
	if a.Name() != logline.AdapterName {
		t.Fatalf("Name() = %q, want %q", a.Name(), logline.AdapterName)
	}
	if a.Last() != nil {
		t.Fatalf("Last() before Export = %q, want nil", a.Last())
	}

	snap := obs.Snapshot{
		Seq:      3,
		TimeNs:   500,
		Instance: "n1",
		Points: []obs.Point{
			{Component: "sim", Metric: "steps", Kind: obs.KindCounter, Value: 12},
		},
	}
	if err := a.Export(snap); err != nil {
		t.Fatalf("Export: %v", err)
	}

	last1 := a.Last()
	if !bytes.Equal(buf.Bytes(), last1) {
		t.Fatalf("written bytes %q != Last() %q", buf.String(), string(last1))
	}
	last1[0] = 'X'
	last2 := a.Last()
	if bytes.Equal(last1, last2) {
		t.Fatal("Last() did not return a defensive copy")
	}

	// Nil writer succeeds; failing writer returns error.
	nilAdapter := logline.New(nil)
	if err := nilAdapter.Export(snap); err != nil {
		t.Fatalf("nil writer Export: %v", err)
	}
	if len(nilAdapter.Last()) == 0 {
		t.Fatal("nil writer Adapter Last() is empty after Export")
	}

	badAdapter := logline.New(errWriter{})
	if err := badAdapter.Export(snap); err == nil {
		t.Fatal("errWriter Export returned nil, want error")
	}
}
