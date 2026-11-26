// Story: Prometheus text exposition renders deterministically and serves valid HTTP scrape responses.
//
// What these tests prove: deterministic # HELP/# TYPE grouping, _total suffixing, label/help escaping, reserved/empty label filtering, defensive Last() copies, 503/200/405 Handler() HTTP behavior, and writer error propagation.
// How they probe: exact format assertions on Render output, httptest GET/HEAD/POST lifecycle checks before and after Collector.Publish, and a failing io.Writer.
// What they do not prove: parsing by an external Prometheus scraper or multi-goroutine HTTP scrape concurrency.
package promtext_test

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/obs/promtext"
)

func TestRenderFormatsAndEscaping(t *testing.T) {
	durHist := obs.NewDurationHistogram()
	durHist.Record(50_000)         // <= 100_000
	durHist.Record(200_000)        // bucket Hi <= 250_000
	durHist.Record(2_000_000)      // bucket Hi <= 2_500_000
	durHist.Record(90_000_000_000) // clamped to 60_000_000_000, still included in Count & +Inf

	cntHist := obs.NewCountHistogram()
	cntHist.Record(1)
	cntHist.Record(10)

	c := obs.NewCollector("node-1")
	c.Register(obs.SourceFunc{
		Name: "net-io",
		Fn: func(e *obs.Emitter) {
			e.Counter("packets", "count", "packets\\sent\nline2", 10,
				obs.Label{Key: "peer", Value: "a\"b\\c\nd"},
				obs.Label{Key: "instance", Value: "ignored"},
				obs.Label{Key: "le", Value: "ignored"},
				obs.Label{Key: "", Value: "ignored"},
			)
			e.Counter("packets", "count", "packets\\sent\nline2", 4,
				obs.Label{Key: "peer", Value: "p2"},
			)
			e.Counter("bytes_total", "bytes", "already has total suffix", 2048)
			e.Gauge("queue-depth", "count", "depth", 7)
			e.Histogram("latency", "ns", "delivery\\latency\nhist", durHist,
				obs.Label{Key: "kind", Value: "rpc"},
			)
			e.Histogram("batch", "count", "batch size", cntHist)
		},
	})

	snap := c.Collect(12345)
	got1 := string(promtext.Render("dsa_", snap))
	got2 := string(promtext.Render("dsa_", snap))
	if got1 != got2 {
		t.Fatalf("Render not deterministic:\n%s\n---\n%s", got1, got2)
	}

	// # HELP and # TYPE emitted once per family, with proper escaping and _total suffix.
	wantSnippets := []string{
		`# HELP dsa_net_io_packets_total packets\\sent\nline2`,
		`# TYPE dsa_net_io_packets_total counter`,
		`dsa_net_io_packets_total{instance="node-1",peer="a\"b\\c\nd"} 10`,
		`dsa_net_io_packets_total{instance="node-1",peer="p2"} 4`,
		`# TYPE dsa_net_io_bytes_total counter`,
		`dsa_net_io_bytes_total{instance="node-1"} 2048`,
		`# TYPE dsa_net_io_queue_depth gauge`,
		`dsa_net_io_queue_depth{instance="node-1"} 7`,
		`# HELP dsa_net_io_latency delivery\\latency\nhist`,
		`# TYPE dsa_net_io_latency histogram`,
		`dsa_net_io_latency_bucket{instance="node-1",kind="rpc",le="100000"} 1`,
		`dsa_net_io_latency_bucket{instance="node-1",kind="rpc",le="250000"} 2`,
		`dsa_net_io_latency_bucket{instance="node-1",kind="rpc",le="1000000"} 2`,
		`dsa_net_io_latency_bucket{instance="node-1",kind="rpc",le="2500000"} 3`,
		`dsa_net_io_latency_bucket{instance="node-1",kind="rpc",le="60000000000"} 4`,
		`dsa_net_io_latency_bucket{instance="node-1",kind="rpc",le="+Inf"} 4`,
		`dsa_net_io_latency_count{instance="node-1",kind="rpc"} 4`,
		`# TYPE dsa_net_io_batch histogram`,
		`dsa_net_io_batch_bucket{instance="node-1",le="1"} 1`,
		`dsa_net_io_batch_bucket{instance="node-1",le="4"} 1`,
		`dsa_net_io_batch_bucket{instance="node-1",le="16"} 2`,
		`dsa_net_io_batch_bucket{instance="node-1",le="+Inf"} 2`,
		`dsa_net_io_batch_count{instance="node-1"} 2`,
	}
	for _, want := range wantSnippets {
		if !strings.Contains(got1, want) {
			t.Fatalf("rendered output missing %q:\n%s", want, got1)
		}
	}

	if strings.Contains(got1, "bytes_total_total") {
		t.Fatalf("counter suffix _total was doubled:\n%s", got1)
	}
	if count := strings.Count(got1, "# TYPE dsa_net_io_packets_total counter"); count != 1 {
		t.Fatalf("expected 1 # TYPE header for packets_total, got %d", count)
	}
}

func TestHandlerAndAdapterLifecycle(t *testing.T) {
	var out bytes.Buffer
	a := promtext.New("", &out)
	if a.Name() != promtext.AdapterName {
		t.Fatalf("Name() = %q, want %q", a.Name(), promtext.AdapterName)
	}
	if a.Last() != nil {
		t.Fatalf("Last() before publish = %q, want nil", a.Last())
	}

	h := a.Handler()

	// 503 before first publish on GET and HEAD.
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req := httptest.NewRequest(method, "/metrics", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s before publish status = %d, want 503", method, rec.Code)
		}
	}

	// 405 on POST before publish.
	reqPost := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	recPost := httptest.NewRecorder()
	h.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", recPost.Code)
	}

	c := obs.NewCollector("inst-1")
	c.Register(obs.SourceFunc{
		Name: "sim",
		Fn: func(e *obs.Emitter) {
			e.Counter("steps", "count", "steps executed", 42)
		},
	})
	c.AddAdapter(a)
	c.Publish(100)

	if c.AdapterErrors(promtext.AdapterName) != 0 {
		t.Fatalf("AdapterErrors = %d, want 0", c.AdapterErrors(promtext.AdapterName))
	}

	last := a.Last()
	if len(last) == 0 || !bytes.Equal(out.Bytes(), last) {
		t.Fatalf("writer bytes and Last() mismatch: out=%q last=%q", out.String(), string(last))
	}
	// Mutate Last() return slice and confirm cached copy is isolated.
	last[0] = 'X'
	if bytes.Equal(a.Last(), last) {
		t.Fatal("Last() did not return a defensive copy")
	}

	// GET after publish -> 200 with ContentType and exposition body.
	reqGet := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recGet := httptest.NewRecorder()
	h.ServeHTTP(recGet, reqGet)
	if recGet.Code != http.StatusOK {
		t.Fatalf("GET after publish status = %d, want 200", recGet.Code)
	}
	if ct := recGet.Header().Get("Content-Type"); ct != promtext.ContentType {
		t.Fatalf("GET Content-Type = %q, want %q", ct, promtext.ContentType)
	}
	if !bytes.Equal(recGet.Body.Bytes(), a.Last()) {
		t.Fatalf("GET body mismatch:\ngot:\n%s\nwant:\n%s", recGet.Body.String(), string(a.Last()))
	}

	// HEAD after publish -> 200 with ContentType and empty body.
	reqHead := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	recHead := httptest.NewRecorder()
	h.ServeHTTP(recHead, reqHead)
	if recHead.Code != http.StatusOK {
		t.Fatalf("HEAD after publish status = %d, want 200", recHead.Code)
	}
	if ct := recHead.Header().Get("Content-Type"); ct != promtext.ContentType {
		t.Fatalf("HEAD Content-Type = %q, want %q", ct, promtext.ContentType)
	}
	if recHead.Body.Len() != 0 {
		t.Fatalf("HEAD body len = %d, want 0", recHead.Body.Len())
	}

	// POST after publish -> 405.
	recPost2 := httptest.NewRecorder()
	h.ServeHTTP(recPost2, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if recPost2.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST after publish status = %d, want 405", recPost2.Code)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestAdapterPropagatesWriterError(t *testing.T) {
	a := promtext.New("custom_", failWriter{})
	err := a.Export(obs.Snapshot{
		Instance: "n1",
		Points:   []obs.Point{{Component: "c", Metric: "m", Kind: obs.KindGauge, Value: 1}},
	})
	if err == nil {
		t.Fatal("Export with failing writer returned nil, want error")
	}
	// Cache is still updated prior to writer error.
	if !strings.Contains(string(a.Last()), "custom_c_m{instance=\"n1\"} 1") {
		t.Fatalf("unexpected Last() after writer error: %q", string(a.Last()))
	}
}

func TestRenderOmitsEmptyBracesWhenAllLabelsFiltered(t *testing.T) {
	out := string(promtext.Render("dsa_", obs.Snapshot{
		Points: []obs.Point{{
			Component: "c",
			Metric:    "m",
			Kind:      obs.KindGauge,
			Value:     7,
			Labels:    []obs.Label{{Key: "", Value: "x"}, {Key: "instance", Value: "dup"}, {Key: "le", Value: "10"}},
		}},
	}))
	if strings.Contains(out, "{}") {
		t.Fatalf("unexpected empty label braces in output:\n%s", out)
	}
	if !strings.Contains(out, "dsa_c_m 7\n") {
		t.Fatalf("expected bare metric line 'dsa_c_m 7', got:\n%s", out)
	}
}
