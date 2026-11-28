// Package promtext renders obs.Snapshot values into Prometheus text exposition
// format 0.0.4 using only the Go standard library.
//
// It can write directly to an io.Writer (for tests and snapshots) and also
// exposes an http.Handler serving the last published exposition body without
// touching any protocol state machine on scrape.
package promtext

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/nickstrad/distributed-systems-algorithms/obs"
)

const (
	AdapterName = "prom"
	ContentType = "text/plain; version=0.0.4; charset=utf-8"
)

// DefaultDurationBounds are 16 cumulative nanosecond bounds (100 µs to 60 s)
// used when exporting duration ("ns") histograms to Prometheus.
var DefaultDurationBounds = []int64{
	100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, 25_000_000,
	50_000_000, 100_000_000, 250_000_000, 500_000_000, 1_000_000_000, 2_500_000_000, 10_000_000_000, 60_000_000_000,
}

// DefaultCountBounds are powers-of-4 cumulative bounds for count/bytes histograms.
var DefaultCountBounds = []int64{
	1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824,
}

// Adapter implements obs.Adapter, caching the latest rendered Prometheus
// exposition body atomically for zero-contention HTTP scrapes.
type Adapter struct {
	prefix string
	w      io.Writer
	latest atomic.Pointer[[]byte]
}

// New creates a Prometheus text Adapter. Metric family names are prefixed with
// prefix (default "dsa_"). If w is non-nil, every Export writes the rendered
// text to w as well as updating the cached scrape body.
func New(prefix string, w io.Writer) *Adapter {
	if prefix == "" {
		prefix = "dsa_"
	}
	return &Adapter{prefix: prefix, w: w}
}

// Name returns AdapterName ("prom").
func (a *Adapter) Name() string { return AdapterName }

// Export renders s, atomically swaps the cached scrape payload, and writes to w
// if set. The cache advances even if the optional stream write fails.
func (a *Adapter) Export(s obs.Snapshot) error {
	body := Render(a.prefix, s)
	a.latest.Store(&body)
	if a.w == nil {
		return nil
	}
	n, err := a.w.Write(body)
	if err == nil && n != len(body) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("promtext: write snapshot %d for %q: %w", s.Seq, s.Instance, err)
	}
	return nil
}

// Last returns the most recently rendered Prometheus exposition bytes, or nil.
func (a *Adapter) Last() []byte {
	p := a.latest.Load()
	if p == nil {
		return nil
	}
	return slices.Clone(*p)
}

// Handler returns an http.Handler suitable for mounting at /metrics.
// Scrapes never invoke protocol or collector code; they return 503 until the
// first snapshot has been published.
func (a *Adapter) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		p := a.latest.Load()
		if p == nil {
			http.Error(w, "no metrics published yet", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", ContentType)
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(*p)
		}
	})
}

// Render formats s as deterministic Prometheus text exposition 0.0.4.
func Render(prefix string, s obs.Snapshot) []byte {
	var b bytes.Buffer
	seenFamily := make(map[string]bool)

	for _, p := range s.Points {
		fam := familyName(prefix, p.Component, p.Metric, p.Kind == obs.KindCounter)
		writeFamilyHeader(&b, seenFamily, fam, p.Help, p.Kind.String())
		b.WriteString(fam)
		writeLabels(&b, s.Instance, p.Labels, "")
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(p.Value, 10))
		b.WriteByte('\n')
	}

	for _, h := range s.Hists {
		fam := sanitizeName(prefix + h.Component + "_" + h.Metric)
		writeFamilyHeader(&b, seenFamily, fam, h.Help, "histogram")
		bounds := DefaultCountBounds
		if h.Unit == "ns" {
			bounds = DefaultDurationBounds
		}
		counts := rebucket(h.Buckets, bounds)
		var cum int64
		for i, bound := range bounds {
			cum += counts[i]
			b.WriteString(fam)
			b.WriteString("_bucket")
			writeLabels(&b, s.Instance, h.Labels, strconv.FormatInt(bound, 10))
			b.WriteByte(' ')
			b.WriteString(strconv.FormatInt(cum, 10))
			b.WriteByte('\n')
		}
		b.WriteString(fam)
		b.WriteString("_bucket")
		writeLabels(&b, s.Instance, h.Labels, "+Inf")
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(h.Count, 10))
		b.WriteByte('\n')

		b.WriteString(fam)
		b.WriteString("_count")
		writeLabels(&b, s.Instance, h.Labels, "")
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(h.Count, 10))
		b.WriteByte('\n')
	}

	return b.Bytes()
}

func writeFamilyHeader(b *bytes.Buffer, seen map[string]bool, fam, help, kind string) {
	if seen[fam] {
		return
	}
	seen[fam] = true
	if help != "" {
		b.WriteString("# HELP ")
		b.WriteString(fam)
		b.WriteByte(' ')
		b.WriteString(escapeHelp(help))
		b.WriteByte('\n')
	}
	b.WriteString("# TYPE ")
	b.WriteString(fam)
	b.WriteByte(' ')
	b.WriteString(kind)
	b.WriteByte('\n')
}

func rebucket(src []obs.Bucket, bounds []int64) []int64 {
	out := make([]int64, len(bounds))
	for _, b := range src {
		for i, bound := range bounds {
			if b.Hi <= bound {
				out[i] += b.Count
				break
			}
		}
	}
	return out
}

func familyName(prefix, component, metric string, isCounter bool) string {
	name := sanitizeName(prefix + component + "_" + metric)
	if isCounter && !strings.HasSuffix(name, "_total") {
		name += "_total"
	}
	return name
}

func sanitizeName(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, s)
}

func writeLabels(b *bytes.Buffer, instance string, labels []obs.Label, le string) {
	first := true
	add := func(k, v string) {
		if first {
			b.WriteByte('{')
			first = false
		} else {
			b.WriteByte(',')
		}
		b.WriteString(sanitizeName(k))
		b.WriteString(`="`)
		b.WriteString(escapeLabel(v))
		b.WriteByte('"')
	}
	if instance != "" {
		add("instance", instance)
	}
	for _, l := range labels {
		if l.Key == "" || l.Key == "instance" || l.Key == "le" {
			continue
		}
		add(l.Key, l.Value)
	}
	if le != "" {
		add("le", le)
	}
	if !first {
		b.WriteByte('}')
	}
}

func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "\n", `\n`)
}

func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, `"`, `\"`)
}
