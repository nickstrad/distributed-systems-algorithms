package scenario

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/obs/csvflush"
)

// Check is one explicit claim evaluated during a suite run and rendered as a
// row in the VERDICT table.
type Check struct {
	Claim    string
	Observed string
	Expected string
	Pass     bool
}

// Story explains a simulation run in plain English so readers can learn by
// probing suites, inspecting metrics, and checking explicit verdicts.
type Story struct {
	Proves       string
	HowCollected string
	DoesNotProve string
	Checks       []Check
}

// Passed reports whether every Check in s passed.
func (s Story) Passed() bool {
	for _, c := range s.Checks {
		if !c.Pass {
			return false
		}
	}
	return true
}

// FormatStory renders a readable narrative report covering what the run proves,
// how data was collected, observed metrics and quantiles, an explicit VERDICT
// table, explicit scope limits, and a copy-pasteable replay command.
func FormatStory(res RunResult, replayCmd string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "=== STORY: %s (seed=%d) ===\n", res.Suite, res.Seed)
	if res.Story.Proves != "" {
		fmt.Fprintf(&b, "\nWHAT THIS RUN PROVES\n  %s\n", res.Story.Proves)
	}

	fmt.Fprintf(&b, "\nHOW THE DATA WAS COLLECTED\n")
	if res.Story.HowCollected != "" {
		fmt.Fprintf(&b, "  %s\n", res.Story.HowCollected)
	}
	fmt.Fprintf(&b, "  steps=%d  virtual_time=%s  digest=%s\n",
		res.Steps, formatDurationNs(int64(res.Now)), shortDigest(res.Digest))
	if res.Summary != "" {
		fmt.Fprintf(&b, "  summary: %s\n", res.Summary)
	}
	if res.CSVDir != "" {
		fmt.Fprintf(&b, "  csv:     %s\n", res.CSVDir)
	}
	if res.PromFile != "" {
		fmt.Fprintf(&b, "  prom:    %s\n", res.PromFile)
	}

	if len(res.Snapshot.Points) > 0 {
		fmt.Fprintf(&b, "\nOBSERVED METRICS\n")
		var tbl strings.Builder
		tw := tabwriter.NewWriter(&tbl, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  METRIC\tVALUE\tKIND\tMEANING")
		for _, p := range res.Snapshot.Points {
			name := p.Component + "." + p.Metric
			if len(p.Labels) > 0 {
				var lp []string
				for _, l := range p.Labels {
					lp = append(lp, l.Key+"="+l.Value)
				}
				name += "{" + strings.Join(lp, ",") + "}"
			}
			val := formatMetricValue(p.Value, p.Unit)
			help := p.Help
			if help == "" {
				help = "-"
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", name, val, p.Kind.String(), help)
		}
		_ = tw.Flush()
		b.WriteString(tbl.String())
	}

	if len(res.Snapshot.Hists) > 0 {
		fmt.Fprintf(&b, "\nDISTRIBUTIONS (bucket upper bounds)\n")
		var tbl strings.Builder
		tw := tabwriter.NewWriter(&tbl, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  HISTOGRAM\tSAMPLES\tP50\tP95\tP99\tCLAMPED")
		for _, h := range res.Snapshot.Hists {
			name := h.Component + "." + h.Metric
			p50 := formatMetricValue(bucketsQuantile(h.Buckets, h.Count, 5000), h.Unit)
			p95 := formatMetricValue(bucketsQuantile(h.Buckets, h.Count, 9500), h.Unit)
			p99 := formatMetricValue(bucketsQuantile(h.Buckets, h.Count, 9900), h.Unit)
			clamped := fmt.Sprintf("lo=%d hi=%d", h.ClampedLow, h.ClampedHigh)
			fmt.Fprintf(tw, "  %s\t%d\t%s\t%s\t%s\t%s\n", name, h.Count, p50, p95, p99, clamped)
		}
		_ = tw.Flush()
		b.WriteString(tbl.String())
	}

	if len(res.Story.Checks) > 0 {
		fmt.Fprintf(&b, "\nVERDICT\n")
		var tbl strings.Builder
		tw := tabwriter.NewWriter(&tbl, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  CLAIM\tOBSERVED\tEXPECTED\tRESULT")
		passed := 0
		for _, c := range res.Story.Checks {
			status := "FAIL"
			if c.Pass {
				status = "PASS"
				passed++
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", c.Claim, c.Observed, c.Expected, status)
		}
		_ = tw.Flush()
		b.WriteString(tbl.String())
		overall := "PASS"
		if passed != len(res.Story.Checks) {
			overall = "FAIL"
		}
		fmt.Fprintf(&b, "  OVERALL: %s (%d/%d checks passed)\n", overall, passed, len(res.Story.Checks))
	}

	if res.Story.DoesNotProve != "" {
		fmt.Fprintf(&b, "\nWHAT THIS RUN DOES NOT PROVE\n  %s\n", res.Story.DoesNotProve)
	}
	if replayCmd != "" {
		fmt.Fprintf(&b, "\nREPLAY\n  %s\n", replayCmd)
	}
	return b.String()
}

// FormatCSVReport reads a completed csvflush directory and renders a readable
// offline analysis report (run metadata, final metrics, and histogram quantiles)
// without requiring DuckDB or Prometheus.
func FormatCSVReport(dir string) (string, error) {
	data, err := csvflush.Read(dir)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "=== CSV ANALYSIS REPORT: %s ===\n", dir)
	fmt.Fprintf(&b, "\nRUN METADATA\n")
	fmt.Fprintf(&b, "  run_id=%s  scenario=%s  seed=%d  instance=%s  flushes=%d  duration=%s\n",
		data.Run.RunID, data.Run.Scenario, data.Run.Seed, data.Run.Instance,
		data.Flushes, formatDurationNs(data.DurationNs))

	// Find final flush sequence for scalar metrics.
	var maxSeq int64 = -1
	for _, m := range data.Metrics {
		if m.FlushSeq > maxSeq {
			maxSeq = m.FlushSeq
		}
	}
	if maxSeq >= 0 {
		fmt.Fprintf(&b, "\nFINAL METRICS (flush_seq=%d)\n", maxSeq)
		var tbl strings.Builder
		tw := tabwriter.NewWriter(&tbl, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  METRIC\tVALUE\tKIND\tUNIT")
		for _, m := range data.Metrics {
			if m.FlushSeq != maxSeq {
				continue
			}
			name := m.Component + "." + m.Metric + formatLabelMap(m.Labels)
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", name, formatMetricValue(m.Value, m.Unit), m.Kind, m.Unit)
		}
		_ = tw.Flush()
		b.WriteString(tbl.String())
	}

	// Group final-flush histogram rows by (component, metric, unit, labels).
	var maxHistSeq int64 = -1
	for _, h := range data.Histograms {
		if h.FlushSeq > maxHistSeq {
			maxHistSeq = h.FlushSeq
		}
	}
	if maxHistSeq >= 0 {
		type histKey struct {
			name string
			unit string
		}
		grouped := make(map[histKey][]obs.Bucket)
		var order []histKey
		for _, h := range data.Histograms {
			if h.FlushSeq != maxHistSeq {
				continue
			}
			k := histKey{name: h.Component + "." + h.Metric + formatLabelMap(h.Labels), unit: h.Unit}
			if _, seen := grouped[k]; !seen {
				order = append(order, k)
			}
			grouped[k] = append(grouped[k], obs.Bucket{Lo: h.BucketLo, Hi: h.BucketHi, Count: h.Count})
		}
		slices.SortFunc(order, func(a, b histKey) int {
			return strings.Compare(a.name, b.name)
		})

		fmt.Fprintf(&b, "\nFINAL DISTRIBUTIONS (flush_seq=%d, bucket upper bounds)\n", maxHistSeq)
		var tbl strings.Builder
		tw := tabwriter.NewWriter(&tbl, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  HISTOGRAM\tSAMPLES\tP50\tP95\tP99")
		for _, k := range order {
			bs := grouped[k]
			var total int64
			for _, bk := range bs {
				total += bk.Count
			}
			p50 := formatMetricValue(bucketsQuantile(bs, total, 5000), k.unit)
			p95 := formatMetricValue(bucketsQuantile(bs, total, 9500), k.unit)
			p99 := formatMetricValue(bucketsQuantile(bs, total, 9900), k.unit)
			fmt.Fprintf(tw, "  %s\t%d\t%s\t%s\t%s\n", k.name, total, p50, p95, p99)
		}
		_ = tw.Flush()
		b.WriteString(tbl.String())
	}

	return b.String(), nil
}

func bucketsQuantile(buckets []obs.Bucket, total, bp int64) int64 {
	if total <= 0 || len(buckets) == 0 {
		return 0
	}
	target := (total*bp + 9999) / 10000
	if target < 1 {
		target = 1
	}
	var cum int64
	for _, b := range buckets {
		cum += b.Count
		if cum >= target {
			return b.Hi
		}
	}
	return buckets[len(buckets)-1].Hi
}

func formatMetricValue(v int64, unit string) string {
	switch unit {
	case "ns":
		return formatDurationNs(v)
	case "bytes":
		return strconv.FormatInt(v, 10) + " B"
	default:
		return strconv.FormatInt(v, 10)
	}
}

func formatDurationNs(ns int64) string {
	abs := ns
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000:
		return fmt.Sprintf("%.3fs", float64(ns)/1e9)
	case abs >= 1_000_000:
		return fmt.Sprintf("%.3fms", float64(ns)/1e6)
	case abs >= 1_000:
		return fmt.Sprintf("%.2fµs", float64(ns)/1e3)
	default:
		return strconv.FormatInt(ns, 10) + "ns"
	}
}

func shortDigest(d string) string {
	if len(d) > 16 {
		return d[:16] + "..."
	}
	return d
}

func formatLabelMap(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + m[k]
	}
	return "{" + strings.Join(parts, ",") + "}"
}
