// Package csvflush writes obs.Snapshot sequences to schema-version-1 CSV files
// (metrics.csv, histograms.csv, and run.csv) for offline inspection, Go test
// assertions, or DuckDB SQL analysis.
//
// run.csv is written last on Close so interrupted or failed runs are visibly
// incomplete and fail closed on readback.
package csvflush

import (
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/nickstrad/distributed-systems-algorithms/obs"
)

const (
	AdapterName   = "csv"
	SchemaVersion = 1
)

var (
	ErrClosed     = errors.New("csvflush: writer closed")
	ErrIncomplete = errors.New("csvflush: incomplete run (missing run.csv)")
)

var (
	runHeader     = []string{"schema_version", "run_id", "scenario", "seed", "instance", "flushes", "duration_ns"}
	metricsHeader = []string{"schema_version", "run_id", "flush_seq", "t_ns", "instance", "component", "metric", "kind", "unit", "labels_json", "value"}
	histHeader    = []string{"schema_version", "run_id", "flush_seq", "t_ns", "instance", "component", "metric", "unit", "labels_json", "bucket_lo", "bucket_hi", "count"}
)

// RunInfo supplies static metadata recorded into run.csv when Close succeeds.
type RunInfo struct {
	RunID    string
	Scenario string
	Seed     int64
	Instance string
}

// Writer implements obs.Adapter and writes deterministic CSV files under Dir.
type Writer struct {
	dir     string
	run     RunInfo
	mFile   *os.File
	hFile   *os.File
	mCSV    *csv.Writer
	hCSV    *csv.Writer
	flushes int64
	lastT   int64
	closed  bool
	failed  bool
}

// New creates a CSV writer inside dir (creating dir if needed) and writes the
// schema-version-1 headers to metrics.csv and histograms.csv.
func New(dir string, run RunInfo) (*Writer, error) {
	if dir == "" {
		return nil, errors.New("csvflush: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("csvflush: create output directory %q: %w", dir, err)
	}
	// Remove any leftover run.csv from an earlier run in the same directory so
	// incomplete runs fail closed.
	if err := os.Remove(filepath.Join(dir, "run.csv")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("csvflush: remove prior completion marker before opening %q: %w", dir, err)
	}

	mf, err := os.Create(filepath.Join(dir, "metrics.csv"))
	if err != nil {
		return nil, fmt.Errorf("csvflush: open metrics data file in %q: %w", dir, err)
	}
	hf, err := os.Create(filepath.Join(dir, "histograms.csv"))
	if err != nil {
		_ = mf.Close()
		return nil, fmt.Errorf("csvflush: open histogram data file in %q: %w", dir, err)
	}

	mw := csv.NewWriter(mf)
	hw := csv.NewWriter(hf)
	if err := mw.Write(metricsHeader); err != nil {
		_ = mf.Close()
		_ = hf.Close()
		return nil, err
	}
	if err := hw.Write(histHeader); err != nil {
		_ = mf.Close()
		_ = hf.Close()
		return nil, err
	}
	mw.Flush()
	hw.Flush()
	if err := errors.Join(mw.Error(), hw.Error()); err != nil {
		_ = mf.Close()
		_ = hf.Close()
		return nil, err
	}

	return &Writer{
		dir:   dir,
		run:   run,
		mFile: mf,
		hFile: hf,
		mCSV:  mw,
		hCSV:  hw,
	}, nil
}

// Name returns AdapterName ("csv").
func (w *Writer) Name() string { return AdapterName }

// Export appends all scalar points and non-empty histogram buckets from s.
func (w *Writer) Export(s obs.Snapshot) error {
	if w.closed {
		return ErrClosed
	}
	if w.failed {
		return errors.New("csvflush: writer stopped after prior error")
	}
	seqStr := strconv.FormatInt(s.Seq, 10)
	tStr := strconv.FormatInt(s.TimeNs, 10)
	verStr := strconv.Itoa(SchemaVersion)

	for _, p := range s.Points {
		lj, err := encodeLabelsJSON(p.Labels)
		if err != nil {
			w.failed = true
			return fmt.Errorf("csvflush: encode labels for %s.%s at snapshot %d: %w", p.Component, p.Metric, s.Seq, err)
		}
		rec := []string{
			verStr, w.run.RunID, seqStr, tStr, s.Instance,
			p.Component, p.Metric, p.Kind.String(), p.Unit, lj,
			strconv.FormatInt(p.Value, 10),
		}
		if err := w.mCSV.Write(rec); err != nil {
			w.failed = true
			return fmt.Errorf("csvflush: write %s.%s to metrics.csv at snapshot %d: %w", p.Component, p.Metric, s.Seq, err)
		}
	}
	for _, h := range s.Hists {
		lj, err := encodeLabelsJSON(h.Labels)
		if err != nil {
			w.failed = true
			return fmt.Errorf("csvflush: encode labels for %s.%s at snapshot %d: %w", h.Component, h.Metric, s.Seq, err)
		}
		for _, b := range h.Buckets {
			rec := []string{
				verStr, w.run.RunID, seqStr, tStr, s.Instance,
				h.Component, h.Metric, h.Unit, lj,
				strconv.FormatInt(b.Lo, 10),
				strconv.FormatInt(b.Hi, 10),
				strconv.FormatInt(b.Count, 10),
			}
			if err := w.hCSV.Write(rec); err != nil {
				w.failed = true
				return fmt.Errorf("csvflush: write %s.%s to histograms.csv at snapshot %d: %w", h.Component, h.Metric, s.Seq, err)
			}
		}
	}
	w.mCSV.Flush()
	w.hCSV.Flush()
	if err := errors.Join(w.mCSV.Error(), w.hCSV.Error()); err != nil {
		w.failed = true
		return fmt.Errorf("csvflush: flush data files in %q at snapshot %d: %w", w.dir, s.Seq, err)
	}
	w.flushes++
	if s.TimeNs > w.lastT {
		w.lastT = s.TimeNs
	}
	return nil
}

// Close flushes and closes metrics.csv and histograms.csv, then writes run.csv
// last as the completion marker (withheld if any prior export failed).
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.mCSV.Flush()
	w.hCSV.Flush()
	if err := errors.Join(w.mCSV.Error(), w.hCSV.Error(), w.mFile.Close(), w.hFile.Close()); err != nil {
		return fmt.Errorf("csvflush: close data files in %q: %w", w.dir, err)
	}
	if w.failed {
		return nil
	}

	// Publish the completion marker only after its contents are fully written.
	// Use the ordinary CSV creation mode so the process umask also applies
	// to the renamed marker when other users read this directory.
	stage := filepath.Join(w.dir, ".run-"+rand.Text()+".csv")
	rf, err := os.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		return fmt.Errorf("csvflush: prepare completion marker in %q: %w", w.dir, err)
	}
	defer os.Remove(rf.Name())
	rw := csv.NewWriter(rf)
	rec := []string{
		strconv.Itoa(SchemaVersion),
		w.run.RunID,
		w.run.Scenario,
		strconv.FormatInt(w.run.Seed, 10),
		w.run.Instance,
		strconv.FormatInt(w.flushes, 10),
		strconv.FormatInt(w.lastT, 10),
	}
	if err := errors.Join(rw.WriteAll([][]string{runHeader, rec}), rf.Close()); err != nil {
		return fmt.Errorf("csvflush: write completion marker in %q: %w", w.dir, err)
	}
	if err := os.Rename(rf.Name(), filepath.Join(w.dir, "run.csv")); err != nil {
		return fmt.Errorf("csvflush: publish completion marker in %q: %w", w.dir, err)
	}
	return nil
}

// MetricRow is one decoded row from metrics.csv.
type MetricRow struct {
	RunID     string
	FlushSeq  int64
	TimeNs    int64
	Instance  string
	Component string
	Metric    string
	Kind      string
	Unit      string
	Labels    map[string]string
	Value     int64
}

// HistRow is one decoded row from histograms.csv.
type HistRow struct {
	RunID     string
	FlushSeq  int64
	TimeNs    int64
	Instance  string
	Component string
	Metric    string
	Unit      string
	Labels    map[string]string
	BucketLo  int64
	BucketHi  int64
	Count     int64
}

// RunData holds all parsed CSV tables from a completed run directory.
type RunData struct {
	Run        RunInfo
	Flushes    int64
	DurationNs int64
	Metrics    []MetricRow
	Histograms []HistRow
}

// Read parses and validates a completed run directory written by Writer.
// Returns ErrIncomplete if run.csv is missing.
func Read(dir string) (RunData, error) {
	var out RunData
	runRows, err := readCSV(filepath.Join(dir, "run.csv"), runHeader)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, ErrIncomplete
		}
		return out, err
	}
	if len(runRows) != 1 {
		return out, fmt.Errorf("csvflush: expected 1 row in run.csv, got %d", len(runRows))
	}
	r := runRows[0]
	if r[0] != strconv.Itoa(SchemaVersion) {
		return out, fmt.Errorf("csvflush: unsupported schema_version %q", r[0])
	}
	seed, err := strconv.ParseInt(r[3], 10, 64)
	if err != nil {
		return out, fmt.Errorf("csvflush: decode run.csv seed: %w", err)
	}
	flushes, err := strconv.ParseInt(r[5], 10, 64)
	if err != nil {
		return out, fmt.Errorf("csvflush: decode run.csv flushes: %w", err)
	}
	durNs, err := strconv.ParseInt(r[6], 10, 64)
	if err != nil {
		return out, fmt.Errorf("csvflush: decode run.csv duration_ns: %w", err)
	}
	out.Run = RunInfo{RunID: r[1], Scenario: r[2], Seed: seed, Instance: r[4]}
	out.Flushes = flushes
	out.DurationNs = durNs

	mRows, err := readCSV(filepath.Join(dir, "metrics.csv"), metricsHeader)
	if err != nil {
		return out, err
	}
	for i, row := range mRows {
		if row[0] != strconv.Itoa(SchemaVersion) {
			return out, fmt.Errorf("csvflush: metrics.csv record %d has unsupported schema_version %q", i+1, row[0])
		}
		seq, err := strconv.ParseInt(row[2], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode metrics.csv flush_seq: %w", err)
		}
		tNs, err := strconv.ParseInt(row[3], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode metrics.csv t_ns: %w", err)
		}
		val, err := strconv.ParseInt(row[10], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode metrics.csv value: %w", err)
		}
		labels, err := decodeLabelsJSON(row[9])
		if err != nil {
			return out, fmt.Errorf("csvflush: decode metrics.csv labels_json: %w", err)
		}
		out.Metrics = append(out.Metrics, MetricRow{
			RunID:     row[1],
			FlushSeq:  seq,
			TimeNs:    tNs,
			Instance:  row[4],
			Component: row[5],
			Metric:    row[6],
			Kind:      row[7],
			Unit:      row[8],
			Labels:    labels,
			Value:     val,
		})
	}

	hRows, err := readCSV(filepath.Join(dir, "histograms.csv"), histHeader)
	if err != nil {
		return out, err
	}
	for i, row := range hRows {
		if row[0] != strconv.Itoa(SchemaVersion) {
			return out, fmt.Errorf("csvflush: histograms.csv record %d has unsupported schema_version %q", i+1, row[0])
		}
		seq, err := strconv.ParseInt(row[2], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode histograms.csv flush_seq: %w", err)
		}
		tNs, err := strconv.ParseInt(row[3], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode histograms.csv t_ns: %w", err)
		}
		lo, err := strconv.ParseInt(row[9], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode histograms.csv bucket_lo: %w", err)
		}
		hi, err := strconv.ParseInt(row[10], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode histograms.csv bucket_hi: %w", err)
		}
		cnt, err := strconv.ParseInt(row[11], 10, 64)
		if err != nil {
			return out, fmt.Errorf("csvflush: decode histograms.csv count: %w", err)
		}
		labels, err := decodeLabelsJSON(row[8])
		if err != nil {
			return out, fmt.Errorf("csvflush: decode histograms.csv labels_json: %w", err)
		}
		out.Histograms = append(out.Histograms, HistRow{
			RunID:     row[1],
			FlushSeq:  seq,
			TimeNs:    tNs,
			Instance:  row[4],
			Component: row[5],
			Metric:    row[6],
			Unit:      row[7],
			Labels:    labels,
			BucketLo:  lo,
			BucketHi:  hi,
			Count:     cnt,
		})
	}
	return out, nil
}

func readCSV(path string, expectedHeader []string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cr := csv.NewReader(f)
	hdr, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("csvflush: read %q header: %w", path, err)
	}
	if len(hdr) != len(expectedHeader) {
		return nil, fmt.Errorf("csvflush: %s header mismatch: got %v", path, hdr)
	}
	for i := range hdr {
		if hdr[i] != expectedHeader[i] {
			return nil, fmt.Errorf("csvflush: %s header column %d mismatch: got %q want %q", path, i, hdr[i], expectedHeader[i])
		}
	}
	var rows [][]string
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("csvflush: read %q record: %w", path, err)
		}
		rows = append(rows, rec)
	}
}

func encodeLabelsJSON(labels []obs.Label) (string, error) {
	if len(labels) == 0 {
		return "{}", nil
	}
	// Deterministic JSON object in already-sorted label order.
	m := make(map[string]string, len(labels))
	for _, l := range labels {
		m[l.Key] = l.Value
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeLabelsJSON(raw string) (map[string]string, error) {
	if raw == "" || raw == "{}" {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}
