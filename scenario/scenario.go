// Package scenario provides a pluggable registry and JSON configuration loader
// for running deterministic algorithm suites from the dsasim CLI or Go tests.
//
// Each new algorithm package registers a Suite with a strongly-typed default
// params struct and a Run function, keeping algorithm-specific settings cleanly
// separated inside JSON config files while sharing common simulator, seed,
// FIFO, logging, CSV, and Prometheus controls.
package scenario

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/obs"
	"github.com/nickstrad/distributed-systems-algorithms/obs/csvflush"
	"github.com/nickstrad/distributed-systems-algorithms/obs/logline"
	"github.com/nickstrad/distributed-systems-algorithms/obs/promtext"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

// NetJSONConfig is the JSON-friendly representation of sim.NetConfig.
type NetJSONConfig struct {
	MinLatencyMs  int64 `json:"min_latency_ms"`
	MaxLatencyMs  int64 `json:"max_latency_ms"`
	DropPPM       int   `json:"drop_ppm"`
	DupPPM        int   `json:"dup_ppm"`
	MaxDuplicates int   `json:"max_duplicates,omitempty"`
}

// ToSimNet converts NetJSONConfig into a sim.NetConfig.
func (n NetJSONConfig) ToSimNet() sim.NetConfig {
	return sim.NetConfig{
		MinLatency:    time.Duration(n.MinLatencyMs) * time.Millisecond,
		MaxLatency:    time.Duration(n.MaxLatencyMs) * time.Millisecond,
		DropPPM:       n.DropPPM,
		DupPPM:        n.DupPPM,
		MaxDuplicates: n.MaxDuplicates,
	}
}

// RunConfig holds common execution, simulator, and observability settings
// shared by every algorithm suite, plus raw JSON Params decoded per suite.
type RunConfig struct {
	Suite            string          `json:"suite"`
	Seed             int64           `json:"seed"`
	Seeds            int             `json:"seeds"`
	FIFO             string          `json:"fifo"`     // "slice" (default) or "chan"
	ChanCap          int             `json:"chan_cap"` // buffer capacity when fifo == "chan"
	LogEnabled       bool            `json:"log_enabled"`
	LogLevel         string          `json:"log_level"` // debug|info|warn|error
	Net              NetJSONConfig   `json:"net"`
	CSVDir           string          `json:"csv_dir,omitempty"`
	PromFile         string          `json:"prom_file,omitempty"`
	Trace            bool            `json:"trace,omitempty"`
	CheckDeterminism bool            `json:"check_determinism,omitempty"`
	Params           json.RawMessage `json:"params,omitempty"`
}

// DefaultRunConfig returns sensible defaults for a suite run.
func DefaultRunConfig(suite string) RunConfig {
	return RunConfig{
		Suite:    suite,
		Seed:     1,
		Seeds:    1,
		FIFO:     "slice",
		ChanCap:  512,
		LogLevel: "info",
		Net: NetJSONConfig{
			MinLatencyMs: 1,
			MaxLatencyMs: 5,
		},
	}
}

// LoadConfig reads and strictly validates a JSON RunConfig file (rejecting
// unknown top-level fields) merged over DefaultRunConfig.
func LoadConfig(path string) (RunConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RunConfig{}, err
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return RunConfig{}, fmt.Errorf("scenario: parse config %s: expected JSON object", path)
	}
	cfg := DefaultRunConfig("")
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return RunConfig{}, fmt.Errorf("scenario: parse config %s: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return RunConfig{}, fmt.Errorf("scenario: parse config %s: trailing data after JSON object", path)
	}
	if err := cfg.Validate(); err != nil {
		return RunConfig{}, fmt.Errorf("scenario: validate config %s: %w", path, err)
	}
	return cfg, nil
}

// Validate checks common RunConfig fields.
func (c RunConfig) Validate() error {
	if c.Seeds < 1 {
		return fmt.Errorf("scenario: seeds must be >= 1, got %d", c.Seeds)
	}
	switch strings.ToLower(c.FIFO) {
	case "", "slice":
	case "chan":
		if c.ChanCap <= 0 {
			return fmt.Errorf("scenario: chan_cap must be > 0 when fifo=chan, got %d", c.ChanCap)
		}
	default:
		return fmt.Errorf("scenario: unknown fifo %q (want slice or chan)", c.FIFO)
	}
	if _, err := ParseLogLevel(c.LogLevel); err != nil {
		return err
	}
	return nil
}

// ParseLogLevel parses a case-insensitive log level string.
func ParseLogLevel(s string) (env.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return env.LevelInfo, nil
	case "debug":
		return env.LevelDebug, nil
	case "warn", "warning":
		return env.LevelWarn, nil
	case "error":
		return env.LevelError, nil
	default:
		return env.LevelInfo, fmt.Errorf("scenario: unknown log_level %q (want debug, info, warn, error)", s)
	}
}

// SimConfig builds a sim.Config for the given seed from RunConfig.
func (c RunConfig) SimConfig(seed int64) (sim.Config, error) {
	lvl, err := ParseLogLevel(c.LogLevel)
	if err != nil {
		return sim.Config{}, err
	}
	var newFIFO func() env.FIFO[func()]
	if strings.EqualFold(c.FIFO, "chan") {
		capVal := c.ChanCap
		if capVal <= 0 {
			capVal = 512
		}
		newFIFO = func() env.FIFO[func()] { return env.NewChanFIFO[func()](capVal) }
	}
	return sim.Config{
		Seed:       seed,
		Net:        c.Net.ToSimNet(),
		LogEnabled: c.LogEnabled,
		LogLevel:   lvl,
		NewFIFO:    newFIFO,
	}, nil
}

// DecodeParams unmarshals raw JSON params into dst (which must be a pointer
// pre-populated with defaults), rejecting unknown fields.
func DecodeParams(raw json.RawMessage, dst any) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("scenario: decode params: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("scenario: decode params: trailing data after JSON object")
	}
	return nil
}

// RunResult summarizes one executed seed of a Suite.
type RunResult struct {
	Suite    string
	Seed     int64
	Steps    int64
	Now      env.Time
	Digest   string
	Summary  string
	Story    Story
	LogLine  string
	Snapshot obs.Snapshot
	Trace    []sim.TraceEntry
	CSVDir   string
	PromFile string
}

// SuiteOutcome is returned by a Suite.Run function with the simulator, metrics
// collector, concise summary, and human-readable Story + Verdict checks.
type SuiteOutcome struct {
	Sim       *sim.Sim
	Collector *obs.Collector
	Summary   string
	Story     Story
}

// Suite describes one runnable algorithm or protocol test suite.
type Suite struct {
	Name          string
	Description   string
	DefaultParams func() any
	Run           func(cfg RunConfig, seed int64, rawParams json.RawMessage) (SuiteOutcome, error)
}

// Registry stores registered algorithm suites keyed by unique Name.
type Registry struct {
	suites map[string]Suite
}

// NewRegistry creates an empty Suite Registry.
func NewRegistry() *Registry {
	return &Registry{suites: make(map[string]Suite)}
}

// Register adds a Suite to the registry, panicking on empty or duplicate names.
func (r *Registry) Register(s Suite) {
	if s.Name == "" || s.Run == nil {
		panic("scenario: suite requires non-empty Name and non-nil Run")
	}
	if _, exists := r.suites[s.Name]; exists {
		panic("scenario: duplicate suite " + s.Name)
	}
	r.suites[s.Name] = s
}

// Get retrieves a Suite by name.
func (r *Registry) Get(name string) (Suite, bool) {
	s, ok := r.suites[name]
	return s, ok
}

// List returns all registered suites sorted by Name.
func (r *Registry) List() []Suite {
	out := make([]Suite, 0, len(r.suites))
	for _, s := range r.suites {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Suite) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return out
}

// DefaultConfigJSON renders a starter JSON RunConfig for the named suite.
func (r *Registry) DefaultConfigJSON(name string) ([]byte, error) {
	s, ok := r.Get(name)
	if !ok {
		return nil, fmt.Errorf("scenario: unknown suite %q", name)
	}
	cfg := DefaultRunConfig(s.Name)
	if s.DefaultParams != nil {
		raw, err := json.Marshal(s.DefaultParams())
		if err != nil {
			return nil, err
		}
		cfg.Params = raw
	}
	return json.MarshalIndent(cfg, "", "  ")
}

// Execute runs cfg for a single seed, optionally double-running to verify
// bit-for-bit determinism and exporting CSV/Prometheus outputs.
// It preserves the simulation diagnostics available on a run failure and
// includes each output path only after the corresponding export succeeds.
func (r *Registry) Execute(cfg RunConfig, seed int64) (RunResult, error) {
	if err := cfg.Validate(); err != nil {
		return RunResult{}, err
	}
	suite, ok := r.Get(cfg.Suite)
	if !ok {
		return RunResult{}, fmt.Errorf("scenario: unknown suite %q", cfg.Suite)
	}

	out, err := suite.Run(cfg, seed, cfg.Params)
	s, col, summary, story := out.Sim, out.Collector, out.Summary, out.Story
	if err != nil {
		return buildResult(suite.Name, seed, s, summary, story, "", obs.Snapshot{}, "", ""), fmt.Errorf("scenario: run suite %q: %w", suite.Name, err)
	}
	if col == nil {
		col = obs.NewCollector(suite.Name)
		col.Register(s.MetricsSource())
	}

	promAdapter := promtext.New("dsa_", nil)
	col.AddAdapter(promAdapter)
	llAdapter := logline.New(nil)
	col.AddAdapter(llAdapter)

	snap := col.Publish(int64(s.Now()))
	res := buildResult(suite.Name, seed, s, summary, story, strings.TrimSpace(string(llAdapter.Last())), snap, "", "")

	if cfg.CSVDir != "" {
		csvOutDir := filepath.Join(cfg.CSVDir, fmt.Sprintf("%s-seed-%d", suite.Name, seed))
		csvWriter, err := csvflush.New(csvOutDir, csvflush.RunInfo{
			RunID:    fmt.Sprintf("%s/%d", suite.Name, seed),
			Scenario: suite.Name,
			Seed:     seed,
			Instance: suite.Name,
		})
		if err != nil {
			return res, fmt.Errorf("scenario: open CSV export %s: %w", csvOutDir, err)
		}
		// The collector counts adapter errors instead of returning them. Export
		// explicitly because the caller requested a persisted run.
		exportErr := csvWriter.Export(snap)
		if err := errors.Join(exportErr, csvWriter.Close()); err != nil {
			return res, fmt.Errorf("scenario: export CSV to %s: %w", csvOutDir, err)
		}
		res.CSVDir = csvOutDir
	}

	if cfg.PromFile != "" {
		promOutPath := cfg.PromFile
		if cfg.Seeds > 1 {
			ext := filepath.Ext(promOutPath)
			base := strings.TrimSuffix(promOutPath, ext)
			promOutPath = fmt.Sprintf("%s-seed-%d%s", base, seed, ext)
		}
		if err := os.MkdirAll(filepath.Dir(promOutPath), 0o755); err != nil {
			return res, fmt.Errorf("scenario: create Prometheus export directory: %w", err)
		}
		if err := os.WriteFile(promOutPath, promAdapter.Last(), 0o644); err != nil {
			return res, fmt.Errorf("scenario: write Prometheus export: %w", err)
		}
		res.PromFile = promOutPath
	}

	if cfg.CheckDeterminism {
		out2, err2 := suite.Run(cfg, seed, cfg.Params)
		if err2 != nil {
			return res, fmt.Errorf("scenario: determinism replay failed: %w", err2)
		}
		s2 := out2.Sim
		detOK := s.Digest() == s2.Digest() && s.Steps() == s2.Steps() && s.Now() == s2.Now()
		res.Story.Checks = append(slices.Clone(story.Checks), Check{
			Claim:    "Deterministic replay (double-run digest/steps/time match)",
			Observed: fmt.Sprintf("steps=%d time=%s digest=%s", s2.Steps(), s2.Now(), shortDigest(s2.Digest())),
			Expected: fmt.Sprintf("steps=%d time=%s digest=%s", s.Steps(), s.Now(), shortDigest(s.Digest())),
			Pass:     detOK,
		})
		if !detOK {
			return res, errors.New("scenario: nondeterministic replay detected (digest/steps/time mismatch)")
		}
	}

	if !res.Story.Passed() {
		return res, errors.New("scenario: one or more verdict checks failed")
	}
	return res, nil
}

func buildResult(suite string, seed int64, s *sim.Sim, summary string, story Story, ll string, snap obs.Snapshot, csvDir, promFile string) RunResult {
	res := RunResult{
		Suite:    suite,
		Seed:     seed,
		Summary:  summary,
		Story:    story,
		LogLine:  ll,
		Snapshot: snap,
		CSVDir:   csvDir,
		PromFile: promFile,
	}
	if s != nil {
		res.Steps = s.Steps()
		res.Now = s.Now()
		res.Digest = s.Digest()
		res.Trace = s.TraceTail(64)
	}
	return res
}

// DefaultRegistry returns a Registry pre-populated with all built-in suites.
func DefaultRegistry() *Registry {
	reg := NewRegistry()
	registerBuiltins(reg)
	return reg
}
