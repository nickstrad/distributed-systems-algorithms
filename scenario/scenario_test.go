// Story: Scenario registry, strict JSON config loading, narrative stories, and
// offline CSV analysis reports.
//
// What these tests prove: every configs/*.json file loads strictly (rejecting
// unknown fields and trailing data), executes deterministically, emits valid
// CSV/Prometheus files, catches nondeterminism and failing verdict checks, and
// renders complete narrative Story and CSV analysis reports.
// How they probe: runs built-in and synthetic suites through Registry.Execute,
// reads back flushed CSV and Prometheus outputs, and checks report sections.
// What they do not prove: does not invoke DuckDB or a live Prometheus scraper.
package scenario

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nickstrad/distributed-systems-algorithms/obs/csvflush"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

func TestLoadConfigAndValidation(t *testing.T) {
	reg := DefaultRegistry()

	configPaths, err := filepath.Glob(filepath.Join("..", "configs", "*.json"))
	if err != nil {
		t.Fatalf("glob configs: %v", err)
	}
	slices.Sort(configPaths)
	if len(configPaths) == 0 {
		t.Fatal("expected at least one config file under ../configs/*.json")
	}

	for _, path := range configPaths {
		t.Run("config/"+filepath.Base(path), func(t *testing.T) {
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig(%s): %v", path, err)
			}
			res, err := reg.Execute(cfg, cfg.Seed)
			if err != nil {
				t.Fatalf("Execute(%s): %v", path, err)
			}
			if res.Digest == "" {
				t.Fatalf("Execute(%s): expected non-empty Digest", path)
			}
			if res.Steps <= 0 {
				t.Fatalf("Execute(%s): expected positive Steps, got %d", path, res.Steps)
			}
		})
	}

	invalidConfigs := []struct {
		name string
		json string
	}{
		{
			name: "null_top_level",
			json: `null`,
		},
		{
			name: "trailing_config",
			json: `{"suite": "job-buffered"} {}`,
		},
		{
			name: "unknown_top_level_field",
			json: `{"suite": "job-buffered", "unexpected_field": true}`,
		},
		{
			name: "seeds_zero",
			json: `{"suite": "job-buffered", "seeds": 0}`,
		},
		{
			name: "seeds_negative",
			json: `{"suite": "job-buffered", "seeds": -2}`,
		},
		{
			name: "unknown_fifo",
			json: `{"suite": "job-buffered", "fifo": "ring"}`,
		},
		{
			name: "chan_fifo_zero_cap",
			json: `{"suite": "job-buffered", "fifo": "chan", "chan_cap": 0}`,
		},
		{
			name: "chan_fifo_negative_cap",
			json: `{"suite": "job-buffered", "fifo": "chan", "chan_cap": -1}`,
		},
		{
			name: "unknown_log_level",
			json: `{"suite": "job-buffered", "log_level": "verbose"}`,
		},
	}

	tmpDir := t.TempDir()
	for _, tc := range invalidConfigs {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			p := filepath.Join(tmpDir, tc.name+".json")
			if err := os.WriteFile(p, []byte(tc.json), 0o644); err != nil {
				t.Fatalf("write temp config: %v", err)
			}
			if _, err := LoadConfig(p); err == nil {
				t.Fatalf("LoadConfig(%s) succeeded, expected validation/parse error", tc.name)
			}
		})
	}

	t.Run("reject/unknown_params_field_decode_params", func(t *testing.T) {
		var p JobBufferedParams
		raw := json.RawMessage(`{"handler": "sync", "unknown_param": 42}`)
		if err := DecodeParams(raw, &p); err == nil {
			t.Fatal("DecodeParams succeeded with unknown field, expected error")
		}
	})

	t.Run("reject/unknown_params_field_execute", func(t *testing.T) {
		p := filepath.Join(tmpDir, "unknown_params.json")
		body := `{"suite": "job-buffered", "params": {"handler": "sync", "unknown_param": 42}}`
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write temp config: %v", err)
		}
		cfg, err := LoadConfig(p)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if _, err := reg.Execute(cfg, cfg.Seed); err == nil {
			t.Fatal("Execute succeeded with unknown params field, expected error")
		}
	})
}

func TestDefaultConfigJSONRoundTrip(t *testing.T) {
	reg := DefaultRegistry()
	suites := reg.List()
	if len(suites) == 0 {
		t.Fatal("DefaultRegistry().List() returned no suites")
	}

	tmpDir := t.TempDir()
	for _, s := range suites {
		t.Run(s.Name, func(t *testing.T) {
			data, err := reg.DefaultConfigJSON(s.Name)
			if err != nil {
				t.Fatalf("DefaultConfigJSON(%q): %v", s.Name, err)
			}
			cfgPath := filepath.Join(tmpDir, s.Name+".json")
			if err := os.WriteFile(cfgPath, data, 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}
			cfg, err := LoadConfig(cfgPath)
			if err != nil {
				t.Fatalf("LoadConfig(%s): %v", cfgPath, err)
			}
			cfg.CheckDeterminism = true

			res, err := reg.Execute(cfg, cfg.Seed)
			if err != nil {
				t.Fatalf("Execute(%q): %v", s.Name, err)
			}
			if res.Suite != s.Name {
				t.Fatalf("res.Suite = %q, want %q", res.Suite, s.Name)
			}
			if res.Seed != cfg.Seed {
				t.Fatalf("res.Seed = %d, want %d", res.Seed, cfg.Seed)
			}
			if res.Digest == "" {
				t.Fatalf("expected non-empty Digest for %q", s.Name)
			}
			if res.Steps <= 0 {
				t.Fatalf("expected positive Steps for %q, got %d", s.Name, res.Steps)
			}
		})
	}
}

func TestExecuteExportsCSVAndProm(t *testing.T) {
	reg := DefaultRegistry()

	cases := []struct {
		suite          string
		expectHistRows bool
	}{
		{suite: "job-buffered", expectHistRows: false},
		{suite: "sim-ping", expectHistRows: true},
	}

	for _, tc := range cases {
		t.Run(tc.suite, func(t *testing.T) {
			raw, err := reg.DefaultConfigJSON(tc.suite)
			if err != nil {
				t.Fatalf("DefaultConfigJSON(%q): %v", tc.suite, err)
			}
			var cfg RunConfig
			if err := json.Unmarshal(raw, &cfg); err != nil {
				t.Fatalf("unmarshal default config: %v", err)
			}
			tmpDir := t.TempDir()
			cfg.CSVDir = filepath.Join(tmpDir, "csv")
			cfg.PromFile = filepath.Join(tmpDir, "prom", tc.suite+".prom")

			res, err := reg.Execute(cfg, cfg.Seed)
			if err != nil {
				t.Fatalf("Execute(%q): %v", tc.suite, err)
			}
			if res.CSVDir == "" {
				t.Fatal("expected non-empty res.CSVDir")
			}
			if res.PromFile != cfg.PromFile {
				t.Fatalf("res.PromFile = %q, want %q", res.PromFile, cfg.PromFile)
			}

			csvData, err := csvflush.Read(res.CSVDir)
			if err != nil {
				t.Fatalf("csvflush.Read(%s): %v", res.CSVDir, err)
			}
			if len(csvData.Metrics) == 0 {
				t.Fatalf("expected non-empty CSV metrics for %q", tc.suite)
			}
			if tc.expectHistRows && len(csvData.Histograms) == 0 {
				t.Fatalf("expected non-empty CSV histograms for %q", tc.suite)
			}

			promBytes, err := os.ReadFile(res.PromFile)
			if err != nil {
				t.Fatalf("read prom file %s: %v", res.PromFile, err)
			}
			promText := string(promBytes)
			if !strings.Contains(promText, "# TYPE") {
				t.Fatalf("prom file missing '# TYPE':\n%s", promText)
			}
			if !strings.Contains(promText, "dsa_") {
				t.Fatalf("prom file missing 'dsa_' metrics:\n%s", promText)
			}
		})
	}

	t.Run("multi_seed_prom_suffix", func(t *testing.T) {
		tmpDir := t.TempDir()
		baseProm := filepath.Join(tmpDir, "out", "metrics.prom")
		cfg := DefaultRunConfig("job-buffered")
		cfg.Seeds = 3
		cfg.PromFile = baseProm

		for seed := int64(1); seed <= int64(cfg.Seeds); seed++ {
			res, err := reg.Execute(cfg, seed)
			if err != nil {
				t.Fatalf("Execute(seed=%d): %v", seed, err)
			}
			wantProm := filepath.Join(tmpDir, "out", fmt.Sprintf("metrics-seed-%d.prom", seed))
			if res.PromFile != wantProm {
				t.Fatalf("seed %d: res.PromFile = %q, want %q", seed, res.PromFile, wantProm)
			}
			promBytes, err := os.ReadFile(res.PromFile)
			if err != nil {
				t.Fatalf("read %s: %v", res.PromFile, err)
			}
			promText := string(promBytes)
			if !strings.Contains(promText, "# TYPE") || !strings.Contains(promText, "dsa_") {
				t.Fatalf("seed %d: unexpected prom output:\n%s", seed, promText)
			}
		}
	})
}

func TestExecuteDetectsNondeterminism(t *testing.T) {
	reg := NewRegistry()
	runCount := 0
	reg.Register(Suite{
		Name: "nondet-synthetic",
		Run: func(cfg RunConfig, seed int64, rawParams json.RawMessage) (SuiteOutcome, error) {
			runCount++
			simCfg, err := cfg.SimConfig(seed)
			if err != nil {
				return SuiteOutcome{}, err
			}
			s := sim.New(simCfg)
			s.Trace("synthetic", fmt.Sprintf("run=%d", runCount))
			if err := s.RunUntilQuiet(); err != nil {
				return SuiteOutcome{Sim: s}, err
			}
			return SuiteOutcome{Sim: s, Summary: fmt.Sprintf("run=%d", runCount)}, nil
		},
	})

	cfg := DefaultRunConfig("nondet-synthetic")
	cfg.CheckDeterminism = true

	res, err := reg.Execute(cfg, cfg.Seed)
	if err == nil {
		t.Fatal("expected Execute to fail when CheckDeterminism=true on nondeterministic suite")
	}
	if !strings.Contains(err.Error(), "nondeterministic") {
		t.Fatalf("expected nondeterministic error, got: %v", err)
	}
	if runCount != 2 {
		t.Fatalf("expected 2 runs for determinism check, got %d", runCount)
	}
	if res.Story.Passed() {
		t.Fatal("expected res.Story.Passed() == false on nondeterminism")
	}
}

func TestExecutePreservesRunOnCSVExportFailure(t *testing.T) {
	cfg := DefaultRunConfig("job-buffered")
	cfg.CSVDir = t.TempDir()
	csvDir := filepath.Join(cfg.CSVDir, "job-buffered-seed-1")
	// A non-empty directory blocks removal of the previous completion marker.
	marker := filepath.Join(csvDir, "run.csv")
	if err := os.MkdirAll(marker, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(marker, "keep"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := DefaultRegistry().Execute(cfg, cfg.Seed)
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || pathErr.Path != marker {
		t.Fatalf("Execute error = %v, want a path error for %s", err, marker)
	}
	if res.Steps == 0 || res.Digest == "" || len(res.Trace) == 0 || res.CSVDir != "" {
		t.Fatalf("Execute result has incomplete diagnostics or advertises failed export: steps=%d digest=%q trace entries=%d csv=%q",
			res.Steps, res.Digest, len(res.Trace), res.CSVDir)
	}
}

func TestStoryFormattingAndVerdictFailure(t *testing.T) {
	reg := DefaultRegistry()
	cfg := DefaultRunConfig("sim-ping")
	cfg.CheckDeterminism = true
	cfg.CSVDir = filepath.Join(t.TempDir(), "csv")

	res, err := reg.Execute(cfg, 1)
	if err != nil {
		t.Fatalf("Execute(sim-ping): %v", err)
	}
	storyText := FormatStory(res, "dsasim run sim-ping --seed 1 --story")
	for _, want := range []string{
		"=== STORY: sim-ping (seed=1) ===",
		"WHAT THIS RUN PROVES",
		"HOW THE DATA WAS COLLECTED",
		"OBSERVED METRICS",
		"DISTRIBUTIONS",
		"VERDICT",
		"OVERALL: PASS",
		"WHAT THIS RUN DOES NOT PROVE",
		"REPLAY",
	} {
		if !strings.Contains(storyText, want) {
			t.Fatalf("FormatStory missing %q:\n%s", want, storyText)
		}
	}

	csvReport, err := FormatCSVReport(res.CSVDir)
	if err != nil {
		t.Fatalf("FormatCSVReport(%s): %v", res.CSVDir, err)
	}
	for _, want := range []string{"RUN METADATA", "FINAL METRICS", "FINAL DISTRIBUTIONS", "sim.net_delivery_latency"} {
		if !strings.Contains(csvReport, want) {
			t.Fatalf("FormatCSVReport missing %q:\n%s", want, csvReport)
		}
	}

	// Failing Check inside Story must cause Execute to return an error while
	// preserving the rendered VERDICT table for inspection.
	badReg := NewRegistry()
	badReg.Register(Suite{
		Name: "failing-verdict",
		Run: func(cfg RunConfig, seed int64, _ json.RawMessage) (SuiteOutcome, error) {
			s := sim.New(sim.Config{Seed: seed})
			return SuiteOutcome{
				Sim: s,
				Story: Story{
					Proves: "demonstrates failing verdict handling",
					Checks: []Check{{Claim: "always true", Observed: "false", Expected: "true", Pass: false}},
				},
			}, nil
		},
	})
	badRes, err := badReg.Execute(DefaultRunConfig("failing-verdict"), 1)
	if err == nil {
		t.Fatal("expected error when a Story check fails")
	}
	if !strings.Contains(FormatStory(badRes, ""), "OVERALL: FAIL (0/1 checks passed)") {
		t.Fatalf("expected OVERALL: FAIL in formatted story:\n%s", FormatStory(badRes, ""))
	}
}

func TestRegistryErrorsAndPanics(t *testing.T) {
	reg := NewRegistry()
	dummyRun := func(cfg RunConfig, seed int64, rawParams json.RawMessage) (SuiteOutcome, error) {
		return SuiteOutcome{}, nil
	}

	if _, err := reg.Execute(DefaultRunConfig("missing-suite"), 1); err == nil {
		t.Fatal("expected error from Execute on unknown suite")
	}
	if _, err := reg.DefaultConfigJSON("missing-suite"); err == nil {
		t.Fatal("expected error from DefaultConfigJSON on unknown suite")
	}

	assertPanics := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic, got none", name)
			}
		}()
		fn()
	}

	assertPanics("empty_name", func() {
		reg.Register(Suite{Name: "", Run: dummyRun})
	})

	assertPanics("nil_run", func() {
		reg.Register(Suite{Name: "nil-run", Run: nil})
	})

	reg.Register(Suite{Name: "dup-suite", Run: dummyRun})
	assertPanics("duplicate_name", func() {
		reg.Register(Suite{Name: "dup-suite", Run: dummyRun})
	})
}
