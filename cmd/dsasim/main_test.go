// Story: End-to-end verification of the dsasim Cobra CLI.
//
// What these tests prove: list, config init, run (with JSON configs, flag
// overrides, --story narrative verdicts, CSV/Prometheus export, and REPLAY on
// failure), report (offline CSV analysis), and test flag construction all behave
// deterministically and fail loudly on bad inputs.
// How they probe: invokes newRootCmd in-process with captured stdout/stderr and
// temporary output directories, reading back written CSV and Prometheus files.
// What they do not prove: does not spawn real cloud VMs or scrape a live
// Prometheus server.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nickstrad/distributed-systems-algorithms/obs/csvflush"
	"github.com/nickstrad/distributed-systems-algorithms/scenario"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

func runCLI(t *testing.T, reg *scenario.Registry, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := newRootCmd(reg, &stdout, &stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func TestListCommand(t *testing.T) {
	out, errOut, err := runCLI(t, scenario.DefaultRegistry(), "list")
	if err != nil {
		t.Fatalf("list failed: %v (stderr: %s)", err, errOut)
	}
	if !strings.Contains(out, "job-buffered") {
		t.Fatalf("expected job-buffered in list output, got:\n%s", out)
	}
	if !strings.Contains(out, "sim-ping") {
		t.Fatalf("expected sim-ping in list output, got:\n%s", out)
	}
	idxJob := strings.Index(out, "job-buffered")
	idxPing := strings.Index(out, "sim-ping")
	if idxJob > idxPing {
		t.Fatalf("expected suites sorted by name, got:\n%s", out)
	}
}

func TestConfigInitCommand(t *testing.T) {
	reg := scenario.DefaultRegistry()

	t.Run("stdout", func(t *testing.T) {
		out, errOut, err := runCLI(t, reg, "config", "init", "job-buffered")
		if err != nil {
			t.Fatalf("config init failed: %v (stderr: %s)", err, errOut)
		}
		if !strings.HasSuffix(out, "\n") {
			t.Fatalf("expected trailing newline in config init output")
		}
		cfgPath := filepath.Join(t.TempDir(), "job-buffered.json")
		if err := os.WriteFile(cfgPath, []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
		cfg, err := scenario.LoadConfig(cfgPath)
		if err != nil {
			t.Fatalf("scenario.LoadConfig failed on generated JSON: %v", err)
		}
		if cfg.Suite != "job-buffered" {
			t.Fatalf("got suite %q, want job-buffered", cfg.Suite)
		}
	})

	t.Run("out_file", func(t *testing.T) {
		cfgPath := filepath.Join(t.TempDir(), "nested", "sim-ping.json")
		out, errOut, err := runCLI(t, reg, "config", "init", "sim-ping", "-o", cfgPath)
		if err != nil {
			t.Fatalf("config init -o failed: %v (stderr: %s)", err, errOut)
		}
		if out != "" {
			t.Fatalf("expected empty stdout when -o is used, got %q", out)
		}
		cfg, err := scenario.LoadConfig(cfgPath)
		if err != nil {
			t.Fatalf("scenario.LoadConfig failed on -o file: %v", err)
		}
		if cfg.Suite != "sim-ping" {
			t.Fatalf("got suite %q, want sim-ping", cfg.Suite)
		}
	})

	t.Run("unknown_suite", func(t *testing.T) {
		_, _, err := runCLI(t, reg, "config", "init", "does-not-exist")
		if err == nil {
			t.Fatal("expected error for unknown suite")
		}
	})
}

func TestRunWithConfigAndSeedOverride(t *testing.T) {
	reg := scenario.DefaultRegistry()
	cfgPath := filepath.Join("..", "..", "configs", "job-buffered.json")

	out, errOut, err := runCLI(t, reg, "run", "--config", cfgPath)
	if err != nil {
		t.Fatalf("run with config failed: %v (stderr: %s)", err, errOut)
	}
	for _, want := range []string{"suite=job-buffered", "seed=1", "steps=", "time=", "digest="} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in stdout, got:\n%s", want, out)
		}
	}

	out, errOut, err = runCLI(t, reg, "run", "--config", cfgPath, "--seed", "7")
	if err != nil {
		t.Fatalf("run with --seed 7 failed: %v (stderr: %s)", err, errOut)
	}
	if !strings.Contains(out, "seed=7") {
		t.Fatalf("expected --seed 7 override in stdout, got:\n%s", out)
	}
}

func TestRunSimPingExportsAndDeterminism(t *testing.T) {
	reg := scenario.DefaultRegistry()
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	promPath := filepath.Join(tmp, "metrics.prom")

	out, errOut, err := runCLI(t, reg,
		"run", "sim-ping",
		"--seed", "3",
		"--seeds", "2",
		"--check-determinism",
		"--csv-dir", csvDir,
		"--prom-file", promPath,
	)
	if err != nil {
		t.Fatalf("run sim-ping failed: %v (stderr: %s)", err, errOut)
	}

	for _, want := range []string{"suite=sim-ping", "seed=3", "seed=4", "csv=", "prom="} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in stdout, got:\n%s", want, out)
		}
	}

	for _, seed := range []int64{3, 4} {
		seedCSVDir := filepath.Join(csvDir, "sim-ping-seed-"+string(rune('0'+seed)))
		rd, err := csvflush.Read(seedCSVDir)
		if err != nil {
			t.Fatalf("csvflush.Read(%s) failed: %v", seedCSVDir, err)
		}
		if rd.Run.Scenario != "sim-ping" || rd.Run.Seed != seed {
			t.Fatalf("unexpected CSV run metadata: %+v", rd.Run)
		}
		if len(rd.Metrics) == 0 {
			t.Fatalf("expected non-empty CSV metrics for seed %d", seed)
		}

		seedPromPath := filepath.Join(tmp, "metrics-seed-"+string(rune('0'+seed))+".prom")
		promBytes, err := os.ReadFile(seedPromPath)
		if err != nil {
			t.Fatalf("read prom file %s failed: %v", seedPromPath, err)
		}
		if len(promBytes) == 0 || !strings.Contains(string(promBytes), "# TYPE") {
			t.Fatalf("expected non-empty Prometheus exposition with # TYPE in %s, got:\n%s", seedPromPath, string(promBytes))
		}
	}
}

func TestRunChanFIFOAndTrace(t *testing.T) {
	reg := scenario.DefaultRegistry()
	out, errOut, err := runCLI(t, reg,
		"run", "job-buffered",
		"--fifo", "chan",
		"--chan-cap", "64",
		"--log",
		"--log-level", "debug",
		"--trace",
	)
	if err != nil {
		t.Fatalf("run with chan fifo and trace failed: %v (stderr: %s)", err, errOut)
	}
	if !strings.Contains(out, "suite=job-buffered") {
		t.Fatalf("unexpected stdout:\n%s", out)
	}
	if !strings.Contains(out, "t=") {
		t.Fatalf("expected trace lines in stdout, got:\n%s", out)
	}
}

func TestRunErrorsAndReplay(t *testing.T) {
	t.Run("missing_suite", func(t *testing.T) {
		_, _, err := runCLI(t, scenario.DefaultRegistry(), "run")
		if err == nil {
			t.Fatal("expected error when suite is omitted")
		}
	})

	t.Run("unknown_suite", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cmd := newRootCmd(scenario.DefaultRegistry(), &stdout, &stderr)
		cmd.SetArgs([]string{"run", "unknown-suite"})
		if code := execute(cmd); code != 1 {
			t.Fatalf("execute returned %d, want status 1", code)
		}
		errOut := stderr.String()
		if count := strings.Count(errOut, `unknown suite "unknown-suite"`); count != 1 {
			t.Fatalf("expected one terminal error, got %d in stderr:\n%s", count, errOut)
		}
		if !strings.Contains(errOut, "REPLAY: dsasim run unknown-suite --seed 1") {
			t.Fatalf("expected REPLAY line in stderr, got:\n%s", errOut)
		}
	})

	t.Run("execution_failure_prints_trace_and_replay", func(t *testing.T) {
		reg := scenario.NewRegistry()
		violation := errors.New("unexpected event")
		reg.Register(scenario.Suite{
			Name:        "always-fail",
			Description: "test suite that fails after emitting a trace event",
			Run: func(cfg scenario.RunConfig, seed int64, _ json.RawMessage) (scenario.SuiteOutcome, error) {
				simCfg, err := cfg.SimConfig(seed)
				if err != nil {
					return scenario.SuiteOutcome{}, err
				}
				s := sim.New(simCfg)
				s.AddInvariant("event safety", func() error { return violation })
				s.ScheduleAfter(1, func() {})
				return scenario.SuiteOutcome{Sim: s}, s.RunUntilQuiet()
			},
		})

		_, errOut, err := runCLI(t, reg, "run", "always-fail", "--seed", "5", "--fifo", "chan", "--chan-cap", "32", "--log")
		var invariant *sim.InvariantError
		if !errors.As(err, &invariant) || !errors.Is(err, violation) || invariant.Seed != 5 || invariant.Step != 1 {
			t.Fatalf("run error = %v, want the invariant violation on seed 5 step 1", err)
		}
		for _, want := range []string{
			"FAIL suite=always-fail seed=5",
			"TRACE TAIL:",
			"REPLAY: dsasim run always-fail --fifo chan --chan-cap 32 --log --seed 5",
		} {
			if !strings.Contains(errOut, want) {
				t.Fatalf("expected %q in stderr, got:\n%s", want, errOut)
			}
		}
	})
}

func TestBuildGoTestSpec(t *testing.T) {
	args, env := buildGoTestSpec(testFlags{race: true, fuzztime: "10s"}, nil, []string{"PATH=/usr/bin"})
	wantArgs := []string{"test", "-count=1", "-race", "./..."}
	if !slices.Equal(args, wantArgs) {
		t.Fatalf("default args = %v, want %v", args, wantArgs)
	}
	if !slices.Equal(env, []string{"PATH=/usr/bin"}) {
		t.Fatalf("unexpected env: %v", env)
	}

	args, env = buildGoTestSpec(testFlags{
		race:     false,
		cover:    true,
		short:    true,
		prop:     true,
		seeds:    9,
		seed:     42,
		hasSeed:  true,
		fuzz:     "FuzzEnv",
		fuzztime: "5s",
	}, []string{"./env", "./sim"}, []string{"DST_SEEDS=1", "HOME=/tmp"})
	wantArgs = []string{"test", "-count=1", "-cover", "-short", "-run", "^TestProp", "-fuzz", "FuzzEnv", "-fuzztime", "5s", "./env", "./sim"}
	if !slices.Equal(args, wantArgs) {
		t.Fatalf("custom args = %v, want %v", args, wantArgs)
	}
	wantEnv := []string{"DST_SEEDS=9", "HOME=/tmp", "DST_SEED=42"}
	if !slices.Equal(env, wantEnv) {
		t.Fatalf("custom env = %v, want %v", env, wantEnv)
	}
}

func TestRunStoryAndReportCommands(t *testing.T) {
	reg := scenario.DefaultRegistry()
	csvRoot := filepath.Join(t.TempDir(), "csv")

	out, errOut, err := runCLI(t, reg,
		"run", "--config", filepath.Join("..", "..", "configs", "sim-ping.json"),
		"--story",
		"--csv-dir", csvRoot,
	)
	if err != nil {
		t.Fatalf("run --story failed: %v (stderr: %s)", err, errOut)
	}
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
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in --story output:\n%s", want, out)
		}
	}

	csvDir := filepath.Join(csvRoot, "sim-ping-seed-1")
	repOut, repErr, err := runCLI(t, reg, "report", csvDir)
	if err != nil {
		t.Fatalf("report failed: %v (stderr: %s)", err, repErr)
	}
	for _, want := range []string{
		"=== CSV ANALYSIS REPORT:",
		"RUN METADATA",
		"FINAL METRICS",
		"FINAL DISTRIBUTIONS",
		"sim.net_delivery_latency",
	} {
		if !strings.Contains(repOut, want) {
			t.Fatalf("expected %q in report output:\n%s", want, repOut)
		}
	}
}
