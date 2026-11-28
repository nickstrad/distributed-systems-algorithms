// Story: Deterministic test-harness plumbing (seed selection, sweeps, replay,
// and failure reports).
//
// What these tests prove: Seeds obeys -seed > DST_SEED > DST_SEEDS precedence
// (including -seed=0), Sweep fans out per-seed subtests, RequireDeterministic
// catches nondeterminism and panics, and Report prints traces and REPLAY lines.
// How they probe: drives in-process sim.Sim runs and subprocess test helpers
// with varied flags and environment variables.
// What they do not prove: does not verify protocol-level safety invariants.
package simtest

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nickstrad/distributed-systems-algorithms/env"
	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

type pingProcess struct {
	env.BaseProcess
	e     env.Env
	count int
}

func newPingProcess(e env.Env) env.Process {
	return &pingProcess{e: e}
}

func (p *pingProcess) Init() {
	p.scheduleNext()
}

func (p *pingProcess) Recover() {
	p.scheduleNext()
}

func (p *pingProcess) scheduleNext() {
	delay := time.Duration(1+p.e.Rand.IntN(5)) * time.Millisecond
	p.e.Clock.AfterFunc(delay, func() {
		peers := p.e.Cluster.Peers
		if len(peers) == 0 {
			return
		}
		target := peers[p.e.Rand.IntN(len(peers))]
		p.count++
		payload := []byte{byte(p.count)}
		_ = p.e.Store.Store("sent", payload)
		_ = p.e.Net.Send(target, payload)
	})
}

func (p *pingProcess) OnMessage(from env.NodeID, data []byte) {
	if len(data) == 0 {
		return
	}
	_ = p.e.Store.Store("last_from_"+string(from), data)
	if p.e.Rand.IntN(3) == 0 {
		_ = p.e.Net.Send(from, data)
	}
}

func runDeterministicSim(seed int64) (*sim.Sim, error) {
	s := sim.New(sim.Config{
		Seed: seed,
		Net: sim.NetConfig{
			MinLatency:    1 * time.Millisecond,
			MaxLatency:    10 * time.Millisecond,
			DropPPM:       50_000,
			DupPPM:        50_000,
			MaxDuplicates: 2,
		},
	})
	peers := []env.NodeID{"n1", "n2", "n3"}
	if err := s.AddCluster(peers, newPingProcess); err != nil {
		return s, err
	}
	s.ScheduleAfter(4*time.Millisecond, func() {
		_ = s.Crash("n2")
	})
	s.ScheduleAfter(12*time.Millisecond, func() {
		_ = s.Recover("n2")
	})
	if err := s.RunFor(25 * time.Millisecond); err != nil {
		return s, err
	}
	if err := s.RunUntilQuiet(); err != nil {
		return s, err
	}
	return s, nil
}

func TestSeedsEnvAndDefault(t *testing.T) {
	if hasExplicitSeed(flag.CommandLine) {
		t.Skip("skipping env seed tests when -seed flag is explicitly passed")
	}

	t.Run("default", func(t *testing.T) {
		t.Setenv("DST_SEED", "")
		t.Setenv("DST_SEEDS", "")
		got := Seeds(t)
		want := []int64{1, 2, 3, 4, 5}
		if !slices.Equal(got, want) {
			t.Fatalf("Seeds() = %v, want %v", got, want)
		}
	})

	t.Run("DST_SEED_positive", func(t *testing.T) {
		t.Setenv("DST_SEED", "42")
		t.Setenv("DST_SEEDS", "9")
		got := Seeds(t)
		want := []int64{42}
		if !slices.Equal(got, want) {
			t.Fatalf("Seeds() = %v, want %v", got, want)
		}
	})

	t.Run("DST_SEED_zero", func(t *testing.T) {
		t.Setenv("DST_SEED", "0")
		got := Seeds(t)
		want := []int64{0}
		if !slices.Equal(got, want) {
			t.Fatalf("Seeds() = %v, want %v", got, want)
		}
	})

	t.Run("DST_SEED_negative", func(t *testing.T) {
		t.Setenv("DST_SEED", "-7")
		got := Seeds(t)
		want := []int64{-7}
		if !slices.Equal(got, want) {
			t.Fatalf("Seeds() = %v, want %v", got, want)
		}
	})

	t.Run("DST_SEEDS_custom", func(t *testing.T) {
		t.Setenv("DST_SEED", "")
		t.Setenv("DST_SEEDS", "3")
		got := Seeds(t)
		want := []int64{1, 2, 3}
		if !slices.Equal(got, want) {
			t.Fatalf("Seeds() = %v, want %v", got, want)
		}
	})
}

func TestSeedsFlagPrecedence(t *testing.T) {
	t.Run("explicit_zero_overrides_env", func(t *testing.T) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		val := fs.Int64("seed", 0, "seed")
		if err := fs.Parse([]string{"-seed=0"}); err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		got := resolveSeeds(t, fs, *val, func(k string) string {
			if k == "DST_SEED" {
				return "99"
			}
			return "10"
		})
		if !slices.Equal(got, []int64{0}) {
			t.Fatalf("resolveSeeds() = %v, want [0]", got)
		}
	})

	t.Run("explicit_nonzero", func(t *testing.T) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		val := fs.Int64("seed", 0, "seed")
		if err := fs.Parse([]string{"-seed", "123"}); err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		got := resolveSeeds(t, fs, *val, func(string) string { return "" })
		if !slices.Equal(got, []int64{123}) {
			t.Fatalf("resolveSeeds() = %v, want [123]", got)
		}
	})

	t.Run("unset_flag_falls_back_to_env", func(t *testing.T) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		val := fs.Int64("seed", 0, "seed")
		if err := fs.Parse(nil); err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		got := resolveSeeds(t, fs, *val, func(k string) string {
			if k == "DST_SEEDS" {
				return "2"
			}
			return ""
		})
		if !slices.Equal(got, []int64{1, 2}) {
			t.Fatalf("resolveSeeds() = %v, want [1 2]", got)
		}
	})
}

func TestSweep(t *testing.T) {
	if hasExplicitSeed(flag.CommandLine) {
		t.Skip("skipping env sweep test when -seed flag is explicitly passed")
	}

	t.Setenv("DST_SEED", "")
	t.Setenv("DST_SEEDS", "3")

	var seen []int64
	var names []string
	Sweep(t, func(t *testing.T, seed int64) {
		seen = append(seen, seed)
		names = append(names, t.Name())
	})

	wantSeeds := []int64{1, 2, 3}
	if !slices.Equal(seen, wantSeeds) {
		t.Fatalf("Sweep visited seeds %v, want %v", seen, wantSeeds)
	}
	wantNames := []string{
		"TestSweep/seed=1",
		"TestSweep/seed=2",
		"TestSweep/seed=3",
	}
	if !slices.Equal(names, wantNames) {
		t.Fatalf("Sweep subtest names %v, want %v", names, wantNames)
	}
}

func TestRequireDeterministic(t *testing.T) {
	t.Setenv("DST_SEED", "")
	t.Setenv("DST_SEEDS", "3")

	Sweep(t, func(t *testing.T, seed int64) {
		s := RequireDeterministic(t, seed, runDeterministicSim)
		if s == nil {
			t.Fatal("RequireDeterministic returned nil")
		}
		if s.Seed() != seed {
			t.Fatalf("s.Seed() = %d, want %d", s.Seed(), seed)
		}
		if s.Steps() == 0 {
			t.Fatalf("expected Steps() > 0, got 0")
		}
		if s.Now() == 0 {
			t.Fatalf("expected Now() > 0, got 0")
		}
		if s.Digest() == "" {
			t.Fatal("expected non-empty Digest()")
		}
	})
}

func TestReportFormatting(t *testing.T) {
	s := sim.New(sim.Config{Seed: 77})
	for i := 1; i <= 55; i++ {
		s.Trace("custom", fmt.Sprintf("entry-%02d", i))
	}
	s.ScheduleAfter(5*time.Millisecond, func() {
		panic("boom")
	})
	err := s.RunUntilQuiet()
	if err == nil {
		t.Fatal("expected PanicError from RunUntilQuiet")
	}
	wrapped := fmt.Errorf("wrapped: %w", err)

	out := formatReport("TestExample/seed=77", s, wrapped)

	checks := []string{
		"seed=77",
		fmt.Sprintf("steps=%d", s.Steps()),
		fmt.Sprintf("now=%s", s.Now()),
		fmt.Sprintf("digest=%s", s.Digest()),
		"err=wrapped: sim: panic",
		fmt.Sprintf("trace (showing 40 of %d):", s.TraceLen()),
		"entry-55",
		"panic stack:",
		"REPLAY: DST_SEED=77 go test ./... -run '^TestExample$/^seed=77$' -count=1",
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q\nfull report:\n%s", want, out)
		}
	}
	if strings.Contains(out, "entry-01") {
		t.Errorf("report should only include last 40 trace entries, unexpectedly found entry-01\n%s", out)
	}

	// Verify Report logs cleanly without panicking.
	Report(t, s, wrapped)

	quoted := formatReport("TestExample/can't", s, wrapped)
	if !strings.Contains(quoted, `-run '^TestExample$/^can'"'"'t$'`) {
		t.Errorf("report did not shell-quote subtest name:\n%s", quoted)
	}
}

func TestReportNilSim(t *testing.T) {
	pe := &sim.PanicError{
		Value: "nil-sim-boom",
		Stack: []byte("goroutine 1 [running]:\nmain.go:1\n"),
		Seed:  19,
		Step:  3,
		At:    100,
	}
	out := formatReport("TestNilSim", nil, pe)
	for _, want := range []string{
		"seed=19",
		"sim=<nil>",
		"panic stack:",
		"REPLAY: DST_SEED=19 go test ./... -run '^TestNilSim$' -count=1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("nil-sim report missing %q\n%s", want, out)
		}
	}

	ie := &sim.InvariantError{
		Name: "safety",
		Err:  errors.New("broken"),
		Seed: 23,
	}
	outIE := formatReport("", nil, ie)
	if !strings.Contains(outIE, "seed=23") || !strings.Contains(outIE, "REPLAY: DST_SEED=23 go test ./... -run '.' -count=1") {
		t.Errorf("unexpected nil-sim InvariantError report:\n%s", outIE)
	}
}

func TestSubprocessCLIAndFailurePaths(t *testing.T) {
	if os.Getenv("SIMTEST_HELPER") != "" {
		return
	}

	runHelper := func(t *testing.T, mode string, extraArgs ...string) (string, error) {
		t.Helper()
		args := append([]string{"-test.v", "-test.run=^TestHelperModes$"}, extraArgs...)
		cmd := exec.Command(os.Args[0], args...)
		cmd.Env = append(os.Environ(), "SIMTEST_HELPER="+mode)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	t.Run("flag_seed_zero", func(t *testing.T) {
		out, err := runHelper(t, "seed_zero", "-seed=0")
		if err != nil {
			t.Fatalf("expected helper to pass with -seed=0, got %v\n%s", err, out)
		}
		if !strings.Contains(out, "SEEDS=[0]") {
			t.Fatalf("expected SEEDS=[0] in output, got:\n%s", out)
		}
	})

	t.Run("invalid_DST_SEED", func(t *testing.T) {
		out, err := runHelper(t, "invalid_dst_seed")
		if err == nil {
			t.Fatalf("expected helper to fail on invalid DST_SEED\n%s", out)
		}
		if !strings.Contains(out, "invalid DST_SEED") {
			t.Fatalf("expected invalid DST_SEED error, got:\n%s", out)
		}
	})

	t.Run("invalid_DST_SEEDS_zero", func(t *testing.T) {
		out, err := runHelper(t, "zero_dst_seeds")
		if err == nil {
			t.Fatalf("expected helper to fail on DST_SEEDS=0\n%s", out)
		}
		if !strings.Contains(out, "DST_SEEDS=0 must be > 0") {
			t.Fatalf("expected DST_SEEDS=0 error, got:\n%s", out)
		}
	})

	t.Run("invalid_DST_SEEDS_nonint", func(t *testing.T) {
		out, err := runHelper(t, "bad_dst_seeds")
		if err == nil {
			t.Fatalf("expected helper to fail on bad DST_SEEDS\n%s", out)
		}
		if !strings.Contains(out, "invalid DST_SEEDS") {
			t.Fatalf("expected invalid DST_SEEDS error, got:\n%s", out)
		}
	})

	t.Run("require_deterministic_nondeterministic", func(t *testing.T) {
		out, err := runHelper(t, "nondet")
		if err == nil {
			t.Fatalf("expected helper to fail on nondeterministic replay\n%s", out)
		}
		for _, want := range []string{
			"nondeterministic replay for seed=11",
			"REPLAY: DST_SEED=11 go test ./... -run '^TestHelperModes$' -count=1",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected %q in output, got:\n%s", want, out)
			}
		}
	})

	t.Run("require_deterministic_panic_error", func(t *testing.T) {
		out, err := runHelper(t, "panic_err")
		if err == nil {
			t.Fatalf("expected helper to fail on PanicError\n%s", out)
		}
		for _, want := range []string{
			"panic stack:",
			"sim: panic",
			"REPLAY: DST_SEED=13 go test ./... -run '^TestHelperModes$' -count=1",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected %q in output, got:\n%s", want, out)
			}
		}
	})
}

func TestHelperModes(t *testing.T) {
	mode := os.Getenv("SIMTEST_HELPER")
	if mode == "" {
		return
	}
	switch mode {
	case "seed_zero":
		t.Setenv("DST_SEED", "99")
		t.Setenv("DST_SEEDS", "5")
		t.Logf("SEEDS=%v", Seeds(t))
	case "invalid_dst_seed":
		t.Setenv("DST_SEED", "not-an-int")
		_ = Seeds(t)
	case "zero_dst_seeds":
		t.Setenv("DST_SEED", "")
		t.Setenv("DST_SEEDS", "0")
		_ = Seeds(t)
	case "bad_dst_seeds":
		t.Setenv("DST_SEED", "")
		t.Setenv("DST_SEEDS", "abc")
		_ = Seeds(t)
	case "nondet":
		var call int
		RequireDeterministic(t, 11, func(seed int64) (*sim.Sim, error) {
			call++
			s := sim.New(sim.Config{Seed: seed})
			s.Trace("run", fmt.Sprintf("call=%d", call))
			return s, nil
		})
	case "panic_err":
		RequireDeterministic(t, 13, func(seed int64) (*sim.Sim, error) {
			s := sim.New(sim.Config{Seed: seed})
			s.ScheduleAfter(time.Millisecond, func() {
				panic("helper panic")
			})
			return s, s.RunUntilQuiet()
		})
	default:
		t.Fatalf("unknown SIMTEST_HELPER=%q", mode)
	}
}
