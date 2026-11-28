// Package simtest provides deterministic seed management, sweeps, failure
// diagnostics, and replay verification helpers built on package sim.
package simtest

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/nickstrad/distributed-systems-algorithms/sim"
)

const (
	defaultSeeds     = 5
	reportTraceLimit = 40
)

var seedFlag = flag.Int64("seed", 0, "deterministic simulation seed (overrides DST_SEED and DST_SEEDS)")

// Seeds returns the list of seeds to run for a simulation test.
// Precedence: explicit -seed flag, DST_SEED env var, then 1..DST_SEEDS (default 5).
func Seeds(t *testing.T) []int64 {
	t.Helper()
	return resolveSeeds(t, flag.CommandLine, *seedFlag, os.Getenv)
}

func hasExplicitSeed(fs *flag.FlagSet) bool {
	if fs == nil {
		return false
	}
	var set bool
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "seed" {
			set = true
		}
	})
	return set
}

func resolveSeeds(t testing.TB, fs *flag.FlagSet, flagVal int64, getenv func(string) string) []int64 {
	t.Helper()
	if hasExplicitSeed(fs) {
		return []int64{flagVal}
	}
	if raw := getenv("DST_SEED"); raw != "" {
		seed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("simtest: invalid DST_SEED=%q: %v", raw, err)
		}
		return []int64{seed}
	}
	count := int64(defaultSeeds)
	if raw := getenv("DST_SEEDS"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("simtest: invalid DST_SEEDS=%q: %v", raw, err)
		}
		if n <= 0 {
			t.Fatalf("simtest: DST_SEEDS=%d must be > 0", n)
		}
		count = n
	}
	seeds := make([]int64, count)
	for i := range seeds {
		seeds[i] = int64(i + 1)
	}
	return seeds
}

// Sweep runs fn in a subtest named "seed=<seed>" for each seed from Seeds(t).
func Sweep(t *testing.T, fn func(t *testing.T, seed int64)) {
	t.Helper()
	for _, seed := range Seeds(t) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			fn(t, seed)
		})
	}
}

// Report formats simulator failure coordinates, the last 40 trace entries,
// any panic stack trace, and a copy-pasteable REPLAY command, logging via t.Log.
func Report(t *testing.T, s *sim.Sim, err error) {
	t.Helper()
	t.Log(formatReport(t.Name(), s, err))
}

func formatReport(testName string, s *sim.Sim, err error) string {
	var b strings.Builder
	var seed int64
	if s != nil {
		seed = s.Seed()
		fmt.Fprintf(&b, "sim failure: seed=%d steps=%d now=%s digest=%s err=%v\n",
			seed, s.Steps(), s.Now(), s.Digest(), err)
		tail := s.TraceTail(reportTraceLimit)
		fmt.Fprintf(&b, "trace (showing %d of %d):\n", len(tail), s.TraceLen())
		for _, entry := range tail {
			fmt.Fprintf(&b, "  %s\n", entry)
		}
	} else {
		var pe *sim.PanicError
		var ie *sim.InvariantError
		switch {
		case errors.As(err, &pe):
			seed = pe.Seed
		case errors.As(err, &ie):
			seed = ie.Seed
		}
		fmt.Fprintf(&b, "sim failure: seed=%d sim=<nil> err=%v\n", seed, err)
	}

	var pe *sim.PanicError
	if errors.As(err, &pe) && len(pe.Stack) > 0 {
		fmt.Fprintf(&b, "panic stack:\n%s\n", strings.TrimRight(string(pe.Stack), "\n"))
	}

	pattern := strings.ReplaceAll(replayRunPattern(testName), "'", "'\"'\"'")
	fmt.Fprintf(&b, "REPLAY: DST_SEED=%d go test ./... -run '%s' -count=1", seed, pattern)
	return b.String()
}

func replayRunPattern(testName string) string {
	if testName == "" {
		return "."
	}
	parts := strings.Split(testName, "/")
	for i, part := range parts {
		parts[i] = "^" + regexp.QuoteMeta(part) + "$"
	}
	return strings.Join(parts, "/")
}

// RequireDeterministic executes run(seed) twice and fails the test if either run
// errors or if their Digest, Steps, or Now diverge.
func RequireDeterministic(t *testing.T, seed int64, run func(seed int64) (*sim.Sim, error)) *sim.Sim {
	t.Helper()
	s1, err1 := run(seed)
	s2, err2 := run(seed)
	if err1 != nil || err2 != nil {
		if err1 != nil {
			Report(t, s1, err1)
		}
		if err2 != nil {
			Report(t, s2, err2)
		}
		t.Fatalf("simtest: run(%d) failed: err1=%v err2=%v", seed, err1, err2)
	}
	if s1 == nil || s2 == nil {
		t.Fatalf("simtest: run(%d) returned nil *sim.Sim (s1=%v s2=%v)", seed, s1, s2)
	}
	if s1.Digest() != s2.Digest() || s1.Steps() != s2.Steps() || s1.Now() != s2.Now() {
		Report(t, s1, nil)
		Report(t, s2, nil)
		t.Fatalf("nondeterministic replay for seed=%d: run1=(digest=%s steps=%d now=%s) run2=(digest=%s steps=%d now=%s)",
			seed, s1.Digest(), s1.Steps(), s1.Now(), s2.Digest(), s2.Steps(), s2.Now())
	}
	return s1
}
