// Command dsasim is the deterministic simulator and test runner CLI for the
// distributed-systems-algorithms repository.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/nickstrad/distributed-systems-algorithms/scenario"
)

func main() {
	cmd := newRootCmd(scenario.DefaultRegistry(), os.Stdout, os.Stderr)
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCmd(reg *scenario.Registry, stdout, stderr io.Writer) *cobra.Command {
	if reg == nil {
		reg = scenario.DefaultRegistry()
	}
	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}

	root := &cobra.Command{
		Use:           "dsasim",
		Short:         "Deterministic simulator and test runner for distributed algorithms",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(stdout)
	root.SetErr(stderr)

	root.AddCommand(
		newListCmd(reg),
		newRunCmd(reg),
		newReportCmd(),
		newConfigCmd(reg),
		newTestCmd(),
	)
	return root
}

func newListCmd(reg *scenario.Registry) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List registered simulation suites",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			for _, s := range reg.List() {
				fmt.Fprintf(tw, "%s\t%s\n", s.Name, s.Description)
			}
			return tw.Flush()
		},
	}
}

type runFlags struct {
	configPath       string
	seed             int64
	seeds            int
	fifo             string
	chanCap          int
	logEnabled       bool
	logLevel         string
	csvDir           string
	promFile         string
	trace            bool
	story            bool
	checkDeterminism bool
}

func newRunCmd(reg *scenario.Registry) *cobra.Command {
	var f runFlags

	cmd := &cobra.Command{
		Use:   "run [suite]",
		Short: "Execute one or more deterministic seeds of a simulation suite",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := resolveRunConfig(cmd, args, f)
			if err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			errOut := cmd.ErrOrStderr()

			var suiteArg string
			if len(args) == 1 {
				suiteArg = args[0]
			}
			for i := 0; i < cfg.Seeds; i++ {
				seed := cfg.Seed + int64(i)
				replay := buildReplayCmd(cfg, f.configPath, suiteArg, seed)
				res, err := reg.Execute(cfg, seed)
				if err != nil {
					if f.story && len(res.Story.Checks) > 0 {
						fmt.Fprint(errOut, scenario.FormatStory(res, replay))
					}
					printFailure(errOut, cfg, f.configPath, suiteArg, seed, res, err)
					return err
				}
				if f.story {
					fmt.Fprint(out, scenario.FormatStory(res, replay))
					if cfg.Trace && len(res.Trace) > 0 {
						fmt.Fprintln(out, "\nTRACE TAIL")
						for _, te := range res.Trace {
							fmt.Fprintf(out, "  %s\n", te)
						}
					}
				} else {
					printSuccess(out, res, cfg.Trace)
				}
			}
			return nil
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&f.configPath, "config", "c", "", "path to a JSON RunConfig file")
	flags.Int64Var(&f.seed, "seed", 1, "starting deterministic seed")
	flags.IntVar(&f.seeds, "seeds", 1, "number of sequential seeds to run starting at --seed")
	flags.StringVar(&f.fifo, "fifo", "slice", "local queue implementation (slice or chan)")
	flags.IntVar(&f.chanCap, "chan-cap", 512, "channel capacity when --fifo=chan")
	flags.BoolVar(&f.logEnabled, "log", false, "enable deterministic node logging into trace")
	flags.StringVar(&f.logLevel, "log-level", "info", "node log level (debug|info|warn|error)")
	flags.StringVar(&f.csvDir, "csv-dir", "", "directory to flush schema-v1 CSV files")
	flags.StringVar(&f.promFile, "prom-file", "", "file path to write Prometheus 0.0.4 text exposition")
	flags.BoolVar(&f.trace, "trace", false, "print trailing trace entries")
	flags.BoolVar(&f.story, "story", false, "print full narrative report (what it proves, metrics, quantiles, VERDICT table)")
	flags.BoolVar(&f.checkDeterminism, "check-determinism", false, "run each seed twice and verify identical digest, steps, and virtual time")

	return cmd
}

func resolveRunConfig(cmd *cobra.Command, args []string, f runFlags) (scenario.RunConfig, error) {
	cfg := scenario.DefaultRunConfig("")
	if f.configPath != "" {
		loaded, err := scenario.LoadConfig(f.configPath)
		if err != nil {
			return scenario.RunConfig{}, err
		}
		cfg = loaded
	}

	flags := cmd.Flags()
	if flags.Changed("seed") {
		cfg.Seed = f.seed
	}
	if flags.Changed("seeds") {
		cfg.Seeds = f.seeds
	}
	if flags.Changed("fifo") {
		cfg.FIFO = f.fifo
	}
	if flags.Changed("chan-cap") {
		cfg.ChanCap = f.chanCap
	}
	if flags.Changed("log") {
		cfg.LogEnabled = f.logEnabled
	}
	if flags.Changed("log-level") {
		cfg.LogLevel = f.logLevel
	}
	if flags.Changed("csv-dir") {
		cfg.CSVDir = f.csvDir
	}
	if flags.Changed("prom-file") {
		cfg.PromFile = f.promFile
	}
	if flags.Changed("trace") {
		cfg.Trace = f.trace
	}
	if flags.Changed("check-determinism") {
		cfg.CheckDeterminism = f.checkDeterminism
	}

	if len(args) == 1 && args[0] != "" {
		cfg.Suite = args[0]
	}
	if cfg.Suite == "" {
		return scenario.RunConfig{}, errors.New("dsasim run: suite name is required (provide [suite] or --config)")
	}
	return cfg, nil
}

func printSuccess(out io.Writer, res scenario.RunResult, printTrace bool) {
	var b strings.Builder
	fmt.Fprintf(&b, "suite=%s seed=%d steps=%d time=%s digest=%s",
		res.Suite, res.Seed, res.Steps, res.Now, res.Digest)
	if res.Summary != "" {
		b.WriteByte(' ')
		b.WriteString(res.Summary)
	}
	if res.CSVDir != "" {
		b.WriteString(" csv=")
		b.WriteString(res.CSVDir)
	}
	if res.PromFile != "" {
		b.WriteString(" prom=")
		b.WriteString(res.PromFile)
	}
	fmt.Fprintln(out, b.String())
	if printTrace && len(res.Trace) > 0 {
		for _, te := range res.Trace {
			fmt.Fprintf(out, "  %s\n", te)
		}
	}
}

func printFailure(errOut io.Writer, cfg scenario.RunConfig, configPath, suiteArg string, seed int64, res scenario.RunResult, err error) {
	fmt.Fprintf(errOut, "FAIL suite=%s seed=%d", cfg.Suite, seed)
	if res.Steps > 0 || res.Digest != "" {
		fmt.Fprintf(errOut, " steps=%d time=%s digest=%s", res.Steps, res.Now, res.Digest)
	}
	fmt.Fprintf(errOut, ": %v\n", err)
	if len(res.Trace) > 0 {
		fmt.Fprintln(errOut, "TRACE TAIL:")
		for _, te := range res.Trace {
			fmt.Fprintf(errOut, "  %s\n", te)
		}
	}
	fmt.Fprintf(errOut, "REPLAY: %s\n", buildReplayCmd(cfg, configPath, suiteArg, seed))
}

func buildReplayCmd(cfg scenario.RunConfig, configPath, suiteArg string, seed int64) string {
	parts := []string{"dsasim", "run"}
	if configPath != "" {
		parts = append(parts, "--config", shellQuote(configPath))
	}
	if suiteArg != "" {
		parts = append(parts, shellQuote(suiteArg))
	} else if configPath == "" && cfg.Suite != "" {
		parts = append(parts, shellQuote(cfg.Suite))
	}
	if cfg.FIFO != "" && !strings.EqualFold(cfg.FIFO, "slice") {
		parts = append(parts, "--fifo", shellQuote(cfg.FIFO))
		if strings.EqualFold(cfg.FIFO, "chan") {
			parts = append(parts, "--chan-cap", strconv.Itoa(cfg.ChanCap))
		}
	}
	if cfg.LogEnabled {
		parts = append(parts, "--log")
	}
	if cfg.LogLevel != "" && !strings.EqualFold(cfg.LogLevel, "info") {
		parts = append(parts, "--log-level", shellQuote(cfg.LogLevel))
	}
	if cfg.CheckDeterminism {
		parts = append(parts, "--check-determinism")
	}
	if cfg.Trace {
		parts = append(parts, "--trace")
	}
	parts = append(parts, "--seed", strconv.FormatInt(seed, 10))
	return strings.Join(parts, " ")
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '/' || c == ':' {
			continue
		}
		return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
	}
	return s
}

func newReportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "report <csv-dir>",
		Short: "Render a narrative analysis report from a flushed CSV run directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			report, err := scenario.FormatCSVReport(args[0])
			if err != nil {
				return err
			}
			_, err = io.WriteString(cmd.OutOrStdout(), report)
			return err
		},
	}
}

func newConfigCmd(reg *scenario.Registry) *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Inspect and generate simulation configuration files",
	}

	var outPath string
	initCmd := &cobra.Command{
		Use:   "init <suite>",
		Short: "Print starter JSON config for a suite",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := reg.DefaultConfigJSON(args[0])
			if err != nil {
				return err
			}
			data = append(data, '\n')
			if outPath != "" {
				if dir := filepath.Dir(outPath); dir != "" && dir != "." {
					if err := os.MkdirAll(dir, 0o755); err != nil {
						return err
					}
				}
				return os.WriteFile(outPath, data, 0o644)
			}
			_, err = cmd.OutOrStdout().Write(data)
			return err
		},
	}
	initCmd.Flags().StringVarP(&outPath, "out", "o", "", "write JSON config to file path")

	configCmd.AddCommand(initCmd)
	return configCmd
}

type testFlags struct {
	race     bool
	cover    bool
	short    bool
	prop     bool
	run      string
	seeds    int
	seed     int64
	hasSeed  bool
	fuzz     string
	fuzztime string
}

func newTestCmd() *cobra.Command {
	var f testFlags

	cmd := &cobra.Command{
		Use:   "test [packages...]",
		Short: "Run go test with deterministic property and fuzzing flags",
		RunE: func(cmd *cobra.Command, args []string) error {
			f.hasSeed = cmd.Flags().Changed("seed")
			testArgs, env := buildGoTestSpec(f, args, os.Environ())
			execCmd := exec.Command("go", testArgs...)
			execCmd.Env = env
			execCmd.Stdout = cmd.OutOrStdout()
			execCmd.Stderr = cmd.ErrOrStderr()
			return execCmd.Run()
		},
	}

	flags := cmd.Flags()
	flags.BoolVar(&f.race, "race", true, "enable Go race detector")
	flags.BoolVar(&f.cover, "cover", false, "enable coverage analysis")
	flags.BoolVar(&f.short, "short", false, "pass -short to go test")
	flags.BoolVar(&f.prop, "prop", false, "run property tests (-run ^TestProp)")
	flags.StringVar(&f.run, "run", "", "pass -run pattern to go test")
	flags.IntVar(&f.seeds, "seeds", 0, "set DST_SEEDS=<n> in environment when > 0")
	flags.Int64Var(&f.seed, "seed", 0, "set DST_SEED=<n> in environment when explicitly provided")
	flags.StringVar(&f.fuzz, "fuzz", "", "pass -fuzz pattern to go test")
	flags.StringVar(&f.fuzztime, "fuzztime", "10s", "pass -fuzztime duration when --fuzz is set")

	return cmd
}

func buildGoTestSpec(f testFlags, pkgs, baseEnv []string) ([]string, []string) {
	args := []string{"test", "-count=1"}
	if f.race {
		args = append(args, "-race")
	}
	if f.cover {
		args = append(args, "-cover")
	}
	if f.short {
		args = append(args, "-short")
	}
	if f.run != "" {
		args = append(args, "-run", f.run)
	} else if f.prop {
		args = append(args, "-run", "^TestProp")
	}
	if f.fuzz != "" {
		args = append(args, "-fuzz", f.fuzz)
		if f.fuzztime != "" {
			args = append(args, "-fuzztime", f.fuzztime)
		}
	}
	if len(pkgs) == 0 {
		args = append(args, "./...")
	} else {
		args = append(args, pkgs...)
	}

	env := slices.Clone(baseEnv)
	if f.seeds > 0 {
		env = setEnvVar(env, "DST_SEEDS", strconv.Itoa(f.seeds))
	}
	if f.hasSeed {
		env = setEnvVar(env, "DST_SEED", strconv.FormatInt(f.seed, 10))
	}
	return args, env
}

func setEnvVar(env []string, key, value string) []string {
	prefix := key + "="
	entry := prefix + value
	for i, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			env[i] = entry
			return env
		}
	}
	return append(env, entry)
}
