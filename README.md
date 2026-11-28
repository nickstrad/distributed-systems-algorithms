# Distributed Systems Algorithms (Go)

A Go-based distributed systems project for implementing, simulating, probing, and verifying fundamental distributed algorithms—from reliable links, failure detectors, broadcast, and shared registers to consensus, group membership, and replication.

Protocol algorithms are written as deterministic, single-threaded state machines against a narrow standard-library runtime contract (`env/`). The exact same algorithm code runs inside a seed-reproducible discrete-event simulator (`sim/`) and can be hosted on real cloud VMs (DigitalOcean, AWS) behind production transport and storage adapters (`runtime/`, planned).

---

## Project Setup

### Prerequisites
- **Go 1.26+** (`go version`)
- **`make`** (optional, for convenience targets)
- **Git**

Core packages (`env/`, `obs/`, `sim/`, `job/`, and future algorithm packages) use **only the Go standard library**. Third-party dependencies are strictly isolated:
- `pgregory.net/rapid` — test files (`*_test.go`) only
- `github.com/spf13/cobra` — CLI (`cmd/dsasim/`) only

### Quickstart
```sh
# 1. Verify Go toolchain
go version

# 2. Run the full local check (formatting, go vet, race + coverage tests, build, all config suites, and CLI end-to-end)
make check

# 3. Build the CLI binary into bin/dsasim
make build

# 4. Run a suite in readable narrative mode (what it proves, metrics, quantiles, VERDICT table)
make story CONFIG=configs/sim-ping.json
```

---

## Important Makefile Commands

Run `make help` at any time to list every target.

| Command | What it does |
| :--- | :--- |
| `make check` | Full local verification gate: `fmt-check`, `vet`, `test-race`, `build`, `test-all-suites`, and `test-e2e`. |
| `make build` | Compiles the `dsasim` CLI binary to `bin/dsasim`. |
| `make test` | Runs the entire Go test suite (`go test -count=1 ./...`). |
| `make test-race` | Runs all unit, property, and simulation tests with `-race` and `-cover`. |
| `make test-unit` | Runs fast unit tests (`go test -short -count=1 ./...`). |
| `make test-prop` | Runs all `pgregory.net/rapid` property tests (`-run '^TestProp'`). |
| `make test-suite` | Builds `bin/dsasim` and runs one suite with `--check-determinism` (`SUITE=job-buffered` or `CONFIG=configs/sim-ping.json`). |
| `make test-e2e` | Runs the built CLI with the buffered-job config and checks the CSV report, Prometheus output, and unknown-suite failure. |
| `make test-all-suites` | Runs every JSON config under `configs/*.json` with `--check-determinism`. |
| `make story` | Runs a suite and prints its full narrative report + `VERDICT` table (`SUITE=job-buffered` or `CONFIG=configs/sim-ping.json`). |
| `make report CSV_DIR=<dir>` | Renders a narrative analysis report (metadata, final metrics, p50/p95/p99 quantiles) from a flushed CSV directory. |
| `make sweep SEEDS=50` | Runs multi-seed DST sweeps (`DST_SEEDS=50`). |
| `make fuzz PKG=./sim FUZZ=FuzzSimSchedule FUZZTIME=10s` | Runs a native Go coverage-guided fuzz target. |
| `make fmt` / `make vet` | Formats all Go files with `gofmt` / runs `go vet ./...`. |
| `make clean` | Removes `bin/` and `test-artifacts/`. |

---

## Using the `dsasim` CLI

`bin/dsasim` (built via `make build`, or invoked directly via `go run ./cmd/dsasim`) drives algorithm suites, per-suite JSON config files, observability exports, and Go tests.

### 1. List Registered Suites
```sh
./bin/dsasim list
```

### 2. Generate a Starter Config File
Each suite defines strongly-typed algorithm parameters inside the `"params"` block of a JSON config file (see [configs/](configs/)).
```sh
./bin/dsasim config init sim-ping -o configs/my-experiment.json
```

### 3. Run a Suite (Config File + Flag Overrides)
CLI flags override only the fields explicitly passed on the command line:
```sh
# Concise single-line output
./bin/dsasim run --config configs/job-buffered.json --seed 7

# Full narrative story + VERDICT table + deterministic replay check
./bin/dsasim run --config configs/sim-ping.json --story --check-determinism

# Swap the node local queue from slice FIFO to bounded channel FIFO and print trace
./bin/dsasim run job-buffered --fifo chan --chan-cap 64 --trace

# Multi-seed sweep with CSV and Prometheus 0.0.4 exports
./bin/dsasim run --config configs/sim-ping.json --seed 1 --seeds 5 --check-determinism \
  --csv-dir test-artifacts/sim-ping --prom-file test-artifacts/sim-ping.prom
```

If a requested metrics export fails, the command exits unsuccessfully and reports the completed simulation coordinates. If any invariant, verdict check, or determinism replay fails, `dsasim run` prints the trace tail and a copy-pasteable `REPLAY:` command to reproduce the exact seed.

### 4. Analyze Flushed CSV Runs Offline
```sh
./bin/dsasim report test-artifacts/sim-ping/sim-ping-seed-1
```
Each CSV run directory contains `metrics.csv`, `histograms.csv`, and (written last on clean close) `run.csv`, ready for both `dsasim report` and DuckDB SQL queries.

### 5. Drive Go Tests from the CLI
```sh
./bin/dsasim test --prop                      # run all rapid property tests
./bin/dsasim test --seeds 50 ./job ./sim      # 50-seed sweep
./bin/dsasim test --seed 7 ./job ./env        # single-seed replay via DST_SEED
./bin/dsasim test --fuzz FuzzSimSchedule --fuzztime 15s ./sim
```

---

## Learning by Probing: The Story Approach

Every suite and test file is designed to be read and probed as a self-contained lab:
- **Suites (`--story`)** print *What This Run Proves*, *How the Data Was Collected*, *Observed Metrics* (with plain-English meanings), *Distributions* (`p50`/`p95`/`p99`), an explicit `VERDICT` table (`CLAIM | OBSERVED | EXPECTED | PASS/FAIL`), *What This Run Does Not Prove*, and a `REPLAY` command.
- **Test files (`*_test.go`)** open with a `Story:` header explaining what they prove, how they probe the package, and what lies outside their scope.

---

## Repository Map

| Path | Responsibility | Key Tests |
| :--- | :--- | :--- |
| [env/](env/) | Minimal node runtime contract: `NodeID`, `InstanceID`, sorted `Cluster`, saturating `Time`, `Dispatcher`, swappable `FIFO[T]` (`SliceFIFO` / `ChanFIFO`), `Clock`, `RawTransport`, `Store`, `Envelope`, `KeyRing` (SHA-256 / HMAC / Ed25519), wire `CodeTable` / `*RemoteError`, and deterministic `Logger`. | [env_test.go](env/env_test.go), [errors_log_test.go](env/errors_log_test.go), [fuzz_test.go](env/fuzz_test.go) |
| [sim/](sim/) | Single-threaded deterministic simulator: indexed `(at, seq)` min-heap, isolated PCG streams, pluggable per-node `env.FIFO`, `SimNet` (latency, drops, duplicates, asymmetric partitions, GST `Stabilize`), dynamic `AddNode`/`AddCluster`, `Crash`/`Recover` with persistent `Store`, continuous `AddInvariant` checks, livelock/panic guards, SHA-256 `Digest()`, and `MetricsSource()`. | [sim_test.go](sim/sim_test.go), [queue_test.go](sim/queue_test.go), [prop_test.go](sim/prop_test.go) |
| [sim/simtest/](sim/simtest/) | Deterministic test harness (`-seed`, `DST_SEED`, `DST_SEEDS`, `Sweep`, `RequireDeterministic`, failure diagnostics with `REPLAY:`). | [simtest_test.go](sim/simtest/simtest_test.go) |
| [obs/](obs/) | Zero-dependency, zero-alloc integer observability spine (`Counter`, `Gauge`, log-linear `Histogram`, `Collector`) + adapters: [obs/csvflush](obs/csvflush/), [obs/promtext](obs/promtext/) (Prometheus `0.0.4` + `/metrics` `http.Handler`), [obs/logline](obs/logline/). | [obs_test.go](obs/obs_test.go), [csvflush_test.go](obs/csvflush/csvflush_test.go), [promtext_test.go](obs/promtext/promtext_test.go), [logline_test.go](obs/logline/logline_test.go) |
| [job/](job/) | Composable request/indication components (`SyncHandler`, `AsyncHandler`, bounded `BufferedTransformer`) proving FIFO non-reentrancy, queue swappability, and `obs.Source` metrics. | [job_test.go](job/job_test.go), [prop_test.go](job/prop_test.go) |
| [scenario/](scenario/) | Pluggable algorithm suite registry, strict JSON config loader, narrative `Story` + `VERDICT` renderer, and offline CSV report generator. | [scenario_test.go](scenario/scenario_test.go) |
| [cmd/dsasim/](cmd/dsasim/) | Cobra CLI (`list`, `run`, `report`, `config init`, `test`). | [main_test.go](cmd/dsasim/main_test.go) |
| [configs/](configs/) | Starter JSON configs per algorithm suite (`job-buffered.json`, `job-buffered-async.json`, `sim-ping.json`). | Exercised by `make test-all-suites` |
| [docs/](docs/) | [future-design.md](docs/future-design.md) (domain package roadmap & Build-vs-Buy matrix) and [foundation-plan.md](docs/foundation-plan.md). | — |


## Developing the next algorithm

Start with the [Chapter 2 implementation checklist](docs/next-link.md).
Use the [Go conventions](docs/go-conventions.md) for errors, logs and function contracts,
the [testing strategy](docs/testing-strategy.md) to choose evidence for each guarantee,
and the [module observability walkthrough](docs/module-observability.md) to trace a measurement from a state transition to a query.
