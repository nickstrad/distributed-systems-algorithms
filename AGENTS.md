# Agent Instructions (`AGENTS.md`)

`CLAUDE.md` is symlinked to this file. Keep all instructions here so every coding agent follows the same rules.

---

## 1. Documentation Hygiene (Always Keep Up to Date)

Whenever you add, rename, or change packages, algorithm suites, JSON configs, `Makefile` targets, or `dsasim` CLI commands/flags, **you MUST update the documentation in the same change**:

1. **`README.md`:** Keep the project overview, setup steps, `Makefile` command table, `dsasim` CLI walkthrough, and repository package/test map strictly in sync with reality.
2. **`docs/future-design.md`:** Update architectural decisions, Build-vs-Buy notes, and the domain package roadmap when new packages or runtime adapters land.
3. **Story Headers on Test Files:** Every new `*_test.go` file MUST begin with a `Story:` doc comment above `package ...`:
   ```go
   // Story: <1-sentence title>
   //
   // What these tests prove: ...
   // How they probe: ...
   // What they do not prove: ...
   ```
4. **Narrative `Story` + `VERDICT` Checks on Suites:** Every new `scenario.Suite` registered in `scenario/` MUST populate `SuiteOutcome.Story` (`Proves`, `HowCollected`, `DoesNotProve`, and explicit `Checks` rows) and ship a starter config file under `configs/<suite>.json`.

---

## 2. Architecture & Dependency Boundaries

- **Domain-named packages only:** Name packages by distributed-systems abstraction (`env`, `obs`, `sim`, `job`, `link`, `detector`, `broadcast`, `register`, `consensus`, `membership`, `replication`, `scenario`, `runtime`), never by textbook chapter numbers.
- **Strict dependency rules:**
  - Core packages (`env/`, `obs/`, `sim/`, `job/`, and future algorithm packages) MUST use **only the Go standard library** (no third-party imports).
  - `pgregory.net/rapid` is allowed **only in `*_test.go` files**.
  - `github.com/spf13/cobra` is allowed **only in `cmd/dsasim/`**.
  - Production third-party libraries (e.g., `google.golang.org/grpc`, `go.etcd.io/bbolt`) belong strictly in `runtime/` when added—never inside algorithm packages.
  - Never import a library that replaces the distributed algorithm being studied (no `hashicorp/raft`, `hashicorp/memberlist`, NATS, or etcd elections).
- **Determinism inside core & simulator packages (`env/`, `obs/`, `sim/`, `job/`, and algorithm packages):**
  - Never call `time.Now`, global `math/rand`, `os`, `net`, `sync`, goroutines (`go`), or `select` over multiple channels.
  - Never depend on Go map iteration order; sort keys before iterating or hashing.
  - Use `env.Dispatcher.Enqueue` for local module indications (mutually exclusive, FIFO, non-reentrant) and keep the backing `env.FIFO[T]` swappable.
  - Keep `obs` hot paths (`Counter.Inc`, `Gauge.Add`, `Histogram.Record`) integer-only and strictly 0 heap allocations.

---

## 3. Standard Verification Gate

Before finishing any code change, run:

```sh
make check          # fmt-check + go vet + go test -race -cover + build + all configs/*.json suites
make test-prop      # all rapid property tests under -race
```

When adding or changing a suite, also inspect its narrative report:

```sh
make story SUITE=<name>   # or CONFIG=configs/<name>.json
```
