# Testing strategy

Start with the public contract and write the smallest test that can observe each guarantee. Build up from examples to generated histories, then exercise the real protocol composition under controlled faults. Keep the CLI and future runtime tests focused on the boundaries they exercise. [Strata Go testing guide](https://github.com/nickstrad/strata/blob/651ca39b42ca4137a93d62589aa0904653a49dda/docs/coding-style/go-testing.md) · [Strata simulation testing guide](https://github.com/nickstrad/strata/blob/651ca39b42ca4137a93d62589aa0904653a49dda/docs/coding-style/simulation-testing.md)

| Test | What it exercises | Where to start |
| --- | --- | --- |
| Unit | One protocol contract with a small fake or a directly driven dispatcher. Use table cases for input partitions, boundary pairs, and fault responses. | [env tests](../env/env_test.go), [job tests](../job/job_test.go) |
| Property | Generated values or operation scripts checked against a small independent model or invariant. Use Rapid for structured generation and shrinking. Name tests `TestProp…` so `make test-prop` includes them. | [Job properties](../job/prop_test.go), [simulator properties](../sim/prop_test.go) |
| Fuzz | Coverage-guided exploration of byte inputs or encoded scripts. Seed parsers with valid and malformed examples, bound decoded work, and keep minimized failures in `testdata/fuzz/`. | [Envelope fuzz target](../env/fuzz_test.go), `FuzzSimSchedule` in [simulator properties](../sim/prop_test.go) |
| Integration | Real components across a boundary in one process. Check module wiring in simulated scenarios and real file parsing and export against temporary directories. For a future runtime adapter, run the same relevant interface contract against its fake and real implementation. | [Scenario tests](../scenario/scenario_test.go), [in-process CLI tests](../cmd/dsasim/main_test.go) |
| Deterministic simulation testing (DST) | Real protocols run together in one virtual-time simulator. Generate reproducible workloads and fault schedules, check safety through the run, then check progress under explicit recovery assumptions. | [Simulator](../sim/sim.go), [seed and replay helpers](../sim/simtest/simtest.go) |
| System or end-to-end | Exercise the built executable through its public flags, config files, output, and exit status. `make test-e2e` exercises the built CLI from config through CSV export and offline reporting. The config sweep builds and invokes `dsasim` for each starter config. Future multi-process tests can exercise real transport and restart wiring when runtime adapters exist. | [Makefile](../Makefile), [runtime roadmap](future-design.md) |

Use statement coverage to inspect missed behavior after deriving tests from the specification. For a retry branch, exercise both the triggering failure and recovery. For an oracle, feed it an intentionally invalid history or use a local mutation, such as disabling deduplication, and confirm that the relevant check fails. The race detector covers actual Go execution, while simulator faults exercise protocol behavior under virtual schedules.

## Build a deterministic simulation

1. Use fresh nodes, checks, and observations for every run. Use `env` interfaces for time, timers, dispatch, transport, and storage. Keep event order deterministic and use separate named simulator streams for workload choices and fault choices. Independently varying those choices helps isolate failures. [Environment interfaces](../env/env.go) · [Simulator streams](../sim/stream.go)
2. Record sends and application deliveries at their public boundaries. Record enough identity and payload data to recognize fabricated messages, duplicates, and sequence violations. Check safety after events with `AddInvariant` or an equivalent history assertion. Keep the checker independent of the implementation's deduplication or ordering state.
3. Exercise a named deterministic fault schedule before sweeping variable schedules. Verify that the fault affected traffic, such as a send dropped during a one-way partition or delivery attempts observed more than once. Run a fault phase and then a bounded heal phase using virtual time. State the delivery, retry, and process assumptions that make progress possible. Use `RunFor` or `RunUntil` for work that keeps scheduling retries.
4. Use `simtest.Sweep` to enumerate seeds and `simtest.RequireDeterministic` to run each seed twice and compare trace digest, steps, and virtual time. Include protocol assertions in the run so matching traces also have to satisfy the contract. The helpers emit a trace tail and a `REPLAY:` command on a run failure. Include the fault settings, failing identity, and observed versus expected events in the error. [Simulator seed and replay helpers](../sim/simtest/simtest.go)
5. Replay with the original seed and revision. If the failure is hard to read, preserve the first violating event and reduce the workload or fault schedule while checking that the same contract still fails. Keep the reduced case as a regression test or fuzz corpus entry.

## Scaffold for the first link implementation

Create the test surfaces as each link layer lands. Keep deterministic fakes and history helpers local to the tests while they have a single use. Give every new Go test file the required `Story:` header with what it proves, how it probes, and the limits of its evidence. [Repository instructions](../AGENTS.md)

- [ ] **Contract examples (`link/link_test.go`).** State the send and delivery semantics and each layer's failure assumptions. Cover no traffic, a single send, repeated logical messages, and equal payloads with distinct identities when the API exposes identity. Test the receiver and sender boundaries that scope the guarantees.
- [ ] **Generated safety checks (`link/prop_test.go`).** When a layer promises safety across histories, generate a bounded sequence of sends and transport indications, including duplicates and reordered arrivals. Track which sends can justify a delivery in a reference history and check each indication. Validate the checker on a bad history.
- [ ] **Joint behavior (`link/dst_test.go`).** Drive two or more simulated nodes with the real layer composition. Pin scenarios for directional partitions and recovery, duplication, and reordering as the contract warrants. For liveness, schedule work that meets the documented retry or repeated-send assumptions, make the network deliver within a known bound after healing, and check observable progress before the virtual deadline.
- [ ] **Public scenario.** Register a documented suite in `scenario/` with a matching starter JSON in `configs/`. Set `SuiteOutcome.Story.Proves`, `HowCollected`, `DoesNotProve`, and explicit pass/fail `Checks`. Build those rows from the observed history and put protocol claims in the verdict. Verify config loading and the built CLI's story output. Update the README and architecture roadmap alongside the suite and package. [Existing suite examples](../scenario/builtins.go)
- [ ] **Adapter boundary.** When a real transport or store lands, share contract cases for the guarantees the adapter implements. Use focused integration tests for encoding, transport errors, deadlines, and persistence. Reserve multi-process runs for interactions that require the real stack, such as reconnects or process restarts.

Use the following starting oracles when a layer lands. Bind each assertion to the API and fault model declared for that layer.

| Layer | Starting oracle |
| --- | --- |
| Fair-loss | Delivered traffic comes from actual sends, a finite send history has finite duplication, and a repeated-send workload makes progress under an explicitly fair schedule. |
| Stubborn | Delivery may repeat. Verify continued retries across a drop window and repeated delivery in a bounded healthy window under the layer's assumptions. |
| Perfect | Observe no fabricated deliveries and at most one delivery per logical send at its destination. Require eligible messages to arrive after the recovery conditions hold. |
| Logged perfect | Crash and recover at the persistence boundaries the spec defines. Check the delivery guarantee across incarnations, including behavior with an old duplicate after recovery. |
| Authenticated perfect | Exercise valid delivery, a modified authenticated field, and a sender mismatch. Check that each rejected input creates no application delivery. |
| FIFO perfect | Deliver later frames before earlier ones at the transport boundary. Check the resulting application delivery order within the contract's sender/receiver scope. |

## Run and review

```sh
make check
make test-prop
make sweep SEEDS=50
make fuzz PKG=./sim FUZZ=FuzzSimSchedule FUZZTIME=10s
```

The first two commands are the code-change gate. `make check` includes formatting, vet, race tests with coverage, and built-CLI config sweeps with determinism checking, plus the CLI end-to-end test. Seed sweeps and active fuzzing are exploration passes. By default, `simtest.Sweep` runs seeds 1 through 5. An explicit `-seed` takes precedence over `DST_SEED`, which takes precedence over `DST_SEEDS`. For a new or changed suite, run `make story SUITE=<registered-name>` or `make story CONFIG=configs/<name>.json` and inspect the evidence and overall verdict. [Makefile](../Makefile) · [Seed precedence](../sim/simtest/simtest.go)
