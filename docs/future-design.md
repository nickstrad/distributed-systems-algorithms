# Future Design & Architectural Notes

This document captures the architectural decisions, trade-offs, and long-term roadmap for building a general-purpose Go distributed algorithms repository grounded in:

1. **Cachin, Guerraoui & Rodrigues**, *Introduction to Reliable and Secure Distributed Programming* (2nd Ed., Springer 2011)
2. **Charron-Bost, Pedone & Schiper (eds.)**, *Replication: Theory and Practice* (Springer LNCS 5959, 2010)

The goal is a clean, production-grade Go codebase where every distributed algorithm runs identically inside a **deterministic simulator** and across **real cloud VMs** (DigitalOcean, AWS, etc.) without hand-rolling commodity networking or storage infrastructure.

---

## 1. Core Architectural Principles

### Single-Threaded Process State Machines + Swappable FIFO Queue
- Every node executes event handlers **mutually exclusively** (one event at a time per process) with **FIFO local event dispatch** and **zero re-entrancy**.
- Modules never import `net`, `os`, `sync`, `time.Now`, or global `math/rand`. They depend solely on the narrow `env` package.
- Local event dispatch is split into two interfaces:
  - `env.Dispatcher` (`Enqueue(fn func())`) — called by modules to trigger local indications/internal steps.
  - `env.FIFO[T]` (`Push`, `Pop`, `Len`, `Clear`) — the pluggable backing queue owned by the node host.
    - **DST default (`env.SliceFIFO[T]`):** unbounded deterministic slice queue; immune to bounded-channel cycle deadlocks (e.g., bidirectional module stacks or $O(N^2)$ broadcast bursts) and cleared instantaneously on `Crash()`.
    - **Channel option (`env.ChanFIFO[T]`):** buffered Go channel implementing `FIFO[T]` for bounded-buffer tests or experiments.
    - **Production host (`runtime/`):** one event-loop goroutine per OS process draining a `chan func()` implementing `env.Dispatcher`. Network I/O goroutines (gRPC/UDP) and timers push into that single channel, keeping algorithm code 100% lock-free.

### Typed Interfaces + `InstanceID` Multiplexing
- Rather than an untyped event bus, each distributed abstraction is a typed Go interface:
  - **Requests** are methods (`Submit`, `Send`, `Broadcast`, `Propose`, `Read`, `Write`).
  - **Indications** are typed callbacks (`OnConfirm`, `OnDeliver`, `OnSuspect`, `OnDecide`), always invoked through `Dispatcher.Enqueue` so callers never observe re-entrant state mutation.
- Every wire `Envelope` carries an `InstanceID` so higher-level protocols (e.g., multi-decree consensus, concurrent broadcast channels, replicated partitions) can run arbitrarily many module instances over one shared transport.
- Internal condition handlers (`upon cond do ...`) are implemented explicitly with local state buffers and a `drain()`/`step()` helper rather than a reflective condition engine.

### Wire Error Handling & Deterministic Structured Logging (`env/errors.go`, `env/log.go`)
- **Wire status codes (`env.Code`, `env.CodeTable`, `*env.RemoteError`):** Built-in sentinels (`ErrTimeout`, `ErrUnreachable`, `ErrClosed`, `ErrTooLarge`, `ErrRemote`, `ErrNotFound`, `ErrIO`, `ErrInvalidArgument`, `ErrUnknownPeer`) map to codes `< 64`; higher-level protocols register domain sentinels at `>= CodeAppMin (64)`. Decoded `*RemoteError` unwraps to both its mapped sentinel and `ErrRemote` (`errors.Is` works transparently across both `sim/` and `gRPC`).
- **Deterministic structured logging (`env.Logger`, `env.Attr`):** Zero-cost `NopLogger{}` when disabled; inside `sim/`, `simLogger` stamps virtual time + `node=<id>` into the trace ring buffer and SHA-256 replay digest; in production (`runtime/`), adapts directly to stdlib `log/slog`.

### Zero-Dependency Observability Spine (`obs/`, `obs/csvflush`, `obs/promtext`, `obs/logline`)
- **One raw export, one direction of dependency:** Protocol packages maintain plain integer `obs.Counter`, `obs.Gauge` (with `Peak`), and zero-alloc log-linear `obs.Histogram` values and expose an `obs.Source`. Core `obs/` has zero locks, zero floats, zero wall clock, and zero third-party SDKs, so algorithms emit identical deterministic metrics in both DST and production.
- **CSV for offline / DuckDB analysis (`obs/csvflush`):** Writes schema-version-1 `metrics.csv`, `histograms.csv`, and (last on `Close()`, fail-closed) `run.csv`, with Go readback (`csvflush.Read`) and direct DuckDB SQL compatibility.
- **Prometheus exposition (`obs/promtext`):** Standard-library Prometheus text format `0.0.4` (`# HELP`, `# TYPE`, `_total`, cumulative `_bucket`/`_count`) plus an atomic copy-on-read `http.Handler` for `/metrics` that never blocks or touches algorithm loops on scrape.
- **Compact publish logs (`obs/logline`):** Single-line `obs.publish seq=... t_ns=... key=value ...` summaries capped at 1,024 bytes.

### Layered Testing Pyramid
1. **Deterministic unit tests (`*_test.go`)**: fast, isolated contract and fault-mode tests.
2. **Property-based tests (`prop_test.go` using `pgregory.net/rapid`, restricted to `*_test.go`)**: proves algebraic invariants (stream independence, clock monotonicity, `CodeTable` round-trips, `Histogram` quantile accuracy, `FIFO` equivalence, `BufferedTransformer` soundness/conservation) across generated inputs.
3. **Deterministic simulation sweeps (`sim/simtest`)**: multi-seed replay verification (`RequireDeterministic`, `Sweep`, `DST_SEEDS=50`, `REPLAY:` failure commands).
4. **Native Go fuzzing (`Fuzz*`)**: fuzzes input schedules and wire/codec parsers (`go test -fuzz=...`).
5. **Production adapter tests (`runtime/`, future)**: `testing/synctest` for goroutine/timer adapters + multi-process Docker Compose tests.

### Supporting All Six Fault & Execution Models
The core `env` + `sim` boundary is designed upfront to support all algorithm classes without redesign:
1. **Fail-stop:** `Crash(id)` drops pending local events and timers immediately; reliable failure detection under synchronous network bounds.
2. **Fail-silent:** asynchronous delays/drops with no failure detector.
3. **Fail-noisy (partial synchrony):** pre-GST arbitrary delays/partitions followed by `HealAll()` / `Stabilize(cfg)` modeling Global Stabilization Time (GST).
4. **Fail-recovery:** `Crash(id)` preserves `env.Store`; `Recover(id)` instantiates a fresh process with zero volatile state, the same `Store`, and the same persistent per-node PRNG stream, calling `Recover()` instead of `Init()`.
5. **Fail-arbitrary (Byzantine):** stdlib cryptography (`crypto/sha256`, `crypto/hmac`, deterministic `crypto/ed25519`) in `env.KeyRing`, plus the ability to register arbitrary malicious `env.Process` implementations for selected `NodeID`s in `sim`.
6. **Randomized:** isolated per-node PCG streams (`math/rand/v2`) derived from `(seed, label)` so network draws never perturb node randomness.

### Future-Proofing for *Replication: Theory and Practice* (LNCS 5959)
Two lightweight design choices keep the foundation ready for dynamic, client-facing replication protocols:
1. **Dynamic node registration & client/replica parity:** `sim.Sim` does not bake a static node set into the simulator kernel. Nodes can be added dynamically via `AddNode`, and clients and replicas are modeled uniformly as `env.Process` instances on the same simulated network.
2. **Domain-agnostic virtual timestamps & invariants:** `sim.Now()` and `AddInvariant(name, fn)` make it straightforward to add client operation-history recorders and consistency checkers later (Linearizability, Sequential Consistency, Causal Consistency, Snapshot Isolation, 1-Copy Serializability) without modifying `sim/`.

---

## 2. Build vs. Buy Matrix (Learning vs. Production on AWS / DigitalOcean)

**Rule:** Never pull in a library that implements the algorithm you are studying (no `hashicorp/raft`, `hashicorp/memberlist`, NATS, or etcd election APIs). Do pull in battle-tested Go libraries for commodity transport, TLS, serialization, and disk durability.

| Layer | In DST (`sim/`) | In Production (`runtime/` on DO / AWS) | Notes |
| :--- | :--- | :--- | :--- |
| **Wire envelope** | `env.Envelope` (`encoding/json` initially; opaque `Payload []byte`) | `google.golang.org/protobuf` (single generic `Envelope` proto) or `json` | Swapping JSON -> Protobuf is localized to `env/envelope.go` |
| **Raw datagram transport** | `SimNet` (drop, dup, reorder, latency, asymmetric partition, GST) | Stdlib `net.UDPConn` (~80 LOC) | Used to run `link/` (`FairLoss` -> `Stubborn` -> `Perfect`) over real VMs |
| **Production reliable/auth links** | Your `link.PerfectLink` / `link.AuthPerfectLink` over `SimNet` | **`google.golang.org/grpc` + mTLS** (~120 LOC) behind the same `PerfectLink` / `AuthPerfectLink` interfaces | Gives HTTP/2 multiplexing, mTLS auth, keepalives, and backpressure without hand-rolling TCP framing; add `MsgID` dedup for strict at-most-once delivery across reconnects |
| **Durable storage (`env.Store`)** | In-memory map surviving `Crash()` + `Recover()` | **`go.etcd.io/bbolt`** (~40 LOC wrapper) | Battle-tested ACID/fsync durability without hand-rolling POSIX WAL/rename/dir-fsync |
| **Cryptography** | Stdlib `crypto/sha256`, `crypto/hmac`, `crypto/ed25519` | Identical Go stdlib | Ed25519 signing is deterministic (no random nonce), making Byzantine DST runs 100% replayable |
| **Real-runtime testing** | `sim/` + `sim/simtest/` | Go 1.26 stdlib `testing/synctest` for `runtime/` goroutine/timer tests + Docker Compose multi-process tests | DST proves protocol logic; `synctest` + live tests verify the production adapter |

---

## 3. Planned Domain Package Roadmap

Packages are organized strictly by distributed-systems domain (no textbook chapter names) and created only when implemented:

1. **`env/`** *(Foundation)* — `NodeID`, `InstanceID`, `Cluster`, `Time`, `Dispatcher`, `FIFO[T]` (`SliceFIFO`, `ChanFIFO`), `Clock`, `Timer`, `RawTransport`, `Store`, `Envelope`, `KeyRing`, `CodeTable`/`RemoteError`, `Logger`, `Process`.
2. **`obs/` & adapters (`csvflush`, `promtext`, `logline`)** *(Foundation)* — Zero-alloc integer `Counter`, `Gauge`, log-linear `Histogram`, `Collector`, schema-v1 CSV writer/reader (for Go assertions & DuckDB), stdlib Prometheus `0.0.4` exporter + `/metrics` `http.Handler`, and bounded publish log lines.
3. **`sim/` & `sim/simtest/`** *(Foundation)* — Single-threaded deterministic event loop, indexed min-heap, named PCG streams, `SimNet` fault injector, `Crash`/`Recover`, invariants, trace digest, `MetricsSource()`, seed sweeps, `REPLAY:` output.
4. **`job/`** *(Foundation smoke test)* — Request/indication composition primitives: `SyncHandler`, `AsyncHandler`, and bounded `BufferedTransformer` composing over `Handler`, each exporting `obs.Source` metrics.
5. **`scenario/`, `configs/`, & `cmd/dsasim/`** *(Foundation CLI & config runner)* — Pluggable algorithm suite registry (`scenario.Registry`) with per-suite strongly-typed JSON config files under `configs/`, driven by a Cobra CLI (`bin/dsasim`: `list`, `run`, `config init`, `test`) and `Makefile` targets (`make build`, `make test-race`, `make test-prop`, `make test-suite`, `make test-all-suites`, `make sweep`, `make fuzz`, `make check`). Cobra is strictly isolated to `cmd/dsasim`.
6. **`link/`** — Point-to-point link hierarchy:
   - `FairLossLink` -> `StubbornLink` -> `PerfectLink` -> `LoggedPerfectLink` -> `AuthPerfectLink` -> `FIFOPerfectLink`.
5. **`detector/`** — Failure detection & leader election:
   - Perfect Failure Detector ($P$), Monarchical Leader Election, Eventually Perfect Failure Detector ($\diamond P$), Eventual Leader Detector ($\Omega$, lower-epoch variant), Byzantine Eventual Leader Detector.
6. **`broadcast/`** — Group communication primitives:
   - Best-Effort, Regular Reliable, Uniform Reliable (All-Ack & Majority-Ack), Stubborn/Logged variants, Probabilistic/Epidemic (Eager & Lazy Gossip), FIFO, Causal (No-Waiting, GC, Waiting), Byzantine Consistent & Reliable Broadcast (Authenticated/Signed Echo, Double-Echo), Total-Order Broadcast.
7. **`register/`** — Distributed shared memory:
   - `(1, N)` Regular & Atomic registers (Read-One Write-All, ABD Majority Voting, Read-Impose), `(N, N)` Atomic registers, Logged registers, Byzantine Safe/Regular/Atomic registers.
   - Pair with an operation-history linearizability checker over `sim`.
8. **`consensus/`** — Agreement algorithms:
   - Flooding, Hierarchical, Uniform Consensus, Epoch-Change & Epoch Consensus (Paxos / Leader-Driven), Logged Uniform Consensus, Randomized Consensus / Common Coin, Weak/Strong/Validated/Fast Byzantine Consensus.
9. **`membership/`** — Dynamic group management:
   - Group Membership, View-Synchronous Communication (Regular & Uniform), dynamic reconfiguration.
10. **`replication/`** — Practical replication protocols (bridging CGR Ch 6 & *Replication: Theory and Practice*):
    - Primary-Backup replication, State-Machine Replication (SMR), Non-Blocking Atomic Commit (NBAC), Virtual Synchrony, Optimistic/Lazy replication (anti-entropy, version vectors), Transactional / Database replication (1-Copy Serializability, Snapshot Isolation, Deferred Update Replication).
11. **`runtime/`** — Production cloud adapter:
    - Single-goroutine channel-mailbox node host (`chan func()` -> `env.Dispatcher`), `realClock`/`realTimer` with generation guards, `bbolt` `env.Store`, `gRPC`+mTLS `PerfectLink`/`AuthPerfectLink`, optional `net.UDPConn` `RawTransport`, and small `cmd/` demo binaries for multi-VM deployment.

---

## 4. Potential Future Testing Enhancements (Only When Needed)

Keep the simulator lean early on; add these only when later packages warrant them:
- **Operation-history checker (`verify/history`)**: Porcupine-style or linearizability/serializability verifier for `register/` and `replication/`.
- **Fault schedule fuzzing & 1-minimal shrinking**: `go test -fuzz` decoding bytes into `[]Fault` schedules with delete-one shrinking (useful once Paxos/Byzantine/SMR land).
- **Determinism static lint (`detlint`)**: lightweight AST check banning `time.Now`, `go`, `sync`, `math/rand`, and unsorted map iteration in algorithm packages once the codebase grows past several packages.
- **Chaos nemesis with fault budgets**: automated alternating chaos + heal windows ensuring quorum survival (`f < N/2` or `f < N/3`) during long multi-seed sweeps.

## 5. First link implementation and telemetry

The [first link checklist](next-link.md) connects the roadmap to a bounded simulation and acceptance criteria. Module-owned stats follow the working example in [job/buffered_stats.go](../job/buffered_stats.go). CSV and Prometheus share an `obs.Snapshot`. A future OTLP adapter belongs in `runtime/`, where network I/O, SDK dependencies, export timeouts and shutdown can be tested independently of simulated time. See the [observability walkthrough](module-observability.md).
