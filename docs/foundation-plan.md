# Phase 1 Foundation Plan

## Goal
Stand up the minimal, zero-dependency Go foundation (`env/`, `sim/`, `sim/simtest/`, `job/`) that:
1. Establishes the single-threaded, mutually exclusive process model with a swappable `FIFO` queue interface (`SliceFIFO` default for DST, `ChanFIFO` swappable).
2. Provides a lean (~350–450 LOC) deterministic simulator supporting all fault classes needed for reliable, secure, and replicated distributed algorithms.
3. Validates the harness end-to-end with composable request/indication job-handling components (`job/`) and deterministic multi-seed tests.

---

## Work Breakdown

### Step 1: Repo Configuration
- `go.mod` (`module github.com/nickstrad/distributed-systems-algorithms`, `go 1.26.0`)
- `.gitignore`
- `Makefile` (`test`, `test-race`, `vet`, `sweep`)

### Step 2: Core Environment Contracts (`env/`)
- `env/env.go`:
  - `NodeID`, `InstanceID`, saturating `Time` (`Add`, `Sub`, `String`, `MinTime`, `MaxTime`)
  - `Cluster` (`NewCluster`, `Rank`, `N`, `Majority`, `ByzantineQuorum`, `Contains`, `PeersCopy`)
  - Narrow interfaces: `Dispatcher`, `Clock`, `Timer`, `RawTransport`, `Store`, `Process`
  - `Env` bundle + sentinel errors (`ErrClosed`, `ErrUnknownPeer`, `ErrInvalidArgument`)
- `env/fifo.go`:
  - `FIFO[T]` (`Push`, `Pop`, `Len`, `Clear`)
  - `SliceFIFO[T]` (unbounded circular/head-compacting FIFO that clears popped slots for GC)
  - `ChanFIFO[T]` (buffered channel implementation with non-blocking `Pop` and panic-on-overflow guard so single-threaded DST never silently hangs)
- `env/envelope.go`:
  - `Envelope{Instance, MsgID, From, Type, Payload, Auth}`
  - JSON `MarshalEnvelope` / `UnmarshalEnvelope` + deterministic `CanonicalBytes()` for MAC/signature coverage
- `env/crypto.go`:
  - `Hash([]byte) [32]byte` (`sha256.Sum256`)
  - `KeyRing` wrapping `crypto/ed25519` (deterministic signing) and `crypto/hmac` (SHA-256)
  - `GenerateKeyRings(peers, rng)` using a seeded reader for reproducible Byzantine tests

### Step 3: Deterministic Simulator Kernel (`sim/`)
- `sim/queue.go`: indexed binary min-heap ordered by `(at, seq)` with $O(\log n)$ `remove` cancellation.
- `sim/stream.go`: `NewStream(seed, label)` via FNV-64a + PCG (`math/rand/v2`).
- `sim/store.go`: `memStore` implementing `env.Store` (persists across `Crash`/`Recover`, clones byte slices on read/write).
- `sim/net.go`: `NetConfig` (integer `DropPPM`, `DupPPM`, `MinLatency`, `MaxLatency`, `MaxDuplicates`), directed partitions (`Partition`, `PartitionBi`, `Isolate`, `Heal`, `HealAll`, `Stabilize`), defensive byte-slice copies on send.
- `sim/sim.go`:
  - `Sim` event loop with `MaxSteps` and `MaxSameInstant` livelock guards, panic recovery (`*PanicError`), sticky errors, bounded trace ring buffer, and running SHA-256 `Digest()`.
  - Pluggable `NewFIFO func() env.FIFO[func()]` per node (defaults to `env.NewSliceFIFO[func()]`).
  - Dynamic `AddNode` + `AddCluster` helpers; per-node `Crash(id)`, `Recover(id)`, `IsAlive(id)`; timers with incarnation check; `AddInvariant(name, fn)` checked after every step; `Step`, `RunUntil`, `RunFor`, `RunUntilQuiet`.

### Step 4: Test Harness & Composition Smoke Test (Delegated to Hydrogen Agents)
- `sim/simtest/simtest.go`:
  - `Seeds(t)` (`-seed`, `DST_SEED`, `DST_SEEDS`), `Sweep(t, fn)`, `RequireDeterministic(t, seed, run)` with trace tail + `REPLAY:` failure diagnostics.
- `job/`:
  - `job/handler.go`: `Job`, `Handler`, `SyncHandler`, `AsyncHandler` (with explicit `Init` + internal `drain` condition).
  - `job/buffered.go`: `Transformer`, `BufferedTransformer` (bounded circular buffer of size `M` composing over `Handler`, enforcing guaranteed response + soundness).
  - `job/job_test.go`: tests covering guaranteed response, soundness, FIFO non-reentrancy, `SliceFIFO` vs `ChanFIFO` swappability, and deterministic multi-seed replay.
- `env/env_test.go` & `sim/sim_test.go`: unit tests for `Cluster`, `FIFO` implementations, `Envelope`/`KeyRing`, min-heap ordering & cancellation, network faults/partitions/GST, crash-recovery persistence, invariant failure reporting, and cross-run determinism.

### Step 5: Simplify Pass (Hydrogen Agents) & Final Verification
- Run parallel simplify review (reuse, quality, efficiency, clarity) following the `simplify-code` / `code-simplifier` rubric.
- Apply only high-confidence, behavior-preserving improvements.
- Verify with `go vet ./...`, `go test -race -count=1 ./...`, and a 50-seed sweep (`DST_SEEDS=50`).
