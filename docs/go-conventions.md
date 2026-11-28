# Go conventions for simulation code

## Errors

Validate configuration, CLI arguments, network messages, and stored data where they enter the system. Return errors for invalid input and expected failures, including injected timeouts, unreachable peers, and storage errors. Reuse an existing sentinel when the caller can act on it. Wrap with `fmt.Errorf("operation and context: %w", err)` at a package boundary when the layer can add useful context. Describe the affected operation and any partial state change or retry implications that the caller needs to know.

Use `errors.Is` for sentinels and `errors.As` for typed details. Keep error text for humans. Across a simulated RPC boundary, use the stable codes in [env/errors.go](../env/errors.go). A decoded `*env.RemoteError` matches `env.ErrRemote` and, for a registered code, the mapped sentinel. Include replay coordinates in simulator failures through the existing `sim.InvariantError` and `sim.PanicError` types.

Pass errors up to the caller that owns the response. If that layer propagates the error, leave reporting to its caller. `dsasim` prints the terminal error at the CLI boundary. Simulator diagnostics can include the seed, step, virtual time, and trace tail to help reproduce a failure. [Strata error handling](https://github.com/nickstrad/strata/blob/651ca39b42ca4137a93d62589aa0904653a49dda/docs/coding-style/go-coding-style.md#4-error-handling)

## Logging

Use the injected `env.Env.Log` for node events worth seeing in a trace. Keep messages stable and put changing values in typed attributes such as `env.Str`, `env.Int64`, and `env.Err`. Supply attributes in a fixed order and sort keys before logging map-derived fields. The simulator attaches the node and virtual time. A runtime adapter can render these events with `log/slog` and add wall-clock timestamps at its own boundary.

Log where an event is handled and the diagnostic explains what happened. Use debug for protocol detail, info for significant transitions, and warn for handled disruptions worth noting. Reserve error for failures that the reporting layer owns. An injected failure that follows an expected recovery path can be a trace event. Keep logs independent of protocol decisions. [Strata logging](https://github.com/nickstrad/strata/blob/651ca39b42ca4137a93d62589aa0904653a49dda/docs/coding-style/go-coding-style.md#5-logging)

## Preconditions, postconditions, and invariants

Document meaningful internal contracts in Go doc comments. A `pre:` line states what a trusted caller establishes before the call. A `post:` line states what the operation guarantees on return, with any condition such as `err == nil`. An `Invariant:` line states what remains true at stable boundaries. The simulator heap checks that an event is detached before insertion. Its postcondition gives timer cancellation a reliable index. The duplicate-insertion regression exercises the violation and checks that the original event can still be removed. [Heap implementation](../sim/queue.go) · [Contract test](../sim/queue_test.go)

Assert contracts that catch programmer errors close to the mutation. A failure should name the violated condition and include the relevant values. Check a precondition before mutating and check a postcondition on the relevant return path. Keep the check free of side effects and run mutation checks when state is coherent. A check in a deferred function can mask the panic that caused the function to unwind. If input or a modeled fault can violate the condition, handle that path as a validation or protocol error. When multiple implementations share an interface, put common guarantees on the interface and run the same contract tests against each.

Use `Sim.AddInvariant` for named safety properties that a scenario or test should check after each event. Return an error from the predicate to get an `*sim.InvariantError` with the violated property and replay coordinates. A panic inside a protected simulator callback or invariant produces an `*sim.PanicError` with a stack. Keep these checks deterministic and enable them during simulation. Put expensive cross-node checks in the scenarios or tests that need them, and exercise a failure case for consequential assertions to verify that the check catches the violation. [Strata contracts](https://github.com/nickstrad/strata/blob/651ca39b42ca4137a93d62589aa0904653a49dda/docs/coding-style/go-coding-style.md#3-validation-vs-contracts), [Strata simulation testing](https://github.com/nickstrad/strata/blob/651ca39b42ca4137a93d62589aa0904653a49dda/docs/coding-style/simulation-testing.md)
