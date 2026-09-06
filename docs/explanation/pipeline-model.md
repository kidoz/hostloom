# The pipeline model

Everything in HostLoom that wraps execution — resilience, instrumentation,
conditional logic, the receive path itself — is built from one model: a
pipe of filters flowing a typed context. This page explains the model and
the guarantees the built-in filters make.

## Pipes, filters, contexts

- A **context** carries the work: cancellation plus lazily created,
  thread-safe **payloads** (`GetOrAddPayload`, `TryGetPayload`) that let
  filters attach state — a stopwatch, a retry attempt — without the
  context type knowing about them in advance. Payloads can be retrieved
  through an implemented interface.
  The context itself can implement a payload type. In that case,
  `AddOrUpdatePayload` calls the update factory with the context; the factory
  must return that same instance after any in-place changes. A replacement
  or null result throws `InvalidOperationException`. Stored payloads can
  still be replaced normally.
- A **filter** (`IFilter<TContext>`) receives the context and the rest of
  the pipeline as `next`, and decides whether to invoke it. Wrapping,
  short-circuiting, branching, and observing are all this one shape.
- A **pipe** (`IPipe<TContext>`) is the composed, immutable chain. Filters
  execute in registration order.

The model is GreenPipes' — carried forward because it composes: a retry
filter does not need to know it wraps a circuit breaker wrapping a
handler; each filter only knows `next`.

## Resilience as filters, not a policy engine

`UseRetry`, `UseCircuitBreaker`, `UseRateLimit`, `UseConcurrencyLimit`,
and `UseTimeout` are ordinary filters, so their placement — and therefore
their scope — is visible in the composition. Their contracts:

- **Retry** re-invokes the rest of the pipeline, exposes the attempt as a
  `RetryAttempt` payload, and never retries cancellation. Policies:
  `Immediate`, `Interval`, `Exponential`, each optionally `WithJitter`.
- **Circuit breaker** throws `CircuitBreakerOpenException` once the
  downstream has failed `failureThreshold` times in a row, then admits one
  trial call per `resetInterval`. Calls admitted before the circuit opened
  cannot close it, extend its reset interval, or change the current trial's
  verdict when they finish later.
- **Rate limit** shapes throughput by *waiting*, not throwing.
- **Timeout** bounds the remainder of the pipeline by swapping a linked
  token into the context for the duration of the downstream call — which
  is why it is constrained to contexts deriving from `PipeContext`, a
  compile-time constraint rather than a runtime surprise. The run fails
  with `PipelineTimeoutException`, and caller cancellation is always
  rethrown as cancellation, never misreported as a timeout.

All timing filters accept a `TimeProvider`, so tests advance time
explicitly instead of sleeping. Circuit resets and rate-limit windows use
monotonic timestamps, so system-clock corrections do not change their
intervals. A fake provider must advance timestamps and timers when elapsed
time advances; changing only `GetUtcNow` does not advance an interval.

`InstrumentedFilter` subtracts the time during which any downstream call is
active from the filter's total duration. Overlapping downstream calls on
separate contexts count once; sequential downstream calls each contribute
their own interval.

## Composed once, shared state on purpose

A pipeline is composed once and reused. That is what gives a circuit
breaker or rate limiter *process-wide* state — a breaker that reset per
message would never open. The complementary rule sits one level up: in
registered pipelines, filter *instances* are resolved transient from a
per-run scope, so stateful infrastructure lives in the filter's own
long-lived internals while dependencies stay scoped.

## One model at three altitudes

1. **Standalone** — `Pipe.Create<TContext>` with no container; also the
   unit-test shape.
2. **Registered** — `AddPipeline<TContext>` adds named stages,
   constructor-injected filters, per-run feature toggles, startup
   validation (duplicate names and missing constructor dependencies fail
   startup, not the first run), and automatic per-filter metrics and
   tracing.
3. **The receive path** — every transport wraps handler execution in a
   pipeline of `ReceiveContext`, configured with
   `ConfigureReceivePipeline`. One pipeline serves requests and events, so
   a breaker tripped by failing requests also rejects events — a single
   verdict on whether this process should be taking work.

## Introspection without execution

Because a pipe is an immutable composition, its structure can be reported
without running it: `PipelineProbe.Inspect` returns an immutable tree,
`HostLoomProbe.ReceivePipeline()` does the same for the receive path, and
a registered runner exposes `Topology`. Composition that can be *asked*
what it is beats composition that must be exercised to be observed.
