# Pipelines

The `HostLoom.Pipelines` family: composition, resilience filters,
dependency-injection registration, and testing. Namespaces follow the
package names.

```text
dotnet add package HostLoom.Pipelines
dotnet add package HostLoom.Pipelines.DependencyInjection   # named stages, DI
dotnet add package HostLoom.Pipelines.Testing               # harnesses, test doubles
```

## Core contracts

| Type | Members |
| --- | --- |
| `IPipe<in TContext>` | `ValueTask SendAsync(TContext context)` |
| `IFilter<TContext>` | `ValueTask SendAsync(TContext context, IPipe<TContext> next)` |
| `IPipeContext` | `CancellationToken`, `HasPayload(Type)`, `TryGetPayload<T>(out T?)`, `GetOrAddPayload<T>(Func<T>)`, `AddOrUpdatePayload<T>(Func<T>, Func<T,T>)` |
| `PipeContext` | base implementation; ctor takes a `CancellationToken`; payloads are lazy and thread-safe |

`Pipe.Create<TContext>(Action<PipeBuilder<TContext>>)` builds a pipeline;
`Pipe.Empty<TContext>()` is a no-op pipe. All context type parameters are
constrained `class, IPipeContext` unless noted.

## PipeBuilder&lt;TContext&gt;

| Method | Notes |
| --- | --- |
| `Use(IFilter<TContext> filter)` | add a filter instance |
| `Use(Func<TContext, IPipe<TContext>, ValueTask> filter, string name = "delegate")` | inline wrapping filter |
| `UseExecute(Func<TContext, ValueTask> action, string name = "execute")` | action, then continue downstream |
| `UseTerminal(Func<TContext, ValueTask> action, string name = "terminal")` | action, intentional short circuit |
| `UseWhen(Func<TContext, bool> predicate, Action<PipeBuilder<TContext>> configure, string name = "conditional")` | conditional branch |
| `UseConcurrencyLimit(int limit)` | bounds concurrent sends |
| `UseRetry(RetryPolicy policy, Func<Exception, bool>? shouldRetry = null, TimeProvider? timeProvider = null)` | never retries cancellation; exposes `RetryAttempt(int Number)` payload (absent on the first attempt) |
| `UseCircuitBreaker(int failureThreshold, TimeSpan resetInterval, TimeProvider? timeProvider = null)` | consecutive failures open the circuit; one trial call per interval |
| `UseRateLimit(int limit, TimeSpan interval, TimeProvider? timeProvider = null)` | shapes throughput by waiting, not throwing |
| `Build()` | single-use — a second `Build` throws `ObjectDisposedException` |

`UseTimeout(TimeSpan, TimeProvider? = null)` is an extension method
constrained to `TContext : PipeContext` — stricter than the builder,
because it swaps a linked token into the context for the downstream call.

## RetryPolicy

| Member | Notes |
| --- | --- |
| `Immediate(int retryLimit)` | retry with no delay |
| `Interval(int retryLimit, TimeSpan interval)` | fixed delay |
| `Exponential(int retryLimit, TimeSpan minInterval, TimeSpan maxInterval, double factor = 2)` | growing delay, capped |
| `WithJitter(double ratio)` | ratio in [0, 1]; returns a new policy |
| `RetryLimit`, `Description`, `GetDelay(int attempt)` | attempt counted from 1; `RetryLimit` counts retries *after* the first attempt |

## Exceptions

| Type | Raised when | Members |
| --- | --- | --- |
| `CircuitBreakerOpenException` | send rejected while the circuit is open | `TimeSpan ResetInterval` |
| `PipelineTimeoutException` (`: TimeoutException`) | `UseTimeout` elapsed; caller cancellation is rethrown as cancellation instead | `TimeSpan Timeout` |

## Probes

`PipelineProbe.Inspect(pipe)` returns a `ProbeResult`
(`Name`, `Properties`, `Children`) without executing anything. Custom
filters participate via `IProbeSite.Probe(IProbeContext)`; the default
implementation reports the type name.

Meter and `ActivitySource` are both named `HostLoom.Pipelines`
([instruments](observability.md)).

## Registered pipelines (DependencyInjection)

`AddPipeline<TContext>(this IServiceCollection, string name, Action<PipelineBuilder<TContext>> configure)`.

Lifetimes: declared filters are **keyed transient** (resolved per run
from a dedicated scope); `IPipelineRunner<TContext>` is a **keyed
singleton** under the pipeline name; an unkeyed `IPipelineRunner<TContext>`
resolves only when exactly one pipeline exists for that context type;
startup validation runs as a hosted service.

| Type | Members |
| --- | --- |
| `PipelineBuilder<TContext>` | `Stage(name, configure)`, `WithRetry(policy, shouldRetry?, timeProvider?)`, `WithoutInstrumentation()`; `WithTimeout(timeout, timeProvider?)` extension (`TContext : PipeContext`) — retry/timeout wrappers nest in declaration order, first outermost |
| `PipelineStageBuilder<TContext>` | `AddFilter<TFilter>(Action<PipelineFilterBuilder>? configure = null)`; default diagnostic name is the filter type name |
| `PipelineFilterBuilder` | `WithName(string)`, `EnabledWhen(Func<IServiceProvider, bool>)` — evaluated once per run |
| `IPipelineRunner<TContext>` | `PipelineName`, `Topology`, `RunAsync(TContext)` |
| `PipelineTopology` | `Describe()` renders `stage[filterA, filterB?]` — a trailing `?` marks a conditional filter; `PipelineStageTopology` / `PipelineFilterTopology` carry the structure |
| `PipelineValidator` | `ValidateAsync(IServiceProvider, CancellationToken)` — the check the startup hosted service runs |

`InvalidOperationException` at build or startup: duplicate pipeline name,
duplicate stage name, duplicate filter name, a stage with no filters, a
pipeline with no stages, a filter with unresolvable constructor
dependencies.

## Testing

| Type | Members |
| --- | --- |
| `PipeHarness.For<TContext>(configure)` → `PipeHarness<TContext>` | `Pipe`, `Probe()`, `SendAsync(context)` — captures, never throws |
| `PipelineHarness.CreateAsync<TContext>(pipelineName, configureServices, ct)` → `PipelineHarness<TContext>` | `Runner`, `Topology`, `RunAsync(context)`, `DisposeAsync()`; builds the provider with `ValidateScopes` and runs validation |
| `PipeSendResult<TContext>` | `Context`, `Exception?`, `Completed` (true when `Exception` is null) |
| `CapturePipe<TContext>` | recording stand-in for `next`: `Sent`, `WasSent`, settable `Fault` thrown on subsequent sends |
| `RecordingFilter<TContext>` | ctor `(string name, ExecutionLog log)`; records pass-through order |
| `ExecutionLog` | `Entries`, `Record(string)` — shared log for recording filters |
| `FaultFilter<TContext>` | ctor `(int failures, Func<Exception>? exceptionFactory = null)`; fails the first *n* sends, counts `Sends` |
