# Test pipelines deterministically

`HostLoom.Pipelines.Testing` provides test doubles and harnesses for
pipelines and filters, so tests observe outcomes instead of throwing and
never rely on real time.

## Before you begin

A pipeline or filters to test — standalone (`Pipe.Create`) or registered
(`AddPipeline`) — and any test framework; the harnesses have no xUnit or
NUnit dependency.

## 1. Install the package

```text
dotnet add package HostLoom.Pipelines.Testing
```

## 2. Test a whole registered pipeline

`PipelineHarness` builds a minimal container around your registration
(with scope validation on), runs startup validation, and captures the
outcome of a run:

```csharp
await using var harness = await PipelineHarness.CreateAsync<IndexingContext>(
    "document-indexing",
    services =>
    {
        services.AddSingleton<IDocumentStore>(fakeStore);
        services.AddPipeline<IndexingContext>("document-indexing", Configure);
    });

var result = await harness.RunAsync(new IndexingContext(batch));
Assert.True(result.Completed);
```

`RunAsync` returns a `PipeSendResult<TContext>` — `Context`,
`Exception?`, and `Completed` — instead of throwing, so a failed run is
an assertable result rather than a test-framework stack trace. For a
standalone composition, `PipeHarness.For<TContext>(configure)` gives the
same capture semantics via `SendAsync`.

## 3. Test a single filter

`CapturePipe` is a recording stand-in for `next` — it lets you run one
filter in isolation and assert on whether, and with what contexts, it
invoked the rest of the pipeline:

```csharp
var next = new CapturePipe<CommandContext>();
await filter.SendAsync(context, next);
Assert.True(next.WasSent);
```

`Sent` holds every context it received; set `Fault` to make every
subsequent send throw, simulating a failing downstream.

## 4. Simulate collaborators and failures

- `RecordingFilter` — records its configured name into a shared
  `ExecutionLog` as the run passes through it, so a test asserts on
  execution order via `log.Entries`. (Contexts themselves are what
  `CapturePipe` records.)
- `FaultFilter` — fails the first *n* sends with an injectable exception,
  for driving retry and circuit-breaker paths without contriving a
  genuinely broken dependency; `Sends` counts the attempts it saw.

## 5. Keep time out of your tests

Every built-in timing filter — retry backoff, circuit breaker reset,
rate limit interval, timeout — accepts a `TimeProvider`. Pass a fake one
and advance it explicitly; a retry test that sleeps is a flaky test.

## Troubleshoot

- **`result.Completed` is false and you don't know why** — read
  `result.Exception`; the harness captured it rather than throwing.
- **`PipelineHarness.CreateAsync` itself throws** — that is startup
  validation running in the test: the registration under test has
  duplicate names or unresolvable filter dependencies, the same failure
  the real host would produce.
- **A test passes alone and flakes in a suite** — look for a real
  `TimeProvider` or a shared stateful filter instance leaking between
  tests; compose a fresh pipeline per test.

## Related

- What the built-in filters guarantee:
  [the pipeline model](../explanation/pipeline-model.md).
- Registration and startup validation:
  [Register pipelines with stages](register-pipelines.md).
- Full testing API: [pipelines reference](../reference/pipelines.md#testing).
