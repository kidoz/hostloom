# Register pipelines with stages

Turn a pipeline into a first-class dependency-injection registration with
`HostLoom.Pipelines.DependencyInjection`: named stages in declared order,
filters resolved from a per-run scope, per-filter feature toggles,
startup validation, and built-in instrumentation.

## Before you begin

- A context type deriving from `PipeContext` and filters implementing
  `IFilter<TContext>` (the
  [standalone pipeline tutorial](../tutorials/first-pipeline.md) covers
  both).
- An application using `Microsoft.Extensions.Hosting` — validation runs
  when the host starts.

## 1. Install the package

```text
dotnet add package HostLoom.Pipelines.DependencyInjection
```

## 2. Declare the pipeline

```csharp
using HostLoom.Pipelines.DependencyInjection;

builder.Services.AddPipeline<IndexingContext>("document-indexing", pipeline => pipeline
    .WithTimeout(TimeSpan.FromMinutes(5))
    .WithRetry(RetryPolicy.Exponential(2, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)))
    .Stage("analyze", stage => stage
        .AddFilter<WordCountFilter>(filter => filter.WithName("word_count"))
        .AddFilter<SentenceCountFilter>(filter => filter
            .EnabledWhen(sp =>
                sp.GetRequiredService<IOptionsMonitor<Flags>>().CurrentValue.SentenceCount)))
    .Stage("summarize", stage => stage.AddFilter<ReadingTimeFilter>())
    .Stage("store", stage => stage.AddFilter<StoreDocumentFilter>()));
```

- **Stages** run in declared order and give the topology readable names.
- **Filters are resolved transient from a per-run scope**, so they take
  repositories and loggers through constructors like any scoped service.
- **`EnabledWhen`** is evaluated on every run, so a feature flag flips a
  filter without restarting the host.

## 3. Run it

The runner is a keyed service, keyed by pipeline name:

```csharp
var runner = provider
    .GetRequiredKeyedService<IPipelineRunner<IndexingContext>>("document-indexing");
await runner.RunAsync(new IndexingContext(batch, cancellationToken: stoppingToken));
```

When exactly one pipeline exists for a context type, the unkeyed
`IPipelineRunner<TContext>` resolves too.

## 4. Verify

Start the host: the resolved topology is logged at startup, and
`runner.Topology.Describe()` renders it on demand — a trailing `?` marks
a conditional filter:

```text
analyze[word_count, SentenceCountFilter?] -> summarize[ReadingTimeFilter] -> store[StoreDocumentFilter]
```

Each filter is automatically wrapped with a duration histogram, a failure
counter, and a tracing span (meter and `ActivitySource` both named
`HostLoom.Pipelines`); the recorded duration is the filter's **own work
with downstream time subtracted**, so the slow filter is visible wherever
it sits. Opt out with `WithoutInstrumentation()`.

## Troubleshoot

- **Startup fails with `InvalidOperationException`** — that is the
  validation working: duplicate pipeline/stage/filter names, a stage with
  no filters, a pipeline with no stages, or a filter whose constructor
  dependencies cannot be resolved all fail startup instead of the first
  run.
- **Resolving the unkeyed `IPipelineRunner<TContext>` throws** — more
  than one pipeline is registered for that context type; resolve by key.
- **A toggled filter never runs** — `EnabledWhen` is evaluated once per
  run against the run's scope; check what the predicate reads there.

## Related

- Compose without a container:
  [Building a standalone pipeline](../tutorials/first-pipeline.md).
- Test registered pipelines:
  [Test pipelines deterministically](test-pipelines.md).
- Full builder and runner API: [pipelines reference](../reference/pipelines.md).
- The runnable tour: `examples/HostLoom.Examples.Pipelines` in the
  repository.
