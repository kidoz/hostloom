# Building a standalone pipeline

In this tutorial you will compose an asynchronous pipeline with
`HostLoom.Pipelines` alone — no broker, no messaging runtime, no host. You
will add filters, branch conditionally, and inspect the pipeline's
structure without executing it. This is the GreenPipes-inspired foundation
everything else in HostLoom stands on.

## 1. Create the project

```text
dotnet new console -n PipelineTour
cd PipelineTour
dotnet add package HostLoom.Pipelines
```

Your project will end up with two files:

```text
PipelineTour/
├── Program.cs
└── CommandContext.cs
```

## 2. Define a context

Every pipeline flows a context. Derive from `PipeContext` to get
cancellation and thread-safe, lazily created payloads.

Create `CommandContext.cs`:

```csharp
using HostLoom.Pipelines;

public sealed class CommandContext(CancellationToken cancellationToken)
    : PipeContext(cancellationToken)
{
    public bool Audited { get; init; }
}
```

## 3. Compose and run the pipeline

`Pipe.Create<TContext>` builds an immutable pipeline. Filters execute in
registration order, and each decides whether to invoke the next stage.

Replace `Program.cs` with:

```csharp
using System.Diagnostics;
using HostLoom.Pipelines;

var pipeline = Pipe.Create<CommandContext>(pipe =>
{
    pipe.UseConcurrencyLimit(32);
    pipe.Use(async (context, next) =>
    {
        var stopwatch = context.GetOrAddPayload(() => Stopwatch.StartNew());
        await next.SendAsync(context);
        Console.WriteLine($"took {stopwatch.Elapsed}");
    }, "timing");
    pipe.UseWhen(
        context => context.Audited,
        branch => branch.UseExecute(context =>
        {
            Console.WriteLine("audited");
            return ValueTask.CompletedTask;
        }));
});

await pipeline.SendAsync(new CommandContext(CancellationToken.None) { Audited = true });
```

Three filter styles appear here:

- `UseConcurrencyLimit` — a built-in resilience filter.
- `Use` — an inline delegate filter that wraps the rest of the pipeline;
  the name `"timing"` shows up in probes and diagnostics.
- `UseWhen` — a conditional branch; `UseExecute` inside it runs an action
  (a `Func<TContext, ValueTask>`) without wrapping downstream filters.

For an intentional short circuit, `UseTerminal` ends the run on purpose
rather than by omission. In a hosted service you would pass the service's
stopping token instead of `CancellationToken.None`.

## 4. Verify

```text
dotnet run
```

You should see both lines — the branch ran because `Audited` was true,
and the timing filter observed the whole downstream:

```text
audited
took 00:00:00.00...
```

Set `Audited = false` and run again: the `audited` line disappears.

## 5. Add resilience

Resilience is composed from the same filter model, not a separate policy
engine. Add these calls **inside the `Pipe.Create` callback**, before the
timing filter:

```csharp
    pipe.UseRetry(RetryPolicy
        .Exponential(3, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5))
        .WithJitter(0.2));
    pipe.UseCircuitBreaker(failureThreshold: 5, resetInterval: TimeSpan.FromSeconds(30));
    pipe.UseRateLimit(limit: 100, interval: TimeSpan.FromSeconds(1));
    pipe.UseTimeout(TimeSpan.FromMinutes(5));
```

Each of these accepts a `TimeProvider`, so their timing is testable
without real waiting. Their exact semantics — what retries, what throws,
what waits — are covered in
[the pipeline model](../explanation/pipeline-model.md).

## 6. Inspect without executing

`PipelineProbe.Inspect` returns an immutable tree of `ProbeResult` nodes
(`Name`, `Properties`, `Children`). Add at the end of `Program.cs`:

```csharp
var structure = PipelineProbe.Inspect(pipeline);
Print(structure, 0);

static void Print(ProbeResult node, int depth)
{
    Console.WriteLine($"{new string(' ', depth * 2)}{node.Name}");
    foreach (var child in node.Children)
    {
        Print(child, depth + 1);
    }
}
```

Run again and the composition prints as a tree — the names are the ones
you gave your filters, and the branch's `execute` filter nests under the
conditional:

```text
pipeline
  concurrencyLimit
  timing
  conditional
    execute
  empty
```

The trailing `empty` is the pipeline's terminal pipe — the `next` that
the last filter sends into. If you added the resilience filters from
step 5, their nodes appear too.
Nothing runs during inspection; you are looking at the composition
itself — the same tree suits a health endpoint or a debug page.

## Where next

- Register pipelines in the container with named stages, per-run filter
  resolution, and startup validation:
  [Register pipelines with stages](../how-to/register-pipelines.md).
- Test filters and whole pipelines deterministically:
  [Test pipelines deterministically](../how-to/test-pipelines.md).
- The runnable tour at `examples/HostLoom.Examples.Pipelines` in the
  repository walks the same ground with more variations.
