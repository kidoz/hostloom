# HostLoom.Pipelines

Transport-neutral asynchronous middleware pipelines for .NET 10 and C# 14, inspired by [GreenPipes](https://github.com/phatboyg/greenpipes).

The package provides generic contexts and filters, thread-safe typed payloads, conditional branches, resilience filters (retry, circuit breaker, rate limit, concurrency limit, timeout), intentional short-circuiting, per-filter metrics and tracing through `InstrumentedFilter` (meter and activity source named `HostLoom.Pipelines`), and immutable diagnostic probes with a default per-type probe, so a filter only implements `Probe` when it has more to say. It has no dependency on HostLoom messaging or a broker.

`HostLoom.Pipelines.DependencyInjection` adds named-stage pipeline registration over this package, and `HostLoom.Pipelines.Testing` adds a deterministic harness.

```csharp
var pipeline = Pipe.Create<MyContext>(pipe =>
{
    pipe.UseConcurrencyLimit(32);
    pipe.Use(async (context, next) =>
    {
        await Before(context);
        await next.SendAsync(context);
        await After(context);
    });
});

await pipeline.SendAsync(new MyContext(cancellationToken));
```
