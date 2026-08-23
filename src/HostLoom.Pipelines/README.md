# HostLoom.Pipelines

Transport-neutral asynchronous middleware pipelines for .NET 10 and C# 14, inspired by [GreenPipes](https://github.com/phatboyg/greenpipes).

The package provides generic contexts and filters, thread-safe typed payloads, conditional branches, concurrency limiting, intentional short-circuiting, and immutable diagnostic probes. It has no dependency on HostLoom messaging or a broker.

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
