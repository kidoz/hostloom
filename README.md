# HostLoom

HostLoom is an experimental, Spring-inspired application framework for .NET 10 and C# 14. The first vertical slice is typed request/response messaging over interchangeable transports, with a transport-neutral asynchronous middleware pipeline.

This is intentionally a small foundation, not a MassTransit reimplementation. It borrows two durable ideas:

- [GreenPipes](https://github.com/phatboyg/greenpipes)' composable asynchronous pipeline becomes the transport-neutral `HostLoom.Pipelines` package; request behaviors remain the messaging-specific middleware API.
- MassTransit's typed contracts, scoped consumers, correlation, faults, and hosted transport lifecycle become a compact request runtime.

## What exists

- generic `IPipe<TContext>` and `IFilter<TContext>` composition, context payloads, conditional branches, concurrency limits, and pipeline probing;
- typed `IRequest<TResponse>`, handler, behavior, and client abstractions;
- one dependency-injection scope per request;
- explicit wire envelopes with message id, correlation id, logical type names, timestamp, and remote faults;
- configurable `System.Text.Json` serialization boundary;
- OpenTelemetry-compatible `ActivitySource` named `HostLoom`;
- in-memory, RabbitMQ, and Kafka broker adapters;
- .NET Generic Host startup and graceful endpoint disposal;
- tests for typed round trips, behavior ordering, and fault propagation.

## Quick start

```csharp
using HostLoom;
using HostLoom.Transport.RabbitMq;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(10))
    .UseRabbitMq(options => options.Uri = new Uri("amqp://guest:guest@localhost:5672/"))
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");

await builder.Build().RunAsync();

public sealed record GetGreeting(string Name) : IRequest<Greeting>;
public sealed record Greeting(string Text);

public sealed class GetGreetingHandler : IRequestHandler<GetGreeting, Greeting>
{
    public ValueTask<Greeting> HandleAsync(GetGreeting request, CancellationToken cancellationToken) =>
        ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
}
```

A client-only application registers the contract without a local handler:

```csharp
builder.Services
    .AddHostLoom()
    .UseRabbitMq()
    .AddRequestClient<GetGreeting, Greeting>();

var client = services.GetRequiredService<IRequestClient<GetGreeting, Greeting>>();
var reply = await client.GetResponseAsync("greetings", new GetGreeting("Ada"), cancellationToken: stoppingToken);
```

For Kafka, use `.UseKafka(...)`. A request address maps to a request topic. Replies go to `KafkaOptions.ResponseTopic`, and correlation is carried in Kafka headers. The response topic must be provisioned. The current adapter gives every client instance a unique response consumer group, so each instance sees the shared response stream and ignores responses it does not own. That is correct for an initial implementation but not the final high-scale topology; partition-affine reply routing is on the roadmap.

For tests and local composition, use `.UseInMemory()`.

## Generic pipelines

`HostLoom.Pipelines` is the GreenPipes-inspired foundation and has no dependency on messaging or a broker. Filters execute in registration order and decide whether to invoke the next stage:

```csharp
using HostLoom.Pipelines;

var pipeline = Pipe.Create<CommandContext>(pipe =>
{
    pipe.UseConcurrencyLimit(32);
    pipe.Use(async (context, next) =>
    {
        var stopwatch = context.GetOrAddPayload(() => Stopwatch.StartNew());
        await next.SendAsync(context);
        Console.WriteLine(stopwatch.Elapsed);
    }, "timing");
    pipe.UseWhen(
        context => context.Audited,
        branch => branch.UseExecute(context => WriteAudit(context)));
});

await pipeline.SendAsync(new CommandContext(cancellationToken));
var structure = PipelineProbe.Inspect(pipeline);

sealed class CommandContext(CancellationToken cancellationToken) : PipeContext(cancellationToken)
{
    public bool Audited { get; init; }
}
```

Use `UseTerminal` for an intentional short circuit. Context payloads are lazy and thread-safe, and can be retrieved through an implemented interface. `PipelineProbe.Inspect` returns an immutable tree suitable for health endpoints and diagnostics.

## Why not hide RabbitMQ and Kafka behind identical topology?

They do not have identical semantics. RabbitMQ naturally supports an exclusive reply queue and AMQP `CorrelationId`/`ReplyTo`. Kafka is a durable partitioned log, so request/reply is an application protocol built from request topics, response topics, keys, headers, retention, and consumer groups. HostLoom keeps the application API common while letting each adapter own its honest transport protocol.

## Build

```bash
dotnet restore HostLoom.slnx
dotnet test HostLoom.slnx
```

The repository pins the .NET 10.0.400 SDK and C# 14.

## Roadmap toward a Spring-like framework

1. Harden messaging: delivery policies, retry/dead-letter behaviors, outbox/inbox, trace-context propagation, health checks, broker integration tests, and source-generated contract manifests.
2. Add starter packages and conditional auto-configuration over `Microsoft.Extensions.*`.
3. Add validation, transactions, persistence conventions, HTTP problem details, security, and observability starters.
4. Add a CLI/template and AOT-safe compile-time registration.
