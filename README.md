# HostLoom

[![Language](https://img.shields.io/badge/language-C%23-512BD4)](https://learn.microsoft.com/dotnet/csharp/)
[![.NET SDK](https://img.shields.io/badge/.NET%20SDK-10.0.400-512BD4)](https://github.com/kidoz/hostloom/blob/main/global.json)
[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/kidoz/hostloom/blob/main/LICENSE)

**HostLoom** — an experimental, Spring-inspired application framework for
.NET 10 and C# 14. The first vertical slice is typed request/response messaging
over interchangeable transports, carried by a transport-neutral asynchronous
middleware pipeline. The current slice implements:

- generic `IPipe<TContext>` and `IFilter<TContext>` composition, with typed
  context payloads, conditional branches, concurrency limits, intentional
  short-circuits, and immutable pipeline probes;
- typed `IRequest<TResponse>` contracts with handler, behavior, and client
  abstractions;
- one dependency-injection scope per request;
- explicit wire envelopes carrying message id, correlation id, logical type
  name, timestamp, and remote faults;
- a configurable `System.Text.Json` serialization boundary;
- an OpenTelemetry-compatible `ActivitySource` named `HostLoom`;
- in-memory, RabbitMQ, and Kafka broker adapters;
- .NET Generic Host startup with graceful endpoint disposal;
- tests for typed round trips, behavior ordering, and fault propagation.

HostLoom is intentionally a small foundation, not a MassTransit
reimplementation. It borrows two durable ideas:
[GreenPipes](https://github.com/phatboyg/greenpipes)' composable asynchronous
pipeline becomes the transport-neutral `HostLoom.Pipelines` package, and
MassTransit's typed contracts, scoped consumers, correlation, faults, and
hosted transport lifecycle become a compact request runtime.

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

## Transports

A request address is a logical name; each adapter maps it onto its own honest
transport topology.

- `.UseInMemory()` — deterministic in-process delivery, for tests and local
  composition.
- `.UseRabbitMq(...)` — the address becomes a durable request queue. Each
  client opens an exclusive reply queue and correlates replies through the
  AMQP `CorrelationId` and `ReplyTo` properties.
- `.UseKafka(...)` — the address becomes a request topic. Replies go to
  `KafkaOptions.ResponseTopic` and correlation travels in Kafka headers. The
  response topic must be provisioned with enough retention for the maximum
  request timeout.

The Kafka adapter gives every client instance a unique response consumer group,
so each instance sees the shared response stream and ignores responses it does
not own. That is correct for an initial implementation but not the final
high-scale topology; partition-affine reply routing is on the roadmap.

RabbitMQ and Kafka are not hidden behind an identical topology because they do
not have identical semantics. RabbitMQ naturally supports an exclusive reply
queue and AMQP correlation. Kafka is a durable partitioned log, so
request/reply is an application protocol built from request topics, response
topics, keys, headers, retention, and consumer groups. HostLoom keeps the
application API common while letting each adapter own its transport protocol.

## Generic pipelines

`HostLoom.Pipelines` is the GreenPipes-inspired foundation and has no
dependency on messaging or a broker. Filters execute in registration order and
decide whether to invoke the next stage:

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

Use `UseTerminal` for an intentional short circuit. Context payloads are lazy
and thread-safe, and can be retrieved through an implemented interface.
`PipelineProbe.Inspect` returns an immutable tree suitable for health endpoints
and diagnostics.

## Requirements

- [.NET SDK 10.0.400](https://dotnet.microsoft.com/download/dotnet/10.0) or
  later (see [global.json](global.json))
- A RabbitMQ or Kafka broker only when those transports are used;
  `HostLoom.Pipelines` and the in-memory transport have no external
  dependencies

## Build and verify

```text
dotnet restore HostLoom.slnx
dotnet build HostLoom.slnx -c Release --no-restore
dotnet test HostLoom.slnx -c Release --no-restore
```

Formatting is enforced with [CSharpier](https://csharpier.com/), pinned as a
local tool:

```text
dotnet tool restore
dotnet csharpier format .
```

[just](https://github.com/casey/just) wraps the same commands; run `just` to
list every recipe:

```text
just format
just build
just test
```

## Repository map

```text
src/HostLoom/                    messaging kernel
  Abstractions/                  request, handler, behavior, client, broker contracts
  Configuration/                 AddHostLoom builder and DI registration
  Diagnostics/                   the HostLoom ActivitySource
  Exceptions/                    remote fault and request timeout types
  Runtime/                       dispatcher, executor, client, hosted endpoint
  Serialization/                 System.Text.Json serialization boundary
  Wire/                          envelope, logical type names, codec
src/HostLoom.Pipelines/          transport-neutral middleware pipelines
  Contexts/                      pipe context and thread-safe typed payloads
  Filters/                       delegate, execute, conditional, concurrency, terminal
  Pipes/                         composition, builder, composer
  Diagnostics/                   immutable pipeline probes
src/HostLoom.Transport.InMemory/ deterministic in-process broker
src/HostLoom.Transport.RabbitMq/ request queues and exclusive reply queues
src/HostLoom.Transport.Kafka/    request/response topics with header correlation
tests/HostLoom.Tests/            pipeline, round-trip, behavior, and fault tests
```

## Roadmap toward a Spring-like framework

1. Harden messaging: delivery policies, retry/dead-letter behaviors,
   outbox/inbox, trace-context propagation, health checks, broker integration
   tests, and source-generated contract manifests.
2. Add starter packages and conditional auto-configuration over
   `Microsoft.Extensions.*`.
3. Add validation, transactions, persistence conventions, HTTP problem details,
   security, and observability starters.
4. Add a CLI/template and AOT-safe compile-time registration.

## License

HostLoom is licensed under the [MIT License](LICENSE).

Copyright (c) 2026 Aleksandr Pavlov.
