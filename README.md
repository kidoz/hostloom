# HostLoom

[![Language](https://img.shields.io/badge/language-C%23-512BD4)](https://learn.microsoft.com/dotnet/csharp/)
[![.NET SDK](https://img.shields.io/badge/.NET%20SDK-10.0.400-512BD4)](https://github.com/kidoz/hostloom/blob/main/global.json)
[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/kidoz/hostloom/blob/main/LICENSE)

**HostLoom** — an experimental, Spring-inspired application framework for
.NET 10 and C# 14. The first vertical slice is typed request/response messaging
over interchangeable transports, carried by a transport-neutral asynchronous
middleware pipeline. The current slice implements:

- generic `IPipe<TContext>` and `IFilter<TContext>` composition, with typed
  context payloads, conditional branches, retry, circuit breaking, rate and
  concurrency limits, timeouts, intentional short-circuits, and immutable
  pipeline probes;
- dependency-injection pipeline registration with named stages, per-run filter
  resolution, feature toggles, startup validation, built-in per-filter metrics
  and tracing, and a deterministic test harness;
- typed `IRequest<TResponse>` contracts with handler, behavior, and client
  abstractions;
- typed `IEvent` contracts published to a topic and fanned out to named
  subscriptions;
- a configurable receive pipeline wrapping handler execution on every transport;
- one dependency-injection scope per delivery attempt;
- explicit wire envelopes carrying message id, correlation id, logical type
  name, timestamp, and remote faults;
- a configurable `System.Text.Json` serialization boundary;
- an OpenTelemetry-compatible `ActivitySource` and `Meter`, both named `HostLoom`;
- liveness and readiness health checks, and an execution-free pipeline probe;
- in-memory, RabbitMQ, and Kafka broker adapters;
- an authenticated raw-WebSocket RPC and live-subscription gateway with JSON,
  MessagePack, and Protocol Buffers subprotocols, bounded per-connection memory,
  and explicit credit;
- .NET Generic Host startup with graceful endpoint disposal;
- tests for typed round trips, behavior ordering, and fault propagation.

HostLoom is intentionally a small foundation, not a MassTransit
reimplementation. It borrows two durable ideas:
[GreenPipes](https://github.com/phatboyg/greenpipes)' composable asynchronous
pipeline becomes the transport-neutral `HostLoom.Pipelines` package, and
MassTransit's typed contracts, scoped consumers, correlation, faults, and
hosted transport lifecycle become a compact request runtime.

## Packages

Runtime packages target .NET 10, the compiler-hosted analyzer targets `netstandard2.0`, and all
packages are versioned together:

| Package | Purpose |
| --- | --- |
| `HostLoom.Analyzers` | Compile-time checks for asynchronous and DI usage |
| `HostLoom` | Typed request/response and event runtime |
| `HostLoom.Pipelines` | Transport-neutral asynchronous pipelines |
| `HostLoom.Pipelines.DependencyInjection` | Named stages, per-run resolution, and instrumentation |
| `HostLoom.Pipelines.Testing` | Deterministic pipeline test doubles and harnesses |
| `HostLoom.Transport.InMemory` | In-process request and event transport |
| `HostLoom.Transport.RabbitMq` | RabbitMQ request and fan-out event transport |
| `HostLoom.Transport.Kafka` | Kafka request and consumer-group event transport |
| `HostLoom.AspNetCore.WebSockets` | Authenticated WebSocket RPC and subscriptions |
| `HostLoom.Logging` | Allocation-free UTF-8 logging provider |

Install only the runtime and transport needed by the application, for example:

```text
dotnet add package HostLoom.Transport.RabbitMq --version 0.1.0
```

The analyzer package is optional and has no runtime dependency:

```text
dotnet add package HostLoom.Analyzers
```

It reports an omitted available cancellation token (`HLM0001`), synchronous blocking over a
HostLoom async operation (`HLM0002`), and singleton registration of handlers or behaviors that
should follow HostLoom's per-delivery scope (`HLM0003`). See the
[analyzer rule reference](src/HostLoom.Analyzers/README.md).

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

## Publish and subscribe

A request addresses one handler and expects one reply. An event is published to a topic
and delivered to every *subscription* on it:

```csharp
builder.Services
    .AddHostLoom()
    .UseInMemory()
    .AddSubscriber<OrderPlaced, AuditHandler>("orders", subscription: "audit")
    .AddSubscriber<OrderPlaced, ShippingHandler>("orders", subscription: "shipping");

await publisher.PublishAsync("orders", new OrderPlaced("A-1"));

public sealed record OrderPlaced(string Reference) : IEvent;
```

Two subscription names on one topic each receive every event. Two handlers under the
*same* name share one delivery and one dependency-injection scope. A subscription that
has no handler for a published contract ignores it rather than failing.

Publish/subscribe is a separate transport capability, `IEventBroker`. Publishing through
a transport that lacks it throws, and registering a subscription against one fails at
startup rather than starting up looking subscribed while nothing is delivered.

Each transport maps a subscription onto its own fan-out primitive:

- **In-memory** — a named handler on the topic, delivered to in process.
- **RabbitMQ** — a **fanout exchange** per topic, and a durable queue named
  `topic.subscription` bound to it, so subscriptions accumulate their own backlog rather
  than competing for one queue. Events publish with no routing key and without
  `mandatory`, so an event nobody subscribes to is dropped instead of failing the publish.
- **Kafka** — the topic is a Kafka topic and each subscription is its own **consumer
  group**, so every group receives every record while instances sharing a group divide the
  partitions. Records are produced without a key, so ordering holds within a partition
  only.

## Receive pipeline

Filters registered with `ConfigureReceivePipeline` wrap handler execution for
every inbound delivery — requests and events alike — on every transport:

```csharp
builder.Services
    .AddHostLoom()
    .UseRabbitMq()
    .ConfigureReceivePipeline(pipe =>
    {
        pipe.UseRetry(RetryPolicy.Exponential(3, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)));
        pipe.UseCircuitBreaker(failureThreshold: 5, resetInterval: TimeSpan.FromSeconds(30));
    })
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");
```

These filters see a handler failure as an exception, before it is encoded as a
fault envelope, which is what makes retry and circuit breaking apply to it. The
wire contract is unchanged: an exhausted retry or an open breaker still reaches
the caller as a `RemoteRequestException`.

Each attempt runs in its own dependency-injection scope, so a retry never
inherits scoped state left behind by the attempt that failed. The pipeline is
composed once, so a circuit breaker or rate limiter shares state across every
delivery rather than resetting per message.

Retrying in process is a different thing from broker redelivery. This pipeline
never moves a broker offset; redelivery is the transport's concern.

A filter receives a `ReceiveContext`, which is a `RequestReceiveContext` or an
`EventReceiveContext`. Both carry `Destination`, `MessageType`, and `Message`;
the event form adds `Subscription`. One pipeline serves both, so a breaker
tripped by failing requests also rejects events — a single verdict on whether
this process should be taking work.

## Raw WebSocket gateway

`HostLoom.AspNetCore.WebSockets` exposes an explicit subset of request operations and event
topics to WebSocket clients without SignalR:

```csharp
builder.Services
    .AddHostLoom()
    .UseRabbitMq()
    .AddWebSocketGateway()
    .AddRequest<GetOrder, OrderView>("orders.get", "orders-api", "orders.read")
    .AddTopic<OrderChanged>("orders.changed", "orders", e => e.CustomerId,
        subscription: "realtime-node-a", authorizationPolicy: "orders.read");

app.UseAuthentication();
app.UseAuthorization();
app.UseHostLoomWebSockets();
app.MapHostLoomWebSocketHub("/realtime");
```

Clients negotiate `hostloom.msgpack.v1`, `hostloom.protobuf.v1`, or `hostloom.json.v1`.
Authentication happens before upgrade and named ASP.NET Core policies are checked again for every
operation and subscription. One receive loop, one socket writer, a byte-bounded outbound queue,
concurrent-request limits, and subscription credit prevent a slow client from creating unbounded
per-connection work or memory.

The initial subscription protocol is live and process-local: acknowledgements record progress but
do not provide replay, and gateway-generated event IDs are not broker offsets. Multi-node services
must choose distinct broker subscription names per node or add a fan-out/backplane according to the
broker's actual queue or consumer-group semantics. See the
[gateway protocol and operating notes](src/HostLoom.AspNetCore.WebSockets/README.md).

Codec encode/decode throughput and allocations are measured separately with BenchmarkDotNet:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*WebSocketProtocol*"
```

The suite covers zero-byte, 256-byte, and 4 KiB payloads. A `--job Dry` run verifies benchmark
discovery and execution but is not statistically meaningful.

## Health and metrics

`AddHealthChecks()` registers two checks, tagged `live` and `ready` so they map to
separate probe endpoints:

```csharp
builder.Services.AddHostLoom().UseRabbitMq().AddHealthChecks();

app.MapHealthChecks("/health/live", new() { Predicate = r => r.Tags.Contains("live") });
app.MapHealthChecks("/health/ready", new() { Predicate = r => r.Tags.Contains("ready") });
```

**Liveness never contacts the broker.** It answers "should this process be restarted",
and a broker outage answering yes turns one outage into a restart storm across every pod
that talks to it. Broker reachability belongs in readiness, which reports unhealthy when
endpoints are not listening or the transport says the broker is unreachable.

A transport reports reachability by implementing `IBrokerHealthProbe`. One that does not
is treated as reachable, because "cannot tell" must not read as "broken".

Metrics are published on a `Meter` named `HostLoom`, tagged by destination and message
type: `hostloom.request.duration`, `hostloom.request.active`, `hostloom.request.faults`,
and `hostloom.request.retries`.

`HostLoomProbe` returns the receive pipeline's structure without executing it, which suits
a debug endpoint:

```csharp
app.MapGet("/diagnostics/pipeline", (HostLoomProbe probe) => probe.ReceivePipeline());
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

Resilience is composed the same way, from filters rather than from a separate
policy engine:

```csharp
pipe.UseRetry(RetryPolicy.Exponential(3, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)).WithJitter(0.2));
pipe.UseCircuitBreaker(failureThreshold: 5, resetInterval: TimeSpan.FromSeconds(30));
pipe.UseRateLimit(limit: 100, interval: TimeSpan.FromSeconds(1));
```

`UseRetry` re-invokes the rest of the pipeline, exposing the attempt as a
`RetryAttempt` payload and never retrying cancellation. `UseCircuitBreaker`
throws `CircuitBreakerOpenException` once the downstream has failed
`failureThreshold` times in a row, then admits one trial call per
`resetInterval`. `UseRateLimit` shapes throughput by waiting rather than
throwing. All of them accept a `TimeProvider`, so their timing is testable
without real waiting.

`UseTimeout` bounds the remainder of the pipeline for contexts deriving from
`PipeContext` — a compile-time constraint, because the filter swaps a linked
token into the context for the duration of the downstream call. Filters that
honour `context.CancellationToken` stop promptly, the run fails with
`PipelineTimeoutException`, and caller cancellation is always rethrown as
cancellation, never misreported as a timeout:

```csharp
pipe.UseTimeout(TimeSpan.FromMinutes(5));
```

### Registered pipelines with stages

`HostLoom.Pipelines.DependencyInjection` turns a pipeline into a first-class
registration: named stages in declared order, filters resolved transient from a
per-run scope so they take repositories and loggers through constructors, and
per-filter feature toggles evaluated on every run:

```csharp
using HostLoom.Pipelines.DependencyInjection;

builder.Services.AddPipeline<IndexingContext>("document-indexing", pipeline => pipeline
    .WithTimeout(TimeSpan.FromMinutes(5))
    .WithRetry(RetryPolicy.Exponential(2, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)))
    .Stage("analyze", stage => stage
        .AddFilter<WordCountFilter>(filter => filter.WithName("word_count"))
        .AddFilter<SentenceCountFilter>(filter => filter
            .EnabledWhen(sp => sp.GetRequiredService<IOptionsMonitor<Flags>>().CurrentValue.SentenceCount)))
    .Stage("summarize", stage => stage.AddFilter<ReadingTimeFilter>())
    .Stage("store", stage => stage.AddFilter<StoreDocumentFilter>()));

var runner = provider.GetRequiredKeyedService<IPipelineRunner<IndexingContext>>("document-indexing");
await runner.RunAsync(new IndexingContext(batch, cancellationToken: stoppingToken));
```

Every pipeline is validated when the host starts — duplicate names and filters
with missing constructor dependencies fail startup instead of the first run —
and its resolved topology is logged and exposed as `runner.Topology`. Each
filter is automatically wrapped with a duration histogram, a failure counter,
and a tracing span (meter and `ActivitySource` both named
`HostLoom.Pipelines`); the recorded duration is the filter's own work with
downstream time subtracted, so the slow filter is visible wherever it sits.
`WithoutInstrumentation()` opts a pipeline out.

`HostLoom.Pipelines.Testing` completes the loop with `CapturePipe` (a recording
stand-in for `next`), `RecordingFilter`, `FaultFilter`, and harnesses that
capture the outcome of a send instead of throwing:

```csharp
await using var harness = await PipelineHarness.CreateAsync<IndexingContext>("document-indexing", services =>
{
    services.AddSingleton<IDocumentStore>(fakeStore);
    services.AddPipeline<IndexingContext>("document-indexing", Configure);
});

var result = await harness.RunAsync(new IndexingContext(batch));
Assert.True(result.Completed);
```

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
  Runtime/                       dispatcher, receive pipeline, executor, client, endpoint
  Serialization/                 System.Text.Json serialization boundary
  Wire/                          envelope, logical type names, codec
src/HostLoom.Analyzers/          Roslyn usage analyzers and rule documentation
src/HostLoom.Pipelines/          transport-neutral middleware pipelines
  Contexts/                      pipe context and thread-safe typed payloads
  Filters/                       delegate, execute, conditional, concurrency, timeout, instrumented, terminal
  Pipes/                         composition, builder, composer
  Diagnostics/                   immutable pipeline probes, pipeline meter and activity source
src/HostLoom.Pipelines.DependencyInjection/ named-stage pipeline registration, runner, startup validation
src/HostLoom.Pipelines.Testing/  capture pipe, recording and fault filters, harnesses
src/HostLoom.Transport.InMemory/ deterministic in-process broker
src/HostLoom.Transport.RabbitMq/ request queues and exclusive reply queues
src/HostLoom.Transport.Kafka/    request/response topics with header correlation
src/HostLoom.AspNetCore.WebSockets/ raw Kestrel WebSocket RPC and subscriptions
benchmarks/HostLoom.Benchmarks/    JSON, MessagePack, and Protobuf codec benchmarks
examples/HostLoom.Examples.Pipelines/ runnable pipeline tour: DI stages, manual and standalone composition
tests/HostLoom.Tests/            pipeline, round-trip, behavior, and fault tests
tests/HostLoom.Analyzers.Tests/  compiler-level analyzer tests
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
