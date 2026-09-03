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
- explicit, compile-time-safe object maps with scoped dependency-injection dispatch and
  reflection-free, Native AOT-compatible map dispatch;
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
| `HostLoom.Analyzers` | Compile-time checks for asynchronous, DI, mapping, and caching usage |
| `HostLoom` | Typed request/response and event runtime |
| `HostLoom.Pipelines` | Transport-neutral asynchronous pipelines |
| `HostLoom.Pipelines.DependencyInjection` | Named stages, per-run resolution, and instrumentation |
| `HostLoom.Pipelines.Testing` | Deterministic pipeline test doubles and harnesses |
| `HostLoom.Transport.InMemory` | In-process request and event transport |
| `HostLoom.Transport.RabbitMq` | RabbitMQ request and fan-out event transport |
| `HostLoom.Transport.Kafka` | Kafka request and consumer-group event transport |
| `HostLoom.AspNetCore.WebSockets` | Authenticated WebSocket RPC and subscriptions |
| `HostLoom.AspNetCore.WebSockets.Testing` | In-process TestServer client for gateway tests |
| `HostLoom.Logging` | Allocation-free UTF-8 logging provider |
| `HostLoom.Diagnostics` | Composition ledger and startup report of registration decisions |
| `HostLoom.Mapping` | Explicit, compile-time-safe, AOT-friendly object mapping |
| `HostLoom.Mapping.DependencyInjection` | Scoped mapper dispatch and explicit map registration |
| `HostLoom.Mapping.Testing` | Container-free mapper composition for tests |
| `HostLoom.Caching` | Two-tier cache contracts, in-process stores, single-flight, fail-open composition |
| `HostLoom.Caching.DependencyInjection` | Cache registration, options validation, warmup, health checks, and the `IDistributedCache` adapter |
| `HostLoom.Caching.Testing` | Container-free cache composition, recording and fault-injecting stores |
| `HostLoom.Caching.Pipelines` | Cache and deduplication filters for HostLoom pipelines |
| `HostLoom.Locking` | Distributed lock contracts, lease handles, retry policy, in-process provider |
| `HostLoom.Locking.DependencyInjection` | Lock registration, options validation, and health checks |
| `HostLoom.Locking.Testing` | Container-free lock composition, scripted, recording, and fault-injecting providers |
| `HostLoom.Locking.Pipelines` | Distributed-lock filter for HostLoom pipelines |
| `HostLoom.Redis` | Redis cache store, invalidation channel, lock provider, and health probes over one connection |

Install only the runtime and transport needed by the application, for example:

```text
dotnet add package HostLoom.Transport.RabbitMq --version 0.2.0
```

The analyzer package is optional and has no runtime dependency:

```text
dotnet add package HostLoom.Analyzers
```

It reports an omitted available cancellation token (`HLM0001`), synchronous blocking over a
HostLoom async operation (`HLM0002`), singleton registration of handlers or behaviors that
should follow HostLoom's per-delivery scope (`HLM0003`), a destination member an explicit map never
assigns (`HLM0004`) and a map body whose completeness cannot be verified (`HLM0005`), and the scoped
mapping dispatcher captured in a singleton (`HLM0006`). See the
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
JSON uses camelCase frame-kind values, omits null optional fields, and carries application payloads
as Base64-encoded bytes.
Authentication happens before upgrade and named ASP.NET Core policies are checked again for every
operation and subscription. One receive loop, one socket writer, a byte-bounded outbound queue,
concurrent-request limits, and subscription credit prevent a slow client from creating unbounded
per-connection work or memory.

Topic policies receive the client-selected subscription key. The built-in
`TopicKeyPolicy.SubjectOnly` policy restricts a keyed topic to the authenticated subject using the
configured subject claim type and exact ordinal matching.

Topics can also register a scoped `IWebSocketTopicSnapshotProvider<TEvent>`. Subscriptions receive
`subscribed`, snapshot events marked by `sequence = 0`, and then any live events buffered within
the existing connection limits while the snapshot was loading.

Supplied browser origins are checked against the effective request origin by default. Native
clients may omit Origin; browser-only endpoints can reject a missing header, and cross-origin
applications can configure an exact allowlist.

Sessions are bounded by credential expiry and a 12-hour maximum, and control-frame floods close
with a policy violation. `IWebSocketSessionDirectory` exposes safe active-session snapshots while
`IWebSocketSessionControl` disconnects one session or every session for a subject after logout or a
role change. Host shutdown sends 1001 `server_shutdown` before broker subscriptions stop.

Gateway lifecycle, delivery, bounded-drop, queue-size, fault, and handler-level handshake metrics
are available from the `HostLoom.AspNetCore.WebSockets` meter. Its low-cardinality tags contain
only protocol, registered topic, and library-controlled reason or fault values—never session ids,
subjects, subscription keys, payloads, or credentials.

Stable structured log events `4100`–`4106` cover session lifecycle, rejected subscriptions,
slow-client aborts, handler-level handshake rejection, and operation or snapshot failures.
Framework-controlled properties never include subscription keys, payloads, credentials, handshake
headers, caller-supplied close text, or remote fault messages.

Registered gateway requests create `hostloom.websocket.request` Server activities from the
`HostLoom.AspNetCore.WebSockets` source. The existing core request activity is a direct child,
providing same-process gateway-to-handler correlation without an OpenTelemetry package dependency;
cross-process broker correlation still requires future trace-header propagation.

`HostLoomWebSocketBuilder.Probe()` describes the configured options and routes during registration;
the DI-resolved `WebSocketGatewayProbe` provides the same immutable, execution-free description for
a protected debug endpoint. Its decisions can be copied explicitly into a composition ledger by an
application that references the optional `HostLoom.Diagnostics` leaf; the gateway package itself
does not take that dependency.

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

Bounded JSON fan-out to 1, 100, and 500 ready session queues has a separate, machine-guarded
regression baseline:

```text
just benchmark-websocket-fanout-check
```

It measures in-process registry dispatch, per-session envelope encoding, credit, and the bounded
queue cycle for a ready writer. It deliberately excludes socket and network I/O, so deployment
capacity still requires a real-socket load test.

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

## Explicit object mapping

`HostLoom.Mapping` is a compile-time-safe alternative to runtime convention mapping. Each map is
an ordinary class, so constructors, required members, conversions, and nullability remain visible
to C# and code review. The core package is dependency-free; the DI adapter uses closed generic
registrations without assembly scanning, reflection-based member mapping, expression compilation,
or runtime code generation:

```csharp
using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;

builder.Services.AddHostLoomMapping(mapping =>
    mapping.Add<CustomerMapper>());

public sealed class CustomerMapper : IMapper<Customer, CustomerDto>
{
    public CustomerDto Map(Customer source) => new(source.Id, source.Name.Trim());
}
```

The pair is read from the interface the map class already declares, so a registration restates
nothing and the file that registers a map needs no `using` for the contracts it maps between. A
generic map class is closed through a factory overload, which is what registers many pairs from one
class.

Inject `IMapper<Customer, CustomerDto>` into a component that needs one pair — that is the shape to
reach for, and the fastest. Orchestration code coordinating several pairs can inject the scoped
`IMapper` dispatcher and write `mapper.From(customer).To<CustomerDto>()`. Map classes are transient
by default, duplicate pairs fail at registration, and a missing pair throws
`MappingNotFoundException` naming both types and what the source *is* registered to map to.

Sequences and null are extension methods whose names carry the policy — `MapMany` rejects a null
source, `MapManyOrEmpty` treats it as empty, `MapOrNull` maps one value through null — so every
place that depends on null tolerance stays greppable, where a convention mapper decides it once and
invisibly. `HostLoom.Analyzers` reports a destination member a map never assigns (`HLM0004`) and a
map body it cannot verify (`HLM0005`), which is what keeps a forgotten member from shipping as
silent data loss.

Mapping is deliberately synchronous and performs no I/O. Fetch and enrich data outside a map, use
distinct destination types for distinct semantic views, and write database projections directly
as `IQueryable.Select` expressions. All three mapping packages enable the .NET SDK Native AOT and
trimming analyzers.

## Caching and locking

`HostLoom.Caching` is a two-tier cache: an in-process tier in front of an optional distributed
tier, per-key single-flight, a best-effort cluster-wide lease, cross-instance invalidation, and
fail-open behaviour when the store misbehaves. `HostLoom.Locking` is a distributed lock with
leases, owner tokens, a retry policy, and lost-lease detection; it is coordination, not
correctness, for persisted state. Both are kernels that compose with `new`; their
`DependencyInjection` packages register them, and `HostLoom.Redis` supplies both backends over one
connection:

```csharp
builder.Services
    .AddHostLoomCaching(caching => caching.Namespace = "catalog")
    .UseRedis(redis => redis.Configuration = "redis:6379")
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = CatalogJsonContext.Default })
    .AddHealthChecks();

builder.Services
    .AddHostLoomLocking(locking => locking.Namespace = "catalog")
    .UseRedis()
    .AddHealthChecks();
```

```csharp
public sealed class CatalogService(ICache cache, IDistributedLock locks)
{
    public ValueTask<Catalog?> GetAsync(string region, CancellationToken ct) =>
        cache.GetOrCreateAsync(
            $"catalog:{region}", region,
            static (region, token) => LoadAsync(region, token),
            new CacheEntryOptions(TimeSpan.FromMinutes(10)) { Tags = ["catalog"] }, ct);

    public ValueTask RefreshAsync(CancellationToken ct) =>
        locks.ExecuteWithLockAsync("catalog:refresh", RebuildAsync, cancellationToken: ct);
}
```

A distributed-store failure never reaches a consumer as an exception from a read or a
get-or-create: the cache serves from the in-process tier and the factory, records a `degraded`
outcome, and logs one warning per key per interval. The lock throws a typed
`LockProviderUnavailableException` instead, because a lock that cannot be taken must not pretend
it was. See the [caching](docs/reference/caching.md) and [locking](docs/reference/locking.md)
references, [Cache and lock over Redis](docs/how-to/use-redis.md), and
[Keep serving when the cache backend is down](docs/how-to/cache-and-lock-fail-open.md).

BenchmarkDotNet comparisons cover HostLoom against Microsoft `HybridCache` and FusionCache for
cache paths, and against Medallion `DistributedLock.Redis` for Redis locking. The deterministic
HostLoom-only cases have a committed, machine-checked 10% regression gate. See the
[benchmark guide](benchmarks/HostLoom.Benchmarks/README.md).

## Logging

`HostLoom.Logging` is a structured `Microsoft.Extensions.Logging` provider: a bounded queue
with a dedicated background writer, typed JSON fields from ordinary `ILogger` template holes
and from the allocation-free `LogFast` interpolated path, Serilog-compatible `{@...}`
destructuring with fail-closed `[NotLogged]`/`[LogMasked]` protection, scopes, enrichers,
and health metrics on the `HostLoom.Logging` meter.

```csharp
builder.Logging.AddHostLoomLogging(
    StreamLogSink.Console(),
    builder.Configuration.GetSection("HostLoom:Logging"),
    formatter: new ClefLogFormatter());
```

Level filtering is standard MEL configuration and runs before the provider — HostLoom does
no level filtering of its own. Migrating from Serilog's section, `MinimumLevel:Default`
becomes `Logging:LogLevel:Default` and each `MinimumLevel:Override:<prefix>` becomes
`Logging:LogLevel:<prefix>`. Provider options bind from `HostLoom:Logging`; a code callback,
when supplied, applies after configuration, and invalid values fail at host startup:

```json
{
  "Logging": { "LogLevel": { "Default": "Information", "Npgsql": "Warning" } },
  "HostLoom": {
    "Logging": {
      "QueueCapacity": 8192,
      "QueueFullPolicy": "DropBelowWarning",
      "EnqueueTimeout": "00:00:02",
      "ShutdownTimeout": "00:00:05",
      "ServiceName": "checkout",
      "Destructuring": { "MaxDepth": 5, "MaxStringLength": 4096 }
    }
  }
}
```

Before the host exists, `HostLoomBootstrapLogger` writes the same event shape synchronously
to stdout — same formatter, masking policy, timestamps, and static fields — with a minimum
level supplied at construction. Dispose it once the hosted provider is up; it retains
nothing, so the hand-off neither replays nor duplicates events.

## Composition diagnostics

Registration builds a plan and executes nothing, so the moment a branch is taken is the
one moment with no logger, no bound options, and no filter configuration — which is why
composition logging so often ends up as a static `Log.Debug` call that fires before the
real sinks and levels exist. `HostLoom.Diagnostics` inverts that: registration records
its decisions into a ledger, and the whole plan is reported once at startup, through the
application's own logging stack.

```csharp
using HostLoom.Diagnostics;

public static IServiceCollection AddOrderPublishing(
    this IServiceCollection services, OrderOptions options)
{
    if (options.Kafka.Enabled)
    {
        services.AddSingleton<IOrderPublisher, KafkaOrderPublisher>();
        services.RecordComposition("OrderPublisher", "Kafka", "Orders:Kafka:Enabled=true");
    }
    else
    {
        services.AddSingleton<IOrderPublisher, InProcessOrderPublisher>();
        services.RecordComposition("OrderPublisher", "InProcess", "Orders:Kafka:Enabled=false");
    }

    if (options.Outbox is null)
    {
        services.RecordSkippedComposition("Outbox", "no Orders:Outbox section bound");
    }

    return services;
}
```

`AddCompositionDiagnostics()` turns on the report; without it nothing is written, so a
library can record unconditionally. The opt-in may appear anywhere in the composition
root, because the report is taken when the host starts rather than when diagnostics are
switched on:

```csharp
builder.Services.AddCompositionDiagnostics();
```

```text
info: HostLoom.Diagnostics.Composition
      HostLoom composition: OrderPublisher=Kafka | Outbox=(skipped) | Scheduler=Quartz
```

One `Information` line carries the whole manifest, because a composition question is
asked when production misbehaves — exactly when a `Debug` line has already been filtered
out. `Debug` adds one line per decision with its reason and the registration method that
recorded it, captured automatically from the call site. A component recorded twice with
choices that disagree raises a `Warning` naming both, without guessing which one the
container resolved. Everything is written under the
`HostLoom.Diagnostics.Composition` category, so standard `Logging` configuration raises,
lowers, or silences it.

Recording a component that was skipped is the part worth the discipline: a log that only
reports what was registered cannot answer "what is missing", because the branch that did
nothing wrote nothing. Nothing else recovers that.

Use it selectively. A branch whose input is already visible in configuration rarely earns
an entry; the ones that do are components deliberately left out, and branches whose
mapping from configuration to registration is not a direct toggle. Nothing enforces the
calls, so an entry left behind by a branch that changed will misreport — keep each one
next to the registration it describes, and prefer no entry to a stale one.

The report is a plan, not a validation, and it is not the cheapest tool for every
question. Keep `ValidateOnBuild` and `ValidateScopes` on in development for unresolvable
dependencies, `ValidateOnStart` on options for settings that are absent or invalid, and
`((IConfigurationRoot)builder.Configuration).GetDebugView(…)` for which provider supplied
which value. Each of those is one line and stays correct on its own; the ledger is the
one that asks something of you in return.

The package stands alone and no other package depends on it, so an application that never
references it carries nothing. Pipeline topology is not recorded here — pipeline
registration logs each resolved topology itself when the host starts.

`CompositionLedger` and its `Snapshot()` are public, so a test can assert on what
registration decided instead of capturing log output to observe it.

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

The suite above is deterministic and in-process. The RabbitMQ and Kafka
adapters are additionally exercised against real brokers, which
`docker-compose.yml` provides:

```text
docker compose up -d
dotnet test tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj -c Release
docker compose down -v
```

Those tests skip themselves when the broker ports are closed, so the standard
gate stays green without Docker — and a skipped result is reported as skipped
rather than passed, because without a broker they prove nothing.

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
src/HostLoom.Diagnostics/        composition ledger, report, and startup reporter
src/HostLoom.Mapping/            dependency-free explicit mapping contracts
src/HostLoom.Mapping.DependencyInjection/ scoped dispatch and closed map registration
src/HostLoom.Mapping.Testing/    container-free mapper composition for tests
src/HostLoom.Caching/            two-tier cache kernel: contracts, in-process stores, serializer, TieredCache
src/HostLoom.Caching.DependencyInjection/ cache registration, validation, warmup, health checks
src/HostLoom.Caching.Testing/    container-free cache composition, recording and faulting stores
src/HostLoom.Caching.Pipelines/  cache and deduplication filters for generic pipelines
src/HostLoom.Locking/            distributed lock kernel: contracts, retry policy, in-process provider
src/HostLoom.Locking.DependencyInjection/ lock registration, validation, health checks
src/HostLoom.Locking.Testing/    container-free lock composition, scripted, recording, faulting providers
src/HostLoom.Locking.Pipelines/  distributed-lock filter for generic pipelines
src/HostLoom.Redis/              Redis store, invalidation channel, lock provider, owned connection
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
src/HostLoom.AspNetCore.WebSockets.Testing/ TestServer gateway integration client
benchmarks/HostLoom.Benchmarks/    cache, lock, codec, logging, and mapping benchmarks
benchmarks/HostLoom.Redis.Benchmarks/ real-Redis cache and lock comparisons
examples/HostLoom.Examples.Pipelines/ runnable pipeline tour: DI stages, manual and standalone composition
examples/HostLoom.Examples.CachingAot/ Native AOT sample for caching and locking
tests/HostLoom.Tests/            pipeline, round-trip, behavior, and fault tests
tests/HostLoom.Conformance/      backend-neutral cache and lock scenarios shared by the unit and integration suites
tests/HostLoom.IntegrationTests/ RabbitMQ and Kafka transports against real brokers
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
