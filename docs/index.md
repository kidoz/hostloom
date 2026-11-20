# HostLoom

HostLoom lets .NET services exchange typed requests and events over
in-memory, RabbitMQ, or Kafka transports without coupling application code
to a broker SDK — and ships the same composable middleware pipeline it is
built on as a standalone package.

Use it to build:

- **Request/response services** — a typed contract, a handler, a client;
  the compiler catches a mismatched reply, and the transport is a
  one-line registration.
- **Event-driven services** — typed events published to topics and fanned
  out to named, independent subscriptions.
- **Processing pipelines** — retry, circuit breaking, rate limits,
  timeouts, and conditional branches composed as filters, with or without
  a message broker, testable without real waiting.

HostLoom targets .NET 10 and C# 14 and is used in production. Each application
can choose the packages it needs. See the [architecture overview](explanation/architecture.md).

```csharp
builder.Services
    .AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(10))
    .UseRabbitMq(options => options.Uri = new Uri("amqp://guest:guest@localhost:5672/"))
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");
```

## Choose your path

**Building messaging services?** Start with
[Your first request/response](tutorials/getting-started.md), continue to
[Publish and subscribe](tutorials/publish-subscribe.md), expose local live state with the
[in-memory WebSocket gateway](how-to/stream-process-local-events-to-websockets.md), then move to a
real broker with [RabbitMQ](how-to/use-rabbitmq.md) or
[Kafka](how-to/use-kafka.md) and harden delivery with the
[receive pipeline](how-to/harden-receive-pipeline.md).

**Building standalone pipelines?** Start with
[Building a standalone pipeline](tutorials/first-pipeline.md), then
[register pipelines with stages](how-to/register-pipelines.md) and
[test them deterministically](how-to/test-pipelines.md).

**Replacing registration discovery?** Read the
[composition reference](reference/composition.md), follow the
[migration guide](how-to/migrate-composition.md), and review
[measured startup and compiler costs](reference/composition-performance.md).

**Sharing sign-in cookies across pods?** Configure a [shared Data Protection key ring](how-to/share-data-protection-keys.md).

**Sharing an API quota across pods?** Configure an [incoming endpoint policy](reference/rate-limiting.md#incoming-endpoint-policies)
or an [outgoing HTTP handler](reference/rate-limiting.md#outgoing-http-behavior).

## How this documentation is organized

The documentation follows the [Diátaxis](https://diataxis.fr/) model: each
section serves one distinct reader need.

<div class="grid cards" markdown>

- **[Tutorials](tutorials/getting-started.md)** — lessons that take you
  from an empty project to a working result. Start here if HostLoom is new
  to you.

- **[How-to guides](how-to/use-rabbitmq.md)** — recipes for specific
  goals: moving to a real broker, hardening the receive pipeline, testing,
  health probes. Come here when you are mid-task.

- **[Reference](reference/packages.md)** — the factual surface: packages,
  APIs, options, analyzer rules, metrics, the wire envelope. Look things
  up here while working.

- **[Explanation](explanation/architecture.md)** — why HostLoom is shaped
  the way it is: package layering, transport semantics, the pipeline
  model, fault behavior. Read here to build a mental model.

</div>

## Requirements

- [.NET SDK 10.0.400](https://dotnet.microsoft.com/download/dotnet/10.0)
  or later.
- A RabbitMQ or Kafka broker only when those transports are used;
  `HostLoom.Pipelines` and the in-memory transport have no external
  dependencies.
