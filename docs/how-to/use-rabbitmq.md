# Run over RabbitMQ

Move a HostLoom application from the in-memory transport onto RabbitMQ.
The contracts, handlers, and clients do not change — only the transport
registration.

## Before you begin

- A working HostLoom application (the
  [getting-started tutorial](../tutorials/getting-started.md) builds one).
- A reachable RabbitMQ broker. For local work, the repository's
  `docker-compose.yml` provides one:

```text
docker compose up -d
```

## 1. Install the package

```text
dotnet add package HostLoom.Transport.RabbitMq
```

## 2. Swap the transport registration

Replace `UseInMemory()` with `UseRabbitMq(...)`:

```csharp
using HostLoom.Transport.RabbitMq;

builder.Services
    .AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(10))
    .UseRabbitMq(options => options.Uri = new Uri("amqp://guest:guest@localhost:5672/"))
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");
```

A client-only application registers the contract without a local handler:

```csharp
builder.Services
    .AddHostLoom()
    .UseRabbitMq()
    .AddRequestClient<GetGreeting, Greeting>();
```

All options and defaults: [transports reference](../reference/transports.md#rabbitmqoptions).

## 3. Verify

Run the application and send a request as in the tutorial — the reply
arrives exactly as before. Then confirm the topology in the RabbitMQ
management UI (`http://localhost:15672`, `guest`/`guest`): the logical
address maps to the durable queue returned by `RabbitMqQueueNames.Request("greetings")`,
with a `hostloom.v2.request.` prefix. While the app
runs, each client holds an exclusive reply queue. The connection is named
`hostloom-{machine}-{pid}` unless you set `ClientProvidedName`.

## Migrate existing queues

The default `QueueNaming` is now `RabbitMqQueueNaming.Version2`. Request producers
and listeners must agree on the mode. Logical contract addresses and event exchange
names stay unchanged; physical request and event subscription queue names change.
V2 separates request/event roles and hashes unambiguous length-prefixed UTF-8 inputs,
so dotted names such as `(catalog.eu, updates)` and `(catalog, eu.updates)` no longer
share an event queue. Names are bounded ASCII; SHA-256 provides practical collision
resistance. Invalid UTF-16 input is rejected instead of silently normalized.

An existing deployment can explicitly select `RabbitMqQueueNaming.Legacy` while
preparing migration. Legacy retains raw request names and `topic.subscription` event
names, including their collision risk. Do not mix request naming modes during a
rolling deployment.

1. Pause new request/event producers and the outbox relay. Leave unrelayed records
   in the application outbox.
2. Drain existing queues and in-flight request replies, then stop old consumers.
3. Deploy matching V2 request producers/listeners and establish V2 event subscriptions.
4. Verify old queues are empty, then retire their bindings and queues through your
   normal administration procedure. HostLoom never deletes or migrates them automatically.
5. Resume producers and the relay; verify request round trips and each subscription.

Do not consume legacy and V2 event queues concurrently as a migration shortcut: both
can receive copies of a new event. Rollback likewise requires pausing traffic and
draining V2 backlogs before restoring legacy routes. Reverting the option alone can
strand queued messages. Use `RabbitMqQueueNames.Request(address, mode)` and
`RabbitMqQueueNames.Subscription(topic, subscription, mode)` to inventory both schemes.

Event publishing now waits for tracked broker confirmations. With `DurableTopics=true`,
events are persistent; a successful confirmation does not prove consumer processing.
A lost confirmation can leave acceptance unknown, so outbox retries remain at-least-once.
Events without any subscriptions retain ordinary fan-out discard behavior.

## Troubleshoot

- **`RequestTimeoutException` on every request** — no listener on the
  queue: the handler application is not running, or client and handler
  disagree on the address string or queue naming mode.
- **Connection refused at startup** — broker not reachable at
  `Uri`; check `docker compose ps` and the port (5672).
- **`RemoteRequestException`** — the request arrived and the handler
  threw; the exception's `ErrorType` names the remote exception type.
- **A broker outage after startup is not reflected in readiness** — the
  RabbitMQ adapter does not yet implement `IBrokerHealthProbe`; see
  [health checks](health-and-metrics.md).

## Related

- Retry and circuit breaking for deliveries:
  [Harden the receive pipeline](harden-receive-pipeline.md) — note that
  in-process retry never moves a broker acknowledgement; redelivery is
  the broker's concern.
- What the address becomes on the wire, and why RabbitMQ and Kafka
  topologies differ: [transport semantics](../explanation/transports.md)
  and the [transports reference](../reference/transports.md).
- Integration tests against a real broker:
  `tests/HostLoom.IntegrationTests` (they skip, and report as skipped,
  when broker ports are closed).
