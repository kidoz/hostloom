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
address appears as a durable queue named `greetings`, and while the app
runs, each client holds an exclusive reply queue. The connection is named
`hostloom-{machine}-{pid}` unless you set `ClientProvidedName`.

## Troubleshoot

- **`RequestTimeoutException` on every request** — no listener on the
  queue: the handler application is not running, or client and handler
  disagree on the address string.
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
