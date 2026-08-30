# Run over Kafka

Run HostLoom request/response and publish/subscribe over Kafka. The
application code does not change — only the transport registration and
one piece of provisioning: the response topic.

## Before you begin

- A working HostLoom application (the
  [getting-started tutorial](../tutorials/getting-started.md) builds one).
- A reachable Kafka broker. For local work:

```text
docker compose up -d
```

- **A provisioned response topic** with retention at least as long as
  your maximum request timeout, or a slow reply can expire before the
  caller reads it. The default name is `hostloom.responses`.

## 1. Install the package

```text
dotnet add package HostLoom.Transport.Kafka
```

## 2. Swap the transport registration

```csharp
using HostLoom.Transport.Kafka;

builder.Services
    .AddHostLoom()
    .UseKafka(options =>
    {
        options.BootstrapServers = "localhost:9092";
        options.ConsumerGroup = "greetings-service";
        options.ResponseTopic = "hostloom.responses";
    })
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");
```

All options and defaults: [transports reference](../reference/transports.md#kafkaoptions).

## 3. Verify

Run the application and send a request — the reply arrives as before.
On the broker you should now see the request topic (`greetings`), the
response topic, and a consumer group per subscription plus a unique
response group per client instance (`kafka-consumer-groups --list`, or
your Kafka UI of choice).

## Troubleshoot

- **`RequestTimeoutException` on every request** — the handler
  application is not consuming the request topic, or the response topic
  is missing so replies have nowhere to go.
- **Replies lost after long processing** — response-topic retention is
  shorter than the request timeout; re-provision it.
- **Events arrive on only one instance** — instances share a consumer
  group and are dividing partitions; give each *subscription* its own
  name if every instance must see every event.
- **Out-of-order events** — records are produced without a key, so
  ordering holds within a partition only.
- **A broker outage after startup is not reflected in readiness** — the
  Kafka adapter does not yet implement `IBrokerHealthProbe`; see
  [health checks](health-and-metrics.md).

## Related

- Retry and circuit breaking for deliveries:
  [Harden the receive pipeline](harden-receive-pipeline.md) — in-process
  retry never moves a consumer offset.
- Why Kafka request/reply is an application protocol, and the current
  reply topology's limits: [transport semantics](../explanation/transports.md).
- Integration tests against a real broker:
  `tests/HostLoom.IntegrationTests`.
