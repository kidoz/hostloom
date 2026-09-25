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

## 3. Secure the connection

The client library defaults to plaintext with no authentication. For
anything but a local broker, set the security options; the transport
applies them to every producer and consumer it builds, and refuses to
start when SASL credentials are given without a SASL protocol rather than
connect unauthenticated.

```csharp
using Confluent.Kafka;
using HostLoom.Transport.Kafka;

builder.Services
    .AddHostLoom()
    .UseKafka(options =>
    {
        options.BootstrapServers = "broker-1:9093,broker-2:9093";
        options.ConsumerGroup = "greetings-service";
        options.ResponseTopic = "greetings-service.replies";
        options.SecurityProtocol = SecurityProtocol.SaslSsl;
        options.SaslMechanism = SaslMechanism.ScramSha512;
        options.SaslUsername = builder.Configuration["Kafka:Username"];
        options.SaslPassword = builder.Configuration["Kafka:Password"];
        options.SslCaLocation = "/etc/kafka/ca.pem";
        // Anything the client library supports, applied last:
        options.ConfigureClient = client =>
        {
            client.SslCertificateLocation = "/etc/kafka/client.pem";
            client.SslKeyLocation = "/etc/kafka/client.key";
        };
    })
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");
```

Read the password from configuration or a secret store; the transport never
logs it or puts it in an exception.

Give each calling service its own response topic, named for the service.
The reply consumer resolves the end of each initially assigned partition before any request
is sent. Reassignments resume local progress; newly added partitions are read from the
beginning to avoid skipping pending replies. If the initial watermark query fails, requests
fail with `MessagingTransportException` without publishing; the failed consumer is released
and the next request retries initialization. Readiness reports the failed or pending initialization. The ACL model stays legible: a handler service needs `Write` on the
response topics of the services that call it and `Read` on its request
topics; a calling service needs `Write` on the request topics it calls
and `Read` on its own response topic. A handler service can pin that
down with `AllowedReplyTopics`, so a request naming any other topic in its
`hostloom-reply-to` header is rejected before the handler runs:

```csharp
options.AllowedReplyTopics.Add("orders-service.replies");
options.AllowedReplyTopics.Add("billing-service.replies");
```

Without the allow list the header is still checked against Kafka's
topic-name rules. `MaxRequestAge` additionally rejects a request record
older than the given age, so a retained or replayed request stream cannot
be pushed through the handlers again.

The request timeout covers reply-consumer startup and partition assignment, producing the
request, and waiting for its reply as one deadline. Caller cancellation remains cancellation;
disposing the transport ends pending requests with `ObjectDisposedException`. A timeout or
cancellation cannot retract a record already accepted by Kafka. Publishing an event has no
deadline of its own: a record the producer cannot deliver fails with
`MessagingTransportException` once the producer's `message.timeout.ms` runs out, five minutes
unless `ConfigureClient` sets it. Disposing the transport ends a publication still waiting
for its delivery report with `ObjectDisposedException`, even one called without a
cancellation token; the producer is still flushed for up to five seconds, so the event may
yet be delivered.

## 4. Verify

Run the application and send a request — the reply arrives as before.
On the broker you should now see the request topic (`greetings`), the
response topic, and a consumer group per subscription plus a unique
response group per client instance (`kafka-consumer-groups --list`, or
your Kafka UI of choice).

To watch requests fail within their timeout while the broker is frozen and
the same host recover once it resumes, run the opt-in outage experiment; it
starts, pauses, and removes a Kafka container of its own:

```text
HOSTLOOM_KAFKA_CHAOS=1 dotnet test tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj -c Release -- --filter-class '*KafkaOutageTests'
```

## Troubleshoot

- **`RequestTimeoutException` on every request** — the handler
  application is not consuming the request topic, or the response topic
  is missing so replies have nowhere to go. When the inner exception says
  the reply consumer was not assigned any partition, the response topic
  does not exist or this client may not read it.
- **Requests skipped as malformed (`KafkaRecordMalformed`, 1422) naming
  "hostloom-reply-to"** — the caller's response topic is not a legal topic
  name or is not in `AllowedReplyTopics`; the request is committed without
  running the handler.
- **`KafkaReplyUnroutable` (1423) in the handler's log** — the handler ran
  but the reply could not be written to the caller's response topic
  (missing topic, no `Write` ACL); the request is committed and not re-run,
  and the caller times out.
- **Repeated `KafkaRecordRewound` (1424)** — a handler keeps failing on the
  same record, which holds its partition. A request is skipped after five
  attempts (`KafkaRecordAttemptsExhausted`, 1425); an event is retried until
  it succeeds or the subscription stops. Every consumer-loop log line has
  an event id of its own, listed in the
  [observability reference](../reference/observability.md#transport-log-events).
- **`MessagingTransportException`** — the producer could not deliver the
  request or event, or the reply consumer could not start; the client
  library's `KafkaException` or `ProduceException` is `InnerException`. A
  failed reply-consumer start is retried by the next request, and readiness
  reports it until then.
- **Replies lost after long processing** — response-topic retention is
  shorter than the request timeout; re-provision it.
- **Events arrive on only one instance** — instances share a consumer
  group and are dividing partitions; give each *subscription* its own
  name if every instance must see every event.
- **Out-of-order events** — records are produced without a key, so
  ordering holds within a partition only.
- **A broker outage after startup is not reflected in readiness** — the
  Kafka adapter's `IBrokerHealthProbe` reports only its local reply-consumer
  state: unhealthy while the consumer awaits its assignment or after it failed
  to start, and healthy otherwise without contacting the broker. Requests
  during a later outage fail with `RequestTimeoutException`; see
  [health checks](health-and-metrics.md).

## Related

- Retry and circuit breaking for deliveries:
  [Harden the receive pipeline](harden-receive-pipeline.md) — in-process
  retry never moves a consumer offset.
- Why Kafka request/reply is an application protocol, and the current
  reply topology's limits: [transport semantics](../explanation/transports.md).
- Integration tests against a real broker:
  `tests/HostLoom.IntegrationTests`.

Successful handling and offset commit are separate failure boundaries: a commit error is logged
(`KafkaCommitFailed`, 1427) without locally rewinding completed work. Kafka can still redeliver uncommitted records after
reassignment, so handlers must tolerate duplicates. Event application failures retry until
shutdown; the adapter does not discard them after five attempts. A failing event can hold its
partition behind that offset. Malformed records remain logged and skipped.
