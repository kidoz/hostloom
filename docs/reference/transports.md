# Transports

Entry points, options, and topology mapping for the three transport
adapters. One transport is registered per application; a second
registration fails.

| Package | Entry point | Options type |
| --- | --- | --- |
| `HostLoom.Transport.InMemory` | `UseInMemory()` | — |
| `HostLoom.Transport.RabbitMq` | `UseRabbitMq(Action<RabbitMqOptions>?)` | `RabbitMqOptions` |
| `HostLoom.Transport.Kafka` | `UseKafka(Action<KafkaOptions>?)` | `KafkaOptions` |

A custom transport registers with
`UseTransport<TBroker>() where TBroker : class, IRequestBroker`.

## RabbitMqOptions

| Option | Default | Meaning |
| --- | --- | --- |
| `Uri` | `amqp://guest:guest@localhost:5672/` | Broker connection URI |
| `ClientProvidedName` | `hostloom-{machine}-{pid}` | Connection name shown in the management UI |
| `PrefetchCount` | `16` | Unacknowledged deliveries per consumer |
| `DurableRequestQueues` | `true` | Request queues survive a broker restart |
| `DurableTopics` | `true` | Topic exchanges and subscription queues are durable |

## KafkaOptions

| Option | Default | Meaning |
| --- | --- | --- |
| `BootstrapServers` | `localhost:9092` | Broker bootstrap list |
| `ConsumerGroup` | `hostloom` | Stable group prefix shared by instances of the same logical service |
| `ResponseTopic` | `hostloom.responses` | Topic on which this client receives replies; provision retention ≥ the maximum request timeout |
| `ClientId` | `{machine}-{pid}-{random}` | Client identifier reported to the broker |
| `EnableIdempotence` | `true` | Idempotent producer |

## Topology mapping

| Concept | In-memory | RabbitMQ | Kafka |
| --- | --- | --- | --- |
| Request address | direct dispatch | durable request queue | request topic |
| Reply path | direct | exclusive reply queue per client | `ResponseTopic`, unique consumer group per client instance |
| Correlation | in envelope | AMQP `CorrelationId` + `ReplyTo` | Kafka headers |
| Event topic | in-process channel | fanout exchange | Kafka topic |
| Subscription | named handler on the topic | durable queue `topic.subscription` | consumer group |
| Cross-subscription order | unspecified | unspecified | unspecified |
| Ordering within a subscription | delivery order | queue order | per partition only (records produced without a key) |

Rationale for the differences: [transport semantics](../explanation/transports.md).

## Capabilities

| Capability | Contract | In-memory | RabbitMQ | Kafka |
| --- | --- | --- | --- | --- |
| Request/response | `IRequestBroker` | yes | yes | yes |
| Publish/subscribe | `IEventBroker` | yes | yes | yes |
| Broker health probe | `IBrokerHealthProbe` | yes | not yet | not yet |

A transport without `IEventBroker` rejects publishing (throws) and fails
subscription registration at startup. A transport without
`IBrokerHealthProbe` is treated as reachable by the readiness check —
for RabbitMQ and Kafka this means readiness cannot currently detect a
broker outage that begins after startup.

## Behavioral differences worth knowing

- **In-memory publishing** attempts every subscription even when one
  throws, then propagates the failures to the publisher as an
  `AggregateException`. Broker-backed publishing decouples the publisher
  from subscribers entirely; a publish never observes a handler failure.
- **RabbitMQ events** publish with no routing key and without
  `mandatory`: an event with no subscribers is dropped, not an error.
- **Kafka replies**: every client instance consumes the shared response
  stream under a unique consumer group and ignores replies it does not
  own; partition-affine reply routing is on the roadmap.
