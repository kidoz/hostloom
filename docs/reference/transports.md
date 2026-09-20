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
| `DurableTopics` | `true` | Topic exchanges/queues are durable and event messages are persistent |
| `QueueNaming` | `RabbitMqQueueNaming.Version2` | Role-qualified hashed request/subscription names; explicit `Legacy` supports migration |

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
| Subscription | named handler on the topic | durable V2 queue for the topic/subscription pair | consumer group |
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
  Publishing awaits broker confirmation; durable event messages are persistent.
  Confirmation establishes broker acceptance, not handler completion. An uncertain
  outcome can lead to duplicate delivery when the outbox retries.
- **RabbitMQ queue identity**: V2 request and event names have separate role prefixes
  and SHA-256 hashes of length-prefixed UTF-8 components. The public
  `RabbitMqQueueNames.Request` and `.Subscription` helpers give the physical names.
  See the [queue migration procedure](../how-to/use-rabbitmq.md#migrate-existing-queues)
  before changing an existing deployment.
- **Kafka replies**: every client instance consumes the shared response
  stream under a unique consumer group and ignores replies it does not
  own; partition-affine reply routing is on the roadmap.
