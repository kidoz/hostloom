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
| `PublishTimeout` | 30 seconds | Event publication deadline, including channel acquisition and confirmation |
| `MaxConcurrentPublishes` | 16 | Maximum outstanding publications on exclusively owned channels |
| `PrefetchCount` | `16` | Unacknowledged deliveries per consumer |
| `DurableRequestQueues` | `true` | Request queues survive a broker restart |
| `DurableTopics` | `true` | Topic exchanges/queues are durable and event messages are persistent |
| `QueueNaming` | `RabbitMqQueueNaming.Version2` | Role-qualified hashed request/subscription names; explicit `Legacy` supports migration |
| `AllowNamedReplyQueues` | `false` | Answer a request whose `ReplyTo` names a declared queue. Off, only server-named queues (`amq.gen-…`) and `amq.rabbitmq.reply-to` are answered, which is what HostLoom's own client uses; anything else is rejected as malformed before the handler runs |
| `DeadLetterExchange` | `null` | When set, request and subscription queues are declared with `x-dead-letter-exchange`, so rejected deliveries are routed there. Declare the exchange yourself; changing it on an existing queue fails the declaration |

## KafkaOptions

| Option | Default | Meaning |
| --- | --- | --- |
| `BootstrapServers` | `localhost:9092` | Broker bootstrap list |
| `ConsumerGroup` | `hostloom` | Stable group prefix shared by instances of the same logical service |
| `ResponseTopic` | `hostloom.responses` | Topic on which this client receives replies; provision retention ≥ the maximum request timeout, one topic per calling service |
| `ClientId` | `{machine}-{pid}-{random}` | Client identifier reported to the broker |
| `EnableIdempotence` | `true` | Idempotent producer |
| `AllowedReplyTopics` | empty | Reply topics a request may name in `hostloom-reply-to`. Empty accepts any syntactically valid topic name; otherwise the header must match an entry exactly, or the request is rejected as malformed before the handler runs |
| `MaxRequestAge` | `null` | When set, a request record whose Kafka timestamp is older than this is rejected as malformed (committed, never handled) |
| `SecurityProtocol` | `null` | `Plaintext`, `Ssl`, `SaslPlaintext`, or `SaslSsl`; unset leaves the client library's plaintext default |
| `SaslMechanism` | `null` | SASL mechanism; requires a SASL `SecurityProtocol` |
| `SaslUsername`, `SaslPassword` | `null` | SASL credentials; require a SASL `SecurityProtocol`, else the transport refuses to start rather than connect unauthenticated. The password is never logged or included in an exception |
| `SslCaLocation` | `null` | CA bundle that signs the brokers' certificates |
| `ConfigureClient` | `null` | `Action<ClientConfig>` run on every producer and consumer configuration after the options above, for client certificates, OAuth bearer, and tuning |

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
| Broker health probe | `IBrokerHealthProbe` | yes | not yet | local startup state |

A transport without `IEventBroker` rejects publishing (throws) and fails
subscription registration at startup. A transport without
`IBrokerHealthProbe` is treated as reachable by the readiness check —
RabbitMQ therefore cannot currently report a broker outage through this contract. Kafka
reports reply-consumer initialization failure and pending assignment; its probe does not
perform a broker connectivity check.

## Behavioral differences worth knowing

- **In-memory publishing** awaits local deliveries for deterministic tests, but logs
  subscriber failures without propagating them to the publisher or triggering outbox retries.
  Caller cancellation ends its wait; accepted handler work uses the listener lifetime token.
  Unbound requests wait for their timeout. This transport remains process-local and non-durable.
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
- **Kafka replies**: every client instance consumes the response topic
  under a unique consumer group, starting at the end of the topic, and
  ignores replies it does not own; the first request waits, within its
  own timeout, for the reply consumer to be assigned partitions, so a
  restart never replays the retained response stream. Partition-affine
  reply routing is on the roadmap.
- **Kafka reply routing**: the `hostloom-reply-to` header is validated
  against Kafka's topic-name rules and `AllowedReplyTopics` before the
  handler runs. A reply that cannot be produced after the handler ran is
  logged with the exception type and the request is committed past, not
  re-run: the handler's side effects would repeat for an answer that
  still had nowhere to go.
- **RabbitMQ cancellation**: a delivery cancelled in flight (the listener
  is stopping, or the client library cancelled it) is nacked with requeue;
  every other failure is rejected without requeue, to
  `DeadLetterExchange` when one is set.
