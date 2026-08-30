# Honest transport semantics

A request address in HostLoom is a logical name. Each transport adapter
maps it onto its *own* topology — and the differences between those
topologies are deliberately visible. This page explains why.

## One API, not one topology

A messaging abstraction can present every broker as interchangeable: one
topology diagram, one set of guarantees, brokers swapped like drivers. A
uniform topology, though, hides differences in recovery, ordering,
retention, and capacity planning — exactly the properties an operator
needs to reason about under load and during outages, where RabbitMQ and
Kafka genuinely differ. HostLoom keeps the *application API* common while
letting each adapter own its transport protocol, so those differences stay
visible where they matter.

## Request/response per transport

**RabbitMQ** natively supports the request/reply shape. The address
becomes a durable request queue; each client opens an exclusive reply
queue and correlates replies through the AMQP `CorrelationId` and
`ReplyTo` properties. Nothing is emulated.

**Kafka** is a durable partitioned log — request/reply there is an
*application protocol*, and HostLoom is explicit about building one: the
address becomes a request topic, replies go to a configured response
topic, and correlation travels in headers. Consequences follow honestly:
the response topic needs retention sized to the maximum request timeout,
and every client instance uses a unique response consumer group, seeing
the shared response stream and ignoring replies it does not own. That is
correct for an initial implementation, not the final high-scale topology;
partition-affine reply routing is on the roadmap — and the documentation
says so rather than implying finished scale.

**In-memory** is direct, deterministic dispatch — the same envelope and
the same receive pipeline, which is what makes it a faithful stand-in for
tests and local composition.

## Publish/subscribe per transport

A *subscription* is HostLoom's unit of independent consumption. Each
transport maps it onto its own fan-out primitive:

- **In-memory** — a named handler on the topic, delivered to in process.
  Two behaviors differ from the networked brokers, both deliberate for a
  transport whose job is tests and local composition: cross-subscription
  delivery order is unspecified (subscriptions live in a concurrent map),
  and `PublishAsync` attempts *every* subscription even when an earlier
  one throws, then propagates the failures to the publisher as an
  `AggregateException` — so a local run surfaces handler failures instead
  of swallowing them. A networked broker decouples the publisher from its
  subscribers entirely; a publish there never observes a handler failure.
- **RabbitMQ** — a fanout exchange per topic and a durable queue named
  `topic.subscription` bound to it, so subscriptions accumulate their own
  backlog rather than competing for one queue. Events publish with no
  routing key and without `mandatory` — an event nobody subscribes to is
  dropped, not an error.
- **Kafka** — each subscription is its own consumer group: every group
  receives every record, while instances sharing a group divide the
  partitions. Records are produced without a key, so ordering holds within
  a partition only.

The rules that hold everywhere: distinct subscription names each receive
every event; handlers sharing a subscription name share one delivery and
one scope; a subscription without a handler for a contract ignores it.

## Capability, not lowest common denominator

Publish/subscribe is a separate capability (`IEventBroker`), not a method
every transport must fake. A transport that lacks it fails fast: publishing
throws, and a subscription registration fails at startup instead of
starting up *looking* subscribed while nothing is delivered. The same
stance appears in health reporting — a transport that cannot report broker
reachability is treated as reachable, because "cannot tell" must not read
as "broken".
