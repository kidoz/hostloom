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

## 3. Decide who may be answered, and where rejections go

A request's `ReplyTo` becomes a routing key on the default exchange, so it
decides which queue the handler's reply is written to. By default a
listener answers only server-named reply queues (`amq.gen-…`), which is
what HostLoom's own client declares, and clients using direct reply-to:
a client that sets `amq.rabbitmq.reply-to` reaches the listener as
`amq.rabbitmq.reply-to.<token>`, the address the broker rewrote it to. A
request naming any other queue is rejected as malformed before its
handler runs. Set `AllowNamedReplyQueues` only for a foreign client that
replies through a queue it declared itself.

A rejected delivery — a handler that failed, a malformed frame, a request
with an unacceptable `ReplyTo` — is dropped unless the queue has a
dead-letter exchange. Declare one and name it:

```csharp
.UseRabbitMq(options =>
{
    options.Uri = new Uri("amqps://greetings:secret@rabbit.internal:5671/");
    options.DeadLetterExchange = "hostloom.dead-letters";
})
```

Request and subscription queues are then declared with
`x-dead-letter-exchange`. Existing queues cannot have their arguments
changed: delete or migrate them first, following the procedure below. A
delivery cancelled while in flight, because the listener is stopping or
the client library cancelled it, is nacked with requeue instead, so a
shutdown never dead-letters healthy work.

## 4. Verify

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
  `Uri`; check `docker compose ps` and the port (5672). Starting a listener
  fails the host with the client library's `BrokerUnreachableException`; a
  request or publication reports it as `MessagingTransportException`.
- **`MessagingTransportException`** — the transport could not carry the
  message: the broker was unreachable, closed the connection or channel, or
  nacked the publication. The client library's exception is
  `InnerException`. Whether the broker accepted the message is unknown, so a
  retry can deliver it twice.
- **`TimeoutException` from publishing** — `PublishTimeout` elapsed before
  the broker confirmed the event; the broker may still have accepted it. A
  rising `hostloom.rabbitmq.channels.closing` alongside it means the broker
  is not answering at all.
- **`RemoteRequestException`** — the request arrived and the handler
  threw; `ErrorType` is `HandlerFault` unless the handler threw a
  `RemoteFaultException` or `HostLoomOptions.IncludeFaultDetails` is on.
  The real exception is in the handler application's log.
- **Requests rejected without the handler running** — the caller's
  `ReplyTo` is neither a server-named queue nor a direct reply-to address;
  a foreign client replying through a queue it declared needs
  `AllowNamedReplyQueues`.
- **`RabbitMqConsumerCancelled` warnings** — something deleted a
  listener's or subscription's queue, or made it unavailable, and the
  broker cancelled the consumer. The transport declares the queue again
  and resubscribes; `hostloom.rabbitmq.consumers` counts both events.
  Messages that were in a deleted queue are lost. Repeated
  `RabbitMqConsumerRestoreFailed` warnings mean the queue cannot be
  declared yet, for example because the node that hosts it is down.
- **`RabbitMqHandlersAbandoned` and `RabbitMqDeliveryUnsettled` warnings
  at shutdown** — a handler ignored its cancellation for longer than the
  five seconds a stop waits, so its listener or subscription stopped
  without it. The broker redelivers that delivery, and the acknowledgement
  or reply the handler sends when it finishes is refused by the closed
  channel, which is what the second warning reports; the handler's work
  may run twice. Make long-running handlers observe their token.
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

Event publication has a `PublishTimeout` of 30 seconds, including waiting for a publisher
channel and broker confirmation. `MaxConcurrentPublishes` defaults to 16; each outstanding
publication exclusively owns a channel. The exclusive reply queue uses a separate channel.
A stalled confirmation therefore does not serialize all publication behind one round trip,
though a broker resource alarm can still prevent the broker accepting any publication.
Request deadlines bound publication and reply waiting.

A channel whose publication was not confirmed is never reused: a late confirmation could
still arrive on it. It is closed in the background, so the call returns at its deadline
rather than after the close, which waits for the broker's reply for up to the client
library's 20-second continuation timeout. At most `MaxConcurrentPublishes` such channels may
still be closing before a publication that needs a new channel waits for one to finish,
again within its own deadline; `hostloom.rabbitmq.channels.closing` reports how many are.

Stopping a listener or subscription asks the broker to stop delivering to it and cancels the
handlers in flight, then waits up to five seconds for them before it closes the channel. A
handler that honours its cancellation has its delivery requeued; one that finishes anyway is
answered and acknowledged as usual, because the channel is still open. A handler still
running after five seconds is left running, and its delivery goes back to the queue when the
channel closes. The close itself runs in the background, like a discarded publisher channel's,
so a host stopping against an unresponsive broker does not wait twenty seconds per consumer.
Disposing the transport waits at most five seconds for channels still closing and at most two
more for the connection, so against an unresponsive broker it returns after about seven
seconds while the client library finishes closing the connection in the background.

On the consuming side, `RequestDispatchConcurrency` (default 16) is how many deliveries a
request listener's channel hands to its handler at once, and `EventDispatchConcurrency`
(default 1) is the same for an event subscription. Both are bounded by `PrefetchCount`,
because the broker never has more deliveries in flight on the channel than that. Requests
are independent of one another, so they run in parallel by default; a subscription keeps
its events in queue order only while its value is 1, and raising it means the handlers
overlap and may finish out of order. Neither setting throttles the handler itself: to cap
how many run at once inside the process, add `UseConcurrencyLimit` to the receive pipeline
rather than lowering the dispatch concurrency, which would also hold back the prefetched
deliveries behind it.

Rejected deliveries log their exception and whether a dead-letter exchange is configured.
Configure `DeadLetterExchange` to retain rejected messages; logging alone does not retain them.
