# Observability surface

Every name an OpenTelemetry (or plain `System.Diagnostics.Metrics`)
configuration needs.

## Sources and meters

| Name | Kind | Published by |
| --- | --- | --- |
| `HostLoom` | `ActivitySource` and `Meter` | messaging runtime |
| `HostLoom.Pipelines` | `ActivitySource` and `Meter` | registered pipelines |
| `HostLoom.Logging` | `Meter` | logging provider health |
| `HostLoom.Caching` | `ActivitySource` and `Meter` | every `TieredCache` |
| `HostLoom.Locking` | `ActivitySource` and `Meter` | every `DistributedLock` |
| `HostLoom.Scheduling` | `ActivitySource` and `Meter` | every `Scheduler` |
| `HostLoom.Leadership` | `ActivitySource` and `Meter` | every `LeaderElector` |
| `HostLoom.Redis` | `ActivitySource` and `Meter` | the Redis connection |
| `HostLoom.Transport.RabbitMq` | `Meter` | the RabbitMQ transport |
| `HostLoom.Transport.Kafka` | `Meter` | the Kafka transport |
| `HostLoom.AspNetCore.WebSockets` | `ActivitySource` and `Meter` | raw WebSocket gateway |

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter(
        "HostLoom", "HostLoom.Pipelines", "HostLoom.Logging", "HostLoom.Caching", "HostLoom.Locking", "HostLoom.Scheduling", "HostLoom.Leadership", "HostLoom.Redis",
        "HostLoom.Transport.RabbitMq", "HostLoom.Transport.Kafka", "HostLoom.AspNetCore.WebSockets"))
    .WithTracing(tracing => tracing.AddSource(
        "HostLoom", "HostLoom.Pipelines", "HostLoom.Caching", "HostLoom.Locking", "HostLoom.Scheduling", "HostLoom.Leadership",
        "HostLoom.AspNetCore.WebSockets"));
```

Requires the `OpenTelemetry.Extensions.Hosting` package.

The WebSocket source creates `hostloom.websocket.request` Server activities for registered
operations. The existing `HostLoom` send activity is their direct child. External transports do
not yet propagate W3C trace context to a consumer process.

## Messaging instruments (`HostLoom`)

The `hostloom.request.*` instruments are tagged `messaging.destination.name`
(the endpoint or topic) and `messaging.message.type`. Inbound requests and
event deliveries share them, so each measurement also carries
`hostloom.message.kind`: `request` or `event`.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.request.duration` | histogram (s) | Handling duration of a request or an event delivery, receive-pipeline retries included |
| `hostloom.request.active` | up-down counter | Requests and event deliveries in flight |
| `hostloom.request.faults` | counter | Requests answered with a fault envelope, framework faults such as `HandlerNotFound` and `HandlerNotRun` included, and event deliveries whose handlers failed; a delivery cancelled because its listener stopped is not a fault |
| `hostloom.request.retries` | counter | Receive-pipeline retry attempts |
| `hostloom.outbox.published` | counter | Outbox messages the relay published and marked, tagged `messaging.destination.name` |
| `hostloom.outbox.failed` | counter | Outbox publish attempts that failed and left the message pending; a publish the store failed to mark is not counted |
| `hostloom.outbox.dead_lettered` | counter | Outbox messages that exhausted `Outbox:MaxAttempts` and are no longer claimed |
| `hostloom.outbox.lag` | histogram (s) | Time between appending an outbox message and publishing it |
| `hostloom.inbox.duplicates` | counter | Redelivered events the inbox recognised, tagged destination and `messaging.consumer.group.name` |

## Pipeline instruments (`HostLoom.Pipelines`)

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.pipeline.filter.duration` | histogram | A filter's own work, downstream time subtracted |
| `hostloom.pipeline.filter.failures` | counter | Filter failures |
| `hostloom.pipeline.run.duration` | histogram | Whole pipeline run duration, recorded once per run across all retry attempts and tagged `hostloom.pipeline.outcome` (`success`, `failure`, `canceled`) with the final outcome |
| `hostloom.pipeline.run.active` | up-down counter | In-flight pipeline runs |

The per-filter duration subtracts downstream time on purpose: the slow
filter is visible wherever it sits in the chain, instead of every upstream
filter inheriting its latency. `WithoutInstrumentation()` opts a registered
pipeline out.

Downstream time is the elapsed time during which at least one downstream call is active.
Concurrent, overlapping calls on separate contexts count once; sequential calls each contribute
their own interval. The resulting duration measures elapsed work outside downstream execution,
not CPU consumption. Await every downstream call before the filter returns so its lifetime falls
within the measurement.

## Logging instruments (`HostLoom.Logging`)

Health of the logging provider itself — the bounded queue and its
background writer.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.logging.records.dropped` | counter | Log records dropped instead of written |
| `hostloom.logging.fields.dropped` | counter | Structured fields dropped from otherwise-shipped records |
| `hostloom.logging.enqueue.blocked` | counter | Log calls that blocked because the queue was full |
| `hostloom.logging.enqueue.blocked.duration` | histogram (s) | Time log calls spent blocked on a full queue |
| `hostloom.logging.failures` | counter | Unexpected component failures inside the logging pipeline |
| `hostloom.logging.queue.depth` | observable gauge | Records waiting in the bounded queue |
| `hostloom.logging.writer.state` | observable gauge | 1 while the background writer is healthy, 0 once faulted or disposed |

## Cache instruments (`HostLoom.Caching`)

Identity is the `hostloom.cache.namespace` tag on every instrument, never an
instrument name, so one dashboard serves every cache.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.cache.operation.duration` | histogram (s) | One cache operation, tagged `hostloom.cache.operation` and `hostloom.cache.outcome` (`hit_l1`, `hit_l2`, `hit_stale`, `miss`, `degraded`, `error`); `hit_stale` is an expired in-process entry served inside `CacheEntryOptions.StaleGrace` while the distributed store was unavailable and another caller refreshed |
| `hostloom.cache.factory.duration` | histogram (s) | A get-or-create factory |
| `hostloom.cache.entries` | observable gauge | Entries in the in-process tier |
| `hostloom.cache.guards.active` | observable gauge | Single-flight guards held or awaited |
| `hostloom.cache.stampede.lease_missed` | counter | Factories run without the cluster-wide lease |
| `hostloom.cache.invalidations` | counter | Invalidation messages, tagged `hostloom.cache.direction` (`sent`, `received`, `echoed`, `flushed`, `dropped`); `echoed` means the channel handed back a message this instance published, which is skipped rather than applied twice; `flushed` means the whole in-process tier was cleared, which a backend channel requests after a reconnect; `dropped` means the queue was at `Caching:Invalidation:MaxPending` and the in-process tier falls back to expiry for that message |
| `hostloom.cache.invalidation.resubscribed` | counter | Subscription re-established after a reconnect |
| `hostloom.cache.errors` | counter | Store and serialization failures, from the tiered cache and from the `IDistributedCache` adapter, tagged `hostloom.cache.kind` (`unavailable`, `timeout`, `serialization`, `other`) |
| `hostloom.cache.compressions` | counter | Payloads compressed before the distributed write |

Activities: `cache.get_or_create`, `cache.get`, `cache.set`, `cache.remove`,
`cache.get_many`, `cache.warmup`, tagged `hostloom.cache.key`,
`hostloom.cache.hit`, `hostloom.cache.tier`, and `hostloom.cache.degraded`.
A `degraded` outcome means the distributed store failed and the cache served
from the in-process tier or the factory instead; it is never an exception.

## Lock instruments (`HostLoom.Locking`)

Identity is the `hostloom.lock.namespace` tag.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.lock.acquire.duration` | histogram (s) | Acquisition, tagged `hostloom.lock.outcome` (`acquired`, `not_acquired`, `unavailable`) |
| `hostloom.lock.hold.duration` | histogram (s) | Time a lock was held |
| `hostloom.lock.active` | up-down counter | Locks currently held |
| `hostloom.lock.lost` | counter | Leases that ended before release |
| `hostloom.lock.orphan_releases` | counter | Best-effort releases of leases an abandoned acquisition may hold: granted after the caller gave up, confirmed after the usable lease, or left uncertain by a provider `Timeout` or `Other` failure; tagged `hostloom.lock.outcome` (`released`, `absent`, `failed`) |
| `hostloom.lock.enabled` | observable gauge | 1 when the lock coordinates, 0 in single-instance mode |

## Schedule instruments (`HostLoom.Scheduling`)

Identity is the `hostloom.schedule.name` tag. The `schedule.run` activity carries the same tag, the run sequence, and the outcome.

| Instrument | Kind | Meaning |
| --- | --- | --- |
| `hostloom.schedule.run.duration` | histogram (s) | Time a run executed, tagged `hostloom.schedule.outcome` (`succeeded`, `failed`, `timed_out`, `canceled`, `claim_lost`) |
| `hostloom.schedule.lag` | histogram (s) | Time between a run's due time and its start |
| `hostloom.schedule.skipped` | counter | Runs that did not execute, tagged `hostloom.schedule.skip_reason` (`claimed_elsewhere`, `guard_failed`) |

## Leadership instruments (`HostLoom.Leadership`)

Identity is the `hostloom.leader.role` tag. The `leader.acquire` activity carries the role and whether the attempt acquired.

| Instrument | Kind | Meaning |
| --- | --- | --- |
| `hostloom.leader.is_leader` | observable gauge | 1 while this instance leads the role, 0 otherwise |
| `hostloom.leader.changes` | counter | Transitions, tagged `hostloom.leader.reason` (`acquired`, `lost`, `resigned`, `stopped`) |
| `hostloom.leader.renew.duration` | histogram (s) | Lease renewals, tagged `hostloom.leader.outcome`: `renewed`; `refused`, which ends leadership; or `failed`, a provider failure while the lease still runs, which is retried |

Activities: `lock.acquire` and `lock.execute`, tagged `hostloom.lock.key`,
`hostloom.lock.acquired`, `hostloom.lock.wait_ms`, and `hostloom.lock.hold_ms`.

## Redis instruments (`HostLoom.Redis`)

Tagged `hostloom.redis.client` with the configured client name.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.redis.connection.state` | observable gauge | 1 while connected, 0 while down or reconnecting |
| `hostloom.redis.reconnects` | counter | Connections restored after a failure |
| `hostloom.redis.invalidation.malformed` | counter | Explicit-channel messages dropped for exceeding the size or item bounds or naming an invalid key or tag; tagged `hostloom.cache.namespace` instead of the client |
| `hostloom.redis.tag.members_rejected` | counter | Tag-index members outside the namespace's cache-data prefix, forgotten from the index instead of unlinked |

A reconnect also increments `hostloom.cache.invalidation.resubscribed` on the
caching meter, because the invalidation subscription is re-established with it.

## RabbitMQ transport instruments (`HostLoom.Transport.RabbitMq`)

Tagged `hostloom.rabbitmq.client` with the configured `RabbitMqOptions.ClientProvidedName`.
These count what the adapter does on the wire; how a handler fared is already on the
`HostLoom` meter as `hostloom.request.*`, so it is not repeated here.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.rabbitmq.publishes` | counter | Request and event publications, tagged `hostloom.rabbitmq.outcome` (`confirmed`, `returned`, `failed`, `timed_out`); `returned` is a mandatory request the broker could not route, which still waits out its timeout; `timed_out` is the request timeout or `PublishTimeout` elapsing. A publication the caller cancelled, or that a disposing broker refused, is not counted |
| `hostloom.rabbitmq.publish.duration` | histogram (s) | One publication, from waiting for a publisher channel to the broker's confirmation, tagged with the same outcome |
| `hostloom.rabbitmq.deliveries.rejected` | counter | Deliveries rejected without requeue, tagged `hostloom.rabbitmq.reason` (`malformed`, `handler_failed`); `malformed` is an undecodable frame or an unacceptable `ReplyTo`, `handler_failed` is everything else, including a reply or acknowledgement an open channel refused. A delivery whose rejection could not be sent is not counted here |
| `hostloom.rabbitmq.deliveries.requeued` | counter | Deliveries handed back to their queue: nacked with requeue because the listener was stopping or the delivery was cancelled, or left unacknowledged because a closing channel refused their reply, acknowledgement, or rejection (`RabbitMqDeliveryUnsettled`), which the broker redelivers |
| `hostloom.rabbitmq.connections` | counter | Tagged `hostloom.rabbitmq.event` (`opened`, `recovered`); a recovery is the client library restoring the same connection after a drop |
| `hostloom.rabbitmq.requests.pending` | observable gauge | Requests published and still awaiting a reply |
| `hostloom.rabbitmq.channels.closing` | observable gauge | Channels closing in the background: a publisher channel given up after a publication that was not confirmed (timed out, cancelled, nacked, returned, or failed); the channel of a stopped listener or subscription, whose close is not awaited by the stop; a channel whose consumer the broker cancelled; and during disposal the pooled and reply channels. A close waits for the broker's reply, so a value that stays up means the broker is not answering; while it is at `MaxConcurrentPublishes`, a publication that needs a new channel waits, within its own deadline, for a close to finish |
| `hostloom.rabbitmq.consumers` | counter | Consumers the broker cancelled and consumers restored afterwards, tagged `hostloom.rabbitmq.event` (`cancelled`, `restored`) and `hostloom.rabbitmq.role` (`request`, `event`, `reply`); a cancellation follows, for example, the deletion of the consumer's queue |

## Kafka transport instruments (`HostLoom.Transport.Kafka`)

Producer-side instruments are tagged `hostloom.kafka.client` with the configured
`KafkaOptions.ClientId`; consumer-loop instruments are tagged
`messaging.destination.name` with the topic the loop consumes. Handler outcomes
are on the `HostLoom` meter, not here.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.kafka.produced` | counter | Records the producer delivered, tagged `hostloom.kafka.kind` (`request`, `reply`, `event`); a produce that failed is not counted, it reaches the caller, the outbox, or the `unroutable_reply` skip below |
| `hostloom.kafka.consumed` | counter | Records a consumer loop received and handed to its handler, including a redelivery after a rewind |
| `hostloom.kafka.committed` | counter | Offsets a consumer loop committed |
| `hostloom.kafka.records.skipped` | counter | Records committed past without being handled to completion, tagged `hostloom.kafka.reason` (`malformed`, `unroutable_reply`, `attempts_exhausted`) |
| `hostloom.kafka.records.rewound` | counter | Records a consumer loop rewound to after a transient failure, so the next consume redelivers them |
| `hostloom.kafka.loop.faults` | counter | Client-library failures a consumer loop survived or reported, tagged `hostloom.kafka.stage` (`consume`, `commit`, `seek`, `close`, `loop`) |
| `hostloom.kafka.reply_consumer.initializations` | counter | Attempts to start the reply consumer and wait for its assignment, tagged `hostloom.kafka.outcome` (`succeeded`, `failed`); more than one per process means the reply consumer had to be re-initialized |
| `hostloom.kafka.requests.pending` | observable gauge | Requests produced and still awaiting a reply |

## Health checks

`AddHealthChecks()` on the HostLoom builder registers:

| Check | Default name | Tag | Contacts broker? |
| --- | --- | --- | --- |
| Liveness | `hostloom-live` | `live` | Never |
| Readiness | `hostloom-ready` | `ready` | Via `IBrokerHealthProbe`, when the transport implements it |

A transport that does not implement `IBrokerHealthProbe` is treated as
reachable — "cannot tell" must not read as "broken".

`AddHealthChecks()` on the caching and locking builders registers readiness
checks `hostloom-cache-ready` and `hostloom-lock-ready`, tagged `ready`, that
ask the store's `ICacheStoreHealthProbe` or the provider's
`ILockProviderHealthProbe`; on Redis that is a `PING` bounded by
`Redis:HealthTimeout`. A backend without a probe, including the in-process
ones, reports healthy with an explanation. `AddWarmup<T>()` adds
`hostloom-cache-warmup`, which reports unhealthy until every warmup finishes
only when `Caching:Warmup:BlocksReadiness` is set. Neither builder registers
a liveness check: a store outage must not read as "restart me".

## Execution-free probes

- `HostLoomProbe.ReceivePipeline()` — the receive pipeline's structure as a
  `ProbeResult` tree, without executing it.
- `PipelineProbe.Inspect(pipe)` — the same for any standalone pipe.
- `IPipelineRunner<TContext>.Topology` — a registered pipeline's resolved
  topology; `Describe()` renders it, marking conditional filters with a
  trailing `?`.
- `CachingProbe.Describe(cache, warmups)` — a cache's namespace, store,
  in-process tier, serializer, invalidation, lease, compression, and warmups,
  each line naming the option that decided it.
- `LockingProbe.Describe(lock)` — a lock's namespace, provider (or
  `(disabled)`), lease defaults, retry policy with its derived maximum wait,
  and reentrancy detection.
- `HostLoomWebSocketBuilder.Probe()` / `WebSocketGatewayProbe.Describe()` —
  immutable registration-time or runtime snapshots of gateway options, request
  routes, topic routes, and optional composition-ledger-shaped decisions;
  neither resolves application services nor contacts a transport.
