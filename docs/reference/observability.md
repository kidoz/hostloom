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
| `HostLoom.RateLimiting` | `Meter` | every `DistributedRateLimiter` |
| `HostLoom.RateLimiting.HttpClient` | `Meter` | outgoing HTTP rate-limit handlers |
| `HostLoom.Scheduling` | `ActivitySource` and `Meter` | every `Scheduler` |
| `HostLoom.Leadership` | `ActivitySource` and `Meter` | every `LeaderElector` |
| `HostLoom.NodeLeasing` | `ActivitySource` and `Meter` | every `NodeIdElector` |
| `HostLoom.Redis` | `ActivitySource` and `Meter` | the Redis connection |
| `HostLoom.Transport.RabbitMq` | `Meter` | the RabbitMQ transport |
| `HostLoom.Transport.Kafka` | `Meter` | the Kafka transport |
| `HostLoom.AspNetCore.WebSockets` | `ActivitySource` and `Meter` | raw WebSocket gateway |
| `HostLoom.DataGrid.Runtime` | `Meter` | experimental grid actor |
| `HostLoom.DataGrid.Client` | `Meter` | experimental grid SDK |
| `HostLoom.DataGrid.Transport` | `Meter` | experimental peer/client listeners |
| `HostLoom.DataGrid.Storage` | `Meter` | experimental file-store recovery |

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter(
        "HostLoom", "HostLoom.Pipelines", "HostLoom.Logging", "HostLoom.Caching", "HostLoom.Locking", "HostLoom.RateLimiting", "HostLoom.RateLimiting.HttpClient", "HostLoom.Scheduling", "HostLoom.Leadership", "HostLoom.NodeLeasing", "HostLoom.Redis",
        "HostLoom.Transport.RabbitMq", "HostLoom.Transport.Kafka", "HostLoom.AspNetCore.WebSockets"))
    .WithTracing(tracing => tracing.AddSource(
        "HostLoom", "HostLoom.Pipelines", "HostLoom.Caching", "HostLoom.Locking", "HostLoom.Scheduling", "HostLoom.Leadership", "HostLoom.NodeLeasing",
        "HostLoom.AspNetCore.WebSockets"));
```

Requires the `OpenTelemetry.Extensions.Hosting` package.

The WebSocket source creates `hostloom.websocket.request` Server activities for registered
operations. The existing `HostLoom` send activity is their direct child. External transports do
not yet propagate W3C trace context to a consumer process.

Framework error spans report `ActivityStatusCode.Error` without copying exception messages into
status descriptions. This covers request, event and stream handling, pipeline runs and filters,
leader election, and node-ID leasing. Exception logging keeps its existing behavior; an application
that adds its own trace tags or exception events owns their contents and export policy.

Every histogram advises its bucket boundaries, so an exporter that honors advice, as the
OpenTelemetry SDK does, needs no view to get useful buckets. Operation durations use boundaries
from 0.1 ms to 30 s, spans and lags (lock holds, scheduled runs and their lag, outbox lag,
WebSocket sessions) from 1 ms to one day, and `hostloom.websocket.queue.bytes` from 256 B to
16 MiB.

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
| `hostloom.messaging.requests.expired` | counter | Requests dropped because `ExpiresAt` had passed, tagged `messaging.destination.name` |
| `hostloom.receive.suspensions` | counter | Times a kill switch paused a destination, tagged `messaging.destination.name` |

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
| `hostloom.logging.writer.state` | observable gauge | 1 while the background writer runs, 0 once a defect stopped it or the provider was disposed; a failing sink leaves it at 1 and shows as `sink_failed` drops |

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

## Rate-limit instruments (`HostLoom.RateLimiting`)

Kernel instruments carry `hostloom.rate.namespace`, never a raw limiter key or URL. Acquisition
instruments also carry `hostloom.rate.outcome` (`acquired`, `rejected`, `degraded`, or
`unavailable`). Store errors replace the outcome tag with `hostloom.rate.failure.kind`
(`unavailable`, `timeout`, or `other`).

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.ratelimit.acquisitions` | counter | Asynchronous store decisions and explicit degraded admissions |
| `hostloom.ratelimit.acquire.duration` | histogram (s) | Time spent obtaining one asynchronous decision |
| `hostloom.ratelimit.store.errors` | counter | Store failures by backend-neutral kind |

Outgoing HTTP instruments use only `hostloom.rate.http.outcome` (`sent`, `degraded`, `rejected`,
`queue_full`, or `unavailable`), with no URL, partition, key, or request-content labels.

| Instrument | Type | Meter | Meaning |
| --- | --- | --- | --- |
| `hostloom.ratelimit.http.requests` | counter | `HostLoom.RateLimiting.HttpClient` | Outgoing requests by local rate-limit outcome |
| `hostloom.ratelimit.http.wait.duration` | histogram (s) | `HostLoom.RateLimiting.HttpClient` | Acquisition time, including bounded local waits |

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

## Node-leasing instruments (`HostLoom.NodeLeasing`)

Identity is the `hostloom.node.pool` tag. The `nodeid.acquire` activity carries the pool, whether the scan acquired, and the acquired ID.

| Instrument | Kind | Meaning |
| --- | --- | --- |
| `hostloom.node.assigned` | observable gauge | 1 while this instance holds an ID in the pool, 0 otherwise |
| `hostloom.node.assignment.changes` | counter | Transitions, tagged `hostloom.node.reason` (`acquired`, `lost`, `released`, `stopped`) |
| `hostloom.node.renew.duration` | histogram (s) | Lease renewals, tagged `hostloom.node.outcome`: `renewed`; `refused`, which ends the assignment; or `failed`, a provider failure while the lease still runs, which is retried |
| `hostloom.node.loop.faults` | counter | Unexpected exceptions the elector loop caught and backed off from |

Activities: `lock.acquire` and `lock.execute`, tagged `hostloom.lock.key`,
`hostloom.lock.acquired`, `hostloom.lock.wait_ms`, and `hostloom.lock.hold_ms`.

## Redis instruments (`HostLoom.Redis`)

Tagged `hostloom.redis.client` with the configured client name.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.redis.connection.state` | observable gauge | 1 while connected, 0 while down or reconnecting |
| `hostloom.redis.reconnects` | counter | Connections restored after a failure |
| `hostloom.redis.invalidation.malformed` | counter | Explicit-channel messages dropped for exceeding the size or item bounds or naming an invalid key or tag; tagged `hostloom.cache.namespace` instead of the client |
| `hostloom.redis.invalidation.dropped` | counter | Received invalidation messages dropped because the fan-in queue reached `Caching:Invalidation:MaxPending`; tagged `hostloom.cache.namespace` instead of the client |
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

## Transport log events

Every line the transports log carries a stable event id and name, so a logging pipeline can
filter or alert on one condition without matching message text. The Kafka consumer-loop lines
come from listeners, subscriptions, and the reply consumer alike and name the topic.

| Id | Name | Level | Meaning |
| --- | --- | --- | --- |
| 1401 | `RabbitMqDeliveryRejected` | Error | A delivery failed and was rejected without requeue, to `DeadLetterExchange` when one is set |
| 1402 | `InMemoryEventFailed` | Error | An in-memory subscriber failed; the publication stays accepted |
| 1403 | `RabbitMqChannelsStillClosing` | Warning | Disposal stopped waiting, after five seconds, for channels still closing |
| 1404 | `InMemoryHandlersAbandoned` | Warning | An in-memory stop gave up, after five seconds, on handlers that ignored their cancellation |
| 1405 | `RabbitMqDeliveryUnsettled` | Warning | A closing channel refused a delivery's reply, acknowledgement, or rejection; the broker redelivers it, so its handler may run again |
| 1406 | `RabbitMqHandlersAbandoned` | Warning | A listener or subscription stopped after five seconds without handlers that ignored their cancellation; their deliveries are redelivered |
| 1407 | `RabbitMqConnectionStillClosing` | Warning | Disposal stopped waiting, after two seconds, for the connection to close, which finishes in the background; also a failure of that background close |
| 1408 | `RabbitMqChannelCloseFailed` | Warning | A channel closed in the background failed to close cleanly |
| 1411 | `RabbitMqConsumerCancelled` | Warning | The broker cancelled a consumer, as it does when its queue is deleted; what the queue held, and what is sent before it is declared again, is lost |
| 1412 | `RabbitMqConsumerRestored` | Information | Consumption resumed after such a cancellation |
| 1413 | `RabbitMqConsumerRestoreFailed` | Warning | An attempt to restore consumption failed and is retried |
| 1421 | `KafkaConsumeFailed` | Error | Consuming failed; the loop waits and consumes again |
| 1422 | `KafkaRecordMalformed` | Error | A record could not be decoded, or a request named an unacceptable reply topic or was too old; it is committed past without being handled |
| 1423 | `KafkaReplyUnroutable` | Error | A request was handled but its reply could not be produced; it is committed past without running the handler again |
| 1424 | `KafkaRecordRewound` | Error | Handling a record failed; the loop rewinds to it and retries after a backoff |
| 1425 | `KafkaRecordAttemptsExhausted` | Error | A request record failed on every attempt and is committed past |
| 1426 | `KafkaSeekFailed` | Error | Rewinding to a failed record failed; whoever is next assigned the partition redelivers it |
| 1427 | `KafkaCommitFailed` | Error | Committing a handled or skipped record failed; the handler is not run again |
| 1428 | `KafkaConsumerLoopFaulted` | Error | The consumer loop itself faulted, a defect, and had stopped consuming |
| 1429 | `KafkaConsumerCloseFailed` | Error | Closing a stopped consumer failed, so its group may wait out the session timeout before rebalancing |
| 1430 | `KafkaConsumerCleanupFailed` | Debug | Disposing a consumer whose subscription failed at startup failed too; the startup failure is the one reported |
| 1431 | `KafkaConsumerCleanupDeferred` | Warning | Consumer shutdown exceeded five seconds; cleanup continues in the background |
| 1432 | `KafkaConsumerCancellationFailed` | Error | A cancellation callback threw; consumer cleanup continues |

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
`Redis:HealthTimeout`. A named profile's default check name gains `:{profile}`, such as
`hostloom-cache-ready:catalog`; an explicitly supplied name must be unique across components and
profiles. A backend without a probe, including the in-process ones, reports healthy with an
explanation.

`AddWarmup<T>()` adds `hostloom-cache-warmup`, with the same profile suffix rule. Its description
reports not started, running, succeeded, failed, or cancelled and retains partial success and
failure counts. Cancellation is neither success nor failure. With
`Caching:Warmup:BlocksReadiness` set, the check is unhealthy only while a run is in progress;
terminal failure and cancellation remain healthy fail-open outcomes. Neither builder registers a
liveness check: a store outage must not read as "restart me".

Named profiles retain the existing low-cardinality metric identities: cache and lock metrics use
the configured namespace, not the profile name. Adapter refresh failures and malformed sliding
envelopes are counted on `hostloom.cache.errors` with the existing failure kind and namespace.

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


## Experimental native grid instruments

The non-packable grid prototype publishes `HostLoom.DataGrid.Runtime`,
`HostLoom.DataGrid.Client`, `HostLoom.DataGrid.Transport` and `HostLoom.DataGrid.Storage` meters.
Their names are also exposed by `DataGridRuntimeDiagnostics.MeterName`,
`DataGridClientDiagnostics.MeterName`, `DataGridTransportDiagnostics.MeterName` and
`DataGridStorageDiagnostics.MeterName` in the corresponding packages. Enable the meters before
constructing actors, clients or listeners. Up-down
counters report deltas; a listener attached halfway through an owner's lifetime has no initial
baseline. Metrics aggregate all instances in one process; use exporter resource attributes
for service identity. There is no grid activity source, trace propagation or exporter setup.

| Instrument | Type / unit | Meaning |
| --- | --- | --- |
| `hostloom.datagrid.runtime.queue.items` | up-down counter / `{item}` | Work waiting in control/client lanes; released on dequeue or shutdown drain |
| `hostloom.datagrid.runtime.queue.bytes` | up-down counter / `By` | Encoded-byte reservations waiting in those lanes; excludes executing work and heap overhead |
| `hostloom.datagrid.runtime.queue.rejected` | counter / `{item}` | Pre-queue refusals by lane and reason: `items`, `bytes`, `principal`, or `stopped` |
| `hostloom.datagrid.runtime.admission.active` | up-down counter / `{operation}` | Held client operation reservations from before enqueue through actor completion/drain; caller cancellation does not refund them |
| `hostloom.datagrid.runtime.admission.bytes` | up-down counter / `By` | Retained encoded submission charges across queued/executing/pending work; excludes responses, log/replay state and heap overhead |
| `hostloom.datagrid.runtime.operations` | counter / `{operation}` | Local submission completions, including rejection, uncertainty, cancellation and faults |
| `hostloom.datagrid.runtime.operations.active` | up-down counter / `{operation}` | Local callers awaiting actor completion |
| `hostloom.datagrid.runtime.operation.duration` | histogram / `s` | Local submission through caller completion, including queue, quorum and persistence waits |
| `hostloom.datagrid.runtime.persistence.duration` | histogram / `s` | One synchronous file-store `Persist` call, including required flush/publication and excluding dependent actor application |
| `hostloom.datagrid.client.calls` | counter / `{call}` | SDK logical completions, including immediate capacity/disabled refusals |
| `hostloom.datagrid.client.calls.active` | up-down counter / `{call}` | Invocations awaiting caller completion, including brief pre-admission decisions |
| `hostloom.datagrid.client.call.duration` | histogram / `s` | Whole logical invocation, including reconnect waits and all attempts |
| `hostloom.datagrid.client.attempts` | counter / `{attempt}` | Entered attempts, including attempts canceled while waiting to reconnect or before request writing |
| `hostloom.datagrid.transport.reservations.active` | up-down counter / `{reservation}` | Held connection, handshake, member, principal, request or inbound snapshot reservations; one socket can hold several kinds |
| `hostloom.datagrid.transport.reservations.bytes` | up-down counter / `By` | Reserved client encoded work (2 MiB per request reservation) or inbound snapshot descriptor bytes; excludes managed overhead |
| `hostloom.datagrid.transport.admission.rejected` | counter / `{rejection}` | Explicit capacity/rate, invalid decoded request, or map/session grant refusals |
| `hostloom.datagrid.transport.exchanges` | counter / `{exchange}` | Accepted inbound sessions and outbound peer invocations by terminal transport outcome; excludes TCP refusals before session creation |
| `hostloom.datagrid.runtime.observation.time` | gauge / `ms` | Physical UTC Unix milliseconds at the last owner publication, for assessing sample age |
| `hostloom.datagrid.runtime.state` | gauge / `{state}` | Local role, fault/stop/busy/storage flags and a local-ready hint; never a current-quorum assertion |
| `hostloom.datagrid.runtime.position` | gauge / `{position}` | Term/log-end/commit/apply/snapshot positions and historical successful actor read-barrier index/term |
| `hostloom.datagrid.runtime.replica.observed` | gauge / `{observation}` | Whether this leader term has a validated acknowledgement from the peer |
| `hostloom.datagrid.runtime.replica.lag` | gauge / `{entry}` | Leader log-end minus last acknowledged index; zero is a placeholder when observed is zero |
| `hostloom.datagrid.runtime.map.keys` | gauge / `{key}` | Charged physical entries and configured key limit |
| `hostloom.datagrid.runtime.map.bytes` | gauge / `By` | Charged encoded key/value bytes and configured byte limit |
| `hostloom.datagrid.runtime.storage.bytes` | gauge / `By` | WAL/snapshot bytes from the last successful owned publication, not current disk inventory |
| `hostloom.datagrid.runtime.storage.records` | gauge / `{record}` | Record count in the last successfully published WAL generation |
| `hostloom.datagrid.storage.recoveries` | counter / `{recovery}` | Explicit file-store Open calls, including failed attempts; excludes bootstrap |
| `hostloom.datagrid.storage.recovery.duration` | histogram / `s` | Synchronous ownership, inventory and WAL/snapshot validation; excludes subsequent actor replay/startup |



Fixed tags are `hostloom.datagrid.lane` (`control`/`client`), `hostloom.datagrid.reason`,
`hostloom.datagrid.operation`, `hostloom.datagrid.outcome`,
`hostloom.datagrid.may_have_been_admitted` and `hostloom.datagrid.submitted`.
Operation names come from fixed request/command kinds; outcomes come from fixed completion or
failure codes plus `canceled`/`faulted`. Persistence outcomes are `persisted`, `canceled` or
`faulted`. The last two tags are Boolean SDK admission/submission observations. No meter label
contains a key, value, map name, directory, endpoint, credential, exception message or user-supplied identity.
The optional payload-bearing history recorder remains a separate experiment facility.

Counts describe their own boundary. A local `applied` result can contain a rejected conditional
or session result; SDK `success` also includes completed conditional rejections. Retries may
recover the same retained result, so neither counter counts unique writes. A canceled local
caller does not establish rollback. SDK `outcome_unknown` retains cumulative admission
uncertainty even if the actor already applied the mutation. An attempt's `submitted` means
request writing began, not that the server admitted it. SDK/local-submission entry cancellation throws before their metric
admission. Persistence measurements include no-op `Persist` calls and do not count fsyncs or
establish device durability. Synchronous metric listeners are trusted diagnostic code and should
not block or throw; this slice does not isolate listener failures.

Actor admission retains 256 control / 128 client queued items and 16 MiB per lane. A waiting
client gets a turn after at most eight control turns. Authenticated principal FIFO queues rotate
one client turn each. A separate retained-operation budget enforces eight operations/4 MiB per
principal inside 128 operations/16 MiB shared across queued/executing/pending client work. The
fixed `principal` refusal reason combines its item/byte limits without exposing identity; shared
limits and stopped state take precedence. Queue budgets exclude executing/pending consensus
work and release on dequeue/drain; operation reservations remain until actor completion/drain.
Canceled queued work remains charged until skipped on dequeue; cancellation/disconnect or a
physical caller timeout cannot release pending ownership early. Trusted local test entry points
share a separate FIFO and the global operation budget in the same rotation. These rules do not guarantee fair
handshake/request admission, completion latency or CPU time. Each SDK still reserves at most eight concurrent exchanges. The active
invocation metric includes brief decisions outside that reservation. The meters retain no
per-owner registry or payload history. Queue/SDK boundary tests and Linux actor tests establish
these measured paths, not collector delivery, full transport abuse isolation, all required
per-map/readiness/storage metrics, performance capacity or production readiness.

The two `runtime.admission.*` series have no labels and include internal trusted submissions.
They can remain positive while `runtime.operations.active` is zero because all callers timed out
or canceled. Queue counts are a subset during the waiting phase; transport reservations describe
another overlapping boundary. Do not sum them as distinct operations or heap bytes. Charges
include requested keys or canonical commands plus 128 bytes of submission overhead, and release
on definite enqueue refusal, canceled dequeue, terminal Raft completion or actual owner drain.
A terminal unknown result can release a wait reservation while the log may still commit later;
retained log/replay state is outside these counts. Stop refuses new charges without releasing
blocked work early. Controlled Linux cases cover pending cancellation, wire refusal, higher-term
completion, quorum recovery and blocked-WAL physical timeout followed by recovery/stop/fault.
Zero admission reservations do not establish completed transport/storage cleanup or expired
replay state; the actual completion/closed diagnostics describe those separate boundaries.


Transport labels are `hostloom.datagrid.transport.role` (`peer`/`client`),
`hostloom.datagrid.transport.direction` (`inbound`/`outbound`),
`hostloom.datagrid.transport.resource` (`connection`, `handshake`, `member`, `principal`,
`request`, `snapshot`), `hostloom.datagrid.transport.reason` (`capacity`, `rate`, `unauthorized`,
`invalid_request`) and `hostloom.datagrid.transport.outcome` (`completed`, `rejected`, `retired`,
`authentication`, `protocol`, `canceled`, `io`, `faulted`). Only peers emit outbound exchanges;
SDK attempts belong to the client meter. Per-member/principal counters aggregate anonymously:
these series cannot identify a hot member or principal. Limits still apply separately to each
bound identity, and principal accounting survives credential replacement.

Transport counters describe separate boundaries. A capacity refusal can increment both admission
rejection and a terminal `rejected` exchange; these counters must not be added as distinct failed
requests. Rate/connection/handshake refusals before an inbound session is created increment only
admission rejection. Authentication/framing failures close an accepted session and appear in
exchange outcomes. A decoded grant refusal or invalid request can send a valid failure response
and finish with transport outcome `completed`. Application reply failures also count as completed
exchanges. `canceled` combines physical deadlines, stop and credential cancellation; `retired`
applies when the explicit retired-generation exception reaches the boundary. Some disconnects or
TLS failures surface as `io`; the meter does not infer their remote cause or committed-write fate.

Reservations decrement exactly once on release, including early hello failure, malformed frames,
shutdown and snapshot disconnect. Snapshot bytes cover the reserved inbound image, including
before allocation or a term refusal; they are not allocated heap measurements. Client request
bytes are fixed encoded-work budget reservations, not actual wire sizes. Collection must begin
before ownership for complete delta baselines. Metrics retain no credentials, endpoints, identities
or payload history. Controlled TLS tests establish these paths and recovery after refusal; full
connection churn, slow-consumer fairness, credential-retirement budgets, managed-memory pressure,
collector delivery and complete production abuse isolation remain unqualified.


State/progress observations add bounded integer tags `hostloom.datagrid.member.ordinal` and
`hostloom.datagrid.peer.ordinal` (0–2), and `hostloom.datagrid.map.ordinal` (0–15). They identify
canonical configuration positions within one group, not GUIDs or arbitrary map names. Gauge
series from multiple groups sharing one process collide at equal ordinals: use the internal
per-runtime snapshot for unambiguous observations, or isolate groups through deployment/exporter
resources. This prototype adds no multi-group hosting or health-check registration contract.
Existing counters continue to aggregate across instances.

Additional fixed tags are `hostloom.datagrid.state`, `hostloom.datagrid.position`,
`hostloom.datagrid.measure` and `hostloom.datagrid.storage.outcome`. State names are `follower`,
`candidate`, `leader`, `faulted`, `stopped`, `local_ready_hint`, `busy`, `storage_faulted`,
`storage_closed`. Position names are `term`, `log_end`, `committed`, `applied`, `snapshot`,
`last_read_barrier`, `last_read_barrier_term`. Map measures are `charged`/`limit`; storage byte
measures are `wal_published`/`snapshot_published`. Recovery outcomes are `recovered`/`failed`.

Internal runtime `Diagnostics` returns the last immutable owner-thread snapshot without posting
to the actor, reading files, probing peers, or changing application state. Its bounded read-only
map and replica collections contain numeric/Boolean metadata only. It remains readable during
blocked persistence and after stop. `CapturedTimestamp` uses the process monotonic clock;
`CapturedUnixMilliseconds` and the observation-time gauge use physical UTC, independent of the
injected logical grid clock. Publications occur after owner work and final cleanup; observations
can become stale during blocked work. UTC clocks can jump, and scrape/export delays are not bounded.
Gauges are recorded publication events; there is no observable callback or owner registry.

The local-ready hint means the node is locally a leader with a committed current-term prefix and
no observed fault/busy/stop condition at publication. It can remain true after losing the majority.
A historical successful actor read barrier records quorum evidence for that completed operation,
including when a caller may have lost its reply; it grants no fresh authority to another request.
It resets on process restart. Only an actual strong read establishes its own quorum-backed result.
The snapshot's embedded local state is coherent with its other fields; independently reading the
older `Status` surface and `Diagnostics` need not observe the same publication.

Replica observations exist only in a healthy active leader term after a validated correlated
acknowledgement. They reset with leadership and become unknown on stop/fault. A stopped or silent
peer's last acknowledgement can remain recorded for that term: zero lag is not fresh reachability,
commit visibility or peer application. An unknown replica has nullable lag in the snapshot and
zero/observed-zero gauge values. Consumers must inspect observed alongside lag.

Map usage counts physically charged entries and encoded key/value bytes. Expired entries remain
charged until deterministic reclamation, so these are not logical live-key or heap counts. All
maps recover under the same group state; no separate map readiness or source-loading guarantee
is inferred. Storage progress uses cached last-successful publication sizes/record count. Failed
writes can change physical file sizes while leaving these cached numbers unchanged; fault/closed
flags qualify them. Record count can fall when a generation rolls. Recovery duration covers
validation in Open, not all runtime startup, supported device durability, progress percentage or
recovery-time acceptance. Per-map error rates, explicit source freshness/redistribution policy,
full overload/fairness and device/shutdown qualification remain open.


Definite queue refusal is distinguished from admitted uncertainty: full actor shared/principal item/byte
admission returns `CapacityExceeded` with admission false; a stopped owner returns
`QuorumUnavailable` with admission false. Already queued or possibly submitted work remains
conservative, and earlier SDK uncertainty cannot be erased by a later refusal. The local runtime
operation metric reports definite refusals as `busy`/`unavailable` rather than a generic fault.

Runtime cleanup belongs to the actor's actual completion, independently of its cached ten-second
dispose wait. A timeout can leave the directory owned and the last diagnostic snapshot unchanged
until blocked work returns. Internal `Completion` joins actual cleanup; final closed/stopped state
and reservation balance follow it. Controlled tests cover this distinction, live client request
saturation with peer control preserved, slow bodies/deadlines/stop, a non-reading response client,
and storage budget refusal. They do not establish universal blocked-write behavior, kernel memory
bounds, external-principal fairness, real disk-full/power-loss safety or numerical release NFRs.

Directed Linux contention/churn checks observe reservation deltas through repeated disconnects
at TCP, TLS, hello and incomplete-frame stages. Client handshake saturation preserves peer vote
traffic and drains on deadline or concurrent stop. An eight-request principal remains charged
across credential replacement while a controlled receiver retains canceled work; a second
principal can proceed, and retirement/quota reuse follows actual release. The counters remain
anonymous and the instrument inventory is unchanged. These are bounded fixture observations;
they do not measure sustained churn, heap amplification, full connection saturation, durable
consensus progress or end-to-end fairness.
