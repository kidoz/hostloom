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
| `HostLoom.Redis` | `ActivitySource` and `Meter` | the Redis connection |
| `HostLoom.AspNetCore.WebSockets` | `ActivitySource` and `Meter` | raw WebSocket gateway |

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter(
        "HostLoom", "HostLoom.Pipelines", "HostLoom.Logging", "HostLoom.Caching", "HostLoom.Locking", "HostLoom.Redis",
        "HostLoom.AspNetCore.WebSockets"))
    .WithTracing(tracing => tracing.AddSource(
        "HostLoom", "HostLoom.Pipelines", "HostLoom.Caching", "HostLoom.Locking",
        "HostLoom.AspNetCore.WebSockets"));
```

Requires the `OpenTelemetry.Extensions.Hosting` package.

The WebSocket source creates `hostloom.websocket.request` Server activities for registered
operations. The existing `HostLoom` send activity is their direct child. External transports do
not yet propagate W3C trace context to a consumer process.

## Messaging instruments (`HostLoom`)

Tagged by destination and message type.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.request.duration` | histogram (s) | Request handling duration |
| `hostloom.request.active` | up-down counter | In-flight requests |
| `hostloom.request.faults` | counter | Failed requests |
| `hostloom.request.retries` | counter | Receive-pipeline retry attempts |

## Pipeline instruments (`HostLoom.Pipelines`)

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.pipeline.filter.duration` | histogram | A filter's own work, downstream time subtracted |
| `hostloom.pipeline.filter.failures` | counter | Filter failures |
| `hostloom.pipeline.run.duration` | histogram | Whole pipeline run duration |
| `hostloom.pipeline.run.active` | up-down counter | In-flight pipeline runs |

The per-filter duration subtracts downstream time on purpose: the slow
filter is visible wherever it sits in the chain, instead of every upstream
filter inheriting its latency. `WithoutInstrumentation()` opts a registered
pipeline out.

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
| `hostloom.cache.operation.duration` | histogram (s) | One cache operation, tagged `hostloom.cache.operation` and `hostloom.cache.outcome` (`hit_l1`, `hit_l2`, `miss`, `degraded`, `error`) |
| `hostloom.cache.factory.duration` | histogram (s) | A get-or-create factory |
| `hostloom.cache.entries` | observable gauge | Entries in the in-process tier |
| `hostloom.cache.guards.active` | observable gauge | Single-flight guards held or awaited |
| `hostloom.cache.stampede.lease_missed` | counter | Factories run without the cluster-wide lease |
| `hostloom.cache.invalidations` | counter | Invalidation messages, tagged `hostloom.cache.direction` (`sent`, `received`, `dropped`); `dropped` means the queue was at `Caching:Invalidation:MaxPending` and the in-process tier falls back to expiry for that message |
| `hostloom.cache.invalidation.resubscribed` | counter | Subscription re-established after a reconnect |
| `hostloom.cache.errors` | counter | Store and serialization failures, tagged `hostloom.cache.kind` (`unavailable`, `timeout`, `serialization`, `other`) |
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
| `hostloom.lock.enabled` | observable gauge | 1 when the lock coordinates, 0 in single-instance mode |

Activities: `lock.acquire` and `lock.execute`, tagged `hostloom.lock.key`,
`hostloom.lock.acquired`, `hostloom.lock.wait_ms`, and `hostloom.lock.hold_ms`.

## Redis instruments (`HostLoom.Redis`)

Tagged `hostloom.redis.client` with the configured client name.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.redis.connection.state` | observable gauge | 1 while connected, 0 while down or reconnecting |
| `hostloom.redis.reconnects` | counter | Connections restored after a failure |

A reconnect also increments `hostloom.cache.invalidation.resubscribed` on the
caching meter, because the invalidation subscription is re-established with it.

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
