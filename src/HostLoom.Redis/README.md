# HostLoom.Redis

The Redis backend for `HostLoom.Caching` and `HostLoom.Locking`, over StackExchange.Redis. One
connection per process serves the cache store, the invalidation channel, and the lock provider:

```csharp
services
    .AddHostLoomCaching(caching => caching.Namespace = "catalog")
    .UseRedis(redis => redis.Configuration = "redis:6379,password=…")
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = CatalogJsonContext.Default })
    .AddHealthChecks();

services
    .AddHostLoomLocking(locking => locking.Namespace = "catalog")
    .UseRedis()
    .AddHealthChecks();
```

`UseRedis` on either builder registers `RedisOptions`, validates them at startup, and creates the
connection lazily on first use; calling it on both builders shares that connection. `RedisOptions`
takes a configuration string, a prebuilt `ConfigurationOptions`, or a `ConnectionFactory` for an
externally owned multiplexer, plus `DatabaseIndex`, `UseHashTags`, the connect, command, and
health timeouts, `FailFast`, and a `ClientName` defaulting to `hostloom-{machine}-{pid}`. A
password is never logged; probe output and the startup line redact it.

## What runs on the server

| Purpose | Commands |
|---|---|
| cache entry | `SET … PX`, a Lua `GET`/`PTTL` snapshot per key (pipelined for bulk reads), deletion |
| set-if-absent and stampede lease | `SET … NX PX` |
| tag index | `SADD`, `EXPIRE NX` then `EXPIRE GT`, `SMEMBERS`, atomic Lua `UNLINK`/`SREM` batches; a tagged set-if-absent indexes only after the write is known to have happened |
| invalidation | `PUBLISH` and `SUBSCRIBE` on `{namespace}:cache:invalidate`; `CLIENT LIST`, `CLIENT TRACKINGINFO`, and `CLIENT TRACKING` in tracking mode; `PSUBSCRIBE __keyspace@{db}__:…` in broadcast mode |
| lock | `SET {namespace}:lock:{key} owner NX PX lease`; release and extend are Lua compare-and-set, sent as `EVALSHA` with `EVAL` fallback |
| readiness | `PING` bounded by `Redis:HealthTimeout` |

Nothing relies on `SELECT`: `DatabaseIndex` is passed per command and exists only so a service
can coexist with keys from a previous library during a migration. `UseHashTags` wraps the
namespace segment in `{…}` so every key of a service lands in one Redis Cluster slot. Keys are the
kernels' fully prefixed keys, so a cache entry, a lease, a tag index, and a lock can never collide.

A tag set gains members and loses them only when the whole index is removed, so an entry rewritten
under different tags stays in its earlier sets and `RemoveByTagAsync` on one of those tags removes
it too. Reading an entry's current tags before every write would cost a round trip on the hot path
to save a refill on the cold one, which is the wrong trade for a cache.

Tag removal consumes only memberships from its snapshot. Each batch removes values and their
memberships atomically; an empty set disappears automatically. Concurrent additions remain indexed,
and a failed batch leaves its memberships available for a later retry. Cache reads also need Lua
permission: reading each value with its remaining TTL atomically prevents an expired or replaced
value from borrowing a different lifetime in L1.

Write payloads are copied before dispatch because cancellation stops the caller's wait while a
queued Redis command can still execute. The SDK owns the copied bytes until it finishes. Cancelling
connection initialization likewise stops only that caller's wait: other callers share the attempt,
and disposal waits for its result, releasing an owned multiplexer even if it arrives during shutdown.
An external `ConnectionFactory` receives the connection's shutdown token and retains ownership of
the multiplexer it returns.

## Fail-open

`Redis:FailFast` is `false` by default: an unreachable Redis lets the host start, the readiness
checks report unhealthy, the cache serves from its in-process tier and factories, and the lock
raises `LockProviderUnavailableException`, until the connection recovers. StackExchange.Redis
reconnects in the background; `hostloom.redis.connection.state` and `hostloom.redis.reconnects`
on the `HostLoom.Redis` meter show it. With `FailFast = true` the host fails to start instead.

Every backend failure reaches the kernels as `CacheStoreException` or `LockProviderException`
with a backend-neutral kind: a connection failure is `Unavailable`, a timeout is `Timeout`, and
anything else is `Other`. Consumers never see a StackExchange.Redis exception type.

## Invalidation

Every instance subscribes to the explicit channel `{namespace}:cache:invalidate`, which carries
what `RemoveAsync` and `RemoveByTagAsync` publish. `Caching:Invalidation:Mode` adds one
server-side transport on top of it, so an entry another instance overwrites, or the server
expires, leaves every in-process tier without anyone publishing:

| Mode | What the package does | Needs |
|---|---|---|
| `Tracking` | `CLIENT TRACKING ON REDIRECT <subscriber> BCAST PREFIX <data-prefix> NOLOOP`: the server reports changes to every cache-data key in the namespace, including entries populated locally by writes or warmup | Redis 6.0 or later |
| `Broadcast` | pattern subscriptions to `__keyspace@{db}__:{prefix}*` for `Caching:Invalidation:KeyPrefixFilters`, or the namespace's entries when the list is empty | `notify-keyspace-events Kxe` on the server |
| `Auto` (default) | `Tracking` on Redis 6.0 or later, read from the server version at connect, otherwise `Broadcast` | |

`NOLOOP` keeps a connection's own writes from evicting the in-process entry it has just written.
Prefix-based tracking remains active after those writes, without requiring a Redis read to register
the key again. It sends more invalidations than read-based tracking, bounded to the namespace's data
prefix. `Broadcast` in the options table remains the separate keyspace-notification transport.
StackExchange.Redis re-establishes every subscription on its own after a reconnect; tracking is
per server connection. The package registers every connected primary and replica using that
node's subscriber client ID, and refreshes registration after reconnects and topology changes.
Subscription recovery is counted on
`hostloom.cache.invalidation.resubscribed`. A subscription connection re-established after a
failure also hands `CacheInvalidation.Flush` to this process's subscribers when
`Caching:Invalidation:FlushLocalOnReconnect` is set (the default), because nothing published,
tracked, or broadcast during the outage was received; the cache clears its in-process tier and
counts it as `flushed`. An interactive-connection blip loses no invalidation and does not flush.
An instance that starts while Redis is down keeps
trying to subscribe with exponential backoff; a mode that cannot be enabled after
`Redis:MaxClientCommandRetries` attempts leaves the explicit channel as the only fan-out, logged
and retried on the next reconnect or topology refresh. `CachingProbe.Describe` reports the
transport in effect. Registration runs serially across channels sharing a connection and checks
existing prefixes before updating tracking. Replacing a subscriber preserves those prefixes.
Disposing a channel removes only its own
subscription handlers.

Two connection settings follow from this and are applied by the package: the client-side
`allowAdmin` flag, because StackExchange.Redis gates every `CLIENT` command behind it (this
grants nothing on the server; ACLs still apply), and RESP2, so subscriptions run on a dedicated
connection that tracking can redirect to. An externally supplied multiplexer needs both for
tracking to work; without them the package falls back to the explicit channel.

## Redis Cluster

Use `UseHashTags = true` and `DatabaseIndex = 0` for cluster deployments so Lua and tag operations
stay within one slot. Every advertised node address must be reachable from the application.
Connections created by HostLoom check topology at most five seconds apart, preserving a supplied
faster `configCheckSeconds` setting. This discovers replica promotions even when an existing
socket stays connected. For an externally owned multiplexer, configure `configCheckSeconds=5`
(or faster) yourself, alongside the tracking settings above. Recovery also depends on server
election time and network availability; five seconds is a refresh interval, not an outage limit.

Tracking covers replicas before promotion and is re-established when a failed node reconnects.
Invalidation is not durable: a message dropped by a full queue leaves its entry in L1 until
expiry, and changes missed during an outage remain there when the reconnect flush is disabled.
Redis replication is asynchronous, so a primary failure can lose an unreplicated cache write or
lock acquisition. Owner-checked release and extension protect a replicated lease against a stale
owner; they do not make Redis locks a consensus-backed mutual-exclusion guarantee across failover.

The repository's `just test-redis-cluster` command creates an isolated local six-node Docker
fixture and tests cache operations, tracking, explicit invalidation, and replicated lock ownership
while crashing each primary. It restores the failed processes and removes its container afterward.

## Compatibility

Works against Redis 7.x and Valkey using only the commands above. StackExchange.Redis is not
annotated for trimming or Native AOT, so this package does not claim `IsAotCompatible`; the
caching and locking kernels and their `DependencyInjection` packages do.
