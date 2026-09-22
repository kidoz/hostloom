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
| tag index | `SADD`, `EXPIRE NX` then `EXPIRE GT`, `SMEMBERS`, atomic Lua `UNLINK`/`SREM` batches; a tagged set-if-absent indexes only after the write is known to have happened. Only members under `{namespace}:cache:data:` are unlinked; any other member is dropped from the index with `SREM`, left untouched as a key, and counted on `hostloom.redis.tag.members_rejected` |
| invalidation | `PUBLISH` and `SUBSCRIBE` on `{namespace}:cache:invalidate`; `CLIENT LIST`, `CLIENT TRACKINGINFO`, and `CLIENT TRACKING` in tracking mode; `PSUBSCRIBE __keyspace@{db}__:…` in broadcast mode |
| lock | `SET {namespace}:lock:{key} owner NX PX lease`; release and extend are Lua compare-and-set, sent as `EVALSHA` with `EVAL` fallback |
| readiness | `PING` bounded by `Redis:HealthTimeout`. The health description reports reachability, latency, and the database index only; endpoints, the client name, and the process identity stay in logs through `RedisConnection.Describe()` |

Nothing relies on `SELECT`: `DatabaseIndex` is passed per command and exists only so a service
can coexist with keys from a previous library during a migration. `UseHashTags` wraps the
namespace segment in `{…}` so every key of a service lands in one Redis Cluster slot. Keys are the
kernels' fully prefixed keys, so a cache entry, a lease, a tag index, and a lock can never collide.

Tagged writes through the store's public constructor require a value key in
`namespace:cache:data:name` and tag keys in `namespace:cache:tag:name` with the same
namespace. The adapter validates every tag before issuing any backend command; invalid
combinations throw `ArgumentException`, including conditional writes. `RemoveByTagAsync`
also requires a canonical tag key. Untagged operations still accept opaque keys. Calls
through `ICache` already use these domains. Direct store callers using custom tagged
keys must migrate them to these forms; changing only the index would leave old values
outside the invalidation filter.

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
the multiplexer it returns. Its `ClientName` becomes the connection's, as it does for a wrapped
multiplexer, because tracking finds the subscriber connection by that name on the server: give it
a value unique to the process. The StackExchange.Redis default, the machine name, is shared by
every process on a host, and tracking refuses a name that more than one pub/sub client carries
rather than redirect this process's invalidations to whichever the server listed first.

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
| `Broadcast` | pattern subscriptions to `__keyspace@{db}__:{prefix}*` for `Caching:Invalidation:KeyPrefixFilters`, or the namespace's entries when the list is empty; `notify-keyspace-events` is read first where `CONFIG GET` is permitted, and a value without keyspace messages for writes and deletes leaves the explicit channel as the only fan-out, logged | `notify-keyspace-events Kg$xe` on the server |
| `Auto` (default) | `Tracking` on Redis 6.0 or later, read from the server version at connect, otherwise `Broadcast` | |

For broadcast mode, `K` enables keyspace messages, `g` covers deletion, `$` covers string
writes, and `x`/`e` cover expiry/eviction. Quote `Kg$xe` in shell commands; in Docker Compose
write `Kg$$xe` so Compose passes a literal `$` to Redis. Tracking also turns Redis's null
`FLUSHDB`/`FLUSHALL` notification into a local cache flush.

`NOLOOP` keeps a connection's own writes from evicting the in-process entry it has just written.
Prefix-based tracking remains active after those writes, without requiring a Redis read to register
the key again. It sends more invalidations than read-based tracking, bounded to the namespace's data
prefix. `Broadcast` in the options table remains the separate keyspace-notification transport.
StackExchange.Redis re-establishes every subscription on its own after a reconnect; tracking is
per server connection. The package registers every connected primary and replica using that
node's subscriber client ID, and refreshes registration after reconnects and topology changes.
Subscription recovery is counted on
`hostloom.cache.invalidation.resubscribed`. A re-established subscription connection also hands
`CacheInvalidation.Flush` to this process's subscribers when
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
subscription handlers, including while the shared multiplexer is disconnected. Disposal joins
pending subscription calls before detaching their queues. An observer that throws is logged
without preventing delivery to the remaining observers.

Only `RemoveAsync` and `RemoveByTagAsync` publish on the explicit channel. `SetAsync`,
`SetIfAbsentAsync`, get-or-create fills, and warmup publish nothing, so what another instance
learns about an overwrite depends on the transport in effect:

- **Tracking** drops another instance's in-process copy when the tracking message arrives. The
  writer keeps the entry it has just written, because of `NOLOOP`. `NOLOOP` belongs to the
  connection, so caches in one process that share a `RedisConnection` do not learn about each
  other's writes this way; removals still reach them over the explicit channel.
- **Broadcast** drops another instance's copy when the keyspace `set` event arrives. Keyspace
  notifications have no `NOLOOP`, so the writer receives its own `set` event as well and evicts
  the entry it has just written: its next read of the key is a Redis round trip, one extra read
  per write. A removal likewise returns to the remover as a keyspace `del` or `unlink` event
  besides the explicit echo. The cache recognises only one of the two as its own echo and
  applies the other, which at most evicts a refill that its own `set` event evicts anyway.
- **Explicit channel only**, when neither mode could be enabled, reports no write: another
  instance keeps serving its old in-process copy until that copy expires.

When other instances must not serve an old value, write the source of truth and then call
`RemoveAsync`, or keep `CacheEntryOptions.LocalExpiration` short. Even tracking and broadcast
deliver asynchronously, so another instance can serve the old value until the message arrives.

Two connection settings follow from this and are applied by the package: the client-side
`allowAdmin` flag, because StackExchange.Redis gates every `CLIENT` command behind it (this
grants nothing on the server; ACLs still apply), and RESP2, so subscriptions run on a dedicated
connection that tracking can redirect to. An externally supplied multiplexer needs both for
tracking to work; without them the package falls back to the explicit channel.

The explicit channel is writable by anything with `PUBLISH` rights on it, so what arrives is
bounded before it reaches the in-process tier. A message over 1 MiB is dropped before it is
decoded, one carrying more than 10 000 keys and tags together is dropped whole, and so is one
naming a key or tag the kernel would reject (empty, longer than `Caching:MaxKeyLength`, or
containing whitespace or control characters). Dropped messages are counted on
`hostloom.redis.invalidation.malformed` and on `RedisCacheInvalidationChannel.MalformedMessages`;
their content is never logged. The bounds cannot tell a well-formed flush or removal from a
foreign publisher apart from one of ours, which is what the channel pattern in the ACL below is
for.

## Server-side account and TLS

The package sets StackExchange.Redis's client-side `allowAdmin` flag because tracking needs the
`CLIENT` subcommands it gates; the flag grants nothing on the server. Give the service its own ACL
user rather than `default`, limited to the namespace's keys and channels and stripped of the
dangerous category:

```text
ACL SETUSER catalog-svc on >replace-me resetkeys resetchannels \
  ~catalog:* "~{catalog}:*" \
  "&catalog:cache:invalidate" "&__redis__:invalidate" "&__keyspace@0__:catalog:cache:data:*" \
  -@all +@read +@write +@set +@scripting +@pubsub +@connection -@dangerous \
  +info +client|setname +client|list +client|tracking +client|trackinginfo
```

- `~catalog:*` and `~{catalog}:*` cover the plain and hash-tagged key layouts; every cache entry,
  lease, tag index, and lock of the service lives under one of them.
- `&catalog:cache:invalidate` limits `PUBLISH` on the explicit channel to this user. Any client
  allowed to publish there can evict or flush every instance's in-process tier, so no other user
  should carry that channel. `&__redis__:invalidate` is the RESP2 tracking redirect channel and
  `&__keyspace@{db}__:…` the broadcast-mode subscription; drop whichever mode is not in use.
- `-@dangerous` removes `FLUSHALL`, `KEYS`, `CONFIG`, `DEBUG`, `CLIENT`, and the rest. The four
  `client|` subcommands are added back because tracking needs them and the client names its
  connections; `info` is added back so the client can read the server version that `Auto` uses to
  choose tracking. The client also sends `CONFIG GET` during its handshake and treats a refusal
  as "not available". Leave the `client|` subcommands out and invalidation falls back to the
  explicit channel only, logged once.

Encrypt the connection: add `ssl=true` (and `sslHost=…` when the certificate name differs from
the endpoint) to `Redis:Configuration`, or set `Ssl` on `Redis:ConfigurationOptions`. The package
does not turn TLS on by itself, because a local or sidecar deployment may not offer it, so an
unencrypted production connection is a configuration choice to review rather than a default.

## Redis Cluster

Use `UseHashTags = true` and `DatabaseIndex = 0` for cluster deployments so Lua and tag operations
stay within one slot. HostLoom rejects incompatible settings when Cluster topology is known,
including during hosted startup even when `FailFast` is false. Every advertised node address must be reachable from the application.
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

The standalone invalidation regressions own a separate loopback-only Redis container, so
`FLUSHDB` and `FLUSHALL` never touch the developer database. With Docker running, execute:

```bash
HOSTLOOM_REDIS_INVALIDATION_TESTS=1 dotnet test \
  --project tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj \
  -c Release -- --filter-class '*RedisInvalidationRegressionTests'
```

## Compatibility

Works against Redis 7.x and Valkey using only the commands above. StackExchange.Redis is not
annotated for trimming or Native AOT, so this package does not claim `IsAotCompatible`; the
caching and locking kernels and their `DependencyInjection` packages do.

Tagged `SetIfAbsentAsync` uses one script to conditionally add tag memberships and write the
value. A losing writer adds no memberships. Caller cancellation or connection loss cannot
interrupt the server between these steps; an ambiguous response still does not prove success.
Tracking recovery flushes L1 after command-connection tracking is re-registered when
`FlushLocalOnReconnect` is enabled, even if the Pub/Sub connection never disconnected.
