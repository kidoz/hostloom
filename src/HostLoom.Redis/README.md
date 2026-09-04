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
| cache entry | `SET … PX`, `GET` with `PTTL` pipelined, `MGET` with `PTTL`, `UNLINK` |
| set-if-absent and stampede lease | `SET … NX PX` |
| tag index | `SADD`, `EXPIRE NX` then `EXPIRE GT`, `SMEMBERS`, `UNLINK` in batches; a tagged set-if-absent indexes only after the write is known to have happened |
| invalidation | `PUBLISH` and `SUBSCRIBE` on `{namespace}:cache:invalidate`; `CLIENT LIST` and `CLIENT TRACKING` in tracking mode; `PSUBSCRIBE __keyspace@{db}__:…` in broadcast mode |
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
| `Tracking` | `CLIENT TRACKING ON REDIRECT <subscriber> NOLOOP`: the server reports every key this connection has read when any other client modifies, deletes, expires, or evicts it | Redis 6.0 or later |
| `Broadcast` | pattern subscriptions to `__keyspace@{db}__:{prefix}*` for `Caching:Invalidation:KeyPrefixFilters`, or the namespace's entries when the list is empty | `notify-keyspace-events Kxe` on the server |
| `Auto` (default) | `Tracking` on Redis 6.0 or later, read from the server version at connect, otherwise `Broadcast` | |

`NOLOOP` keeps a connection's own writes from evicting the in-process entry it has just written.
StackExchange.Redis re-establishes every subscription on its own after a reconnect; tracking is
per connection, so the package registers it again on `ConnectionRestored` and counts both on
`hostloom.cache.invalidation.resubscribed`. An instance that starts while Redis is down keeps
trying to subscribe with exponential backoff; a mode that cannot be enabled after
`Redis:MaxClientCommandRetries` attempts leaves the explicit channel as the only fan-out, logged
once, and `CachingProbe.Describe` reports the transport in effect.

Two connection settings follow from this and are applied by the package: the client-side
`allowAdmin` flag, because StackExchange.Redis gates every `CLIENT` command behind it (this
grants nothing on the server; ACLs still apply), and RESP2, so subscriptions run on a dedicated
connection that tracking can redirect to. An externally supplied multiplexer needs both for
tracking to work; without them the package falls back to the explicit channel.

## Compatibility

Works against Redis 7.x and Valkey using only the commands above. StackExchange.Redis is not
annotated for trimming or Native AOT, so this package does not claim `IsAotCompatible`; the
caching and locking kernels and their `DependencyInjection` packages do.
