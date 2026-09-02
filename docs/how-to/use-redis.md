# Cache and lock over Redis

Move a HostLoom cache from the in-process store onto Redis, and a HostLoom
lock from the in-process provider onto Redis, sharing one connection. The
consumers do not change: they still inject `ICache` and `IDistributedLock`.

## Before you begin

- A service registered with `HostLoom.Caching.DependencyInjection` or
  `HostLoom.Locking.DependencyInjection`.
- A reachable Redis 7.x or Valkey. For local work, the repository's
  `docker-compose.yml` provides one:

```text
docker compose up -d redis
```

## 1. Install the package

```text
dotnet add package HostLoom.Redis
```

## 2. Swap the store and the provider

Replace `UseInMemory()` with `UseRedis(...)` on each builder. The first call
configures the connection; the second reuses it:

```csharp
using HostLoom.Redis;

builder.Services
    .AddHostLoomCaching(caching => caching.Namespace = "catalog")
    .UseRedis(redis => redis.Configuration = builder.Configuration["Redis:Configuration"])
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = CatalogJsonContext.Default })
    .AddHealthChecks();

builder.Services
    .AddHostLoomLocking(locking => locking.Namespace = "catalog")
    .UseRedis()
    .AddHealthChecks();
```

A distributed tier needs a serializer. The `TypeInfoResolver` is required, so
a trimmed or Native AOT publish stays warning-free; `UseReflectionSerialization()`
is the annotated opt-out.

## 3. Bind the options from configuration

`RedisOptions` binds like any options class:

```json
{
  "Redis": {
    "Configuration": "redis-primary:6379,ssl=true,user=catalog",
    "DatabaseIndex": 0,
    "UseHashTags": true,
    "ConnectTimeout": "00:00:05",
    "CommandTimeout": "00:00:02",
    "HealthTimeout": "00:00:02",
    "FailFast": false
  }
}
```

```csharp
builder.Services.Configure<RedisOptions>(builder.Configuration.GetSection("Redis"));
```

Put the password in the configuration string from a secret store. The package
never logs it: the startup line and the probe output show the endpoints,
database index, and client name only.

## 4. Decide what an outage means

`FailFast` is `false` by default. With Redis unreachable the host still starts,
the readiness checks report unhealthy, `ICache` serves from its in-process tier
and the factories, and `IDistributedLock` throws
`LockProviderUnavailableException`. StackExchange.Redis reconnects in the
background and the cache resumes on its own. Set `FailFast = true` where a
service must not run without Redis at all.

## 5. Check what is on the server

Every key of a namespace lives in its own domain, so nothing collides:

```text
catalog:cache:data:{key}      the entry, SET … PX
catalog:cache:lease:{key}     the stampede lease, SET … NX PX
catalog:cache:tag:{tag}       a set of entry keys, expiring with its longest member
catalog:cache:invalidate      the pub/sub channel
catalog:lock:{key}            the lock, SET … NX PX with a random owner token
```

With `UseHashTags` the namespace segment is wrapped as `{catalog}` so a
Redis Cluster keeps them in one slot. Nothing relies on `SELECT`.

## 6. Choose how other instances learn about changes

`RemoveAsync` always tells every instance through the explicit channel. For
entries another instance simply overwrites, or the server expires, set
`Caching:Invalidation:Mode`:

- `Auto`, the default, uses client tracking on Redis 6.0 or later and
  keyspace notifications below that.
- `Tracking` asks the server to report every key this instance has read when
  any other client changes it. Nothing to configure on the server.
- `Broadcast` subscribes to keyspace notifications for the namespace's
  entries, or for `Caching:Invalidation:KeyPrefixFilters` when set, and needs
  `notify-keyspace-events Kxe` on the server.

`CachingProbe.Describe(cache)` reports the transport in effect. A mode that
cannot be enabled falls back to the explicit channel and logs once.

## 7. Watch it

Enable the `HostLoom.Caching`, `HostLoom.Locking`, and `HostLoom.Redis`
meters. `hostloom.redis.connection.state` drops to 0 during an outage and
`hostloom.redis.reconnects` counts recoveries; `hostloom.cache.operation.duration`
shows a `degraded` outcome for every call served without Redis. See the
[observability surface](../reference/observability.md).

## Related

- [Packages](../reference/packages.md) for the dependency edges.
- [Expose health checks and metrics](health-and-metrics.md) for the readiness
  wiring.
