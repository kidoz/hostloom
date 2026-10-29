# Cache and lock over Redis

Move a HostLoom cache from the in-process store onto Redis, and a HostLoom
lock from the in-process provider onto Redis. The default registrations share one connection;
named profiles can isolate runtimes, connections, and databases.

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

Replace `UseInMemory()` with `UseRedis(...)` on each builder. The first default call
configures the connection; the second reuses it. Existing consumers still inject unkeyed `ICache`
and `IDistributedLock`:

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

### Isolate named runtimes and connections

Pass a profile name when a cache or lock needs independent options or backend composition, then
resolve the keyed service:

```csharp
builder.Services.AddHostLoomCaching("catalog-read", options => options.Namespace = "catalog-read")
    .UseRedis(redis =>
    {
        redis.Configuration = "redis-read:6379";
        redis.DatabaseIndex = 2;
    })
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = CatalogJsonContext.Default });
builder.Services.AddHostLoomLocking("catalog-jobs", options => options.Namespace = "catalog-jobs")
    .UseRedis(redis =>
    {
        redis.Configuration = "redis-jobs:6379";
        redis.DatabaseIndex = 1;
    });

var cache = provider.GetRequiredKeyedService<ICache>("catalog-read");
var jobs = provider.GetRequiredKeyedService<IDistributedLock>("catalog-jobs");
```

Cache and lock registrations with the same profile name share one owned Redis connection.
Different profiles own different connections. Configure a shared profile once and attach the other
capability with parameterless `UseRedis()`. A repeated configuration must match; a conflict fails
and names both registrations and conflicting option keys without exposing setting values.

Provider disposal disposes each owned connection once. Profiles do not have independent teardown:
their resources live until provider disposal. A multiplexer supplied through `ConnectionFactory`
stays caller-owned and survives provider disposal.

### Share one multiplexer across standalone databases

Different databases over one multiplexer require standalone Redis and an externally owned
multiplexer. Give each profile the same factory and its own database and namespace. HostLoom
creates one unowned wrapper per profile:

```csharp
IConnectionMultiplexer multiplexer = await ConnectionMultiplexer.ConnectAsync(
    "localhost:6379,allowAdmin=true,protocol=resp2");
Func<CancellationToken, Task<IConnectionMultiplexer>> factory =
    _ => Task.FromResult(multiplexer);

builder.Services.AddHostLoomCaching("catalog", options => options.Namespace = "catalog")
    .UseRedis(redis => { redis.ConnectionFactory = factory; redis.DatabaseIndex = 2; })
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = CatalogJsonContext.Default });
builder.Services.AddHostLoomLocking("catalog-jobs", options => options.Namespace = "catalog-jobs")
    .UseRedis(redis => { redis.ConnectionFactory = factory; redis.DatabaseIndex = 1; });
```

This is not a Redis Cluster composition: Cluster requires database zero and hash tags. At most one
cache profile on a multiplexer may use tracking or broadcast. Other profiles on that multiplexer
fall back to explicit invalidation even when their mode requests `Auto`, `Tracking`, or
`Broadcast`; independent wrappers cannot create independent multiplexer-wide tracking channels.

### Bind consumers and Microsoft cache adapters

Select the exact lock profile for leadership, node-ID leasing, and exclusive schedules:

```csharp
builder.Services.AddHostLoomLeadership()
    .AddRole("catalog-reconciler", lockProfile: "catalog-jobs");
builder.Services.AddHostLoomNodeLeasing()
    .AddPool("catalog-workers", lockProfile: "catalog-jobs");
builder.Services.AddHostLoomScheduling()
    .UseDistributedLock(lockProfile: "catalog-jobs");
```

A missing profile fails startup and names its consumer and profile. Lease limits, key lengths, and
uncoordinated behavior come from the bound lock, not from an enabled default lock.

A named cache's `AddDistributedCacheAdapter(...)` registers keyed `IDistributedCache` and
`IBufferDistributedCache`. Only the parameterless cache registration publishes the unkeyed
adapters used by `HybridCache`. The primary `ICache` API remains absolute-only. To opt the
Microsoft adapter into sliding expiration:

```csharp
builder.Services.AddHostLoomCaching(options => options.Namespace = "catalog")
    .UseRedis(redis => redis.Configuration = "localhost:6379")
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = CatalogJsonContext.Default })
    .AddDistributedCacheAdapter(enableSlidingExpiration: true);
```

Without that opt-in, `DistributedCacheEntryOptions.SlidingExpiration` throws
`NotSupportedException`. With it, a successful read is followed by a generation-checked,
expire-only renewal bounded by the absolute deadline on Redis's clock. A deleted or replaced key
is not refreshed. If renewal fails after a valid read, the adapter returns that value fail-open;
caller cancellation still propagates. See [Caching](../reference/caching.md) for the storage-format
and mixed-version rules.

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
catalog:cache:data:{key}          the tiered-cache entry, SET … PX
catalog:cache:lease:{key}         the stampede lease, SET … NX PX
catalog:cache:tag:{tag}           a set of entry keys, expiring with its longest member
catalog:cache:invalidate          the pub/sub channel
catalog:cache:external:{key}      legacy/default Microsoft-adapter bytes
catalog:cache:external-v2:{key}   opted-in sliding envelope
catalog:lock:{key}                the lock, SET … NX PX with a random owner token
```

With `UseHashTags` the namespace segment is wrapped as `{catalog}` so a
Redis Cluster keeps them in one slot. `DatabaseIndex` is passed per command rather than relying on
`SELECT`.

### Migrate cache and lock formats

The default Microsoft adapter keeps raw payloads and absolute expiration. The sliding opt-in adds
a binary envelope and moves writes to the `external-v2` domain; it still reads legacy raw entries.
A successful enveloped write retires the legacy value for that consumer key, and removal deletes
both forms. Old readers miss new enveloped entries, and legacy entries cannot gain sliding behavior. Stop or drain old writers before the cutover, or implement application-owned
dual writes for an overlapping rollout. Dual deletion by itself does not make arbitrary
mixed-version writers coherent. Roll back only after stopping the opted-in writers and draining
or repopulating data the old readers need.

Canonical locks use `{namespace}:lock:{key}`. If an existing deployment uses a different physical
identity such as `lock:{key}`, an application-owned compatibility `ILockProvider` must map to that
exact key while retaining owner tokens, lease accounting, typed failures, and owner-checked
release and extension. HostLoom does not ship a namespace-validation bypass or a production
legacy-identity switch.

Without that compatibility provider, canonical and legacy locks are independent and do not
contend. For an incompatible transition, stop all old workers, let existing leases expire, switch
all instances, then start the new workers. Rollback requires the same stop-and-drain procedure;
never run both identities for one logical lock. Compatibility does not add fencing or consensus:
Redis replication and failover can still lose an unreplicated lease, so persisted-state invariants
belong in transactions, constraints, or idempotency.

## 6. Choose how other instances learn about changes

`RemoveAsync` always tells every instance through the explicit channel. For
entries another instance simply overwrites, or the server expires, set
`Caching:Invalidation:Mode`:

- `Auto`, the default, uses client tracking on Redis 6.0 or later and
  keyspace notifications below that.
- `Tracking` uses `CLIENT TRACKING BCAST PREFIX` to report changes to the
  namespace's cache-data keys, covering entries populated by writes and warmup
  as well as reads. Own writes are suppressed with `NOLOOP`. Nothing to configure
  on the server.
- `Broadcast` subscribes to keyspace notifications for the namespace's
  entries, or for `Caching:Invalidation:KeyPrefixFilters` when set, and needs
  `notify-keyspace-events Kg$xe` on the server (`g` for deletion, `$` for string writes,
  `x`/`e` for expiry/eviction). Quote this value in shell commands; use `Kg$$xe` in Docker
  Compose to preserve the literal `$`.

`CachingProbe.Describe(cache)` reports the transport in effect. A mode that
cannot be enabled falls back to the explicit channel and logs once.

Whatever arrives on the explicit channel is bounded before it touches the
in-process tier: a message over 1 MiB, one with more than 10 000 keys and
tags, or one naming a key the kernel would reject is dropped whole and
counted on `hostloom.redis.invalidation.malformed`. A well-formed message
from any client that may publish there still evicts or flushes every
instance, which is what the next step is for.

Well-formed or not, received messages funnel into one queue bounded by
`Caching:Invalidation:MaxPending` (default 1 000); a full queue drops the
newest message, counted on `hostloom.redis.invalidation.dropped`, and the
in-process entries it would have removed stay until they expire.

## 7. Restrict the account and encrypt the connection

The package sets StackExchange.Redis's client-side `allowAdmin` flag so it
can issue the `CLIENT` subcommands tracking needs; that flag grants nothing
on the server. Give the service its own ACL user, limited to the namespace's
keys and channels and stripped of the dangerous category:

```text
ACL SETUSER catalog-svc on >replace-me resetkeys resetchannels \
  ~catalog:* "~{catalog}:*" \
  "&catalog:cache:invalidate" "&__redis__:invalidate" "&__keyspace@0__:catalog:cache:data:*" \
  -@all +@read +@write +@set +@scripting +@pubsub +@connection -@dangerous \
  +info +client|setname +client|list +client|tracking +client|trackinginfo
```

`&catalog:cache:invalidate` is the important line: only a user carrying that
channel pattern can publish invalidations, and so only that user can evict
other instances' in-process entries. Keep `__redis__:invalidate` for
tracking mode or the `__keyspace@…` pattern for broadcast mode, not both.
The `client|` subcommands and `info` are added back after `-@dangerous`
because tracking and the `Auto` mode's version check need them; without them
invalidation falls back to the explicit channel only.

Then put `ssl=true` in `Redis:Configuration` (with `sslHost=…` when the
certificate name differs from the endpoint), or set `Ssl` on
`Redis:ConfigurationOptions`. The package does not default TLS on, so an
unencrypted production connection is a choice you made in configuration.

## 8. Watch it

Enable the `HostLoom.Caching`, `HostLoom.Locking`, and `HostLoom.Redis`
meters. `hostloom.redis.connection.state` drops to 0 during an outage and
`hostloom.redis.reconnects` counts recoveries; `hostloom.cache.operation.duration`
shows a `degraded` outcome for every call served without Redis. See the
[observability surface](../reference/observability.md).

## Related

- [Packages](../reference/packages.md) for the dependency edges.
- [Expose health checks and metrics](health-and-metrics.md) for the readiness
  wiring.
