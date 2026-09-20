# HostLoom.Caching

A two-tier cache for services that need one: an in-process tier in front of an optional
distributed tier, per-key single-flight, a best-effort cluster-wide lease, cross-instance
invalidation, and fail-open behaviour when the distributed store misbehaves. This package is the
kernel: contracts, the in-process stores, the serializer, the tiered composition, metrics, and an
execution-free probe. It references only `Microsoft.Extensions.Logging.Abstractions`, so a
consumer that takes `ICache` in a constructor pulls in nothing else. Registration lives in
`HostLoom.Caching.DependencyInjection`; backends such as Redis live in their own package.

## What a consumer sees

```csharp
public sealed class CatalogService(ICache cache)
{
    public ValueTask<Catalog?> GetAsync(string region, CancellationToken cancellationToken) =>
        cache.GetOrCreateAsync(
            $"catalog:{region}",
            region,
            static (region, token) => LoadCatalogAsync(region, token),
            new CacheEntryOptions(TimeSpan.FromMinutes(10)) { Tags = ["catalog"] },
            cancellationToken);
}
```

`GetOrCreateAsync` looks in the in-process tier, then the distributed tier, then takes the
per-key guard and rechecks both tiers before running the factory. The recheck also covers a delayed
distributed miss arriving after another caller has already filled the cache and released the guard.
A distributed hit repopulates the in-process tier with the remaining time to live. The state-carrying
overload shown above captures nothing, so an in-process hit allocates nothing on the caller's side; the simpler
`Func<CancellationToken, ValueTask<T>>` overload exists for ergonomics.

`TryGetAsync<T>` returns a `CacheLookup<T>` with `Found`, `Value`, `Tier`, and `Degraded`, and is
the member new code uses. `GetAsync<T>` returns `default(T)` on a miss, which for a value type
makes a cached `0` indistinguishable from an absence; it exists for call sites written against
that contract. `SetAsync`, `SetIfAbsentAsync`, `RemoveAsync`, `RemoveByTagAsync`, `GetManyAsync`,
and `WarmupAsync` complete the surface. Every member returns `ValueTask`, takes a trailing
optional `CancellationToken`, and is thread-safe.

## Fail-open

A distributed-store failure never reaches a consumer as an exception from a read or a
get-or-create. The cache degrades to the factory, keeps the in-process tier, records
`hostloom.cache.errors` and a `degraded` outcome on `hostloom.cache.operation.duration`, and logs
one warning per key per `Caching:Diagnostics:DegradedLogInterval`. The exceptions to this rule are
deliberate: `SetIfAbsentAsync` throws `CacheUnavailableException` when the caller chose
`UnavailableBehavior.Throw`, so a rate limiter can choose between allow and deny, and cancellation
always propagates. A factory exception propagates unchanged and nothing is stored.

Two per-call options shape what a miss costs. `CacheEntryOptions.NullExpiration` remembers a null
factory result in both tiers for its own time to live, so a lookup for something that does not
exist stops reaching the source; `TryGetAsync` reports it as found with a null value. Without it a
null result is returned and not stored. `CacheEntryOptions.StaleGrace` keeps an expired in-process
copy for an outage: while the distributed store is unavailable, one caller refreshes through the
factory and every other caller receives the copy at once, as the `hit_stale` outcome. After a
reconnect, a backend channel clears the in-process tier (`Caching:Invalidation:FlushLocalOnReconnect`),
because invalidations published during the outage were never delivered.

An applied invalidation also prevents overlapping distributed reads, writes, warmup batches,
and factory results from refilling the in-process tier. A factory invalidated before its write
starts returns its result without caching it. This uses one generation per cache instance, so
an unrelated invalidation can also suppress an in-flight fill. It does not make distributed
operations atomic: an overlapping caller can still receive an older result, and missed or
queued invalidations still leave a window of staleness.

Tracking notifications and distributed reads can arrive in either order. Even a read of the
latest value can overlap a delayed notification for that same key; the cache cannot determine
which write the notification refers to. That fill is suppressed, or evicted if already inserted,
so the next read may also come from L2. Once the notification has been applied, a subsequent
read can populate L1 normally. Per-key generations would still need this same-key protection.

## Keys

`CachingOptions.Namespace` is required and prefixes every key. Each kind of key has its own domain,
so a consumer key can never collide with a lease, a tag index, or a lock:

| Purpose | Key |
|---|---|
| cache entry | `{namespace}:cache:data:{key}` |
| stampede lease | `{namespace}:cache:lease:{key}` |
| tag index | `{namespace}:cache:tag:{tag}` |
| invalidation channel | `{namespace}:cache:invalidate` |

Consumers never see or repeat the prefix. Keys are opaque strings without whitespace or control
characters, bounded by `Caching:MaxKeyLength`; `CacheKey.Validate` enforces this and
`CacheKey.IsValid` is its non-throwing form. `CacheKey.FromSensitive(value)` hashes a high-entropy
credential such as a token or session id so it never reaches the store or a log; because that hash
is unkeyed, a low-entropy secret such as a password or PIN goes through
`CacheKey.FromSensitive(value, key)`, HMAC-SHA256 under a key held in configuration or a secret
store and shared by every instance of the service. `CacheKey.Versioned` appends a per-call-site
schema version; `CachingOptions.PayloadVersion` bumps the whole cache.

The single-flight guard map is bounded to four guards per `Caching:L1:MaxEntries`: at that size
idle guards are reclaimed at once rather than after `Caching:L1:GuardIdleTime`, and a key that
still finds no room shares one of 256 striped guards, so memory follows in-flight callers rather
than the number of distinct keys seen.

## Serialization

Payloads go through `ICacheValueSerializer`, generic over `T` so a source-generated
`JsonSerializerContext` works without reflection. `SystemTextJsonCacheValueSerializer` requires
`JsonSerializerOptions.TypeInfoResolver` to be set; `CreateReflectionBased` is the annotated
opt-out. Each payload carries a one-byte header with the format version and flags; payloads at or
above `Caching:Compression:ThresholdBytes` are Brotli-compressed. A payload from another format
version is a silent miss so a rolling deploy does not log errors; a payload that fails to
deserialize is a miss logged at error level and overwritten by the next factory result. The
uncompressed length a compressed payload declares comes from the store, so it is believed only up
to `Caching:MaxPayloadBytes`; the same bound is applied to the body when writing, so an entry this
cache wrote always reads back.

## Composing without a container

Every runtime type has a public constructor. A test or a non-hosted program composes a cache with
`new`, and the dependency-injection package composes exactly the same object graph:

```csharp
var options = new CachingOptions { Namespace = "catalog" };
var store = new InMemoryDistributedCacheStore(timeProvider);
var serializer = new SystemTextJsonCacheValueSerializer(jsonOptions);
await using var cache = new TieredCache(options, store, serializer, timeProvider: timeProvider);
```

`TieredCache` with no store is the in-process-only cache. `InMemoryDistributedCacheStore` is a
byte-payload store and invalidation channel in process memory: two caches over one instance share
payloads and invalidate each other, which is what the conformance suite and the Native AOT sample
use. `LocalCacheStore` is the in-process tier on its own.

## Observability

Meter and activity source are both named `HostLoom.Caching`. Instruments are
`hostloom.cache.operation.duration`, `hostloom.cache.factory.duration`, `hostloom.cache.entries`,
`hostloom.cache.guards.active`, `hostloom.cache.stampede.lease_missed`,
`hostloom.cache.invalidations`, `hostloom.cache.invalidation.resubscribed`,
`hostloom.cache.errors`, and `hostloom.cache.compressions`; identity is the
`hostloom.cache.namespace` tag. `CachingProbe.Describe(cache)` returns the composition, each line
naming the option that decided it, without executing anything.
