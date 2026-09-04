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
per-key guard and runs the factory once per key per process. A distributed hit repopulates the
in-process tier with the remaining time to live. The state-carrying overload shown above captures
nothing, so an in-process hit allocates nothing on the caller's side; the simpler
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
characters, bounded by `Caching:MaxKeyLength`. `CacheKey.FromSensitive` hashes a credential so it
never reaches the store or a log; `CacheKey.Versioned` appends a per-call-site schema version;
`CachingOptions.PayloadVersion` bumps the whole cache.

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
