# Caching

The `HostLoom.Caching` family: a two-tier cache with per-key single-flight,
a best-effort cluster-wide lease, cross-instance invalidation, and fail-open
behaviour when the distributed store misbehaves. The kernel references only
`Microsoft.Extensions.Logging.Abstractions` and composes without a container;
the `DependencyInjection` package registers it; a backend package such as
`HostLoom.Redis` supplies the distributed tier. Namespaces follow the package
names.

```text
dotnet add package HostLoom.Caching
dotnet add package HostLoom.Caching.DependencyInjection   # container registration
dotnet add package HostLoom.Redis                         # Redis backend
dotnet add package HostLoom.Caching.Testing               # container-free composition
```

## Consumer contract (`ICache`)

Every member returns `ValueTask`, takes a trailing optional
`CancellationToken`, and is thread-safe.

| Member | Behaviour |
| --- | --- |
| `GetOrCreateAsync<T>(key, factory, CacheEntryOptions, ct)` | In-process tier, distributed tier, then the per-key guard and the factory, once per key per process. A distributed hit repopulates the in-process tier with the remaining time to live. |
| `GetOrCreateAsync<T>(key, factory, TimeSpan expiration, ct)` | The same with only an absolute expiration. |
| `GetOrCreateAsync<TState, T>(key, state, factory, options, ct)` | The state-carrying form; the factory receives `state` instead of closing over it, so an in-process hit allocates nothing on the caller's side. |
| `TryGetAsync<T>(key, ct)` | `CacheLookup<T>` with `Found`, `Value`, `Tier` (`None`, `L1`, `L2`), and `Degraded`. The member new code uses. |
| `GetAsync<T>(key, ct)` | `default(T)` on a miss or under degradation. For a value type a cached `0` is indistinguishable from a miss; kept for call sites written against that contract. |
| `SetAsync<T>(key, value, options, ct)` | Distributed tier, then in-process tier. Null throws `ArgumentNullException`. |
| `SetIfAbsentAsync<T>(key, value, options \| expiration, ct)` | Atomic in the distributed tier (in-process when there is none). Tags are indexed in both tiers when the write happened. `false` when present, or when the store is unavailable and `OnUnavailable` is `ReturnFalse`; `CacheUnavailableException` under `Throw`. |
| `RemoveAsync(key \| keys, ct)` | In-process tier first, then one batched distributed call, then one invalidation message. |
| `RemoveByTagAsync(tag, ct)` | Evicts every entry written with the tag, on every instance. |
| `GetManyAsync<T>(keys, ct)` | In-process tier, then one batched distributed read; only found entries; partial under failure, never throws. |
| `WarmupAsync<T>(entries, expiration, progress, ct)` | Writes in batches of `Caching:Warmup:BatchSize`, fills both tiers, reports `CacheWarmupProgress`, fail-open. |

A factory exception propagates unchanged and nothing is stored. A null
factory result, or a non-positive expiration, is returned and not stored.

## Per-call options (`CacheEntryOptions`)

| Property | Meaning |
| --- | --- |
| `Expiration` | Absolute time to live in both tiers; non-positive means "do not store". |
| `LocalExpiration` | Shorter time to live for the in-process tier; must not exceed `Expiration`. |
| `Tags` | Tag names for `RemoveByTagAsync`; carried in the distributed payload so another instance indexes them too. A distributed tag index only gains members, so an entry rewritten under different tags stays in its earlier indexes and `RemoveByTagAsync` may evict more than currently carries the tag — a refill, never a wrong value. |
| `Size` | Approximate bytes for the in-process byte bound when the value did not arrive serialized. |
| `OnUnavailable` | `ReturnFalse` (default) or `Throw` for set-if-absent under a store failure. |

`CacheKey.FromSensitive(value)` hashes a credential (SHA-256, 32 hex
characters) so it never reaches a store, a log, or a span.
`CacheKey.Versioned(key, version)` appends a per-call-site schema version;
`CachingOptions.PayloadVersion` bumps the whole cache.

## Configuration (`CachingOptions`)

Defaults reproduce the platform behaviour a migrating service expects. Every
duration is a `TimeSpan`; `Validate()` returns every violation naming its
option key, and the `DependencyInjection` package runs it at startup.

| Key | Default | Meaning |
| --- | --- | --- |
| `Caching:Namespace` | required | `[a-z0-9-]+`; prefixes every key |
| `Caching:L1:Enabled` | `true` | in-process tier on |
| `Caching:L1:MaxEntries` | 10 000 | above it a sampled least-recently-accessed `EvictionFraction` (0.25) is evicted; at 150 % everything is cleared |
| `Caching:L1:MaxBytes` | unbounded | approximate byte bound |
| `Caching:L1:MaxEntryAge` | 30 min | time to live when none is given |
| `Caching:L1:CleanupInterval` | 1 min | expired-entry and idle-guard reclaim |
| `Caching:L1:GuardIdleTime` | 10 min | idle single-flight guard lifetime |
| `Caching:L1:ExpirationJitter` | 0 | subtracted from each in-process expiry so instances do not miss together |
| `Caching:Stampede:LeaseDuration` | 30 s | cluster-wide single-flight lease |
| `Caching:Stampede:Attempts` | 2 | re-checks of the distributed tier after a missed lease |
| `Caching:Stampede:WaitBeforeFallback` | 50 ms | pause between those re-checks |
| `Caching:Invalidation:Mode` | `Auto` | `Auto`, `Tracking`, or `Broadcast`; what the backend adds to the explicit channel |
| `Caching:Invalidation:KeyPrefixFilters` | empty | prefixes for broadcast mode |
| `Caching:Invalidation:Timeout` | 5 s | bound on one publish |
| `Caching:Invalidation:MaxPending` | 1 000 | bound of the queue applying received invalidations |
| `Caching:Compression:ThresholdBytes` | 1 024 | Brotli above this size |
| `Caching:Warmup:BatchSize` | 100 | entries per distributed write during warmup |
| `Caching:Warmup:BlocksReadiness` | `false` | readiness waits for registered warmups |
| `Caching:Diagnostics:DegradedLogInterval` | 1 min | one degraded warning per key per interval |
| `Caching:MaxKeyLength` | 512 | longest consumer key |
| `Caching:MaxPayloadBytes` | 10 MB | bounds the serialized body and the stored payload; oversize values stay in-process only, logged at error, and a stored entry declaring more is read as corrupt |
| `Caching:PayloadVersion` | none | appended to every entry key |

## Keys

Every kind of key lives in its own domain, so a consumer key can never
collide with a lease, a tag index, or a lock:

| Purpose | Key |
| --- | --- |
| cache entry | `{namespace}:cache:data:{key}` |
| stampede lease | `{namespace}:cache:lease:{key}` |
| tag index | `{namespace}:cache:tag:{tag}` |
| invalidation channel | `{namespace}:cache:invalidate` |
| `IDistributedCache` adapter entry | `{namespace}:cache:external:{key}` |

Consumers never see or repeat the prefix. Keys are opaque strings without
whitespace or control characters.

## Backend contract (`IDistributedCacheStore`)

The store sees fully prefixed keys and byte payloads, never CLR types,
namespaces, or serializers. Members: `GetAsync` (payload and remaining time
to live), `SetAsync` (with tag-index keys), `SetIfAbsentAsync`, `RemoveAsync`
(batched), `GetManyAsync`, `SetManyAsync`, `RemoveByTagAsync`, and a
`Capabilities` flags value (`Tags`, `InvalidationChannel`,
`ServerAssistedTracking`). A failure is `CacheStoreException` carrying a
`CacheFailureKind` (`Unavailable`, `Timeout`, `Other`); cancellation
propagates as `OperationCanceledException`. Memory passed to a write member
is borrowed until the returned task completes.

`ICacheInvalidationChannel` fans invalidations out between instances that
share a store; `ICacheStoreHealthProbe` is the optional readiness capability.
`InMemoryDistributedCacheStore` implements both the store and the channel in
process memory, which is what the conformance suite and the Native AOT
sample use.

## Serialization

`ICacheValueSerializer` is generic over `T`, writes to an `IBufferWriter<byte>`
and reads from a span, so a source-generated `JsonSerializerContext` works
without reflection. `SystemTextJsonCacheValueSerializer` requires
`JsonSerializerOptions.TypeInfoResolver`; `CreateReflectionBased` is the
annotated opt-out. Each payload carries a one-byte header (format version and
flags), the tag names when tagged, and the body, Brotli-compressed at or above
the threshold. A payload from another format version is a silent miss; one
that fails to deserialize is a miss logged at error level and overwritten by
the next factory result. A compressed payload carries its uncompressed length,
which comes from the store and is therefore not trusted past
`Caching:MaxPayloadBytes`: a larger declared length is corrupt rather than a
buffer to allocate, which is why the same bound is applied to the body when
writing.

## Registration (DependencyInjection)

```csharp
CachingBuilder AddHostLoomCaching(this IServiceCollection services,
    Action<CachingOptions>? configure = null);
```

| Builder member | Effect |
| --- | --- |
| `UseInMemory()` | in-process tier as the only tier |
| `UseStore<TStore>(name)` | a distributed store; also registers it as channel and probe when it implements them; backend packages call this from their `Use*` |
| `UseSystemTextJson(options)` | serializer; requires a type-info resolver |
| `UseSerializer<TSerializer>()` | any serializer; replaces an earlier choice |
| `UseReflectionSerialization()` | the annotated non-AOT opt-out |
| `AddWarmup<TWarmup>()` | runs an `ICacheWarmup` after startup, with the readiness contributor `Caching:Warmup:BlocksReadiness` governs |
| `AddHealthChecks(name)` | readiness check tagged `ready` over the store's probe; never liveness |
| `AddDistributedCacheAdapter(defaultExpiration)` | `IDistributedCache` and `IBufferDistributedCache` over the store for `HybridCache`; asynchronous members only; store failures answer as a miss, counted on `hostloom.cache.errors` and logged once per key per `Caching:Diagnostics:DegradedLogInterval` |

Exactly one store per builder; a second choice throws naming the first.
Repeated `AddHostLoomCaching` calls return a builder over the same
registration. A composition with a distributed store and no serializer fails
validation naming `UseSystemTextJson`.

## Diagnostics

Meter and activity source `HostLoom.Caching`; instruments and activities are
listed in the [observability surface](observability.md).
`CachingProbe.Describe(cache, warmups)` returns a `CacheDescription` whose
lines name the option that decided each part of the composition, without
executing anything.

## Testing

`HostLoom.Caching.Testing` composes a `TieredCache` without a container
(`TestCache.InMemory()`, `TestCache.Tiered(store, serializer)`), and
decorates a store to inject failures (`FaultingCacheStore`) or record calls
(`RecordingCacheStore`). The in-process store implements the whole contract,
so a consumer's tests need no backend.

## Limitations

- Sliding expiration, negative caching, and stale-while-revalidate are not
  implemented.
- The in-process tier is per process; without a distributed tier, staleness
  across instances is bounded by expiry only.
- The `IDistributedCache` adapter has no touch operation, so `RefreshAsync`
  is a no-op and sliding windows become absolute.
