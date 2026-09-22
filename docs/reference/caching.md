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
| `SetAsync<T>(key, value, options, ct)` | Distributed tier, then in-process tier. Publishes no invalidation; see [write visibility](#write-visibility-across-instances). Null throws `ArgumentNullException`. |
| `SetIfAbsentAsync<T>(key, value, options \| expiration, ct)` | Atomic in the distributed tier (in-process when there is none). Tags are indexed in both tiers when the write happened. Publishes no invalidation. `false` when present, or when the store is unavailable and `OnUnavailable` is `ReturnFalse`; `CacheUnavailableException` under `Throw`. |
| `RemoveAsync(key \| keys, ct)` | In-process tier first, then one batched distributed call, then one invalidation message. |
| `RemoveByTagAsync(tag, ct)` | Evicts every entry written with the tag, on every instance. |
| `GetManyAsync<T>(keys, ct)` | In-process tier, then one batched distributed read; only found entries; partial under failure, never throws. |
| `WarmupAsync<T>(entries, expiration, progress, ct)` | Writes in batches of `Caching:Warmup:BatchSize`, fills both tiers, reports `CacheWarmupProgress`, fail-open. |

A factory exception propagates unchanged and nothing is stored. A null
factory result, or a non-positive expiration, is returned and not stored,
unless the call sets `NullExpiration` (below), which remembers the null for
its own time to live.

## Per-call options (`CacheEntryOptions`)

| Property | Meaning |
| --- | --- |
| `Expiration` | Absolute time to live in both tiers; non-positive means "do not store". |
| `LocalExpiration` | Shorter time to live for the in-process tier; must not exceed `Expiration`. |
| `Tags` | Tag names for `RemoveByTagAsync`; carried in the distributed payload so another instance indexes them too. A distributed tag index only gains members, so an entry rewritten under different tags stays in its earlier indexes and `RemoveByTagAsync` may evict more than currently carries the tag — a refill, never a wrong value. |
| `Size` | Approximate bytes for the in-process byte bound when the value did not arrive serialized. |
| `OnUnavailable` | `ReturnFalse` (default) or `Throw` for set-if-absent under a store failure. |
| `NullExpiration` | Negative caching. A null factory result is remembered in both tiers for this long, so a lookup for something that does not exist stops reaching the source on every call. A remembered null is a hit: `TryGetAsync` reports `Found` with a null `Value`, get-or-create returns null without running the factory, and `GetManyAsync` includes the key with a null value. For a non-nullable value type the entry is a miss. `SetAsync` still rejects null. Unset means a null result is not stored. |
| `StaleGrace` | Stale-while-revalidate for an outage. The in-process copy is kept for this long past its expiry; while the distributed store is unavailable and a get-or-create finds such a copy, one caller refreshes through the factory and every other caller receives the copy at once, with the `hit_stale` outcome. While the store answers, an expired entry is an ordinary miss. Has no effect without a distributed store. |

`CacheKey.FromSensitive(value)` hashes a high-entropy credential such as a
bearer token, refresh token, or session id (SHA-256, 32 hex characters) so
it never reaches a store, a log, or a span. The hash is unkeyed, so anyone
who can read the key can confirm a guess of the input; a low-entropy secret
such as a password, PIN, or one-time code goes through
`CacheKey.FromSensitive(value, key)` instead, which is HMAC-SHA256 under a
key you hold in configuration or a secret store, with the same 32-character
output. Share that key across the instances of one service; rotating it
turns every derived key into a miss. `CacheKey.IsValid(key, maxLength)` is
the non-throwing form of `CacheKey.Validate` for input that arrives over the
wire. `CacheKey.Versioned(key, version)` appends a per-call-site schema
version; `CachingOptions.PayloadVersion` bumps the whole cache.

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
| `Caching:L1:ExpirationJitter` | 0 | a random amount up to this, never more than half of the entry's time to live, is subtracted from each in-process expiry so instances do not miss together; set it to a fraction of the shortest expiration in use |
| `Caching:Stampede:LeaseDuration` | 30 s | cluster-wide single-flight lease |
| `Caching:Stampede:Attempts` | 2 | re-checks of the distributed tier after a missed lease |
| `Caching:Stampede:WaitBeforeFallback` | 50 ms | pause between those re-checks |
| `Caching:Invalidation:Mode` | `Auto` | `Auto`, `Tracking`, or `Broadcast`; what the backend adds to the explicit channel |
| `Caching:Invalidation:KeyPrefixFilters` | empty | prefixes for broadcast mode |
| `Caching:Invalidation:Timeout` | 5 s | bound on one publish |
| `Caching:Invalidation:MaxPending` | 1 000 | bound of the queue applying received invalidations |
| `Caching:Invalidation:FlushLocalOnReconnect` | `true` | a backend channel clears the in-process tier after its connection is restored, because invalidations published during the outage were never delivered; the cost is one cold in-process tier per reconnect |
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
whitespace or control characters, at most `Caching:MaxKeyLength` long;
`CacheKey.Validate` enforces this on every consumer entry point, including
the `IDistributedCache` adapter's members.

The single-flight guard map that backs get-or-create is bounded to four
guards per `Caching:L1:MaxEntries`. Once it is full, idle guards are
reclaimed at once instead of after `Caching:L1:GuardIdleTime`, and a key
that still finds no room shares one of 256 striped guards, so memory grows
with in-flight callers rather than with distinct keys.

## Backend contract (`IDistributedCacheStore`)

The store sees fully prefixed keys and byte payloads, never CLR types,
namespaces, or serializers. Members: `GetAsync` (payload and remaining time
to live), `SetAsync` and `SetIfAbsentAsync` (both with tag-index keys),
`RemoveAsync` (batched), `GetManyAsync`, `SetManyAsync`, `RemoveByTagAsync`, and a
`Capabilities` flags value (`Tags`, `InvalidationChannel`,
`ServerAssistedTracking`). A failure is `CacheStoreException` carrying a
`CacheFailureKind` (`Unavailable`, `Timeout`, `Other`); cancellation
propagates as `OperationCanceledException`. Memory passed to a write member
is borrowed until the returned task completes.

`ICacheInvalidationChannel` fans invalidations out between instances that
share a store. A `CacheInvalidation` carries keys and tags, or, as
`CacheInvalidation.Flush`, clears every in-process entry; the Redis and Valkey
channels raise the flush to their own subscribers after a reconnect and carry
a published one on the wire. `ICacheStoreHealthProbe` is the optional
readiness capability.
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

## Write visibility across instances

Only removals publish. `RemoveAsync` and `RemoveByTagAsync` send an invalidation on the
explicit channel, and every instance drops the named entries from its in-process tier.
`SetAsync`, `SetIfAbsentAsync`, a get-or-create fill, and `WarmupAsync` write the distributed
tier and the writer's own in-process tier and publish nothing. Whether another instance that
already holds the key in process sees an overwrite therefore depends on what reports store
writes:

| Invalidation in effect | Another instance holding the key | The instance that wrote |
| --- | --- | --- |
| Explicit channel only: `InMemoryDistributedCacheStore`, `HostLoom.Valkey`, or Redis when neither tracking nor broadcast could be enabled | keeps serving its old in-process copy until that copy expires | keeps the value it wrote |
| Redis `Tracking` | drops its copy when the server's tracking message arrives and reads the new value from Redis | keeps the value it wrote: tracking is registered with `NOLOOP` |
| Redis `Broadcast` | drops its copy when the keyspace `set` event arrives | drops it too: keyspace notifications have no `NOLOOP`, so every write evicts the writer's own in-process entry and its next read of the key goes to Redis |
| No channel (a store without one) | keeps its copy until it expires | keeps the value it wrote |

The old copy lives for its in-process time to live: the `LocalExpiration` of the call that
filled it, or otherwise the remaining distributed time to live when it was read. Even where the
backend reports writes, the report travels asynchronously, so another instance can serve the old
value until it arrives.

When other instances must not keep serving an old value, invalidate instead of overwriting:
write the source of truth, then call `RemoveAsync` for the key (or `RemoveByTagAsync`), and let
the next read on each instance fill both tiers. Where overwriting with `SetAsync` is the pattern,
a short `LocalExpiration` bounds how long other instances serve the previous value.

The explicit channel also delivers every publish back to its publisher. A cache recognises the
echo of a message it published within `Caching:Invalidation:Timeout` and skips it, so the refill
that follows its own removal stays cached. The recognition covers the explicit channel only:
under Redis broadcast a removal also returns to the remover as a keyspace `del` or `unlink` event
naming the same key, one of the two is taken for the echo, and the other is applied, which at
most evicts a refill that its own `set` event evicts anyway. Tracking reports neither the
writer's writes nor its removals back to it.

## What to keep in the in-process tier

The in-process tier answers in microseconds and the distributed tier in a
network round trip, but only the distributed tier is shared, so every
instance holds its own copy and learns about a change through an
invalidation or its expiry. That decides what belongs where:

- **Read often, changed rarely.** Reference data, configuration, feature
  flags, and lookups that are expensive to fetch are the entries that pay
  for an in-process copy. Give them the longest `Expiration` the source
  allows and let invalidation, not expiry, be what usually removes them.
- **Changed often, or read on many instances without affinity.** Keep the
  in-process life short with `LocalExpiration`, so a copy is never far
  behind the distributed tier, or leave the entry to the distributed tier
  alone with `Caching:L1:Enabled = false` on a cache of its own. Every
  instance must fill its own copy, so a value read once per instance gains
  nothing from the in-process tier.
- **Must never be stale.** A two-tier cache is eventually consistent:
  between a change and the arrival of its invalidation, an instance can
  serve the old value, and a lost message costs one in-process expiry.
  Read such data through the distributed tier only, or from the source.
- **Absent as often as present.** A lookup that usually finds nothing
  should set `NullExpiration`, otherwise every call reaches the source.
- **Must keep answering through an outage.** Set `StaleGrace` so an expired
  copy is served while the distributed store is down and one caller
  refreshes, and bound the in-process tier with `Caching:L1:MaxEntries`,
  `Caching:L1:MaxBytes`, and `Caching:L1:MaxEntryAge` so the process cannot
  grow without limit while it does.
- **Expires together.** Entries written in one warmup or at one moment
  expire at the same instant on every instance and refill together.
  `Caching:L1:ExpirationJitter` staggers the in-process expiries.

Watch the `hit_l1`, `hit_l2`, and `miss` outcomes on
`hostloom.cache.operation.duration` per namespace and `hostloom.cache.entries`
against the bound: a low in-process hit rate with a full tier means the
working set does not fit and entries are evicted before they are read
again; a low rate with a sparse tier means the data is not read often
enough to cache in process.

## Limitations

- Sliding expiration is not implemented.
- Stale-while-revalidate is limited to an outage of the distributed store:
  while the store answers, an expired entry is a miss and no stale value is
  served. There is no background refresh ahead of expiry.
- The in-process tier is per process; without a distributed tier, staleness
  across instances is bounded by expiry only.
- Only removals publish an invalidation. An overwrite reaches another
  instance's in-process copy only through Redis tracking or broadcast;
  otherwise that copy is served until it expires.
- Invalidation is not durable. A message dropped by a full queue costs one
  in-process expiry; a reconnect clears the in-process tier when
  `Caching:Invalidation:FlushLocalOnReconnect` is set, which is the default.
- The `IDistributedCache` adapter has no touch operation, so `RefreshAsync`
  is a no-op and sliding windows become absolute.
