# HostLoom.Caching.DependencyInjection

This package registers the `HostLoom.Caching` kernel with `Microsoft.Extensions.DependencyInjection`:

```csharp
services
    .AddHostLoomCaching(caching => caching.Namespace = "catalog")
    .UseInMemory();
```

Pass a profile name to compose independently configured caches and resolve them as keyed services:

```csharp
services.AddHostLoomCaching("catalog", caching => caching.Namespace = "catalog").UseInMemory();
services.AddHostLoomCaching("inventory", caching => caching.Namespace = "inventory").UseInMemory();

var catalog = provider.GetRequiredKeyedService<ICache>("catalog");
var inventory = provider.GetRequiredKeyedService<ICache>("inventory");
```

Each profile has its own named options, store, serializer, warmup, and readiness resources; replacing a
serializer affects only that profile. Default health-check names gain a `:{profile}` suffix, while a
caller-supplied name is used as given and must be unique. `AddDistributedCacheAdapter` on a named
profile registers keyed `IDistributedCache` and `IBufferDistributedCache` services only; the default
registration publishes the unkeyed services used by `HybridCache`. Service-provider disposal disposes
all owned profiles; profiles do not have independent teardown lifetimes.

## Microsoft distributed-cache adapter

`AddDistributedCacheAdapter` publishes `IDistributedCache` and `IBufferDistributedCache` over the
selected distributed store. Its default registration preserves raw payloads and absolute expiration;
supplying `SlidingExpiration` without opting in throws `NotSupportedException`. `ICache`,
`TieredCache`, and `CacheEntryOptions` remain absolute-only.

```csharp
services.AddHostLoomCaching(caching => caching.Namespace = "catalog")
    .UseStore<CatalogCacheStore>("Catalog")
    .AddDistributedCacheAdapter(enableSlidingExpiration: true);
```

The opted-in store must implement `ISlidingExpirationCacheStore` and advertise
`CacheStoreCapabilities.SlidingExpiration`; resolution names the profile and missing capability
otherwise. Reads and explicit refreshes renew only entries with sliding metadata. Renewal compares
the complete read snapshot before applying an expire-only update, so deletion or replacement wins a
delayed refresh. An absolute cap is enforced on the store's clock. A refresh failure returns the
completed-read value fail-open, unless the store established that it expired; caller cancellation
propagates. Absolute-only refresh is a no-op.

Opted-in writes use a 40-byte versioned binary envelope under
`{namespace}:cache:external-v2:{key}`, with no reflection serializer. `Caching:MaxPayloadBytes`
includes that envelope. Legacy raw entries under `{namespace}:cache:external:{key}` remain readable
without gaining sliding behavior. A successful enveloped write retires the legacy value for that
consumer key, and removal deletes both representations. Older readers miss new entries. Drain
mixed-version writers or use application-owned dual writes; dual deletion alone does not make
arbitrary mixed-version writes coherent.

`AddHostLoomCaching` registers `ICache`, binds `CachingOptions`, and validates them when the host
starts, with every message naming the option key. The builder chooses exactly one store:
`UseInMemory()` composes the in-process tier alone, and `UseStore<TStore>(name)` composes a
distributed tier, which is the primitive a backend package such as `HostLoom.Redis` calls from
its own `UseRedis()`. A second choice throws and names the first.

A distributed tier needs a serializer. `UseSystemTextJson(options)` takes `JsonSerializerOptions`
whose `TypeInfoResolver` is set, typically a source-generated `JsonSerializerContext`, which is
what keeps a trimmed or Native AOT publish warning-free. `UseReflectionSerialization()` is the
documented opt-out, annotated so the publish reports it. `UseSerializer<TSerializer>()` installs
any `ICacheValueSerializer`.

`AddWarmup<TWarmup>()` runs an `ICacheWarmup` once after the host starts, in the background, and
registers a readiness contributor that `Caching:Warmup:BlocksReadiness` controls. Its description
distinguishes not started, running, succeeded, failed, and cancelled, with successful and failed
warmup counts retained for partial progress. Cancellation is neither success nor failure. The flag
can make readiness unhealthy only while a run is in progress: terminal failure and cancellation
remain fail-open. The contributor exists under every store, so the flag means the same thing
everywhere. `AddHealthChecks()` registers a readiness check tagged `ready` that asks the store's
`ICacheStoreHealthProbe`; a store without one reports healthy with an explanation, and liveness is
never touched.

The registration composes exactly what the kernel's public constructors compose, so a cache built
with `new TieredCache(...)` in a test behaves like the one the container builds. See the
`HostLoom.Caching` package README for the contracts and the fail-open rules.
