# HostLoom.Caching.DependencyInjection

This package registers the `HostLoom.Caching` kernel with `Microsoft.Extensions.DependencyInjection`:

```csharp
services
    .AddHostLoomCaching(caching => caching.Namespace = "catalog")
    .UseInMemory();
```

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
registers a readiness contributor that `Caching:Warmup:BlocksReadiness` controls. The contributor
exists under every store, so the flag means the same thing everywhere. `AddHealthChecks()`
registers a readiness check tagged `ready` that asks the store's `ICacheStoreHealthProbe`; a store
without one reports healthy with an explanation, and liveness is never touched.

The registration composes exactly what the kernel's public constructors compose, so a cache built
with `new TieredCache(...)` in a test behaves like the one the container builds. See the
`HostLoom.Caching` package README for the contracts and the fail-open rules.
