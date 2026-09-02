# HostLoom.Caching.Testing

Composes a `HostLoom.Caching` cache for a test without a container, and decorates a store so a
test can inject failures or assert what the cache asked for.

```csharp
var clock = new FakeTimeProvider();
var store = new InMemoryDistributedCacheStore(clock);
var serializer = new SystemTextJsonCacheValueSerializer(jsonOptions);

await using var cache = TestCache.Tiered(store, serializer, timeProvider: clock);
```

`TestCache.InMemory()` composes the in-process tier alone; `TestCache.Tiered(store, serializer)`
composes both tiers. Two tiered caches over one `InMemoryDistributedCacheStore` behave like two
service instances sharing a backend: they share payloads and invalidate each other. The kernel's
constructors are public, so this adds nothing a test could not write itself; it removes the
boilerplate and keeps every test on one composition, with jitter and the stampede pause off.

`FaultingCacheStore` wraps any store and fails the next `n` calls, or every call, with a chosen
`CacheFailureKind`, which is how a test proves a consumer behaves when the distributed tier is
down: the cache serves from the in-process tier and the factory, and nothing throws.
`RecordingCacheStore` wraps any store and records every call, so a test asserts that a stampede
took one lease, a bulk lookup made one batched read, or a null factory result wrote nothing.

Substituting `ICache` is the other option and a worse one. A substitute returns what the test told
it to, so the test passes whether or not the consumer would have been served by a real cache.
This package keeps the real composition and moves the fault to where it belongs, the store.

Time is a `TimeProvider` everywhere, so a test drives expiry and leases with a fake clock instead
of waiting. This package ships no clock; `Microsoft.Extensions.TimeProvider.Testing` provides
`FakeTimeProvider`, and any `TimeProvider` works.
