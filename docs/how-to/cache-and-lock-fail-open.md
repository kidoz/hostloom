# Keep serving when the cache backend is down

Configure a HostLoom cache and lock so a Redis outage degrades the service
instead of failing it, and prove the behaviour in a test before the outage
happens.

## Before you begin

- A service registered with `AddHostLoomCaching` and `AddHostLoomLocking`
  over `UseRedis()` (see [Cache and lock over Redis](use-redis.md)).
- `HostLoom.Caching.Testing` and `HostLoom.Locking.Testing` in the test
  project.

## 1. Know what degraded means

The cache never throws for a store failure from a read or a get-or-create.
It serves from the in-process tier, runs the factory, records
`hostloom.cache.errors` and a `degraded` outcome on
`hostloom.cache.operation.duration`, and logs one warning per key per
`Caching:Diagnostics:DegradedLogInterval`. Two calls are deliberately
different:

- `SetIfAbsentAsync` returns `false` under an outage by default, so a rate
  limiter treats "cannot tell" as "deny". Pass
  `OnUnavailable = UnavailableBehavior.Throw` where the caller must decide.
- The lock throws `LockProviderUnavailableException` with the failure kind.
  A lock that cannot be taken must not pretend it was.

At startup, `Redis:FailFast = false` (the default) lets the host start with
Redis unreachable; readiness reports unhealthy and everything above applies
until the connection recovers.

## 2. Bound the damage

Set the in-process tier so a long outage does not serve stale data forever:

```csharp
services.AddHostLoomCaching(caching =>
{
    caching.Namespace = "catalog";
    caching.L1.MaxEntryAge = TimeSpan.FromMinutes(10);
    caching.Diagnostics.DegradedLogInterval = TimeSpan.FromMinutes(5);
});
```

Per call, `CacheEntryOptions.LocalExpiration` shortens the in-process life
of one entry below its distributed expiration.

## 3. Decide what the lock's absence means

Where work must not run twice, let `LockProviderUnavailableException`
propagate and retry later. Where a duplicate run is cheaper than a stall,
catch it and run:

```csharp
try
{
    await locks.ExecuteWithLockAsync("catalog:refresh", RefreshAsync, cancellationToken: ct);
}
catch (LockProviderUnavailableException)
{
    await RefreshAsync(ct);   // idempotent; a duplicate run is acceptable here
}
```

The lock is coordination, not correctness: the database's own constraints
must still hold either way.

## 4. Prove it in a test

`FaultingCacheStore` and `FaultingLockProvider` fail the next `n` calls, or
every call, with a chosen kind. Compose the real cache over them:

```csharp
var clock = new FakeTimeProvider();
var faults = new FaultingCacheStore(new InMemoryDistributedCacheStore(clock));
await using var cache = TestCache.Tiered(faults, serializer, timeProvider: clock);

faults.FailAll(CacheFailureKind.Unavailable);

var catalog = await cache.GetOrCreateAsync("eu", LoadAsync, TimeSpan.FromMinutes(5));
var lookup = await cache.TryGetAsync<Catalog>("eu");

Assert.True(lookup.Found);              // served from the in-process tier
Assert.Equal(CacheTier.L1, lookup.Tier);
```

For the lock:

```csharp
var faults = new FaultingLockProvider(new InMemoryLockProvider(clock));
await using var locks = TestLock.Create(provider: faults, timeProvider: clock);
faults.FailAll(LockFailureKind.Unavailable);

await Assert.ThrowsAsync<LockProviderUnavailableException>(
    () => locks.ExecuteWithLockAsync("job", _ => ValueTask.FromResult(1)).AsTask());
```

## 5. Watch it in production

Alert on `hostloom.redis.connection.state` at 0 and on the rate of
`degraded` outcomes. `hostloom.cache.stampede.lease_missed` rising during an
outage is expected: every instance runs its own factory when the lease
cannot be taken. Readiness turns healthy again on its own when the connection
returns.

## Related

- [Caching reference](../reference/caching.md)
- [Locking reference](../reference/locking.md)
- [Observability surface](../reference/observability.md)
