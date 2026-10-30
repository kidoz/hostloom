# HostLoom.RateLimiting

Backend-neutral asynchronous token-bucket rate limiting over
`System.Threading.RateLimiting.RateLimiter`. The package includes an in-process reference store;
`HostLoom.Redis` and `HostLoom.Valkey` provide shared stores, and
`HostLoom.RateLimiting.HttpClient` integrates outgoing HTTP clients.

```csharp
var store = new InMemoryRateLimitStore();
var limiter = new DistributedRateLimiter(
    new RateLimitingOptions
    {
        Namespace = "catalog",
        TokenBucket = new TokenBucketRateLimitPolicy
        {
            TokenLimit = 100,
            TokensPerPeriod = 100,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
        },
    },
    "catalog-api",
    store);

using var lease = await limiter.AcquireAsync(cancellationToken: stoppingToken);
if (!lease.IsAcquired)
{
    // Inspect MetadataName.RetryAfter before scheduling independent work.
}
```

`AcquireAsync` is the supported acquisition operation. `AttemptAcquire` returns a conservative
failed lease without store I/O so hosts such as ASP.NET Core can fall back to asynchronous
acquisition. Token-bucket leases do not refund permits on disposal.

The store receives fully prefixed `{namespace}:rate:{key}` keys. A rejected lease can contain
`MetadataName.RetryAfter` and `RateLimitMetadata.RemainingPermits`. Store failure is different from
quota exhaustion: `OnUnavailable = Deny` (the default) returns unavailable metadata, while explicit
`Allow` admits work with degraded metadata. Caller cancellation propagates, and an ambiguous
backend result is never replayed or refunded.

Every instance using the same namespace and key must use the same token-bucket policy. Redis and
Valkey replication is asynchronous, so failover can lose recently consumed tokens; this is shared
coordination, not an absolute quota guarantee during failover.
