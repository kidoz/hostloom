# HostLoom.RateLimiting.DependencyInjection

Registers default and named `RateLimiter` profiles, backend stores, options validation, and
readiness checks for HostLoom rate limiting.

```csharp
builder.Services
    .AddHostLoomRateLimiting("catalog-api", options =>
    {
        options.Namespace = "catalog";
        options.TokenBucket = new TokenBucketRateLimitPolicy
        {
            TokenLimit = 100,
            TokensPerPeriod = 100,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
        };
    })
    .UseRedis()
    .AddHealthChecks();
```

Use `UseInMemory()` for process-local operation, `UseRedis()` from `HostLoom.Redis`, or
`UseValkey()` from `HostLoom.Valkey`. Each profile selects exactly one store. Redis or Valkey uses
the same named connection as caching and locking when the profile and backend match; different
profiles isolate their connections. Readiness uses the backend probe when available and never
changes liveness.

Resolve the default limiter as `RateLimiter`, or a named limiter with
`GetRequiredKeyedService<RateLimiter>("catalog-api")`. `DistributedRateLimiter` is registered as a
concrete service too. The container owns the limiter and store; an HTTP handler does not dispose
them.
