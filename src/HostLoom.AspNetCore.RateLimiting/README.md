# HostLoom.AspNetCore.RateLimiting

Apply a shared token bucket per authenticated tenant or application-selected identity to MVC controllers or individual endpoints.

```csharp
using HostLoom.AspNetCore.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using HostLoom.Redis;

builder.Services.AddHostLoomRateLimiting("orders-read", options =>
{
    options.Namespace = "orders";
    options.TokenBucket.TokenLimit = 100;
    options.TokenBucket.TokensPerPeriod = 1;
    options.TokenBucket.ReplenishmentPeriod = TimeSpan.FromMilliseconds(600);
})
.UseRedis(redis => redis.Configuration = builder.Configuration.GetConnectionString("Redis")!)
.AddTenantRateLimitPolicy("tenant_id");
```

In an application with authentication and authorization configured, put `app.UseRateLimiter()`
after routing, authentication, and authorization, and before `app.MapControllers()`. Apply
`[EnableRateLimiting("orders-read")]` from `Microsoft.AspNetCore.RateLimiting` to a controller
or action. Minimal API endpoints can use `.RequireRateLimiting("orders-read")`.

The policy reads the claim from the primary authenticated identity. An anonymous request receives
401. A missing, conflicting or invalid tenant ID receives 403. Neither consumes a permit.
Use stable, non-secret tenant IDs suitable for storage. Each ID must satisfy the profile's key
length and character rules. Exhaustion returns 429 with Retry-After when available. A store failure
under Deny returns 503. Explicit Allow admits a degraded request.

All pods sharing a budget must use the same backend, namespace, profile name, tenant IDs and bucket
settings. Apply `[DisableRateLimiting]` or `.DisableRateLimiting()` for an exempt endpoint.
For an operator, an anonymous bucket, or a custom response, register
`AddEndpointRateLimitPolicy(partitionKeySelector, onRejected)` on the named profile.
The callback receives ASP.NET Core's `OnRejectedContext` after the adapter sets the status and retry header.
Each policy holds one local adapter. Tenant buckets live in the selected store and idle
Redis and standalone Valkey buckets expire. In-memory stores coordinate within one process.
Asynchronous replication can lose recent deductions on failover.
