# HostLoom.RateLimiting.HttpClient

Outgoing `HttpClient` integration for HostLoom rate limiter profiles. The handler acquires one
permit per physical HTTP attempt, supports bounded local waiting, and throws typed exceptions for
local rejection or backend denial instead of manufacturing HTTP 429 responses.

```csharp
builder.Services
    .AddHttpClient("catalog", client =>
        client.BaseAddress = new Uri("https://catalog.example"))
    .AddHostLoomRateLimit("catalog-api", options =>
    {
        options.MaxWait = TimeSpan.FromSeconds(2);
        options.QueueLimit = 32;
    });
```

Register the rate limiter after retry or hedging handlers so it sits closest to the network and
charges every physical attempt. Register it before those handlers to charge only the logical
request instead.

`MaxWait` includes store acquisition and local delay time. `QueueLimit` bounds callers in the wait
loop for one handler pipeline; it is not a global queue and has no oldest-first or cross-instance
FIFO guarantee. Both defaults are zero, which rejects immediately. A positive `MaxWait` requires a
positive `QueueLimit`.

- `RateLimitExceededException` reports local exhaustion and an optional `RetryAfter`.
- `RateLimiterUnavailableException` reports a store failure when `OnUnavailable` is `Deny`.
- Explicitly degraded admissions and ordinary responses, including a real downstream 429, proceed
  unchanged.
- Handler disposal does not dispose the shared limiter.

`RateLimitedHttpMessageHandler` also has a public constructor for container-free composition.
