# Rate limiting

HostLoom rate limiting supplies an asynchronous token-bucket `RateLimiter`, an in-process
reference store, Redis and standalone Valkey backends, and an outgoing `HttpClient` handler. The
same kernel can be used by a future incoming policy; this page currently documents outgoing HTTP
support only.

## Register a limiter and client

```csharp
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using HostLoom.RateLimiting.HttpClient;
using HostLoom.Redis;

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
    .UseRedis(redis => redis.Configuration = "redis:6379")
    .AddHealthChecks();

builder.Services
    .AddHttpClient("catalog", client =>
        client.BaseAddress = new Uri("https://catalog.example"))
    .AddHostLoomRateLimit("catalog-api", options =>
    {
        options.MaxWait = TimeSpan.FromSeconds(2);
        options.QueueLimit = 32;
    });
```

Use `.UseValkey()` from `HostLoom.Valkey` for a standalone Valkey deployment, or `.UseInMemory()`
for process-local quotas and deterministic tests. Each named profile chooses exactly one store.
The same profile and backend share a connection with caching and locking; different profiles
isolate connections.

The profile is also the consumer key. The physical store key is
`catalog:rate:catalog-api` in this example. All instances sharing a key must use the same policy.
For manual composition, construct `DistributedRateLimiter` with `RateLimitingOptions`, a consumer
key, and an `IRateLimitStore`.

## Token-bucket behavior

A bucket starts full. Each acquisition refills whole elapsed periods, capped at `TokenLimit`, then
atomically consumes the requested permits. `AcquireAsync(0)` probes and refills without consuming
a permit. Requesting more than `TokenLimit` throws `ArgumentOutOfRangeException` before store I/O.

| Option | Default | Contract |
| --- | --- | --- |
| `TokenLimit` | 1 | Capacity, from 1 through 1,000,000 |
| `TokensPerPeriod` | 1 | Whole permits added per period; cannot exceed capacity |
| `ReplenishmentPeriod` | 1 second | Positive, at most one day; sub-millisecond values round up |
| `RateLimiting:MaxKeyLength` | 512 | Longest consumer key |
| `RateLimiting:OnUnavailable` | `Deny` | Explicit behavior when no store decision is available |

Remote stores use server time and one Lua operation per acquisition. Idle keys expire after enough
time to refill the bucket, so a later acquisition starts with the same full state. The in-memory
store uses `TimeProvider` and reclaims expired entries during activity and count inspection.

`AcquireAsync` is the supported operation. `AttemptAcquire` returns a conservative failed lease
without store I/O, which lets ASP.NET Core-style hosts fall back to asynchronous acquisition; it
does not report current remote availability. Token-bucket lease disposal does not refund permits.

A successful lease can report `RateLimitMetadata.RemainingPermits`. An exhausted lease can carry
`MetadataName.RetryAfter`. There is no background replenisher and no globally ordered queue.

## Outgoing HTTP behavior

The handler acquires one permit around each physical `SendAsync` call and holds the lease until
the downstream attempt finishes. Register it after retry or hedging handlers, closest to the
network, to charge every attempt. Registration before those handlers charges only the logical
request.

`MaxWait` and `QueueLimit` are both zero by default, so rejection is immediate. A positive
`MaxWait` requires a positive `QueueLimit`. Waiting uses `RetryAfter`, includes store acquisition
time, and is bounded per handler pipeline. It is not globally ordered and does not guarantee FIFO
even within one process.

| Outcome | Caller observes |
| --- | --- |
| Permit acquired | Downstream response, unchanged |
| Exhausted within the wait budget | `RateLimitExceededException`, with `RetryAfter` when known |
| Wait queue full or `RetryAfter` exceeds `MaxWait` | `RateLimitExceededException` |
| Store failure under `Deny` | `RateLimiterUnavailableException` |
| Store failure under explicit `Allow` | Degraded downstream send and metrics |
| Real downstream 429 | The original `HttpResponseMessage`, not a locally generated response |
| Caller cancellation | `OperationCanceledException` |

The handler is transient and never disposes the shared limiter. For container-free HTTP setup,
construct `RateLimitedHttpMessageHandler` over any `RateLimiter` and optional
`HttpRateLimitOptions` and `TimeProvider`.

## Failure and consistency limits

Quota exhaustion is an ordinary decision. Backend failure is not: `Deny` produces unavailable
metadata and the HTTP backend exception, while `Allow` produces an acquired degraded lease. A
future incoming adapter should map these differently—normally 429 for exhaustion and 503 for a
denied backend outage.

Caller cancellation ends the wait. A script already dispatched can still have consumed a permit;
HostLoom never replays or refunds an ambiguous result. Valkey preserves its non-replay rule for
transport-failed scripts, with only a received `NOSCRIPT` eligible for script-cache recovery.

Redis Cluster uses the existing database-zero and hash-tag rules. Valkey support is standalone.
Asynchronous replication can lose recently consumed tokens during failover and permit additional
work. These backends provide shared coordination, not a strict quota guarantee during failover.

## Keys, health, and metrics

Use bounded, non-secret consumer keys. `RateLimitKey.FromSensitive` hashes high-entropy values or
uses HMAC for low-entropy secrets. Namespaces match `[a-z0-9-]+`; keys reject whitespace and
control characters. Rate-limit keys use their own `:rate:` domain and cannot collide with cache or
lock keys.

`AddHealthChecks()` registers readiness only. The default name is `hostloom-ratelimit-ready`, and a
named profile appends `-{profile}`. A store without a health probe is reported as unknown-but-
assumed-reachable; liveness never contacts a backend.

The `HostLoom.RateLimiting` meter publishes:

- `hostloom.ratelimit.acquisitions`, tagged namespace and outcome (`acquired`, `rejected`,
  `degraded`, or `unavailable`);
- `hostloom.ratelimit.acquire.duration` in seconds, with the same tags;
- `hostloom.ratelimit.store.errors`, tagged namespace and failure kind.

The `HostLoom.RateLimiting.HttpClient` meter publishes
`hostloom.ratelimit.http.requests` and `hostloom.ratelimit.http.wait.duration`, tagged only with a
bounded outcome (`sent`, `degraded`, `rejected`, `queue_full`, or `unavailable`). Raw keys, URLs,
partitions, and request content are never metric labels.
