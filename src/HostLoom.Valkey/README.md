# HostLoom.Valkey

Standalone Valkey caching and coordination locks over [ValkeyDotNet 1.1.0](https://github.com/kidoz/valkey-dotnet).
Targets .NET 10. Applications continue to consume `ICache` and `IDistributedLock` from the
backend-neutral kernels. The Redis adapter remains separately available.

```csharp
using System.Text.Json;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Valkey;
using ValkeyDotNet;

builder.Services.AddHostLoomCaching(options => options.Namespace = "catalog-worker")
    .UseValkey(options =>
    {
        options.Connection = new ValkeyClientOptions
        {
            Host = "localhost",
            Port = 6379,
            // Configure UseTls and ACL credentials for your deployment.
        };
    })
    .UseSystemTextJson(new JsonSerializerOptions
    {
        TypeInfoResolver = CatalogJsonContext.Default,
    })
    .AddHealthChecks();
builder.Services.AddHostLoomLocking(options => options.Namespace = "catalog-worker")
    .UseValkey()
    .AddHealthChecks();
```

`CatalogJsonContext` is an application-owned source-generated `JsonSerializerContext` containing
its cached types. A runnable example is in `examples/HostLoom.Examples.ValkeyAot` in the repository.

Both registrations share one lazily connected `ValkeyConnection`, owned by the container.
Configuration callbacks run in registration order; configure the endpoint once. Public constructors
also work without a container: create `ValkeyConnection`, then `ValkeyCacheStore`,
`ValkeyLockProvider`, and optionally `ValkeyCacheInvalidationChannel`. Stores and providers borrow
the connection; dispose channels before disposing the connection. Connection settings are
snapshotted at construction. Command recovery reuses the configured TLS, ACL and database settings.
A standalone `ServiceCollection` needs a DI container implementation to call `BuildServiceProvider`.

## Cache behavior

- Binary payloads and fully prefixed keys pass through unchanged. The kernel owns serialization,
  key domains, L1, single-flight, failure metrics and fail-open behavior.
- Lua reads the value and remaining TTL together. Bulk reads pipeline these scripts in one call.
- Untagged writes use `SET PX`; conditional writes use `SET NX PX`. Tagged writes atomically add
  memberships and extend tag expiry only when the value is written. Lua runtime errors do not roll
  back partial writes; reserved key domains must contain the expected value/index types.
- Tag memberships are monotonic: rewriting under other tags can leave earlier memberships.
  Tag removal is best-effort across concurrent writers, as in the Redis adapter.
- Pipeline server errors are checked individually and surfaced as `CacheStoreException`.

## Invalidation

`Auto` uses explicit Pub/Sub only. `Tracking` and `Broadcast` are rejected, since server-assisted
tracking and keyspace notification handling are not implemented. Changes made outside HostLoom
are observed through expiry. The probe describes this explicit-channel fallback.

The dedicated subscriber starts on the first subscription. `StartAsync(token)` can be used to wait
for its first server acknowledgement before beginning work. The channel is
`{namespace}:cache:invalidate:db:{database}` because Valkey Pub/Sub ignores logical database
selection. Messages use a bounded versioned JSON format, separate from the Redis adapter's wire
format; mixed Redis/Valkey adapters do not share invalidation messages.

Recovery uses exponential backoff from 100 ms to 30 seconds. Pub/Sub cannot replay messages lost
during a disconnection. Queue overflow drops incoming messages. In either case **L1 expiry bounds
staleness**; choose a suitable `CacheEntryOptions.LocalExpiration`. No stronger consistency is
promised. Handler exceptions do not stop other handlers. Handlers must not block.

The `HostLoom.Valkey` meter records invalidation failures, queue drops, malformed messages, and
handler failures; recovery also records the kernel's resubscription counter. Warnings are limited
to one per channel per 30 seconds. ValkeyDotNet connection-owner telemetry is opt-in through
`EnableTelemetry`. Neither adapter warnings nor probe text include credentials or payloads.

## Lock behavior

Acquisition is `SET key owner NX PX lease`. Lua release and extension compare the owner before
`DEL` or `PEXPIRE`. Positive durations round up to milliseconds. Contention and owner mismatch
return false; backend failures become `LockProviderException`; caller cancellation propagates.

Command, pipeline, and script transport failures are never automatically replayed. A lost reply
may mean the operation executed; an unconfirmed acquisition is reported as failure and its lease
expires. The next independent operation can reconnect. Script-cache recovery retries only a
received `NOSCRIPT` response.

These are coordination leases on one primary, without fencing or consensus guarantees. A primary
failover can lose a lease. Database constraints, transactions, or idempotency own persisted-state
correctness. Cluster routing, Sentinel, replica reads, tracking and sharded invalidation are outside
this adapter's current scope.

## Options and verification

| Option | Default |
|---|---|
| `Connection` | ValkeyDotNet standalone settings, localhost:6379, RESP3 |
| `CommandTimeout` | 5 seconds per command, script, or pipeline including connection acquisition |
| `HealthTimeout` | 2 seconds per PING |
| `FailFast` | true; startup PING failure prevents host startup |
| `InvalidationQueueCapacity` | 1,000 messages |
| `EnableTelemetry` | false |

`FailFast=false` permits startup while unavailable; later independent operations reconnect and
readiness reports backend health. It does not turn lock failures into successful acquisition.

```sh
docker compose up -d --wait valkey
HOSTLOOM_REQUIRE_VALKEY=1 dotnet test --project tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj -c Release -- --filter-class '*Valkey*'
dotnet publish examples/HostLoom.Examples.ValkeyAot -c Release -r linux-x64 -o artifacts/valkey-aot
artifacts/valkey-aot/HostLoom.Examples.ValkeyAot
```

Tests and the sample default to localhost:16379; override `HOSTLOOM_VALKEY_PORT` for another local
port. Use your platform's RID for Native AOT (for example `osx-arm64`). The sample verifies a
source-generated serialized L2 round trip, leases and explicit invalidation against a real server.
`HOSTLOOM_REQUIRE_VALKEY=1` or `HOSTLOOM_REQUIRE_BROKERS=1` makes an absent listener fail tests.

Validation on 2026-09-06: 44 shared conformance cases and eight adapter cases passed on Valkey
9.1.2; the eight adapter cases also passed on 8.1.10 and 7.2.14. Binary cache and lease cases cover
RESP2 and RESP3; shared conformance and invalidation use RESP3. Twelve deterministic adapter tests
cover configuration, DI, error mapping, invalidation encoding, lifecycle, startup policy and
non-replay after ambiguous lock writes. Source and package-only Native AOT consumers passed on
macOS arm64. Live servers were disposable Linux arm64 containers without TLS or persistence;
these runs do not certify live TLS, ACL, primary failover, or cluster behavior.
