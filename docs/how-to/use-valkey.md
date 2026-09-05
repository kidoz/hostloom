# Cache and lock over Valkey

Install `HostLoom.Valkey` to use the standalone ValkeyDotNet backend with HostLoom's existing
`ICache` and `IDistributedLock` contracts. It shares one command connection owner between caching
and locking, with a dedicated socket for explicit invalidation.

```csharp
using System.Text.Json;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Valkey;
using ValkeyDotNet;

builder.Services.AddHostLoomCaching(options => options.Namespace = "catalog-worker")
    .UseValkey(options => options.Connection = new ValkeyClientOptions
    {
        Host = "localhost",
        Port = 6379,
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

Provide an application-owned source-generated `CatalogJsonContext` for cached types. Configure
TLS and ACL credentials through `ValkeyClientOptions` as required by the deployment. Configure the
shared endpoint once; both builders use the same registration. Public constructors support
container-free use too.

The cache supports binary payloads, expiry, tags, bulk operations and atomic set-if-absent.
Backend read failures retain the kernel's fail-open behavior. The lock uses expiring owner tokens,
atomic owner-checked release and extension, and the kernel's retry and lost-lease behavior.
Transport failures are never automatically replayed: a missing acknowledgement cannot be treated
as a confirmed acquisition.

`Auto` selects explicit-channel invalidation only; explicitly requesting `Tracking` or `Broadcast`
fails validation. External writes and missed Pub/Sub messages rely on L1 expiry, so set
`LocalExpiration` to the application's staleness budget. The channel reports degradation through
rate-limited logs and the `HostLoom.Valkey` meter. Its versioned wire messages and database-qualified
channel are separate from the Redis adapter's invalidation protocol.

This adapter supports standalone Valkey. It does not implement cluster routing, Sentinel, replica
reads, server-assisted tracking, or sharded invalidation. Its locks coordinate work on one primary;
they do not provide fencing or protect persisted state across primary failover.

See the [package reference](../reference/packages.md) for dependency boundaries. The repository's
`HostLoom.Examples.ValkeyAot` sample verifies serialized L2, lock leases and invalidation under
Native AOT. The Valkey service in `docker-compose.yml` listens on localhost:16379 for integration
tests; those tests can be required with `HOSTLOOM_REQUIRE_VALKEY=1`.
