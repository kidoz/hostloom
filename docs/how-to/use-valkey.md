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


## Validate TLS, ACL and restart recovery

On Linux or macOS, install Docker and OpenSSL, then run:

```sh
docker pull valkey/valkey:9.1
just test-valkey-deployment
```

The runner builds the solution in Release, creates a temporary certificate authority and server
certificate, and starts a TLS-only Valkey container with the default user disabled. The application
user has explicit cache/lock/script/Pub/Sub permissions restricted to the fixture's key and channel
prefixes. See Valkey's [TLS](https://valkey.io/topics/tls/) and [ACL](https://valkey.io/topics/acl/)
references for the server configuration rules.

The tests validate certificate trust and hostname matching; they do not install certificates in the
host trust store. The fixture uses server-authenticated TLS and password-based ACL authentication,
not mutual TLS. The private CA's callback is test infrastructure, not a production TLS helper.

The seven cases cover RESP2/RESP3 operations, untrusted certificates, hostname mismatch, wrong
credentials, denied keys/commands/channels, and a full stop/start using the same client and
subscriber objects. Recovery must retain TLS, credentials and database selection, reload Lua
scripts, restore invalidation delivery and readiness, and report a lost old lease without deleting
a replacement owner's lock. A manual kernel clock verifies the L1 expiry boundary after the server
loses its data; server recovery and commands still run against the real container.

The fixture uses one CPU, 128 MiB of memory, a read-only configuration mount and a 16 MiB data tmpfs,
with persistence disabled. Its randomly selected loopback port stays fixed across restart. The
runner controls only its exact container ID and ownership label, removes the fixture in `finally`,
and deletes temporary credentials and certificates. It never restarts the shared compose services.

To repeat against another cached server image after building:

```sh
docker pull valkey/valkey:8.1
uv run --locked python scripts/test-valkey-deployment.py --no-build --image valkey/valkey:8.1
```

The allowed image tags are `9.1`, `8.1`, and `7.2`. Images must already be available locally; the
runner uses `--pull=never`. Ordinary test runs skip these seven cases without the runner's fixture.
The release workflow invokes the runner separately, where missing infrastructure or failed tests
fail the step. These tests cover restart of one nonpersistent primary; they do not certify replica
promotion, consensus/fencing, cluster behavior, or mutual TLS.
