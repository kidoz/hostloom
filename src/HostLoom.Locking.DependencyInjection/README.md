# HostLoom.Locking.DependencyInjection

This package registers the `HostLoom.Locking` kernel with
`Microsoft.Extensions.DependencyInjection`:

```csharp
services
    .AddHostLoomLocking(locking => locking.Namespace = "billing")
    .UseInMemory()
    .AddHealthChecks();
```

Pass a profile name to compose independently configured locks and resolve them as keyed services:

```csharp
services.AddHostLoomLocking("orders", locking => locking.Namespace = "orders").UseInMemory();
services.AddHostLoomLocking("catalog", locking => locking.Namespace = "catalog").UseInMemory();

var orders = provider.GetRequiredKeyedService<IDistributedLock>("orders");
var catalog = provider.GetRequiredKeyedService<IDistributedLock>("catalog");
```

Each profile has its own named options, provider, and readiness resources. Disabling one profile leaves
the others coordinated, and default health-check names gain a `:{profile}` suffix. A caller-supplied
health-check name is used as given and must be unique. Service-provider disposal disposes all owned
profiles; profiles do not have independent teardown lifetimes.

Leadership roles, node-ID pools, and exclusive schedules accept a `lockProfile` argument and use
that exact keyed lock for provider behavior, lease and key-length limits, and uncoordinated policy.
A missing profile fails startup and names its consumer and profile; an enabled default lock does
not override a bound disabled profile.

`AddHostLoomLocking` binds `LockingOptions`, validates them when the host starts with messages
that name the option key at fault, and registers `IDistributedLock` as a singleton composed from
the provider chosen on the builder. It works without `AddHostLoom()`; nothing here references the
messaging kernel. Repeated calls return a builder over the same registration, and every
registration is `TryAdd`, so an application can register its own `TimeProvider` or provider first.

Exactly one provider per profile: `UseInMemory()` installs the per-process provider, and a backend
package such as `HostLoom.Redis` adds its own `Use*` extension over `UseProvider<TProvider>(name)`.
A second `Use*` on that profile throws and names the provider already chosen. An enabled lock with
no provider fails startup validation; `Locking:Enabled = false` is single-instance mode and needs
none.

When several features share locking, select the provider once at the composition root. Repeated
`AddHostLoomLocking()` calls are allowed; calling `UseRedis()`, `UseInMemory()`, or another provider
selector again is rejected, even when selecting the same provider. A duplicate selection reports
that locking was configured more than once so conflicting backend settings cannot be silently hidden.

`AddHealthChecks()` registers a readiness check tagged `ready` that asks the provider's
`ILockProviderHealthProbe` whether the backend is reachable. A provider without a probe, including
the in-memory one, reports healthy with a description saying so. Liveness is never touched.

See the `HostLoom.Locking` package README for the contract, the retry policy, and the lost-lease
semantics.
