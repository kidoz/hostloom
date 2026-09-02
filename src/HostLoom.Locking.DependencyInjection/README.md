# HostLoom.Locking.DependencyInjection

This package registers the `HostLoom.Locking` kernel with
`Microsoft.Extensions.DependencyInjection`:

```csharp
services
    .AddHostLoomLocking(locking => locking.Namespace = "billing")
    .UseInMemory()
    .AddHealthChecks();
```

`AddHostLoomLocking` binds `LockingOptions`, validates them when the host starts with messages
that name the option key at fault, and registers `IDistributedLock` as a singleton composed from
the provider chosen on the builder. It works without `AddHostLoom()`; nothing here references the
messaging kernel. Repeated calls return a builder over the same registration, and every
registration is `TryAdd`, so an application can register its own `TimeProvider` or provider first.

Exactly one provider per service collection: `UseInMemory()` installs the per-process provider,
and a backend package such as `HostLoom.Redis` adds its own `Use*` extension over
`UseProvider<TProvider>(name)`. A second `Use*` throws and names the provider already chosen. An
enabled lock with no provider fails startup validation; `Locking:Enabled = false` is single-instance
mode and needs none.

`AddHealthChecks()` registers a readiness check tagged `ready` that asks the provider's
`ILockProviderHealthProbe` whether the backend is reachable. A provider without a probe, including
the in-memory one, reports healthy with a description saying so. Liveness is never touched.

See the `HostLoom.Locking` package README for the contract, the retry policy, and the lost-lease
semantics.
