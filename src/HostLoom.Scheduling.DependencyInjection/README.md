# HostLoom.Scheduling.DependencyInjection

This package registers the `HostLoom.Scheduling` kernel with
`Microsoft.Extensions.DependencyInjection` and starts it with the host:

```csharp
services
    .AddHostLoomScheduling()
    .AddSchedule<RebuildCatalogJob>(
        "catalog:rebuild",
        ScheduleTrigger.Cron("0 0 2 * * *"),
        options => options.Exclusive = true)
    .AddSchedule(
        "inventory:sync",
        ScheduleTrigger.FixedDelay(TimeSpan.FromSeconds(30)),
        static (provider, run, token) => provider.GetRequiredService<InventorySync>().RunAsync(token))
    .UseDistributedLock();   // from HostLoom.Scheduling.Locking
```

`AddHostLoomScheduling` binds `SchedulingOptions`, validates them when the host starts with
messages that name the option at fault, registers the `Scheduler` as a singleton composed from
every schedule and the guard chosen on the builder, and adds the hosted service that starts and
stops it with the host. It works without `AddHostLoom()`; nothing here references the messaging
kernel. Repeated calls return a builder over the same registration, and every registration is
`TryAdd`, so an application can register its own `TimeProvider` first.

`AddSchedule<TJob>` resolves the job from a fresh dependency-injection scope for every run, so a
job takes scoped dependencies through its constructor like a request handler does; the job type
is registered scoped if nothing registered it already. The delegate overload receives the root
provider and creates its own scope when it needs one. Schedule names are unique within a service
collection, and a repeat is refused at registration rather than at startup.

Exactly one guard per service collection: an adapter such as `HostLoom.Scheduling.Locking` adds
its own `Use*` extension over `UseGuard<TGuard>(name)`, and a second `Use*` throws naming the
guard already chosen. A schedule registered with `Exclusive = true` and no guard fails startup
validation with a message that names the builder method to call.

See the `HostLoom.Scheduling` package README for the triggers, the run contract, and the outcome
and lifecycle semantics.
