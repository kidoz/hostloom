# HostLoom.Scheduling.Locking

This package runs an exclusive HostLoom schedule on one instance at a time by claiming it through
the HostLoom distributed lock, the way ShedLock guards a Spring `@Scheduled` method:

```csharp
services
    .AddHostLoomLocking(locking => locking.Namespace = "catalog")
    .UseRedis();

services
    .AddHostLoomScheduling()
    .UseDistributedLock()
    .AddSchedule<RebuildCatalogJob>(
        "catalog:rebuild",
        ScheduleTrigger.Cron("0 0 2 * * *"),
        options => options.Exclusive = true);
```

`DistributedLockScheduleGuard` is an `IScheduleGuard` over `IDistributedLock`. Each run of an
exclusive schedule makes one skip-if-busy acquisition of the key `schedule:{name}` for the
schedule's lease (`ScheduleOptions.Lease`, else `Scheduling:DefaultLease`), so the lock's
namespace, provider, and `Locking:MaxLease` apply. The scheduler keeps the claim past the run
until the lease ends or its own next occurrence is due, so another instance reaching the same
occurrence is refused. The lease is extended automatically while the claim is held, up to
`Locking:MaxHold`; a run that outlives that bound loses its claim, and the claim's lost token
cancels it. An instance that crashes leaves its key to expire at the lease end. A lock provider that cannot be reached throws, and the scheduler
skips that run as `GuardFailed` rather than running unguarded.

The guard composes without a container, `new DistributedLockScheduleGuard(distributedLock)`, for
a scheduler built with `new`. `UseDistributedLock()` registers it as the one guard for the
service collection and resolves the lock registered by `AddHostLoomLocking`.

Over a disabled lock (`Locking:Enabled = false`) every claim is granted on every instance, which
is the lock's single-instance mode; the guard reports `IsCoordinated = false`, the scheduler warns
once as `ScheduleGuardUncoordinated`, and its probe reports the guard as uncoordinated, so that
mode is never silent.

The lock is coordination, not correctness: two instances can still run the same job when a lease
is lost and the job ignores its token. Design every scheduled job so that running it twice is
safe.
