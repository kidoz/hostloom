# HostLoom.Scheduling.Leadership

This package runs exclusive HostLoom schedules on the elected leader only:

```csharp
services
    .AddHostLoomLeadership()
    .AddRole("scheduler");

services
    .AddHostLoomScheduling()
    .UseLeader("scheduler")
    .AddSchedule<RebuildCatalogJob>(
        "catalog:rebuild",
        ScheduleTrigger.Cron("0 0 2 * * *"),
        options => options.Exclusive = true);
```

`LeaderScheduleGuard` is an `IScheduleGuard` over `ILeadership`. A claim is granted while this
instance leads the role and refused otherwise, so no lock round trip happens per occurrence and
the winner is sticky until the leader changes; followers record every occurrence as `Skipped`.
The claim's lost token is the leadership token: a run in progress ends as `ClaimLost` when
leadership ends, and a job that ignores its token can overlap the next leader's run.

Compared with `HostLoom.Scheduling.Locking`, which claims each occurrence through the lock and
spreads work across instances, this guard keeps every exclusive schedule on one instance and
inherits the leadership guarantee: at most one leader while clocks and the lock backend behave,
no fencing. Choose one guard per service collection; `UseGuard` refuses a second.

Over a disabled lock (`Locking:Enabled = false`) the guard reports `IsCoordinated = false` and
follows the role's `Leadership:WhenUncoordinated`: `Follow`, the default, skips every occurrence
on every instance; `Lead` runs every occurrence on every instance. The scheduler warns once and
its probe reports the guard as uncoordinated.
