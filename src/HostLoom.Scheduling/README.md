# HostLoom.Scheduling

`HostLoom.Scheduling` is the backend-neutral scheduled-job kernel: the job contract
`IScheduledJob`, the triggers that decide when a schedule runs, the `Scheduler` that waits on a
`TimeProvider` and runs each schedule sequentially, and the `IScheduleGuard` contract that decides
which instance runs a schedule marked exclusive. The package references only
`Microsoft.Extensions.Logging.Abstractions`; every runtime type has a public constructor, so a
scheduler composes without a container.

**A schedule is a loop, not a queue.** Each schedule runs one job at a time in this process: a run
never starts while the previous run of the same schedule is still executing, and a run that is
already late starts at once without replaying the occurrences it missed. Across processes, an
`Exclusive` schedule claims itself through the guard before every run and is skipped, never queued,
when another instance holds the claim.

```csharp
var nightly = new ScheduleDefinition(
    "catalog:rebuild",
    ScheduleTrigger.Cron("0 0 2 * * *"),               // 02:00 UTC every day
    async (run, token) => await RebuildCatalogAsync(token),
    new ScheduleOptions { Exclusive = true, Timeout = TimeSpan.FromMinutes(30) });

var sync = new ScheduleDefinition(
    "inventory:sync",
    ScheduleTrigger.FixedDelay(TimeSpan.FromSeconds(30)), // 30 s after each completion
    async (run, token) => await SyncInventoryAsync(run.DueAt, token));

await using var scheduler = new Scheduler(new SchedulingOptions(), [nightly, sync], guard);
await scheduler.StartAsync();
```

Three trigger shapes follow the Spring conventions. `Cron` takes five fields (minute, hour, day of
month, month, day of week) or six with a leading seconds field, with `*`, lists, ranges, steps,
month and day names, and `?`; when both day fields are restricted an occurrence matches either, as
in Vixie cron, and occurrences are evaluated in the zone given to the trigger, UTC by default.
`FixedRate` counts each period from the previous due time, so a slow run does not drift the
schedule. `FixedDelay` counts from the previous completion, so two runs are always separated by at
least the delay.

A run receives a `ScheduledRun` (name, due time, start time, sequence) and a token that is
cancelled when the host stops, when `ScheduleOptions.Timeout` elapses, or when an exclusive claim
is lost. An exception ends the run as `Failed`; the schedule continues. Outcomes are recorded per
schedule (`Scheduler.GetState`), on the `hostloom.schedule.run.duration` histogram by outcome, and
in the log events listed in `SchedulingEvents`. A guard that throws skips the run as
`GuardFailed`: a job that must not run twice cannot run when nothing can say whether another
instance already does.

`SchedulingOptions.Enabled = false` runs nothing and says so at startup, for a deployment that
keeps its registrations but must not execute jobs. `SchedulingProbe.Describe(scheduler)` reports
the composition and the current state of every schedule without running anything. Install
`HostLoom.Scheduling.DependencyInjection` to register the scheduler with the built-in .NET
container and `HostLoom.Scheduling.Locking` to guard exclusive schedules with the HostLoom
distributed lock.
