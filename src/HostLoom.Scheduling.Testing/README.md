# HostLoom.Scheduling.Testing

This package tests consumers of `HostLoom.Scheduling` without a backend. It references the
kernel only.

`ManualScheduleGuard` is an in-process `IScheduleGuard` a test can script: `Hold(name)` refuses
the next claims as if another instance ran the schedule, `Release(name)` lets them through,
`Lose(name)` cancels the lost token of an active claim as an expired lease would, and
`FailNext(exception)` makes the next claim throw as an unreachable backend would. `Claims` lists
every attempt with its lease and whether it was granted; `Active` lists the schedules claimed
right now.

```csharp
var guard = new ManualScheduleGuard();
guard.Hold("catalog:rebuild");
await using var scheduler = new Scheduler(new SchedulingOptions(), [nightly], guard, clock);
await scheduler.StartAsync();
clock.Advance(TimeSpan.FromHours(2));
// The run was skipped; guard.Claims[^1].Granted is false and the job never ran.
```

Drive time with a deterministic `TimeProvider`: the scheduler waits, times out, and stamps runs
on the clock it was given, so a test advances the clock instead of sleeping.
