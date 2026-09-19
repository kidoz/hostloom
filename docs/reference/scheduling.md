# Scheduling

The `HostLoom.Scheduling` family: cron, fixed-rate, and fixed-delay triggers over
`TimeProvider`, one sequential loop per schedule, typed run outcomes, and a guard
contract that runs an exclusive schedule on one instance at a time. The kernel
references only `Microsoft.Extensions.Logging.Abstractions` and composes without
a container; the `DependencyInjection` package registers it and starts it with
the host; the `Locking` package guards exclusive schedules with the HostLoom
distributed lock. Namespaces follow the package names.

```text
dotnet add package HostLoom.Scheduling
dotnet add package HostLoom.Scheduling.DependencyInjection   # container registration and hosting
dotnet add package HostLoom.Scheduling.Locking               # exclusivity through IDistributedLock
dotnet add package HostLoom.Scheduling.Testing               # scripted guard for tests
```

A schedule is a loop, not a queue. A run never starts while the previous run of
the same schedule is still executing in this process, and a run that is already
late starts at once without replaying the occurrences it missed. Across
processes, an `Exclusive` schedule claims itself through the guard before every
run and is skipped, never queued, when another instance holds the claim.

## Triggers (`ScheduleTrigger`)

| Factory | Next due time |
| --- | --- |
| `Cron(expression, timeZone)` | the first occurrence strictly after now, evaluated in the zone (UTC by default) |
| `FixedRate(period, initialDelay)` | `initialDelay` (zero by default) after start, then the previous due time plus the period; a late run is due now and missed periods are not replayed |
| `FixedDelay(delay, initialDelay)` | `initialDelay` (the delay by default) after start, then the previous completion plus the delay |

`CronExpression` accepts five fields (minute, hour, day of month, month, day of
week) or six with a leading seconds field, as Spring reads them. Each field takes
`*`, values, ranges (`a-b`), lists (`a,b`), and steps (`*/n`, `a/n`, `a-b/n`);
months and days of week also take their three-letter English names; the day
fields take `?` as a synonym for `*`. Sunday is `0` or `7`, so `FRI-SUN` is a
range. When both day fields are restricted an occurrence matches either, as in
Vixie cron. `L`, `W`, and `#` are not supported. A local time a daylight-saving
transition skips is not an occurrence; an ambiguous local time occurs once, at
its earlier instant. `Parse` throws `FormatException` naming the field at fault;
`TryParse` reports failure instead.

## The run (`ScheduledRun`, `IScheduledJob`)

A job receives a `ScheduledRun` with `Schedule`, `DueAt`, `StartedAt`, `Lag`,
and `Sequence`, and a token that is cancelled when the host stops, when
`ScheduleOptions.Timeout` elapses, or when an exclusive claim is lost. Its
outcome is one of:

| `ScheduleRunOutcome` | When |
| --- | --- |
| `Succeeded` | the job returned |
| `Failed` | the job threw; the schedule continues |
| `TimedOut` | the job observed the timeout cancellation |
| `Canceled` | the job observed the stop cancellation; the loop ends |
| `ClaimLost` | the exclusive claim was lost mid-run and the job observed it |
| `Skipped` | another instance held the claim; the job did not run |
| `GuardFailed` | the guard threw; the job did not run |

A job that ignores its token keeps running past a timeout or a lost claim, and
the next run of that schedule waits for it. `Scheduler.GetState(name)` reports
the next due time, whether a run is executing, the run count, and the last
outcome; `SchedulingProbe.Describe(scheduler)` reports every schedule without
running anything.

## Per-schedule options (`ScheduleOptions`)

| Property | Meaning |
| --- | --- |
| `Exclusive` | claim the schedule through the guard before every run; requires a guard |
| `Lease` | how long a claim is held; defaults to `Scheduling:DefaultLease` |
| `Timeout` | cancel the run's token after this long; default none |

## Configuration (`SchedulingOptions`)

| Key | Default | Meaning |
| --- | --- | --- |
| `Scheduling:Enabled` | `true` | `false` runs nothing: a startup warning and the probe reporting `(disabled)` |
| `Scheduling:DefaultLease` | 5 minutes | claim lease for exclusive schedules that give none |

`Validate()` returns every violation naming the option key, and the scheduler's
constructor throws with the same text, so a container-free composition fails as
a hosted one does. The constructor also rejects a repeated schedule name and an
exclusive schedule composed without a guard.

## The guard (`IScheduleGuard`)

`TryClaimAsync(schedule, lease, ct)` returns an `IScheduleClaim` or `null` when
another instance holds the claim. A guard never waits, and a backend failure is
thrown rather than swallowed, because a job that must not run twice cannot run
when nothing can say whether another instance already does. The claim exposes
`IsHeld` and `LostToken`; disposing releases it.

`HostLoom.Scheduling.Locking` supplies `DistributedLockScheduleGuard` over
`IDistributedLock`: one skip-if-busy acquisition of `schedule:{name}` for the
lease, extended automatically up to `Locking:MaxHold`, with the lock's lost
token as the claim's. `UseDistributedLock()` chooses it on the builder; the
application registers the lock with `AddHostLoomLocking` as usual.
`HostLoom.Scheduling.Testing` supplies `ManualScheduleGuard`, which a test
scripts with `Hold`, `Release`, `Lose`, and `FailNext`.

## Registration (`HostLoom.Scheduling.DependencyInjection`)

```csharp
services
    .AddHostLoomScheduling(scheduling => scheduling.DefaultLease = TimeSpan.FromMinutes(10))
    .AddSchedule<RebuildCatalogJob>("catalog:rebuild", ScheduleTrigger.Cron("0 0 2 * * *"),
        options => options.Exclusive = true)
    .AddSchedule("inventory:sync", ScheduleTrigger.FixedDelay(TimeSpan.FromSeconds(30)),
        static (provider, run, token) => provider.GetRequiredService<InventorySync>().RunAsync(token))
    .UseDistributedLock();
```

`AddSchedule<TJob>` resolves the job from a fresh scope for every run, so it
takes scoped dependencies through its constructor like a request handler. The
delegate overload receives the root provider. Names are unique within a service
collection and a repeat is refused at registration. Exactly one guard per
service collection; an exclusive schedule with none fails startup validation
naming the builder method to call. The hosted service starts the scheduler with
the host and stops it with the host's shutdown token; a job that ignores its
token is not waited for past that bound.

## Observability

Metrics and activities live under the `HostLoom.Scheduling` meter and activity
source; the schedule name is the `hostloom.schedule.name` tag. See the
[observability surface](observability.md) for the instruments and
`SchedulingEvents` for the stable log event ids (3200 to 3208).
