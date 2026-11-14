# Run a schedule on every instance or on one

Choose whether every instance runs a job or replicas coordinate each run through a shared
lease. Each replica computes its own due times; exclusivity does not establish a shared
occurrence stream or exactly-once execution.

## Before you begin

- A service registered with `HostLoom.Scheduling.DependencyInjection`.
- For cluster schedules, `HostLoom.Scheduling.Locking` and a lock registered with
  `HostLoom.Locking.DependencyInjection` over a shared backend such as Redis or Valkey. For local
  work, the repository's `docker-compose.yml` provides one:

```text
docker compose up -d redis
```

## 1. Local: every instance runs it

A schedule without `Exclusive` runs on every instance that hosts it. Each computes the trigger on
its own clock, waits, and runs. Use it for per-instance work such as refreshing an in-process
cache or flushing a local buffer.

```csharp
builder.Services
    .AddHostLoomScheduling()
    .AddSchedule<RefreshLocalCacheJob>(
        "cache:refresh",
        ScheduleTrigger.FixedDelay(TimeSpan.FromMinutes(1)));
```

Within one instance a schedule never overlaps itself: a run starts only after the previous one
ended, a late run starts at once, and missed occurrences are not replayed.

## 2. Cluster: claim a lease before running

Set `Exclusive = true` and choose the guard. Before every run the instance claims
`schedule:{name}` through the distributed lock; the instance that wins runs the job, every other
instance skips that occurrence and records it as `Skipped`. Nothing is queued on the losers.

```csharp
builder.Services
    .AddHostLoomLocking(locking => locking.Namespace = "catalog")
    .UseRedis(redis => redis.Configuration = builder.Configuration["Redis:Configuration"]);

builder.Services
    .AddHostLoomScheduling(scheduling => scheduling.DefaultLease = TimeSpan.FromMinutes(2))
    .UseDistributedLock()
    .AddSchedule<RebuildCatalogJob>(
        "catalog:rebuild",
        ScheduleTrigger.Cron("0 0 2 * * *"),
        options =>
        {
            options.Exclusive = true;
            options.Lease = TimeSpan.FromMinutes(10);
            options.Timeout = TimeSpan.FromMinutes(30);
        });
```

The claim is retained after a run until the lease ends or this replica's next due time,
whichever comes first. It suppresses nearby attempts but does not assign a durable occurrence
identity. Fixed schedules started at different times can execute on different local cadences;
failover does not preserve a fixed-delay completion history. While a job runs, the lock extends
its lease automatically up to `Locking:MaxHold`.

This setup uses direct lock contention. To run schedules only on an elected leader, configure
[`UseLeader(role)`](run-one-leader.md#3-run-exclusive-schedules-on-the-leader) instead.
Neither mode provides fencing or exactly-once side effects. Keep jobs idempotent and honour
cancellation; the probe reports coordination and the current claim hold.

## 3. What each instance records under failure

| Condition | What happens | Outcome |
| --- | --- | --- |
| Another instance holds the claim | the job does not run here | `Skipped` |
| The lock backend is unreachable | the job runs nowhere, because nobody can tell whether another instance already runs it | `GuardFailed` on every instance |
| The lock is disabled (`Locking:Enabled = false`) | every instance is granted the claim, which is the lock's single-instance mode; the scheduler warns once and the probe reports the guard as uncoordinated | `Succeeded` on every instance |
| The instance stops mid-run | the job's token is cancelled and the claim is released; the next occurrence goes to whichever instance claims it | `Canceled` |
| The instance crashes mid-run | nothing releases the claim; it expires at the lease end because the automatic extension stopped with the process, and the next occurrence after that runs elsewhere | none recorded |
| The lease is lost mid-run, for example past `Locking:MaxHold` | the job's token is cancelled; a job that ignores its token keeps running and can overlap the next claimant | `ClaimLost` |
| The run exceeds `Timeout` | the job's token is cancelled | `TimedOut` |

A lost lease, asynchronous-backend failover, or work that ignores cancellation can permit
overlap. Use transactions, constraints, or idempotency records for persisted-state correctness.

## 4. Verify

The deterministic cases run in the ordinary suite with two scheduler instances over one shared
in-process lock backend and a fake clock, in `SchedulingClusterTests`: local schedules run
everywhere, coordinated schedules exclude concurrent claimants, the claim hold refuses
a late instance and admits it after the lease, a stopped instance hands over, a backend outage
skips everywhere until it recovers, and a lease that cannot be extended ends the run as
`ClaimLost`.

The cluster contract against a real Redis runs in `RedisSchedulingTests` when the compose Redis
is listening: three instances with their own connections contend on the system clock, and the
job body never overlaps. The outage experiment is opt-in, with a private loopback proxy per test
that disconnects only its own clients:

```text
docker compose up -d --wait redis
HOSTLOOM_REDIS_CHAOS=1 dotnet test tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj -c Release -- --filter-class '*RedisSchedulingTests'
```

It records a healthy baseline, cuts the proxy, waits until every instance reports `GuardFailed`
and no run succeeds while nothing can claim, restores the proxy, and waits until runs resume one
at a time.

## Troubleshoot

- **Nearby runs occur on different instances.** Local cadences differ and a claim can be
  acquired after the preceding claim ends. Increasing the lease changes suppression time;
  it does not create shared occurrence identities or exactly-once execution.
- **Nothing runs and every instance reports `GuardFailed`.** The lock backend is unreachable from
  every instance; the schedule resumes when the readiness check for the lock turns healthy.
- **Runs land on one instance only.** Expected when its clock runs ahead or its lease exceeds the
  period; the winner is whichever instance claims first.

## Related

- [Scheduling reference](../reference/scheduling.md)
- [Locking reference](../reference/locking.md)
- [Cache and lock over Redis](use-redis.md)
