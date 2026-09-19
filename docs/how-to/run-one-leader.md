# Run one leader among replicas

Give one instance of a replicated service a role, run leader-only work on it,
and let another instance take the role over when it stops, crashes, or loses
its lease.

## Before you begin

- A lock registered with `HostLoom.Locking.DependencyInjection` over a shared
  backend such as Redis or Valkey. For local work:

```text
docker compose up -d redis
```

## 1. Register the role

```csharp
using HostLoom.Leadership.DependencyInjection;

builder.Services
    .AddHostLoomLocking(locking => locking.Namespace = "billing")
    .UseRedis(redis => redis.Configuration = builder.Configuration["Redis:Configuration"]);

builder.Services
    .AddHostLoomLeadership()
    .AddRole("reconciler", leadership =>
    {
        leadership.Lease = TimeSpan.FromSeconds(15);      // how long a crashed leader keeps the role
        leadership.RenewInterval = TimeSpan.FromSeconds(5);
    });
```

## 2. Run leader-only work

```csharp
public sealed class ReconcilerLoop(ILeadership leadership, Reconciler reconciler) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await leadership.WaitForLeadershipAsync(stoppingToken);
            var leadershipToken = leadership.LeadershipToken;
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken, leadershipToken);
            try
            {
                await reconciler.RunAsync(linked.Token);   // returns when leadership ends
            }
            catch (OperationCanceledException) when (leadershipToken.IsCancellationRequested)
            {
                // Lost the role; wait to lead again.
            }
        }
    }
}
```

Capture the token for each run: the elector may acquire a new term before the
previous run finishes handling cancellation. Catch cancellation from that captured
token so the loop can resume on the next term.

With several roles, inject `[FromKeyedServices("reconciler")] ILeadership`.

## 3. Run exclusive schedules on the leader

```csharp
builder.Services
    .AddHostLoomScheduling()
    .UseLeader("reconciler")
    .AddSchedule<RebuildCatalogJob>(
        "catalog:rebuild",
        ScheduleTrigger.Cron("0 0 2 * * *"),
        options => options.Exclusive = true);
```

The leader runs every exclusive schedule; followers record `Skipped`.

## 4. Gate a producer on the leader

When every instance consumes the same feed but only the leader may act on it,
put a `LeaderChannel<T>` between the consumer and the actor. Every instance
writes; only the leader's items reach the reader.

```csharp
builder.Services.AddSingleton(provider => new LeaderChannel<InventoryChange>(
    "inventory-changes",
    provider.GetRequiredService<ILeadership>(),
    new LeaderChannelOptions { Capacity = 10_000 },
    provider.GetRequiredService<ILogger<LeaderChannel<InventoryChange>>>()));
```

```csharp
public sealed class InventoryChangeConsumer(LeaderChannel<InventoryChange> changes)
    : IEventHandler<InventoryChanged>
{
    public ValueTask HandleAsync(InventoryChanged @event, CancellationToken cancellationToken) =>
        changes.Writer.WriteAsync(@event.Change, cancellationToken);   // discarded on a follower
}

public sealed class InventoryChangeActor(LeaderChannel<InventoryChange> changes, Inventory inventory)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var change in changes.Reader.ReadAllAsync(stoppingToken))
        {
            await inventory.ApplyAsync(change, stoppingToken);
        }
    }
}
```

A follower's write is accepted and discarded, not refused, so the consumer
needs no leadership check of its own. Discards are counted on
`hostloom.leader.channel.dropped` and summarised in the log once per
`LeaderChannel:DropReportInterval`; a full channel under the default
`DropOldest` mode is reported as a warning.

## 5. What happens when things fail

| Condition | What happens |
| --- | --- |
| The leader stops gracefully | it releases the lease; a candidate acquires on its next attempt, within one retry interval |
| The leader crashes | nothing releases; the lease expires after `Leadership:Lease` and a candidate acquires then |
| A renewal is refused or the backend is cut | the leader cancels `LeadershipToken`, releases, and waits one retry interval; a candidate acquires when the server-side lease expires |
| The lock backend is unreachable | nobody becomes leader; the outage is logged once per elector |
| The leader resigns | same as a graceful stop, but it stays a candidate |

Between a cut-off leader's last successful renewal and its local lease timer
there is a window in which it still believes it leads while the follower may
already hold the lease. It is bounded by the lease plus clock skew. Work that
must stop when leadership ends honours the token; nothing else bounds a stale
leader, and there is no fencing token a storage layer can check.

## 6. Verify

```text
docker compose up -d --wait redis
HOSTLOOM_REDIS_CHAOS=1 dotnet test tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj -c Release -- --filter-class '*RedisLeadershipTests'
```

The first case proves two electors agree on one leader and hand over on stop.
The chaos case cuts the leader's connection through a loopback proxy, waits for
the follower to take the lease, restores the connection, and asserts the
overlap between the old belief and the new lease is shorter than one lease.

## Related

- [Leadership reference](../reference/leadership.md)
- [Run a schedule on every instance or on one](schedule-on-one-instance.md)
- [Locking reference](../reference/locking.md)
