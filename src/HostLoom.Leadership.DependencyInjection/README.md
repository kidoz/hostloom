# HostLoom.Leadership.DependencyInjection

This package registers `HostLoom.Leadership` electors with
`Microsoft.Extensions.DependencyInjection` and starts them with the host:

```csharp
services
    .AddHostLoomLocking(locking => locking.Namespace = "billing")
    .UseRedis();

services
    .AddHostLoomLeadership()
    .AddRole("scheduler")
    .AddRole("reconciler", leadership => leadership.Lease = TimeSpan.FromSeconds(30));
```

`AddRole` registers one `LeaderElector` per role over the `IDistributedLock` registered by
`AddHostLoomLocking`, resolved as `ILeadership` and `LeaderElector` keyed by the role, with
`LeadershipOptions` named by the role and validated when the host starts, including
`Leadership:Lease` against the lock's `Locking:MaxLease`. When exactly one role
is registered the unkeyed `ILeadership` resolves it too; with several, inject
`[FromKeyedServices("scheduler")] ILeadership leadership`. A repeated role is refused at
registration, and so is an `ILeadership` registered before `AddHostLoomLeadership` or one keyed by
a role before `AddRole`, because it would have taken precedence over the elector; register a test
double after the builder to replace the electors deliberately, or register it keyed by a role you
do not add. The hosted service starts every elector with the host and stops each with the host,
releasing the leases they hold so a graceful restart hands over at once.

A leader-only background loop is then:

```csharp
public sealed class ReconcilerLoop(ILeadership leadership) : BackgroundService
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
                await RunWhileLeaderAsync(linked.Token);
            }
            catch (OperationCanceledException) when (leadershipToken.IsCancellationRequested)
            {
                // Lost this term; wait to lead again.
            }
        }
    }
}
```

See the `HostLoom.Leadership` package README for the guarantee statement and the elector's
behaviour on loss, resignation, and stop.
