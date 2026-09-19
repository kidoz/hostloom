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
`LeadershipOptions` named by the role and validated when the host starts. When exactly one role
is registered the unkeyed `ILeadership` resolves it too; with several, inject
`[FromKeyedServices("scheduler")] ILeadership leadership`. A repeated role is refused at
registration. The hosted service starts every elector with the host and stops each with the host,
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
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, leadership.LeadershipToken);
            await RunWhileLeaderAsync(linked.Token);   // returns when leadership ends
        }
    }
}
```

See the `HostLoom.Leadership` package README for the guarantee statement and the elector's
behaviour on loss, resignation, and stop.
