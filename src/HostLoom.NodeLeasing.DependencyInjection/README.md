# HostLoom.NodeLeasing.DependencyInjection

This package registers `HostLoom.NodeLeasing` electors with
`Microsoft.Extensions.DependencyInjection` and starts them with the host:

```csharp
services
    .AddHostLoomLocking(locking => locking.Namespace = "orders")
    .UseRedis();

services
    .AddHostLoomNodeLeasing()
    .AddPool("orders-workers")
    .AddPool("inventory-workers", leasing => leasing.PoolSize = 128);
```

`AddPool` registers one `NodeIdElector` per pool over the `IDistributedLock` registered by
`AddHostLoomLocking`, resolved as `INodeLeasing` and `NodeIdElector` keyed by the pool, with
`NodeLeasingOptions` named by the pool and validated when the host starts, including
`NodeLeasing:Lease` against the lock's `Locking:MaxLease`. When exactly one pool
is registered the unkeyed `INodeLeasing` resolves it too; with several, inject
`[FromKeyedServices("orders-workers")] INodeLeasing leasing`. A repeated pool is refused at
registration, and so is an `INodeLeasing` registered before `AddHostLoomNodeLeasing` or one keyed
by a pool before `AddPool`, because it would have taken precedence over the elector; register a
test double after the builder to replace the electors deliberately, or register it keyed by a
pool you do not add. The hosted service starts every elector with the host and stops each with
the host, releasing the leases they hold so a graceful restart hands the IDs over at once.

A consumer that needs a node ID is then:

```csharp
public sealed class ShipmentIdSource(INodeLeasing leasing) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var nodeId = await leasing.WaitForAssignmentAsync(stoppingToken);
            var assignmentToken = leasing.AssignmentToken;
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken, assignmentToken);
            try
            {
                await IssueIdentifiersAsync(nodeId, linked.Token);
            }
            catch (OperationCanceledException) when (assignmentToken.IsCancellationRequested)
            {
                // Lost this assignment; wait for the next one.
            }
        }
    }
}
```

See the `HostLoom.NodeLeasing` package README for the guarantee statement and the elector's
behaviour on loss, release, and stop.
