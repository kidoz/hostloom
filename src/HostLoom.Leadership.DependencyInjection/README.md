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

A role can select a named lock with `AddRole(role, configure, lockProfile)`, for example
`.AddRole("scheduler", lockProfile: "catalog-jobs")`. Construction and startup validation resolve
that exact keyed lock, including its lease and key limits and its coordinated or disabled state. A
missing profile fails startup with the role and profile named; an enabled default lock does not
change the bound profile's `Leadership:WhenUncoordinated` behavior.

Each role has a configurable consumer lock key, before the lock's namespace prefix.
Share a format with named-options configuration, and override individual roles when needed:

```csharp
services.ConfigureAll<LeadershipOptions>(options =>
    options.LockKeyFormat = "election:eu:{role}");
services.AddHostLoomLeadership()
    .AddRole("scheduler")
    .AddRole("reconciler", options => options.LockKey = "legacy:reconciler");
```

With namespace `billing`, the keys above are `billing:lock:election:eu:scheduler` and
`billing:lock:legacy:reconciler`. The default format is `leader:{role}`; an exact `LockKey`
overrides it without placeholder expansion. Formats require `{role}` and replace every
occurrence; other characters are literal. Startup validation checks the resolved key against
`Locking:MaxKeyLength` as well as checking key syntax. `LeaderElector.LockKey` and its probe
report the captured consumer key. Changing a deployed key or namespace requires a coordinated
cutover: stop all old replicas and wait for their leases to expire before starting the new
identity, including on rollback.

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
