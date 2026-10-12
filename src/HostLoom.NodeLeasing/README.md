# HostLoom.NodeLeasing

`HostLoom.NodeLeasing` leases a unique small integer node ID — a Snowflake-style worker ID —
to each instance of a service, over the HostLoom distributed lock. A pool hands out the IDs
zero to `NodeLeasing:PoolSize` minus one, and an instance holds at most one ID per pool. The
mechanics are the same lease-based election as `HostLoom.Leadership`, scaled from one role to a
pool of IDs: a candidate scans the pool — the ID it last held first, for stickiness across a
restart, then ascending from a random offset — and takes the first ID a skip-if-busy acquisition
grants on `nodeid:{pool}:{id}`; the holder renews the lease on its own cadence, and a renewal the
backend refuses or a lease that runs out steps the instance back to candidate at once. A renewal
that fails at the provider while the lease still runs keeps the assignment and the term: the
holder retries, at the renewal cadence or at half the remaining lease when that is sooner, until
an extension succeeds or the lease ends. The package references `HostLoom.Locking`
and `Microsoft.Extensions.Logging.Abstractions`; every type has a public constructor, so an
elector composes without a container.

```csharp
var elector = new NodeIdElector("orders-workers", locks, new NodeLeasingOptions());
await elector.StartAsync();

var nodeId = await elector.WaitForAssignmentAsync(stoppingToken);
// Build generators with nodeId until the assignment ends:
await RunWithNodeIdAsync(nodeId, elector.AssignmentToken);
```

**The guarantee.** At most one holder per ID while clocks and the lock backend behave. On
lease expiry there is a window, bounded by `NodeLeasing:Lease` plus clock skew, in which the
previous holder may still believe it holds the ID; `AssignmentToken` bounds what it does with
that belief only if the work honours the token. There is no fencing: `Term` counts this
instance's acquisitions and is not a token a storage layer can check. Asynchronous replication
in Redis and Valkey can lose an acquisition on failover, as it can for any lease. This is
coordination, not correctness — the same tier as the Kubernetes Lease election, not consensus.

`INodeLeasing` is what consumers inject: `Pool`, `NodeId` (null while unassigned),
`IsCoordinated`, `Term`, `AssignmentToken`, `WaitForAssignmentAsync`, and `OnChange`.
`NodeIdElector` implements it and adds `StartAsync`; `StopAsync`, which releases the lease so a
graceful restart hands the ID over at once; and `ReleaseAsync`, which steps down and waits one
retry interval before scanning again so another instance gets the first chance. A crashed
process hands its ID over when the lease expires.

The elector owns renewal instead of the lock's automatic extension, which stops at
`Locking:MaxHold`; a holder renews for as long as it lives. Keep `NodeLeasing:Lease` within
`Locking:MaxLease`: a hosted elector fails at startup naming the pool and both values, while a
container-free one is capped by the lock. Keep `NodeLeasing:RenewInterval` at most half the
lease. An unreachable lock provider assigns no IDs, is logged once per outage, and is retried on
the candidate cadence.

Over a lock that does not coordinate (`Locking:Enabled = false`) the lease is a placeholder
every instance is granted, so the elector follows `NodeLeasing:WhenUncoordinated`: `None`, the
default, never assigns an ID, so replicas cannot all take one by accident; `AssignZero` assigns
ID zero without coordination, deterministic for a single instance or local development. The
condition is logged once per pool and reported by `IsCoordinated` and the probe. An exception
the loop did not expect is logged as `NodeLeasingLoopFaulted`, backed off one retry interval,
and survived; the loop never ends on its own, and every cadence is validated against the longest
wait it can schedule.

`NodeLeasingProbe.Describe(elector)` reports the composition without executing anything. Metrics
and activities live under the `HostLoom.NodeLeasing` meter and activity source. Install
`HostLoom.NodeLeasing.DependencyInjection` to register electors by pool, and
`HostLoom.NodeLeasing.Testing` for a scripted `INodeLeasing` double.
