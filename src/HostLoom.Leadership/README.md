# HostLoom.Leadership

`HostLoom.Leadership` elects one leader for a role among the instances of a service, over the
HostLoom distributed lock. It is the lease-based election that Kubernetes controllers, Spring
Integration's leader initiator, and most cloud services use: a candidate tries to take the lease
`leader:{role}`, the leader renews it on its own cadence, and a renewal the backend refuses or a
lease that runs out steps the instance back to candidate at once. A renewal that fails at the
provider while the lease still runs keeps leadership and the term: the leader retries, at the
renewal cadence or at half the remaining lease when that is sooner, until an extension succeeds
or the lease ends. The package references `HostLoom.Locking`
and `Microsoft.Extensions.Logging.Abstractions`; every type has a public constructor, so an
elector composes without a container.

```csharp
var elector = new LeaderElector("billing", locks, new LeadershipOptions());
await elector.StartAsync();

await elector.WaitForLeadershipAsync(stoppingToken);
await RunLeaderLoopAsync(elector.LeadershipToken);   // ends when leadership does
```

**The guarantee.** At most one leader per role while clocks and the lock backend behave. On
lease expiry there is a window, bounded by `Leadership:Lease` plus clock skew, in which the
previous leader may still believe it leads; `LeadershipToken` bounds what it does with that
belief only if the work honours the token. There is no fencing: `Term` counts this instance's
acquisitions and is not a token a storage layer can check. Asynchronous replication in Redis and
Valkey can lose an acquisition on failover, as it can for any lease. This is the same tier as the
Kubernetes Lease election and is not consensus.

`ILeadership` is what consumers inject: `Role`, `IsLeader`, `IsCoordinated`, `Term`,
`LeadershipToken`, `WaitForLeadershipAsync`, and `OnChange`. `LeaderElector` implements it and
adds `StartAsync`, `StopAsync`, which releases the lease so a graceful restart hands over at once,
and `ResignAsync`, which steps down and waits one retry interval before running again so another
instance gets the first chance. A crashed process hands over when its lease expires.

The elector owns renewal instead of the lock's automatic extension, which stops at
`Locking:MaxHold`; a leader renews for as long as it lives. Keep `Leadership:Lease` within
`Locking:MaxLease`: a hosted elector fails at startup naming the role and both values, while a
container-free one is capped by the lock. Keep `Leadership:RenewInterval` at most half the lease. An unreachable lock provider makes nobody leader, is logged once per outage, and is
retried on the candidate cadence.

Over a lock that does not coordinate (`Locking:Enabled = false`) the lease is a placeholder every
instance is granted, so the elector follows `Leadership:WhenUncoordinated`: `Follow`, the default,
never leads, so replicas cannot all lead by accident; `Lead` leads without coordination, for a
single instance or local development. The condition is logged once per role and reported by
`IsCoordinated` and the probe. An exception the loop did not expect is logged as
`LeadershipLoopFaulted`, backed off one retry interval, and survived; the loop never ends on its
own, and every cadence is validated against the longest wait it can schedule.

`LeaderChannel<T>` is a bounded channel gated on a role: producers on every instance write to
it, a leader's items reach the reader, and a follower's are accepted and discarded. That keeps a
warm standby current without letting it act. Drops are counted on the meter by reason
(`follower`, `full`, or `loss`) and summarised in the log at most once per
`LeaderChannel:DropReportInterval`, with the tail written when the instance becomes leader.

```csharp
var changes = new LeaderChannel<InventoryChange>("inventory-changes", leadership);
await changes.Writer.WriteAsync(change);          // discarded unless this instance leads
await foreach (var change in changes.Reader.ReadAllAsync(stoppingToken)) { ... }
```

With `FullMode=Wait`, losing leadership wakes producers waiting for capacity; follower
readiness succeeds even while the old buffer is full. Asynchronous writes recheck the role
after waiting. Already accepted buffered items remain readable: use the leadership token
around leader-only side effects, because channel admission is not a fencing guarantee.
`LeaderChannelOptions.DrainOnLoss` discards the buffer when leadership ends instead, counted and
logged as `loss`; it narrows that window without closing it.

`LeadershipProbe.Describe(elector)` reports the composition without executing anything. Metrics
and activities live under the `HostLoom.Leadership` meter and activity source. Install
`HostLoom.Leadership.DependencyInjection` to register electors by role, and
`HostLoom.Scheduling.Leadership` to run exclusive schedules on the leader only.
