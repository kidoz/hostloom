# Node leasing

The `HostLoom.NodeLeasing` family: lease-based node-ID leasing over the HostLoom
distributed lock. A pool hands out the small integer IDs — Snowflake-style worker IDs —
zero to `NodeLeasing:PoolSize` minus one, and an instance holds at most one ID per pool. The
kernel references `HostLoom.Locking` and `Microsoft.Extensions.Logging.Abstractions`
and composes without a container; the `DependencyInjection` package registers
electors by pool and starts them with the host. Namespaces follow the package
names.

```text
dotnet add package HostLoom.NodeLeasing
dotnet add package HostLoom.NodeLeasing.DependencyInjection   # pools, hosting
dotnet add package HostLoom.NodeLeasing.Testing               # scripted INodeLeasing
```

## The guarantee

At most one holder per ID while clocks and the lock backend behave. On lease
expiry there is a window, bounded by `NodeLeasing:Lease` plus clock skew, in
which the previous holder may still believe it holds the ID; `AssignmentToken`
bounds what it does with that belief only if the work honours the token. There
is no fencing: `Term` counts this instance's acquisitions and is not a token a
storage layer can check. Fencing would have to come from the lock provider, as
a monotonic token issued with each lease that storage compares on every write;
no HostLoom provider issues one today, and until one does persisted-state
correctness belongs to transactions, constraints, and idempotency records.
Asynchronous replication in Redis and Valkey can lose
an acquisition on failover, as it can for any lease. This is coordination, not
correctness — the same tier as the Kubernetes Lease election, not consensus.

## Consumer contract (`INodeLeasing`)

| Member | Behaviour |
| --- | --- |
| `Pool` | the pool name; the lock keys are `nodeid:{pool}:{id}` under the lock's namespace |
| `NodeId` | the ID held, zero to `NodeLeasing:PoolSize` minus one; `null` while unassigned |
| `IsCoordinated` | `false` when the lock does not coordinate (`Locking:Enabled = false`); `NodeId` then follows `NodeLeasing:WhenUncoordinated` on every instance alike, and nothing excludes another instance from holding the same ID |
| `Term` | rises on every acquisition by this instance; not a fencing token |
| `AssignmentToken` | cancelled when the assignment is lost, released, or stopped; already cancelled while unassigned |
| `WaitForAssignmentAsync(ct)` | completes with the ID when this instance acquires one, at once when it already holds one |
| `OnChange(listener)` | observes every `NodeLeaseChange` (`Acquired`, `Lost`, `Released`, `Stopped`) |

A consumer that needs an ID is `var nodeId = await leasing.WaitForAssignmentAsync(ct)` followed
by work that runs on a token linked to `AssignmentToken`, rebuilt from the new ID on the next
assignment.

## The elector (`NodeIdElector`)

`new NodeIdElector(pool, locks, options)` composes one; `StartAsync` begins as a
candidate, `StopAsync` releases the lease so a graceful restart hands the ID over
at once, `ReleaseAsync` steps down and stays a candidate. The loop:

- A **candidate** scans the pool on every `NodeLeasing:RetryInterval` plus jitter:
  the ID it last held first, so a restart or a release-then-reacquire keeps its
  ID, then every other ID ascending from a random offset, so contenders after a
  mass restart do not all probe the same low IDs first. Each candidate is one
  skip-if-busy acquisition on `nodeid:{pool}:{id}`; the first grant wins. An
  exhausted pool is normal and is retried on the same cadence. An unreachable
  provider is logged once per outage and assigns no IDs.
- A **holder** renews through the lock's `ExtendAsync` every
  `NodeLeasing:RenewInterval`, on its own timer, so the lock's automatic
  extension cap `Locking:MaxHold` does not apply. A renewal the backend
  refuses, or a lease that runs out, cancels the token, releases, raises
  `Lost`, and waits one retry interval before the next scan so another
  instance gets the first chance and a flapping backend cannot turn renewals
  into a hot loop.
- A renewal that **fails** at the provider, such as a timeout, leaves the lease
  the lock holds intact until its end. The holder keeps its ID and the same
  term, and retries after `NodeLeasing:RenewInterval` or half of what is left
  of the lease, whichever is sooner, but not sooner than a twentieth of the
  lease. It steps down when a retry is refused or when the lease ends, never
  later. Each failed attempt is recorded on `hostloom.node.renew.duration` with
  outcome `failed` and logged by the lock as `LockExtendFailed` (3105).
- A **released** holder waits one retry interval as well. A **stopped**
  elector releases and does not run again.
- Over a lock that does not coordinate (`Locking:Enabled = false`) a lease is a
  placeholder every instance is granted, so the elector follows
  `NodeLeasing:WhenUncoordinated`: `None`, the default, never assigns an ID;
  `AssignZero` assigns ID zero without coordination, deterministic for a single
  instance or local development. Either way `NodeLeasingUncoordinated` (3509) is
  logged once per pool, `IsCoordinated` is `false`, and the probe says which
  posture applies.
- An exception the loop did not expect, from a misbehaving lock or a defect,
  never ends the loop: `NodeLeasingLoopFaulted` (3510) is logged with the
  exception, `hostloom.node.loop.faults` is incremented, and the elector backs
  off one retry interval plus jitter before continuing as a candidate. An
  assignment in progress is stepped down first, so its token cancels and its
  change is raised.

`NodeLeasingProbe.Describe(elector)` reports pool, status, coordination, ID,
term, lease, and cadence without executing anything.

## Configuration (`NodeLeasingOptions`)

| Key | Default | Meaning |
| --- | --- | --- |
| `NodeLeasing:PoolSize` | 1024 | how many IDs the pool hands out, zero to `PoolSize` minus one; between 1 and 65535 |
| `NodeLeasing:Lease` | 15 seconds | how long the lease lasts without a renewal; a crashed holder keeps the ID this long; at most `Locking:MaxLease`, checked when the host starts |
| `NodeLeasing:RenewInterval` | 5 seconds | renewal cadence; at most half the lease |
| `NodeLeasing:RetryInterval` | 2 seconds | candidate cadence, and the pause after a loss or a release |
| `NodeLeasing:RetryJitter` | 1 second | additive jitter on the candidate cadence |
| `NodeLeasing:WhenUncoordinated` | `None` | what to do over a disabled lock: `None` never assigns, so replicas cannot all take an ID by accident; `AssignZero` assigns ID zero without coordination, for a single instance or local development |

`Validate()` returns every violation naming the key; the elector's constructor
throws with the same text, and the `DependencyInjection` package validates each
pool's named options when the host starts. There it also checks
`NodeLeasing:Lease` against `Locking:MaxLease` of the registered lock and fails
naming the pool and both values, since the lock would otherwise cap the lease
silently; a container-free elector gets the capped lease. Single-instance mode
takes no lease and is not checked. `Validate()` also keeps every cadence within
the longest wait the elector can schedule (`NodeLeasingOptions.MaxDelay`, about
49 days), so the loop can never fail on an out-of-range delay.

## Registration (`HostLoom.NodeLeasing.DependencyInjection`)

```csharp
services
    .AddHostLoomLocking(locking => locking.Namespace = "orders")
    .UseRedis();

services
    .AddHostLoomNodeLeasing()
    .AddPool("orders-workers")
    .AddPool("inventory-workers", leasing => leasing.PoolSize = 128);
```

Each pool's `NodeIdElector` and `INodeLeasing` are keyed by the pool name; with
exactly one pool the unkeyed `INodeLeasing` resolves it too, and with several
the unkeyed resolution throws naming them. A repeated pool is refused at
registration, and so is an `INodeLeasing` registered before
`AddHostLoomNodeLeasing` or one keyed by a pool before `AddPool(pool)`: it
would have taken precedence over the elector while the composition looked
complete, so the builder names it instead. To stand in for the electors
deliberately, register the double after the builder; a double for a pool that
is not added needs no elector at all. The hosted service starts every elector
with the host and stops each with the host.

Pool names match `[a-z0-9-]+`, the same shape as `Locking:Namespace`, so
provider keys stay unambiguous across services. Every instance of a deployment
configures the same `NodeLeasing:PoolSize` for a pool; a smaller pool on one
instance overlaps, never partitions, the larger one.

## Scripted assignments (`HostLoom.NodeLeasing.Testing`)

`ManualNodeLeasing` is an `INodeLeasing` a test flips by hand: `Assign(id)`
gives the instance an ID for a new term and releases `WaitForAssignmentAsync`
waiters, `Lose()` ends the assignment as an expired lease would, and
`Release()` ends it as a release on request. `Changes` lists every change
raised, so a consumer's reaction to each transition is asserted in order.
`IsCoordinated` is settable, `true` by default, to stand in for an elector over
a disabled lock.

## Observability

Meter and activity source `HostLoom.NodeLeasing`, tagged `hostloom.node.pool`:
`hostloom.node.assigned` (gauge), `hostloom.node.assignment.changes` (counter
by reason), `hostloom.node.renew.duration` (histogram by outcome), and
`hostloom.node.loop.faults` (counter of unexpected exceptions the loop
survived), plus the `nodeid.acquire` activity. Log events in
`NodeLeasingEvents` (3500 to 3510). A throwing assignment-token cancellation
callback is logged without preventing lease release or later assignments, and
a throwing change listener is logged without blocking the loop or the other
listeners.

## Evidence

Deterministic tests run electors over one in-process lock and a fake clock
(`NodeLeasingTests`): two instances holding distinct IDs and a third waiting on
an exhausted pool, renewal past the lock's extension cap keeping the ID, a
transient renewal failure keeping the term, persistent renewal failures
stepping down exactly at the lease end, a refused renewal handing the ID over,
release and stickiness of the last-held ID, stop releasing for immediate reuse,
a provider outage assigning nothing until it answers, a throwing listener not
killing the loop, and listener, waiter, and probe order.
`NodeLeasingRegistrationTests` cover keyed and unkeyed resolution, refused
duplicate and foreign registrations, the lease check against
`Locking:MaxLease` at resolution and at host start, the scripted
`ManualNodeLeasing`, and the uncoordinated postures: nobody is assigned under
the default `None`, every instance holds node zero under `AssignZero`, the
warning is written once per pool, and the probe reports the posture.
Integration tests against a real Redis or Valkey are a planned follow-up.
