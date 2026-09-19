# Leadership

The `HostLoom.Leadership` family: lease-based leader election over the HostLoom
distributed lock, one elector per role, explicit renewal, a leadership token,
and a scheduling guard that runs exclusive schedules on the leader only. The
kernel references `HostLoom.Locking` and `Microsoft.Extensions.Logging.Abstractions`
and composes without a container; the `DependencyInjection` package registers
electors by role and starts them with the host. Namespaces follow the package
names.

```text
dotnet add package HostLoom.Leadership
dotnet add package HostLoom.Leadership.DependencyInjection   # roles, hosting
dotnet add package HostLoom.Scheduling.Leadership            # leader-only schedules
dotnet add package HostLoom.Leadership.Testing               # scripted ILeadership
```

## The guarantee

At most one leader per role while clocks and the lock backend behave. On lease
expiry there is a window, bounded by `Leadership:Lease` plus clock skew, in
which the previous leader may still believe it leads; `LeadershipToken` bounds
what it does with that belief only if the work honours the token. There is no
fencing: `Term` counts this instance's acquisitions and is not a token a
storage layer can check. Asynchronous replication in Redis and Valkey can lose
an acquisition on failover, as it can for any lease. This is the same tier as
the Kubernetes Lease election and is not consensus.

## Consumer contract (`ILeadership`)

| Member | Behaviour |
| --- | --- |
| `Role` | the role name; the lock key is `leader:{role}` under the lock's namespace |
| `IsLeader` | `true` while this instance believes it leads |
| `Term` | rises on every acquisition by this instance; not a fencing token |
| `LeadershipToken` | cancelled when leadership is lost, resigned, or stopped; already cancelled while not leading |
| `WaitForLeadershipAsync(ct)` | completes when this instance becomes leader, at once when it already is |
| `OnChange(listener)` | observes every `LeadershipChange` (`Acquired`, `Lost`, `Resigned`, `Stopped`) |

A leader-only loop is `await leadership.WaitForLeadershipAsync(ct)` followed by
work that runs on a token linked to `LeadershipToken`.

## The elector (`LeaderElector`)

`new LeaderElector(role, locks, options)` composes one; `StartAsync` begins as a
candidate, `StopAsync` releases the lease so a graceful restart hands over at
once, `ResignAsync` steps down and stays a candidate. The loop:

- A **candidate** tries a skip-if-busy acquisition on every
  `Leadership:RetryInterval` plus jitter. A refusal is normal. An unreachable
  provider is logged once per outage and makes nobody leader.
- A **leader** renews through the lock's `ExtendAsync` every
  `Leadership:RenewInterval`, on its own timer, so the lock's automatic
  extension cap `Locking:MaxHold` does not apply. A renewal that is refused or
  fails, or a lease the lock reports lost, cancels the token, releases, raises
  `Lost`, and waits one retry interval before the next attempt so another
  instance gets the first chance and a flapping backend cannot turn renewals
  into a hot loop.
- A **resigned** leader waits one retry interval as well. A **stopped**
  elector releases and does not run again.

`LeadershipProbe.Describe(elector)` reports role, status, term, lease, and
cadence without executing anything.

## Configuration (`LeadershipOptions`)

| Key | Default | Meaning |
| --- | --- | --- |
| `Leadership:Lease` | 15 seconds | how long the lease lasts without a renewal; a crashed leader keeps the role this long; keep it within `Locking:MaxLease` |
| `Leadership:RenewInterval` | 5 seconds | renewal cadence; at most half the lease |
| `Leadership:RetryInterval` | 2 seconds | candidate cadence, and the pause after a loss or a resignation |
| `Leadership:RetryJitter` | 1 second | additive jitter on the candidate cadence |

`Validate()` returns every violation naming the key; the elector's constructor
throws with the same text, and the `DependencyInjection` package validates each
role's named options when the host starts.

## Registration (`HostLoom.Leadership.DependencyInjection`)

```csharp
services
    .AddHostLoomLocking(locking => locking.Namespace = "billing")
    .UseRedis();

services
    .AddHostLoomLeadership()
    .AddRole("scheduler")
    .AddRole("reconciler", leadership => leadership.Lease = TimeSpan.FromSeconds(30));
```

Each role's `LeaderElector` and `ILeadership` are keyed by the role name; with
exactly one role the unkeyed `ILeadership` resolves it too, and with several the
unkeyed resolution throws naming them. A repeated role is refused at
registration. The hosted service starts every elector with the host and stops
each with the host.

## Leader-only schedules (`HostLoom.Scheduling.Leadership`)

`UseLeader(role)` chooses `LeaderScheduleGuard`: a claim is granted while this
instance leads and refused otherwise, so an exclusive schedule runs on the
leader with no lock round trip per occurrence, followers record `Skipped`, and
a run in progress ends as `ClaimLost` when leadership ends. Compared with
`HostLoom.Scheduling.Locking`, which claims each occurrence and spreads work
across instances, this keeps every exclusive schedule on one instance. One
guard per service collection.

## Observability

Meter and activity source `HostLoom.Leadership`, tagged `hostloom.leader.role`:
`hostloom.leader.is_leader` (gauge), `hostloom.leader.changes` (counter by
reason), and `hostloom.leader.renew.duration` (histogram by outcome). Log
events in `LeadershipEvents` (3400 to 3408). A throwing leadership-token
cancellation callback is logged without preventing lease release or subsequent
elections.

## Evidence

Deterministic tests run two electors over one in-process lock and a fake clock
(`LeadershipTests`): one leader, renewal past the lock's extension cap, a refused
renewal handing over, resignation, stop, a provider outage, and listener order.
`RedisLeadershipTests` runs two electors over a real Redis and, behind
`HOSTLOOM_REDIS_CHAOS=1`, cuts the leader's connection through a loopback proxy
and measures the hand-over: the follower acquires when the server-side lease
expires, the cut-off leader steps down when its local lease timer fires, and
the overlap between the two is asserted to be shorter than one lease, which is
the window the guarantee states.
