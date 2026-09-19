# HostLoom.Leadership.Testing

This package tests leader-only consumers of `HostLoom.Leadership` without an elector or a lock
backend. It references the kernel only.

`ManualLeadership` is an `ILeadership` a test flips by hand: `Acquire()` makes the instance leader
for a new term and releases `WaitForLeadershipAsync` waiters, `Lose()` ends leadership as an
expired lease would, cancelling `LeadershipToken` and raising `Lost`, and `Resign()` ends it as a
resignation. `Changes` lists every change raised, so a consumer's reaction to each transition is
asserted in order.

```csharp
using var leadership = new ManualLeadership("scheduler");
var loop = new ReconcilerLoop(leadership);
await loop.StartAsync(token);

leadership.Acquire();          // the loop's leader-only work starts
leadership.Lose();             // its token cancels; the work must stop
```

Pair it with `LeaderScheduleGuard` from `HostLoom.Scheduling.Leadership` to drive a scheduler
through leadership changes with a deterministic clock.
