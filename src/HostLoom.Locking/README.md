# HostLoom.Locking

`HostLoom.Locking` is the backend-neutral distributed lock kernel: the consumer contract
`IDistributedLock`, the backend contract `ILockProvider`, the composed `DistributedLock` that
turns a provider into a lock with leases, retries, owner tokens, and lost-lease detection, and an
in-process provider with the same state machine a distributed backend has. The package references
only `Microsoft.Extensions.Logging.Abstractions`; every runtime type has a public constructor, so a
lock composes without a container.

**The lock is coordination, not correctness.** It keeps two instances from doing the same work at
the same time. It does not make persisted state correct: a lease can expire while an action is
still running, and a provider can lose a lease it granted. Database transactions, row locks, unique
constraints, and idempotency records own correctness. Design every action so that running it twice,
or running it after the lease was lost, is safe.

```csharp
var options = new LockingOptions { Namespace = "billing" };
IDistributedLock locks = new DistributedLock(options, new InMemoryLockProvider());

// Acquire, run, release in a finally. Contention past the retry policy throws
// LockNotAcquiredException; a backend failure throws LockProviderUnavailableException.
await locks.ExecuteWithLockAsync(
    $"invoice:{invoiceId}",
    async token => await SettleAsync(invoiceId, token),
    new LockOptions { Lease = TimeSpan.FromSeconds(10), OnLost = LostLeaseBehavior.Cancel },
    cancellationToken);

// Skip if busy: one attempt, no wait, null when another owner holds the key.
await using var handle = await locks.TryAcquireAsync("nightly-report", cancellationToken: cancellationToken);
if (handle is not null)
{
    await RunReportAsync(handle.LostToken);
}
```

Keys are opaque strings without whitespace or control characters. The composed lock prefixes them
as `{namespace}:lock:{key}` before they reach a provider, so consumers never repeat the prefix.
Build a key from a credential with `LockKey.FromSensitive`, which hashes the value so it never
reaches the provider, a log line, or a span.

Every lease has an owner token generated per acquisition; release and extension succeed only for
the owner. The handle's `LostToken` is cancelled, `IsHeld` turns false, and `hostloom.lock.lost`
increments when the lease expires on the local clock or the provider refuses an extend or a
release. With `LockOptions.OnLost = LostLeaseBehavior.Cancel` the token handed to the action is
cancelled too; the default, `Observe`, keeps the action running and only reports the loss.

`LockRetryPolicy` shapes the wait between attempts and never depends on `HostLoom.Pipelines`. The
default reproduces the platform's historical behaviour: ten retries at a linear 50 ms step with up
to 50 ms of additive jitter, about 3 s in total; `LockOptions.MaxWait` is a hard wall-clock bound
on top of it, and `TimeSpan.Zero` makes exactly one attempt. `LockingOptions.Enabled = false` is
single-instance mode: a startup warning, `hostloom.lock.enabled = 0`, and every action running
immediately.

`LockingProbe.Describe(lock)` reports the composition without executing anything. Metrics and
activities live under the `HostLoom.Locking` meter and activity source. Install
`HostLoom.Locking.DependencyInjection` to register the lock with the built-in .NET container.
