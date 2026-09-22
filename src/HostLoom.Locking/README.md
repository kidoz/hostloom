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
reaches the provider, a log line, or a span. The one-argument form suits high-entropy inputs such
as tokens; a password or PIN goes through `LockKey.FromSensitive(value, key)`, whose keyed hash
cannot be brute-forced from a leaked key namespace.

Every lease has an owner token generated per acquisition; release and extension succeed only for
the owner. The handle's `LostToken` is cancelled, `IsHeld` turns false, and `hostloom.lock.lost`
increments when the lease expires on the local clock or the provider refuses an extend or a
release. The local clock starts at the request, not at the answer: a provider begins the lease
when it accepts the call, so the round trip is subtracted from the lease the handle reports. With `LockOptions.OnLost = LostLeaseBehavior.Cancel` the token handed to the action is
cancelled too; the default, `Observe`, keeps the action running and only reports the loss.
A provider call that throws is not a refusal: an extension that throws returns `false` and
keeps the previous lease end, and a release that throws is logged as `LockReleaseFailed` while
the backend keeps the lease until it expires, so neither is reported as a loss. With `AutoExtend`
a heartbeat that fails is retried halfway to the lease end, then halfway again down to a
twentieth of the lease, until an extension succeeds or the lease runs out; a single backend
hiccup does not end automatic extension.

An acquisition the caller stops waiting for, through its token or because `MaxWait` cancelled a
provider call still in flight, leaves one of three states behind. Cancelled or timed out before
the server applied the write: nothing is held. After the server applied it: the key stays held
for the abandoned owner until the lease expires and retries see the not-acquired outcome
meanwhile; when the provider still delivers the late confirmation, the lock issues one
best-effort owner-checked release, bounded by the lease and at most five seconds, counted on
`hostloom.lock.orphan_releases` (`released`, `absent`, `failed`) and logged at Debug as
`LockOrphanRelease` with the key only. The same release follows a confirmation that arrives once
the usable lease has already run out, which is rejected with `LockFailureKind.Timeout`. A
provider that threw (`Unavailable` or `Timeout`) confirmed nothing: the state is unknown, the key
is held for at most one lease, and nothing is released.

`LockRetryPolicy` shapes the wait between attempts and never depends on `HostLoom.Pipelines`. The
default reproduces the platform's historical behaviour: ten retries at a linear 50 ms step with up
to 50 ms of additive jitter, about 3 s in total; `LockOptions.MaxWait` is a hard wall-clock bound
on top of it, cancelling the provider call it would otherwise outlive, and `TimeSpan.Zero` makes
exactly one attempt. `LockingOptions.Enabled = false` is
single-instance mode: a startup warning, `hostloom.lock.enabled = 0`, and every action running
immediately; `IDistributedLock.IsCoordinated` and every handle's `IsCoordinated` report `false`
there, so a consumer that gates exclusive work on a lease can tell the placeholder from a real
one.

`LockingProbe.Describe(lock)` reports the composition without executing anything. Metrics and
activities live under the `HostLoom.Locking` meter and activity source. Install
`HostLoom.Locking.DependencyInjection` to register the lock with the built-in .NET container.
