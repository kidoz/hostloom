# HostLoom.Locking.Testing

Composes a `HostLoom.Locking` lock for a test without a container, and decorates a provider so a
test can script contention, inject failures, or assert what the lock asked for.

```csharp
var clock = new FakeTimeProvider();
var provider = new ManualLockProvider(clock);
await using var locks = TestLock.Create(provider: provider, timeProvider: clock);

provider.Hold(TestLock.ProviderKey("job:42"));           // another instance owns it
Assert.Null(await locks.TryAcquireAsync("job:42"));      // skip-if-busy
provider.Release(TestLock.ProviderKey("job:42"));
```

`TestLock.Create()` composes a `DistributedLock` over the in-process provider with the test
namespace and an immediate, jitter-free retry policy, so a contended acquisition resolves in a
few attempts rather than three seconds. `ManualLockProvider` is the in-process provider with two
extra verbs, `Hold` and `Release`, for the key another instance would own; leases still expire on
the supplied clock, so a lost lease is a clock advance away. `FaultingLockProvider` fails the next
`n` calls, or every call, with a chosen `LockFailureKind`, which is how a test proves a consumer
sees `LockProviderUnavailableException` rather than a backend exception, and that a release
failure is logged rather than thrown. `RecordingLockProvider` records every call with its
outcome, so a test asserts the retry count, the owner token on release, or an extension before
the lease ended.

The in-process provider implements the whole contract, including lease expiry, owner-only release,
and extension, so a consumer's tests need no backend. Time is a `TimeProvider` everywhere; this
package ships no clock, and `Microsoft.Extensions.TimeProvider.Testing` provides `FakeTimeProvider`.

The lock is coordination, not correctness, for persisted state: a test that proves two runs were
serialised has proven scheduling, and the database's own constraints still own the invariant.
