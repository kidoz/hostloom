# Locking

The `HostLoom.Locking` family: a distributed lock with leases, owner tokens,
a retry policy, lost-lease detection, and typed outcomes. The kernel
references only `Microsoft.Extensions.Logging.Abstractions` and composes
without a container; the `DependencyInjection` package registers it; a
backend package such as `HostLoom.Redis` supplies the provider. Namespaces
follow the package names.

```text
dotnet add package HostLoom.Locking
dotnet add package HostLoom.Locking.DependencyInjection   # container registration
dotnet add package HostLoom.Redis                         # Redis backend
dotnet add package HostLoom.Locking.Testing               # container-free composition
```

The lock is coordination, not correctness, for persisted state. It keeps two
instances from doing the same work at the same time; a database's
transactions, row locks, unique constraints, and idempotency records own
the invariant.

## Consumer contract (`IDistributedLock`)

| Member | Behaviour |
| --- | --- |
| `ExecuteWithLockAsync<T>(key, action, options, ct)` | Acquire, run `action`, release in `finally`, propagate the action's exception. The action receives the caller's token, linked to the lost-lease token when `OnLost = Cancel`. |
| `ExecuteWithLockAsync(key, action, options, ct)` | The same without a result. |
| `TryAcquireAsync(key, options, ct)` | An `ILockHandle`, or null when not acquired. With no options this is skip-if-busy (one attempt). |

`ILockHandle` is `IAsyncDisposable` and exposes `Key`, `IsHeld`, `LeaseEnd`,
`LostToken` (cancelled when the lease ends before release), and
`ExtendAsync(lease, ct)`, which returns `false` when the lease was already
lost. Disposing releases; a release failure is logged, never thrown.

## Outcomes

| Type | When |
| --- | --- |
| `LockNotAcquiredException` | contention past `MaxWait` or the retry policy; carries `Key`, `Waited`, `Attempts` |
| `LockProviderUnavailableException` | the provider failed; carries `Key`, `Waited`, `Attempts`, and a `LockFailureKind` (`Unavailable`, `Timeout`, `Other`) |
| `LockReentrancyException` | the same asynchronous flow already holds the key and `Locking:DetectReentrancy` is on |
| `OperationCanceledException` | the caller's token; nothing stays held |

`TimeoutException` is never thrown.

## Per-call options (`LockOptions`)

| Property | Meaning |
| --- | --- |
| `Lease` | time the provider holds the key for this owner; defaults to `Locking:DefaultLease`, capped by `Locking:MaxLease` |
| `MaxWait` | hard wall-clock bound on acquisition: no attempt starts on or after it, no delay reaches it, and a provider call still running at it is cancelled; `TimeSpan.Zero` is one attempt bounded only by the caller's token; null is bounded by the retry policy alone |
| `Retry` | a `LockRetryPolicy`; defaults to `Locking:Retry` |
| `AutoExtend` | heartbeat at half the lease, bounded by `Locking:MaxHold` |
| `OnLost` | `Observe` (default; the action keeps running while `IsHeld` and `LostToken` report the loss) or `Cancel` (the action's token is cancelled) |

`LockRetryPolicy` is immutable: `Immediate(retries)`, `Interval(retries,
interval)`, `Linear(retries, step)`, `Exponential(retries, min, max,
factor)`, and `WithJitter(jitter)` for uniform additive jitter. `GetDelay(n)`
is the delay before retry `n`, counting from one; `MaxTotalDelay` is the
derived maximum wait, logged at startup. The default is ten linear retries at
a 50 ms step with 50 ms of jitter, about three seconds in total.

## Configuration (`LockingOptions`)

| Key | Default | Meaning |
| --- | --- | --- |
| `Locking:Namespace` | required | `[a-z0-9-]+`; keys become `{namespace}:lock:{key}` |
| `Locking:Enabled` | `true` | `false` is single-instance mode: a startup warning, the gauge `hostloom.lock.enabled = 0`, and actions run immediately |
| `Locking:DefaultLease` | 30 s | lease when a call gives none |
| `Locking:MaxLease` | 5 min | cap on any lease |
| `Locking:MaxHold` | 10 min | bound on automatic extension |
| `Locking:Retry` | linear, 10 × 50 ms, 50 ms jitter | default retry policy |
| `Locking:AutoExtend` | `false` | heartbeat by default |
| `Locking:DetectReentrancy` | `true` | throw on same-key re-entry within one asynchronous flow |
| `Locking:MaxKeyLength` | 512 | longest consumer key |

`Validate()` returns every violation naming its option key; the
`DependencyInjection` package runs it at startup. `LockKey.FromSensitive`
hashes a credential so it never reaches the provider or a log.

## Backend contract (`ILockProvider`)

`TryAcquireAsync(key, owner, lease, ct)`, `ReleaseAsync(key, owner, ct)`, and
`ExtendAsync(key, owner, lease, ct)`, all returning `bool`: `false` means
contention on acquire and owner mismatch on release or extend. A backend
failure is `LockProviderException` with a `LockFailureKind`. The composed
lock passes the fully prefixed key, generates a random owner token per
acquisition, and maps the provider exception for consumers.
`ILockProviderHealthProbe` is the optional readiness capability.
`InMemoryLockProvider` implements the whole contract, including lease expiry
on a `TimeProvider`, so the same state machine runs in tests as on a backend.

## Registration (DependencyInjection)

```csharp
LockingBuilder AddHostLoomLocking(this IServiceCollection services,
    Action<LockingOptions>? configure = null);
```

| Builder member | Effect |
| --- | --- |
| `UseInMemory()` | the in-process provider |
| `UseProvider<TProvider>(name)` | any provider; also registered as probe when it implements one; backend packages call this from their `Use*` |
| `AddHealthChecks(name)` | readiness check tagged `ready` over the provider's probe; never liveness |

Exactly one provider per builder; a second choice throws naming the first.
`Locking:Enabled = false` needs no provider.

## Diagnostics

Meter and activity source `HostLoom.Locking`; instruments and activities are
listed in the [observability surface](observability.md).
`LockingProbe.Describe(lock)` returns a `LockDescription` whose lines name
the option that decided each part, including the retry policy's derived
maximum wait.

## Testing

`HostLoom.Locking.Testing` composes a `DistributedLock` without a container
(`TestLock.Create()`), scripts contention (`ManualLockProvider.Hold` and
`Release`), injects failures (`FaultingLockProvider`), and records calls
(`RecordingLockProvider`). Leases expire on the supplied `TimeProvider`, so a
lost lease is a clock advance away.
