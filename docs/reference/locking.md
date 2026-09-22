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
| `IsCoordinated` | `true` when a lease is a real claim on a shared backend; `false` in single-instance mode, where every acquisition is granted at once and coordinates nothing. |

`ILockHandle` is `IAsyncDisposable` and exposes `Key`, `IsHeld`, `LeaseEnd`,
`IsCoordinated` (`false` for the placeholder single-instance mode hands out,
which is held for ever and excludes nobody), `LostToken` (cancelled when the
lease ends before release), and
`ExtendAsync(lease, ct)`, which returns `false` when the lease was already
lost. Disposing releases; a release failure is logged, never thrown.

A lease starts when the provider accepts the request, not when its answer
arrives, so `LeaseEnd` and the local expiry timer count the round trip as
spent lease. An answer that takes longer than the lease itself yields a handle
that is already lost, on acquisition and on extension alike.

## Outcomes

| Type | When |
| --- | --- |
| `LockNotAcquiredException` | contention past `MaxWait` or the retry policy; carries `Key`, `Waited`, `Attempts`. A `MaxWait` that expires while the provider has not answered is reported the same way, so a backend that stops answering looks like contention here |
| `LockProviderUnavailableException` | the provider failed; carries `Key`, `Waited`, `Attempts`, and a `LockFailureKind` (`Unavailable`, `Timeout`, `Other`) |
| `LockReentrancyException` | the same asynchronous flow already holds the key and `Locking:DetectReentrancy` is on |
| `OperationCanceledException` | the caller's token; the lock stops waiting, and a grant the backend still delivers is released, see below |
| `ObjectDisposedException` | the `DistributedLock` was disposed before or during the acquisition |

`TimeoutException` is never thrown.

### What an abandoned acquisition leaves behind

The owner token is generated locally before the provider is called, and the
provider call runs under a token the lock owns. That token is cancelled only
when the lease has run out since the request, which leaves no usable grant, or
when the `DistributedLock` is disposed. The caller's token and `MaxWait` end
the lock's wait for the reply, never the call. An attempt the lock does not
turn into a handle ends in one of these cases:

- Nothing was sent: the caller's token was already cancelled, or `MaxWait` had
  run out, before the attempt started. Nothing is held.
- Sent and granted, but abandoned: the caller cancelled or `MaxWait` expired
  while the reply was outstanding, or the confirmation arrived once the usable
  lease had already run out, which is rejected as
  `LockProviderUnavailableException` with kind `Timeout`. The key is held for
  the abandoned owner until the reply arrives, and retries see the
  not-acquired outcome meanwhile; once the reply confirms the grant, the lock
  releases it.
- The provider threw `LockProviderException` with kind `Timeout` or `Other`,
  or stopped on its token at the lease end or on disposal: the command may
  have been applied, so the grant is uncertain. The caller gets its
  `LockProviderUnavailableException` at once, and the lock releases
  immediately without making the caller wait.
- The provider threw with kind `Unavailable`: it could not reach the backend,
  so there is nothing to release. The Redis provider also reports a connection
  that fails while a command is in flight as `Unavailable`; such a key is held
  for at most one lease.

Each release is one best-effort, owner-checked call for the abandoned owner,
bounded by the lease and at most five seconds, and nobody waits for it. The
outcome is counted on `hostloom.lock.orphan_releases` (`released`, `absent`,
or `failed`) and logged at Debug as `LockOrphanRelease` with the key only. A
release can only remove this owner's lease, never a successor's. A reply that
never arrives releases nothing; its attempt keeps one timer, which stops the
call at the lease end.

The cost: cancelling no longer stops a command the lock has already issued. A
provider call can outlive its caller by up to one lease, and an abandoned
attempt can cost one extra `SET` plus one release on the backend. The Redis
and Valkey providers honour their token in the standard .NET way, as any
provider may, because the lock never cancels an acquisition early.

## Per-call options (`LockOptions`)

| Property | Meaning |
| --- | --- |
| `Lease` | time the provider holds the key for this owner; defaults to `Locking:DefaultLease`, capped by `Locking:MaxLease` |
| `MaxWait` | hard wall-clock bound on acquisition: no attempt starts on or after it, no delay reaches it, and a provider call still running at it is abandoned, not cancelled, and released should it still grant; `TimeSpan.Zero` is one attempt bounded only by the caller's token; null is bounded by the retry policy alone |
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
| `Locking:Enabled` | `true` | `false` is single-instance mode: a startup warning, the gauge `hostloom.lock.enabled = 0`, actions run immediately, and `IsCoordinated` is `false` on the lock and on every handle so leadership and schedule guards can tell a placeholder from a lease |
| `Locking:DefaultLease` | 30 s | lease when a call gives none |
| `Locking:MaxLease` | 5 min | cap on any lease |
| `Locking:MaxHold` | 10 min | bound on automatic extension |
| `Locking:Retry` | linear, 10 × 50 ms, 50 ms jitter | default retry policy |
| `Locking:AutoExtend` | `false` | heartbeat by default |
| `Locking:DetectReentrancy` | `true` | throw on same-key re-entry within one asynchronous flow |
| `Locking:MaxKeyLength` | 512 | longest consumer key |

`Validate()` returns every violation naming its option key; the
`DependencyInjection` package runs it at startup.
`LockKey.FromSensitive(value)` hashes a high-entropy credential such as a
token, session id, or API key (SHA-256, 32 hex characters) so it never
reaches the provider or a log. The hash is unkeyed, so a low-entropy secret
such as a password or PIN goes through `LockKey.FromSensitive(value, key)`
instead: HMAC-SHA256 under a key held outside the provider, with the same
output shape. Every instance of a service must use the same key, or they
will not contend for the same lock.

## Backend contract (`ILockProvider`)

`TryAcquireAsync(key, owner, lease, ct)`, `ReleaseAsync(key, owner, ct)`, and
`ExtendAsync(key, owner, lease, ct)`, all returning `bool`: `false` means
contention on acquire and owner mismatch on release or extend. A backend
failure is `LockProviderException` with a `LockFailureKind`; report
`Unavailable` only when the command cannot have reached the backend, since
the lock releases after `Timeout` or `Other`. Every call honours its
cancellation token as any .NET API does. The composed lock passes the fully
prefixed key, generates a random owner token per acquisition, passes
`TryAcquireAsync` a token of its own that it cancels only at the lease end or
on disposal, and maps the provider exception for consumers.
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
`Release`), injects failures and late replies (`FaultingLockProvider`, whose
`HoldReplies` lets acquisitions reach the backend and withholds their answers
until `DeliverReplies`), and records calls (`RecordingLockProvider`). Leases expire on the supplied `TimeProvider`, so a
lost lease is a clock advance away.
