using Microsoft.Extensions.Logging;

namespace HostLoom.Locking;

/// <summary>
/// One acquisition sent to the provider: the owner token, the key, the lease and when it was
/// requested, the provider's reply, and the token the provider call runs under. The lock owns that
/// token. It is cancelled only when the lease has run out since the request, which leaves no usable
/// grant, or when the <see cref="DistributedLock"/> is disposed; the caller's token and
/// <see cref="LockOptions.MaxWait"/> end only the lock's wait for the reply, never the call itself.
/// </summary>
/// <remarks>
/// A reply the lock does not turn into a handle is abandoned: the caller cancelled or
/// <see cref="LockOptions.MaxWait"/> expired while it was outstanding, it confirmed a grant after
/// the usable lease had run out, or the provider failed in a way that leaves the grant uncertain.
/// Once the outcome is known, an abandoned attempt that may hold the key gets exactly one bounded,
/// owner-checked release that nobody waits for. A reply that never arrives releases nothing and
/// keeps no timer beyond the lease.
/// </remarks>
internal sealed class AcquisitionAttempt
{
    /// <summary>Upper bound on releasing a lease granted to an abandoned attempt.</summary>
    private static readonly TimeSpan OrphanReleaseBound = TimeSpan.FromSeconds(5);

    private readonly DistributedLock _lock;
    private readonly CancellationTokenSource _providerToken = new();
    private ITimer? _leaseTimer;
    private CancellationTokenRegistration _lockDisposed;
    private Task _settled = Task.CompletedTask;
    private int _abandoned;

    /// <summary>Sends the acquisition at once.</summary>
    /// <param name="owner">The lock the attempt belongs to; its clock, provider, and logger are used.</param>
    /// <param name="key">The consumer key, for logs.</param>
    /// <param name="prefixedKey">The key the provider sees.</param>
    /// <param name="ownerToken">The owner token generated for this acquisition.</param>
    /// <param name="lease">The lease requested, already capped.</param>
    /// <param name="lockDisposed">Cancelled when <paramref name="owner"/> is disposed.</param>
    public AcquisitionAttempt(
        DistributedLock owner,
        string key,
        string prefixedKey,
        string ownerToken,
        TimeSpan lease,
        CancellationToken lockDisposed
    )
    {
        _lock = owner;
        Key = key;
        PrefixedKey = prefixedKey;
        OwnerToken = ownerToken;
        Deadline = LeaseDeadline.StartingNow(owner.Clock, lease);
        Reply = Send(lockDisposed);
    }

    public string Key { get; }

    public string PrefixedKey { get; }

    public string OwnerToken { get; }

    /// <summary>The lease requested and when; the grant is usable only while it has time left.</summary>
    public LeaseDeadline Deadline { get; }

    /// <summary>The provider's reply: granted, refused, or the provider's exception.</summary>
    public Task<bool> Reply { get; }

    /// <summary>
    /// The lock will not use this reply. Whatever the provider may have granted this owner is
    /// released once, on the thread pool, as soon as the reply shows the key could be held:
    /// a confirmed grant, a provider that gave up at the lease end or on disposal, or a
    /// <see cref="LockProviderException"/> of kind <see cref="LockFailureKind.Timeout"/> or
    /// <see cref="LockFailureKind.Other"/>. <see cref="LockFailureKind.Unavailable"/> means the
    /// backend was not reached, and a refusal holds nothing, so neither is released. Repeated
    /// calls do nothing; the caller never waits for the release.
    /// </summary>
    public void Abandon()
    {
        if (Interlocked.Exchange(ref _abandoned, 1) == 0)
        {
            _ = ReleaseWhenSettledAsync();
        }
    }

    /// <summary>
    /// Whether a finished reply leaves a grant the backend may hold for this owner. A provider
    /// that throws <see cref="LockFailureKind.Timeout"/> or <see cref="LockFailureKind.Other"/>, or
    /// stops on its token, may have had its command applied before, or after, it gave up.
    /// </summary>
    private static bool MayHold(Task<bool> reply) =>
        reply.Status switch
        {
            TaskStatus.RanToCompletion => reply.Result,
            TaskStatus.Canceled => true,
            _ => reply.Exception?.InnerException
                is not LockProviderException { Kind: LockFailureKind.Unavailable },
        };

    private Task<bool> Send(CancellationToken lockDisposed)
    {
        Task<bool> reply;
        try
        {
            reply = _lock
                .Provider.TryAcquireAsync(
                    PrefixedKey,
                    OwnerToken,
                    Deadline.Lease,
                    _providerToken.Token
                )
                .AsTask();
        }
        catch (Exception exception)
        {
            // A provider that throws before returning a task failed like one whose task faults.
            reply = Task.FromException<bool>(exception);
        }

        if (reply.IsCompleted)
        {
            // Answered synchronously: nothing to bound, so no timer is ever armed.
            _providerToken.Dispose();
            return reply;
        }

        _lockDisposed = lockDisposed.UnsafeRegister(
            static state => ((AcquisitionAttempt)state!).StopProvider(),
            this
        );
        var remaining = Deadline.Remaining(_lock.Clock);
        if (remaining > TimeSpan.Zero)
        {
            _leaseTimer = _lock.Clock.CreateTimer(
                static state => ((AcquisitionAttempt)state!).StopProvider(),
                this,
                remaining,
                Timeout.InfiniteTimeSpan
            );
        }
        else
        {
            StopProvider();
        }

        // Started last, so the cleanup sees the timer and the registration it has to undo.
        _settled = SettleAsync(reply);
        return reply;
    }

    /// <summary>
    /// Ends the provider call: the lease ran out, so no grant it could still deliver is usable,
    /// or the lock was disposed. Runs on a timer or a cancellation callback, so it never throws.
    /// </summary>
    private void StopProvider()
    {
        try
        {
            _providerToken.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The reply arrived first and the attempt was settled: nothing is left to stop.
        }
        catch (AggregateException exception)
        {
            _lock.Logger.LogWarning(
                LockingEvents.CancellationCallbackFailed,
                exception,
                "A provider cancellation callback for lock '{Key}' threw while its acquisition was stopped; the lock continues.",
                Key
            );
        }
    }

    /// <summary>Drops the lease timer and the disposal registration once the reply is in.</summary>
    private async Task SettleAsync(Task<bool> reply)
    {
        // Yield first, so the cleanup never runs inside the cancellation that completed the reply.
        await ((Task)reply).ConfigureAwait(
            ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ForceYielding
        );
        _leaseTimer?.Dispose();
        await _lockDisposed.DisposeAsync().ConfigureAwait(false);
        _providerToken.Dispose();
    }

    private async Task ReleaseWhenSettledAsync()
    {
        // After the cleanup, so a released orphan leaves no timer behind; and always yielding, so
        // the caller that abandons the attempt never runs the release inline.
        await _settled.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
        if (MayHold(Reply))
        {
            await ReleaseOrphanAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// One owner-checked release for a lease the backend may hold for an attempt nobody uses,
    /// bounded by the shorter of the lease and <see cref="OrphanReleaseBound"/> so it can never
    /// outlive what it releases. The release script only touches this owner's lease, so a
    /// successor that took the key after expiry is never affected. Never throws.
    /// </summary>
    private async Task ReleaseOrphanAsync()
    {
        var lease = Deadline.Lease;
        var bound = lease < OrphanReleaseBound ? lease : OrphanReleaseBound;
        string outcome;
        Exception? failure = null;
        try
        {
            using var timeout = new CancellationTokenSource(bound, _lock.Clock);
            var released = await _lock
                .Provider.ReleaseAsync(PrefixedKey, OwnerToken, timeout.Token)
                .ConfigureAwait(false);
            outcome = released ? "released" : "absent";
        }
        catch (Exception exception)
        {
            outcome = "failed";
            failure = exception;
        }

        if (_lock.Logger.IsEnabled(LogLevel.Debug))
        {
            _lock.Logger.LogDebug(
                LockingEvents.OrphanRelease,
                failure,
                "Lock '{Key}' may have been granted to an acquisition nobody uses; the best-effort release reported {Outcome}.",
                Key,
                outcome
            );
        }

        LockingDiagnostics.OrphanReleases.Add(
            1,
            _lock.NamespaceTag,
            new KeyValuePair<string, object?>(LockingDiagnostics.OutcomeTag, outcome)
        );
    }
}
