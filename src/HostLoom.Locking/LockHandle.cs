using Microsoft.Extensions.Logging;

namespace HostLoom.Locking;

/// <summary>
/// One acquired lease and its timers: expiry on the local clock, the 80 % hold warning, and the
/// optional heartbeat. State moves Held → Lost or Held → Released once; every transition out of
/// Held decrements <c>hostloom.lock.active</c> exactly once.
/// </summary>
/// <remarks>
/// A provider starts the lease when it accepts the request, so the local timers run from the
/// moment the request was sent rather than the moment its answer arrived. The round trip is spent
/// lease; counting it out is what keeps <see cref="IsHeld"/> from outliving the backend's key.
/// </remarks>
internal sealed class LockHandle : ILockHandle
{
    private const int HeldState = 0;
    private const int LostState = 1;
    private const int ReleasedState = 2;

    private readonly DistributedLock _owner;
    private readonly string _prefixedKey;
    private readonly string _token;
    private readonly bool _autoExtend;
    private readonly CancellationTokenSource _lost = new();
    private readonly CancellationToken _lostToken;
    private readonly Lock _gate = new();
    private readonly SemaphoreSlim _renewalGate = new(1, 1);
    private int _renewalUsers;
    private readonly long _acquiredAt;
    private readonly ITimer _leaseTimer;
    private readonly ITimer _warnTimer;
    private readonly ITimer? _extendTimer;
    private TimeSpan _lease;
    private DateTimeOffset _leaseEnd;
    private int _state;
    private bool _counted = true;

    /// <param name="deadline">
    /// The lease the acquiring provider call asked for and the timestamp taken before it, which is
    /// the earliest instant the backend can have started the lease.
    /// </param>
    public LockHandle(
        DistributedLock owner,
        string key,
        string prefixedKey,
        string token,
        LeaseDeadline deadline,
        bool autoExtend
    )
    {
        _owner = owner;
        Key = key;
        _prefixedKey = prefixedKey;
        _token = token;
        _lease = deadline.Lease;
        _autoExtend = autoExtend;
        _acquiredAt = deadline.RequestedAt;
        // Read once: the source is disposed with the handle, and the token stays readable after.
        _lostToken = _lost.Token;
        var remaining = deadline.Remaining(owner.Clock);
        _leaseEnd = owner.Clock.GetUtcNow() + remaining;

        // Timers last, so a lease short enough to fire immediately still finds a complete handle.
        // A round trip that outlasted the lease leaves nothing to hold: the expiry timer fires at
        // once and the handle reports the loss through the usual path.
        _leaseTimer = owner.Clock.CreateTimer(
            static state => ((LockHandle)state!).OnLeaseExpired(),
            this,
            NotBefore(remaining),
            Timeout.InfiniteTimeSpan
        );
        _warnTimer = owner.Clock.CreateTimer(
            static state => ((LockHandle)state!).OnHoldThreshold(),
            this,
            NotBefore(remaining * 0.8),
            Timeout.InfiniteTimeSpan
        );
        _extendTimer = autoExtend
            ? owner.Clock.CreateTimer(
                static state => ((LockHandle)state!).OnHeartbeat(),
                this,
                NotBefore(remaining / 2),
                Timeout.InfiniteTimeSpan
            )
            : null;
    }

    public string Key { get; }

    public bool IsCoordinated => true;

    public string PrefixedKey => _prefixedKey;

    public bool IsHeld => Volatile.Read(ref _state) == HeldState;

    public DateTimeOffset LeaseEnd
    {
        get
        {
            lock (_gate)
            {
                return _leaseEnd;
            }
        }
    }

    public CancellationToken LostToken => _lostToken;

    public TimeSpan HoldDuration => _owner.Clock.GetElapsedTime(_acquiredAt);

    public async ValueTask<bool> ExtendAsync(
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        lock (_gate)
        {
            if (_state != HeldState)
            {
                return false;
            }

            // Include queued callers so disposal never destroys a semaphore still in use.
            _renewalUsers++;
        }

        try
        {
            await _renewalGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                // Serialize the backend command AND its local deadline update. An older
                // reply must not overwrite a later, shorter lease, including heartbeats.
                return await ExtendCoreAsync(lease, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _renewalGate.Release();
            }
        }
        finally
        {
            lock (_gate)
            {
                if (--_renewalUsers == 0 && _state == ReleasedState)
                {
                    _renewalGate.Dispose();
                }
            }
        }
    }

    private async ValueTask<bool> ExtendCoreAsync(
        TimeSpan lease,
        CancellationToken cancellationToken
    )
    {
        // Ownership can be lost or disposed while this caller waits for another renewal.
        if (!IsHeld)
        {
            return false;
        }

        if (lease > _owner.Options.MaxLease)
        {
            lease = _owner.Options.MaxLease;
        }

        bool extended;
        var deadline = LeaseDeadline.StartingNow(_owner.Clock, lease);
        try
        {
            extended = await _owner
                .Provider.ExtendAsync(_prefixedKey, _token, lease, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            _owner.Logger.LogWarning(
                LockingEvents.ExtendFailed,
                exception,
                "Extending lock '{Key}' failed; the lease keeps its previous end.",
                Key
            );
            return false;
        }

        if (!extended)
        {
            MarkLost("the provider reported an owner mismatch on extend");
            return false;
        }

        // The extended lease also started when the request was accepted, so a round trip longer
        // than the lease itself leaves nothing extended.
        var remaining = deadline.Remaining(_owner.Clock);
        if (remaining <= TimeSpan.Zero)
        {
            MarkLost("the extended lease was already spent when the provider answered");
            return false;
        }

        lock (_gate)
        {
            if (_state != HeldState)
            {
                return false;
            }

            _lease = lease;
            _leaseEnd = _owner.Clock.GetUtcNow() + remaining;
            _leaseTimer.Change(remaining, Timeout.InfiniteTimeSpan);
            _warnTimer.Change(remaining * 0.8, Timeout.InfiniteTimeSpan);
            _extendTimer?.Change(remaining / 2, Timeout.InfiniteTimeSpan);
        }

        return true;
    }

    public async ValueTask DisposeAsync()
    {
        bool wasHeld;
        lock (_gate)
        {
            if (_state == ReleasedState)
            {
                return;
            }

            wasHeld = _state == HeldState;
            _state = ReleasedState;
            if (_renewalUsers == 0)
            {
                _renewalGate.Dispose();
            }
        }

        // The state moved under the gate first, so a heartbeat or extension that checks it
        // under the same gate can no longer re-arm a timer disposed here.
        _leaseTimer.Dispose();
        _warnTimer.Dispose();
        _extendTimer?.Dispose();

        // The caller's token is deliberately not used: a cancelled action must still release.
        var released = false;
        var failed = false;
        try
        {
            released = await _owner
                .Provider.ReleaseAsync(_prefixedKey, _token, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            // Not a loss: the backend still holds this owner's lease until it expires, so
            // exclusivity was kept for as long as the handle promised it.
            failed = true;
            _owner.Logger.LogWarning(
                LockingEvents.ReleaseFailed,
                exception,
                "Releasing lock '{Key}' failed; the lease expires on its own at {LeaseEnd}.",
                Key,
                LeaseEnd
            );
        }

        if (wasHeld && !released && !failed)
        {
            ReportLost("the provider reported an owner mismatch on release");
        }

        LockingDiagnostics.HoldDuration.Record(HoldDuration.TotalSeconds, _owner.NamespaceTag);
        Decrement();
        _lost.Dispose();
    }

    private static TimeSpan NotBefore(TimeSpan due) => due > TimeSpan.Zero ? due : TimeSpan.Zero;

    private void OnLeaseExpired() => MarkLost("the lease expired on the local clock");

    private void OnHoldThreshold()
    {
        // Also reported for a lease already marked lost: the hold really did pass the threshold,
        // and the two timers may fire in either order when the clock jumps past both.
        if (Volatile.Read(ref _state) != ReleasedState)
        {
            _owner.Logger.LogWarning(
                LockingEvents.HoldThreshold,
                "Lock '{Key}' has been held for 80 % of its {Lease} lease and has not been released or extended.",
                Key,
                _lease
            );
        }
    }

    private void OnHeartbeat()
    {
        if (!IsHeld)
        {
            return;
        }

        if (HoldDuration >= _owner.Options.MaxHold)
        {
            if (_owner.Logger.IsEnabled(LogLevel.Information))
            {
                _owner.Logger.LogInformation(
                    LockingEvents.AutoExtendStopped,
                    "Lock '{Key}' reached Locking:MaxHold ({MaxHold}); automatic extension stops and the lease expires at {LeaseEnd}.",
                    Key,
                    _owner.Options.MaxHold,
                    LeaseEnd
                );
            }

            return;
        }

        _ = HeartbeatAsync();
    }

    private async Task HeartbeatAsync()
    {
        bool extended;
        try
        {
            extended = await ExtendAsync(_lease, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            extended = false;
            _owner.Logger.LogWarning(
                LockingEvents.ExtendFailed,
                exception,
                "The heartbeat for lock '{Key}' failed.",
                Key
            );
        }

        if (!extended)
        {
            RetryHeartbeat();
        }
    }

    /// <summary>
    /// A failed heartbeat keeps the previous lease end, and a successful one re-arms the timer
    /// itself; without this the first backend hiccup would end automatic extension for good.
    /// The next attempt is due halfway to the lease end, then halfway again, down to a floor of
    /// one twentieth of the lease, until an extension succeeds or the lease runs out.
    /// </summary>
    private void RetryHeartbeat()
    {
        lock (_gate)
        {
            if (_state != HeldState || _extendTimer is null)
            {
                return;
            }

            var remaining = _leaseEnd - _owner.Clock.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return;
            }

            var due = remaining / 2;
            var floor = _lease / 20;
            if (due < floor)
            {
                due = floor < remaining ? floor : remaining;
            }

            _extendTimer.Change(due, Timeout.InfiniteTimeSpan);
        }
    }

    private void MarkLost(string reason)
    {
        lock (_gate)
        {
            if (_state != HeldState)
            {
                return;
            }

            _state = LostState;
        }

        ReportLost(reason);
        Decrement();
    }

    private void ReportLost(string reason)
    {
        LockingDiagnostics.Lost.Add(1, _owner.NamespaceTag);
        _owner.Logger.LogWarning(
            LockingEvents.LeaseLost,
            "Lock '{Key}' was lost: {Reason}. Exclusivity is no longer guaranteed.",
            Key,
            reason
        );
        try
        {
            _lost.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Lost after release: nothing is waiting on the token any more.
        }
        catch (AggregateException exception)
        {
            _owner.Logger.LogWarning(
                LockingEvents.CancellationCallbackFailed,
                exception,
                "A cancellation callback for lost lock '{Key}' failed; lock cleanup continues.",
                Key
            );
        }
    }

    private void Decrement()
    {
        lock (_gate)
        {
            if (!_counted)
            {
                return;
            }

            _counted = false;
        }

        LockingDiagnostics.Active.Add(-1, _owner.NamespaceTag);
    }
}
