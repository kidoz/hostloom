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
    private readonly Lock _gate = new();
    private readonly long _acquiredAt;
    private readonly ITimer _leaseTimer;
    private readonly ITimer _warnTimer;
    private readonly ITimer? _extendTimer;
    private TimeSpan _lease;
    private DateTimeOffset _leaseEnd;
    private int _state;
    private bool _counted = true;

    /// <param name="requestedAt">
    /// The timestamp taken before the acquiring provider call, which is the earliest instant the
    /// backend can have started the lease.
    /// </param>
    public LockHandle(
        DistributedLock owner,
        string key,
        string prefixedKey,
        string token,
        TimeSpan lease,
        long requestedAt,
        bool autoExtend
    )
    {
        _owner = owner;
        Key = key;
        _prefixedKey = prefixedKey;
        _token = token;
        _lease = lease;
        _autoExtend = autoExtend;
        _acquiredAt = requestedAt;
        var remaining = lease - owner.Clock.GetElapsedTime(requestedAt);
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

    public CancellationToken LostToken => _lost.Token;

    public TimeSpan HoldDuration => _owner.Clock.GetElapsedTime(_acquiredAt);

    public async ValueTask<bool> ExtendAsync(
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        if (!IsHeld)
        {
            return false;
        }

        if (lease > _owner.Options.MaxLease)
        {
            lease = _owner.Options.MaxLease;
        }

        bool extended;
        var requestedAt = _owner.Clock.GetTimestamp();
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
        var remaining = lease - _owner.Clock.GetElapsedTime(requestedAt);
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
        }

        _leaseTimer.Dispose();
        _warnTimer.Dispose();
        _extendTimer?.Dispose();

        // The caller's token is deliberately not used: a cancelled action must still release.
        var released = false;
        try
        {
            released = await _owner
                .Provider.ReleaseAsync(_prefixedKey, _token, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _owner.Logger.LogWarning(
                LockingEvents.ReleaseFailed,
                exception,
                "Releasing lock '{Key}' failed; the lease expires on its own at {LeaseEnd}.",
                Key,
                LeaseEnd
            );
        }

        if (wasHeld && !released)
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
        try
        {
            await ExtendAsync(_lease, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _owner.Logger.LogWarning(
                LockingEvents.ExtendFailed,
                exception,
                "The heartbeat for lock '{Key}' failed.",
                Key
            );
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
