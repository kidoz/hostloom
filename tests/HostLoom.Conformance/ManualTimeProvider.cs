namespace HostLoom.Conformance;

/// <summary>
/// Deterministic <see cref="TimeProvider"/>: timestamps, the wall clock, and timers move only
/// when a scenario calls <see cref="Advance"/>, so expiry and lease behaviour need no waiting.
/// </summary>
public sealed class ManualTimeProvider : TimeProvider
{
    private readonly Lock _gate = new();
    private readonly List<ManualTimer> _timers = [];
    private long _timestampTicks;
    private DateTimeOffset _now = DateTimeOffset.UnixEpoch;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp()
    {
        lock (_gate)
        {
            return _timestampTicks;
        }
    }

    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate)
        {
            return _now;
        }
    }

    public override ITimer CreateTimer(
        TimerCallback callback,
        object? state,
        TimeSpan dueTime,
        TimeSpan period
    )
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new ManualTimer(this, callback, state);
        timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>Moves the clock forward and fires every timer that became due.</summary>
    public void Advance(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        List<ManualTimer> due;
        lock (_gate)
        {
            _timestampTicks += delta.Ticks;
            _now += delta;
            due = _timers.Where(timer => timer.IsDue(_now)).ToList();
        }

        // Fired outside the gate: a callback may cancel tokens whose continuations re-enter the clock.
        foreach (var timer in due)
        {
            timer.Fire();
        }
    }

    private void Register(ManualTimer timer)
    {
        lock (_gate)
        {
            if (!_timers.Contains(timer))
            {
                _timers.Add(timer);
            }
        }
    }

    private void Remove(ManualTimer timer)
    {
        lock (_gate)
        {
            _timers.Remove(timer);
        }
    }

    private sealed class ManualTimer(
        ManualTimeProvider clock,
        TimerCallback callback,
        object? state
    ) : ITimer
    {
        private DateTimeOffset? _due;
        private TimeSpan _period = Timeout.InfiniteTimeSpan;

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            _period = period;
            if (dueTime == Timeout.InfiniteTimeSpan)
            {
                _due = null;
                clock.Remove(this);
                return true;
            }

            if (dueTime == TimeSpan.Zero)
            {
                _due = null;
                callback(state);
                Reschedule();
                return true;
            }

            _due = clock.GetUtcNow() + dueTime;
            clock.Register(this);
            return true;
        }

        public bool IsDue(DateTimeOffset now) => _due is { } due && now >= due;

        public void Fire()
        {
            _due = null;
            clock.Remove(this);
            callback(state);
            Reschedule();
        }

        public void Dispose()
        {
            _due = null;
            _period = Timeout.InfiniteTimeSpan;
            clock.Remove(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        private void Reschedule()
        {
            if (_period > TimeSpan.Zero && _period != Timeout.InfiniteTimeSpan)
            {
                _due = clock.GetUtcNow() + _period;
                clock.Register(this);
            }
        }
    }
}
