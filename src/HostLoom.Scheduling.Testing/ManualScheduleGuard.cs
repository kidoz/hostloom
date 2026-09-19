namespace HostLoom.Scheduling.Testing;

/// <summary>
/// An in-process <see cref="IScheduleGuard"/> a test can script: hold a schedule as if another
/// instance ran it, release it, lose an active claim, make the next claim throw, and see every
/// claim that was made. Claims are exclusive within the process, as a real guard's are within
/// the cluster.
/// </summary>
public sealed class ManualScheduleGuard : IScheduleGuard
{
    private readonly Lock _gate = new();
    private readonly HashSet<string> _heldByTest = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Claim> _active = new(StringComparer.Ordinal);
    private readonly List<ScheduleClaimRecord> _claims = [];
    private Exception? _nextFailure;

    /// <summary>Every claim attempt, in order, with whether it was granted.</summary>
    public IReadOnlyList<ScheduleClaimRecord> Claims
    {
        get
        {
            lock (_gate)
            {
                return [.. _claims];
            }
        }
    }

    /// <summary>Schedules whose claim is active right now.</summary>
    public IReadOnlyCollection<string> Active
    {
        get
        {
            lock (_gate)
            {
                return [.. _active.Keys];
            }
        }
    }

    /// <summary>Holds <paramref name="schedule"/> as if another instance ran it, until <see cref="Release"/>.</summary>
    /// <returns><see langword="false"/> when the schedule is already held or claimed.</returns>
    public bool Hold(string schedule)
    {
        lock (_gate)
        {
            return !_active.ContainsKey(schedule) && _heldByTest.Add(schedule);
        }
    }

    /// <summary>Releases a schedule held through <see cref="Hold"/>.</summary>
    public bool Release(string schedule)
    {
        lock (_gate)
        {
            return _heldByTest.Remove(schedule);
        }
    }

    /// <summary>Cancels the lost token of the active claim on <paramref name="schedule"/>, as a lost lease would.</summary>
    /// <returns><see langword="false"/> when no claim is active.</returns>
    public bool Lose(string schedule)
    {
        Claim? claim;
        lock (_gate)
        {
            _ = _active.TryGetValue(schedule, out claim);
        }

        if (claim is null)
        {
            return false;
        }

        claim.MarkLost();
        return true;
    }

    /// <summary>Makes the next <see cref="TryClaimAsync"/> throw <paramref name="failure"/>, as an unreachable backend would.</summary>
    public void FailNext(Exception failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        lock (_gate)
        {
            _nextFailure = failure;
        }
    }

    /// <inheritdoc />
    public ValueTask<IScheduleClaim?> TryClaimAsync(
        string schedule,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(schedule);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_nextFailure is { } failure)
            {
                _nextFailure = null;
                _claims.Add(new ScheduleClaimRecord(schedule, lease, false));
                throw failure;
            }

            if (_heldByTest.Contains(schedule) || _active.ContainsKey(schedule))
            {
                _claims.Add(new ScheduleClaimRecord(schedule, lease, false));
                return ValueTask.FromResult<IScheduleClaim?>(null);
            }

            var claim = new Claim(this, schedule);
            _active[schedule] = claim;
            _claims.Add(new ScheduleClaimRecord(schedule, lease, true));
            return ValueTask.FromResult<IScheduleClaim?>(claim);
        }
    }

    private void Remove(Claim claim)
    {
        lock (_gate)
        {
            if (
                _active.TryGetValue(claim.Schedule, out var active)
                && ReferenceEquals(active, claim)
            )
            {
                _active.Remove(claim.Schedule);
            }
        }
    }

    private sealed class Claim(ManualScheduleGuard guard, string schedule) : IScheduleClaim
    {
        private readonly CancellationTokenSource _lost = new();
        private bool _disposed;

        public string Schedule { get; } = schedule;

        public bool IsHeld => !_disposed && !_lost.IsCancellationRequested;

        public CancellationToken LostToken => _lost.Token;

        public void MarkLost()
        {
            if (!_disposed)
            {
                _lost.Cancel();
            }
        }

        public ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                guard.Remove(this);
                _lost.Dispose();
            }

            return ValueTask.CompletedTask;
        }
    }
}

/// <summary>One claim attempt.</summary>
/// <param name="Schedule">The schedule claimed.</param>
/// <param name="Lease">The lease requested.</param>
/// <param name="Granted">Whether the claim was granted.</param>
public sealed record ScheduleClaimRecord(string Schedule, TimeSpan Lease, bool Granted);
