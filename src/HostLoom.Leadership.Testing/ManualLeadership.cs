namespace HostLoom.Leadership.Testing;

/// <summary>
/// An <see cref="ILeadership"/> a test flips by hand: <see cref="Acquire"/> makes this instance
/// leader for a new term, <see cref="Lose"/> and <see cref="Resign"/> end it with the matching
/// reason, and listeners and waiters see the same changes an elector would raise.
/// </summary>
public sealed class ManualLeadership(string role) : ILeadership, IDisposable
{
    private readonly Lock _gate = new();
    private readonly HashSet<Action<LeadershipChange>> _listeners = [];
    private readonly List<LeadershipChange> _changes = [];
    private TaskCompletionSource _becameLeader = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private CancellationTokenSource? _leaderToken;
    private long _term;

    /// <inheritdoc />
    public string Role { get; } = role;

    /// <inheritdoc />
    public bool IsLeader
    {
        get
        {
            lock (_gate)
            {
                return _leaderToken is not null;
            }
        }
    }

    /// <inheritdoc />
    public long Term => Interlocked.Read(ref _term);

    /// <inheritdoc />
    public CancellationToken LeadershipToken
    {
        get
        {
            lock (_gate)
            {
                return _leaderToken?.Token ?? new CancellationToken(canceled: true);
            }
        }
    }

    /// <summary>Every change raised so far, in order.</summary>
    public IReadOnlyList<LeadershipChange> Changes
    {
        get
        {
            lock (_gate)
            {
                return [.. _changes];
            }
        }
    }

    /// <summary>Makes this instance leader for a new term. Does nothing while already leading.</summary>
    public void Acquire()
    {
        TaskCompletionSource signal;
        long term;
        lock (_gate)
        {
            if (_leaderToken is not null)
            {
                return;
            }

            _leaderToken = new CancellationTokenSource();
            term = Interlocked.Increment(ref _term);
            signal = _becameLeader;
            _becameLeader = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
        }

        signal.TrySetResult();
        Raise(new LeadershipChange(Role, true, term, LeadershipChangeReason.Acquired));
    }

    /// <summary>Ends leadership as a lost lease would: the token cancels and <see cref="LeadershipChangeReason.Lost"/> is raised.</summary>
    public void Lose() => End(LeadershipChangeReason.Lost);

    /// <summary>Ends leadership as a resignation.</summary>
    public void Resign() => End(LeadershipChangeReason.Resigned);

    /// <inheritdoc />
    public ValueTask WaitForLeadershipAsync(CancellationToken cancellationToken = default)
    {
        Task signal;
        lock (_gate)
        {
            if (_leaderToken is not null)
            {
                return ValueTask.CompletedTask;
            }

            signal = _becameLeader.Task;
        }

        return new ValueTask(signal.WaitAsync(cancellationToken));
    }

    /// <inheritdoc />
    public IDisposable OnChange(Action<LeadershipChange> listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        lock (_gate)
        {
            _listeners.Add(listener);
        }

        return new Subscription(this, listener);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            _leaderToken?.Dispose();
            _leaderToken = null;
        }
    }

    private void End(LeadershipChangeReason reason)
    {
        CancellationTokenSource? token;
        lock (_gate)
        {
            token = _leaderToken;
            _leaderToken = null;
        }

        if (token is null)
        {
            return;
        }

        token.Cancel();
        token.Dispose();
        Raise(new LeadershipChange(Role, false, Term, reason));
    }

    private void Raise(LeadershipChange change)
    {
        Action<LeadershipChange>[] listeners;
        lock (_gate)
        {
            _changes.Add(change);
            listeners = [.. _listeners];
        }

        foreach (var listener in listeners)
        {
            listener(change);
        }
    }

    private sealed class Subscription(ManualLeadership owner, Action<LeadershipChange> listener)
        : IDisposable
    {
        public void Dispose()
        {
            lock (owner._gate)
            {
                owner._listeners.Remove(listener);
            }
        }
    }
}
