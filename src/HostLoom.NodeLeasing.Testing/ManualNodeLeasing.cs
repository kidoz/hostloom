namespace HostLoom.NodeLeasing.Testing;

/// <summary>
/// An <see cref="INodeLeasing"/> a test flips by hand: <see cref="Assign"/> gives this instance
/// an ID for a new term, <see cref="Lose"/> and <see cref="Release"/> end it with the matching
/// reason, and listeners and waiters see the same changes an elector would raise.
/// </summary>
public sealed class ManualNodeLeasing(string pool) : INodeLeasing, IDisposable
{
    private readonly Lock _gate = new();
    private readonly HashSet<Action<NodeLeaseChange>> _listeners = [];
    private readonly List<NodeLeaseChange> _changes = [];
    private readonly Queue<(NodeLeaseChange Change, Action<NodeLeaseChange>[] Listeners)> _pending =
        new();
    private bool _publishing;
    private TaskCompletionSource<int> _becameAssigned = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private CancellationTokenSource? _assignmentToken;
    private int? _nodeId;
    private long _term;

    /// <inheritdoc />
    public string Pool { get; } = pool;

    /// <inheritdoc />
    public int? NodeId
    {
        get
        {
            lock (_gate)
            {
                return _nodeId;
            }
        }
    }

    /// <summary>
    /// What <see cref="INodeLeasing.IsCoordinated"/> reports; <see langword="true"/> unless a
    /// test sets it to stand in for an elector over a disabled lock.
    /// </summary>
    public bool IsCoordinated { get; set; } = true;

    /// <inheritdoc />
    public long Term => Interlocked.Read(ref _term);

    /// <inheritdoc />
    public CancellationToken AssignmentToken
    {
        get
        {
            lock (_gate)
            {
                return _assignmentToken?.Token ?? new CancellationToken(canceled: true);
            }
        }
    }

    /// <summary>Every change raised so far, in order.</summary>
    public IReadOnlyList<NodeLeaseChange> Changes
    {
        get
        {
            lock (_gate)
            {
                return [.. _changes];
            }
        }
    }

    /// <summary>Gives this instance <paramref name="id"/> for a new term. Does nothing while assigned.</summary>
    public void Assign(int id)
    {
        TaskCompletionSource<int> signal;
        bool publish;
        lock (_gate)
        {
            if (_assignmentToken is not null)
            {
                return;
            }

            _assignmentToken = new CancellationTokenSource();
            _nodeId = id;
            var term = Interlocked.Increment(ref _term);
            signal = _becameAssigned;
            _becameAssigned = new TaskCompletionSource<int>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            publish = Enqueue(new NodeLeaseChange(Pool, id, term, NodeLeaseChangeReason.Acquired));
        }

        signal.TrySetResult(id);
        if (publish)
            Publish();
    }

    /// <summary>Ends the assignment as a lost lease would: the token cancels and <see cref="NodeLeaseChangeReason.Lost"/> is raised.</summary>
    public void Lose() => End(NodeLeaseChangeReason.Lost);

    /// <summary>Ends the assignment as a release on request.</summary>
    public void Release() => End(NodeLeaseChangeReason.Released);

    /// <inheritdoc />
    public ValueTask<int> WaitForAssignmentAsync(CancellationToken cancellationToken = default)
    {
        Task<int> signal;
        lock (_gate)
        {
            if (_nodeId is { } id)
            {
                return new ValueTask<int>(id);
            }

            signal = _becameAssigned.Task;
        }

        return new ValueTask<int>(signal.WaitAsync(cancellationToken));
    }

    /// <inheritdoc />
    public IDisposable OnChange(Action<NodeLeaseChange> listener)
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
            _assignmentToken?.Dispose();
            _assignmentToken = null;
        }
    }

    private void End(NodeLeaseChangeReason reason)
    {
        CancellationTokenSource token;
        bool publish;
        lock (_gate)
        {
            if (_assignmentToken is null)
                return;
            token = _assignmentToken;
            _assignmentToken = null;
            _nodeId = null;
            publish = Enqueue(new NodeLeaseChange(Pool, null, _term, reason));
        }
        try
        {
            token.Cancel();
        }
        finally
        {
            token.Dispose();
            if (publish)
                Publish();
        }
    }

    // The transition holds _gate. One publisher delivers snapshots in transition order,
    // including changes triggered by cancellation callbacks and listeners.
    private bool Enqueue(NodeLeaseChange change)
    {
        _changes.Add(change);
        _pending.Enqueue((change, [.. _listeners]));
        if (_publishing)
            return false;
        _publishing = true;
        return true;
    }

    private void Publish()
    {
        List<Exception>? failures = null;
        while (true)
        {
            (NodeLeaseChange Change, Action<NodeLeaseChange>[] Listeners) next;
            lock (_gate)
            {
                if (!_pending.TryDequeue(out next))
                {
                    _publishing = false;
                    break;
                }
            }
            foreach (var listener in next.Listeners)
            {
                try
                {
                    listener(next.Change);
                }
                catch (Exception ex)
                {
                    (failures ??= []).Add(ex);
                }
            }
        }
        if (failures is not null)
            throw new AggregateException(failures);
    }

    private sealed class Subscription(ManualNodeLeasing owner, Action<NodeLeaseChange> listener)
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
