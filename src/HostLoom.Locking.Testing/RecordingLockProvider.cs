namespace HostLoom.Locking.Testing;

/// <summary>One call a <see cref="RecordingLockProvider"/> saw.</summary>
/// <param name="Operation"><c>acquire</c>, <c>release</c>, or <c>extend</c>.</param>
/// <param name="Key">The fully prefixed key.</param>
/// <param name="Owner">The owner token the composed lock generated.</param>
/// <param name="Result">What the inner provider answered.</param>
public readonly record struct RecordedLockCall(
    string Operation,
    string Key,
    string Owner,
    bool Result
);

/// <summary>
/// Decorates a provider and records every call with its outcome, so a test asserts that a
/// contended acquisition retried the expected number of times, that release used the owner
/// token, or that an extension happened before the lease ended.
/// </summary>
public sealed class RecordingLockProvider(ILockProvider inner) : ILockProvider
{
    private readonly Lock _gate = new();
    private readonly List<RecordedLockCall> _calls = [];

    /// <summary>The wrapped provider.</summary>
    public ILockProvider Inner { get; } = inner;

    /// <summary>Every call so far, in order.</summary>
    public IReadOnlyList<RecordedLockCall> Calls
    {
        get
        {
            lock (_gate)
            {
                return [.. _calls];
            }
        }
    }

    /// <summary>Forgets the calls recorded so far.</summary>
    public void Reset()
    {
        lock (_gate)
        {
            _calls.Clear();
        }
    }

    /// <summary>How many calls named <paramref name="operation"/> were recorded.</summary>
    public int Count(string operation)
    {
        lock (_gate)
        {
            return _calls.Count(call => call.Operation == operation);
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        var result = await Inner
            .TryAcquireAsync(key, owner, lease, cancellationToken)
            .ConfigureAwait(false);
        Record("acquire", key, owner, result);
        return result;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    )
    {
        var result = await Inner.ReleaseAsync(key, owner, cancellationToken).ConfigureAwait(false);
        Record("release", key, owner, result);
        return result;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        var result = await Inner
            .ExtendAsync(key, owner, lease, cancellationToken)
            .ConfigureAwait(false);
        Record("extend", key, owner, result);
        return result;
    }

    private void Record(string operation, string key, string owner, bool result)
    {
        lock (_gate)
        {
            _calls.Add(new RecordedLockCall(operation, key, owner, result));
        }
    }
}
