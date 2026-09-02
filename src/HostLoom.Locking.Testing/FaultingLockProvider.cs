namespace HostLoom.Locking.Testing;

/// <summary>
/// Decorates a lock provider so a scenario can make the next <c>n</c> calls, or every call, fail
/// with a chosen <see cref="LockFailureKind"/>, which is how the failure matrix is driven without
/// a backend.
/// </summary>
public sealed class FaultingLockProvider(ILockProvider inner) : ILockProvider
{
    private readonly Lock _gate = new();
    private LockFailureKind _kind;
    private int _remaining;
    private bool _all;

    /// <summary>The wrapped provider.</summary>
    public ILockProvider Inner { get; } = inner;

    /// <summary>Calls that reached the inner provider.</summary>
    public int Calls { get; private set; }

    /// <summary>Calls that were failed by this decorator.</summary>
    public int Faulted { get; private set; }

    /// <summary>Fails the next <paramref name="count"/> calls with <paramref name="kind"/>.</summary>
    public void Fail(LockFailureKind kind, int count)
    {
        lock (_gate)
        {
            _kind = kind;
            _remaining = count;
            _all = false;
        }
    }

    /// <summary>Fails every call with <paramref name="kind"/> until <see cref="Heal"/>.</summary>
    public void FailAll(LockFailureKind kind)
    {
        lock (_gate)
        {
            _kind = kind;
            _all = true;
        }
    }

    /// <summary>Stops failing calls.</summary>
    public void Heal()
    {
        lock (_gate)
        {
            _all = false;
            _remaining = 0;
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        Gate("acquire");
        return Inner.TryAcquireAsync(key, owner, lease, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    )
    {
        Gate("release");
        return Inner.ReleaseAsync(key, owner, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        Gate("extend");
        return Inner.ExtendAsync(key, owner, lease, cancellationToken);
    }

    private void Gate(string operation)
    {
        lock (_gate)
        {
            if (_all || _remaining > 0)
            {
                if (!_all)
                {
                    _remaining--;
                }

                Faulted++;
                throw new LockProviderException(
                    _kind,
                    $"Injected {_kind} failure during {operation}."
                );
            }

            Calls++;
        }
    }
}
