namespace HostLoom.Locking.Testing;

/// <summary>
/// Decorates a lock provider so a scenario can make the next <c>n</c> calls, or every call, fail
/// with a chosen <see cref="LockFailureKind"/>, or hold acquisition replies back, which is how the
/// failure matrix is driven without a backend.
/// </summary>
/// <remarks>
/// <see cref="HoldReplies"/> models a reply that arrives after the caller stopped waiting: each
/// acquisition still reaches the inner provider, which applies it, and only its answer is withheld
/// until <see cref="DeliverReplies"/> or <see cref="Heal"/>. While held, the call honours its token
/// as any provider does, so a lock that cancelled the provider call on the caller's behalf would
/// lose the grant, and one that did not sees it arrive and can release it.
/// </remarks>
public sealed class FaultingLockProvider(ILockProvider inner) : ILockProvider
{
    private readonly Lock _gate = new();
    private LockFailureKind _kind;
    private int _remaining;
    private bool _all;
    private TaskCompletionSource? _replies;
    private TaskCompletionSource _replyHeld = NewSignal();

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

    /// <summary>
    /// Holds the reply of every acquisition from now on: the inner provider still applies it, and
    /// its answer reaches the caller only at <see cref="DeliverReplies"/> or <see cref="Heal"/>,
    /// unless the call's token is cancelled first. Release and extension are not held.
    /// </summary>
    public void HoldReplies()
    {
        lock (_gate)
        {
            _replies ??= NewSignal();
            if (_replyHeld.Task.IsCompleted)
            {
                _replyHeld = NewSignal();
            }
        }
    }

    /// <summary>Delivers every held acquisition reply and stops holding new ones.</summary>
    public void DeliverReplies()
    {
        TaskCompletionSource? replies;
        lock (_gate)
        {
            replies = _replies;
            _replies = null;
        }

        replies?.TrySetResult();
    }

    /// <summary>
    /// Completes once an acquisition reply is being held since the last <see cref="HoldReplies"/>,
    /// so a scenario knows the inner provider decided the grant before it stops waiting.
    /// </summary>
    public Task WaitForHeldReplyAsync(CancellationToken cancellationToken = default)
    {
        Task held;
        lock (_gate)
        {
            held = _replyHeld.Task;
        }

        return held.WaitAsync(cancellationToken);
    }

    /// <summary>Stops failing calls and delivers every held reply.</summary>
    public void Heal()
    {
        lock (_gate)
        {
            _all = false;
            _remaining = 0;
        }

        DeliverReplies();
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        Gate("acquire");
        var acquired = await Inner
            .TryAcquireAsync(key, owner, lease, cancellationToken)
            .ConfigureAwait(false);
        Task? replies;
        TaskCompletionSource held;
        lock (_gate)
        {
            replies = _replies?.Task;
            held = _replyHeld;
        }

        if (replies is not null)
        {
            held.TrySetResult();
            await replies.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        return acquired;
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

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

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
