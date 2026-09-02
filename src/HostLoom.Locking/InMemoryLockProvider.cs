namespace HostLoom.Locking;

/// <summary>
/// A per-process <see cref="ILockProvider"/> with real lease expiry on a <see cref="TimeProvider"/>,
/// owner tokens, extension, and expired-lease takeover, so tests and single-instance deployments
/// exercise the same state machine as a distributed backend. It reports no health probe: there is
/// no infrastructure to reach.
/// </summary>
public sealed class InMemoryLockProvider(TimeProvider? timeProvider = null) : ILockProvider
{
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;
    private readonly Dictionary<string, Lease> _leases = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();

    /// <summary>Leases currently held and unexpired.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                var now = _clock.GetUtcNow();
                var count = 0;
                foreach (var lease in _leases.Values)
                {
                    if (lease.ExpiresAt > now)
                    {
                        count++;
                    }
                }

                return count;
            }
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
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(owner);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var now = _clock.GetUtcNow();
            if (
                !_leases.TryGetValue(key, out var current)
                || current.ExpiresAt <= now
                || string.Equals(current.Owner, owner, StringComparison.Ordinal)
            )
            {
                _leases[key] = new Lease(owner, now + lease);
                return ValueTask.FromResult(true);
            }

            return ValueTask.FromResult(false);
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(owner);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (IsHeldBy(key, owner, out _))
            {
                _leases.Remove(key);
                return ValueTask.FromResult(true);
            }

            return ValueTask.FromResult(false);
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(owner);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (IsHeldBy(key, owner, out var now))
            {
                _leases[key] = new Lease(owner, now + lease);
                return ValueTask.FromResult(true);
            }

            return ValueTask.FromResult(false);
        }
    }

    private bool IsHeldBy(string key, string owner, out DateTimeOffset now)
    {
        now = _clock.GetUtcNow();
        return _leases.TryGetValue(key, out var current)
            && current.ExpiresAt > now
            && string.Equals(current.Owner, owner, StringComparison.Ordinal);
    }

    private readonly record struct Lease(string Owner, DateTimeOffset ExpiresAt);
}
