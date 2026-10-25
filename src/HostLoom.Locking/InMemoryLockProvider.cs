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
    private DateTimeOffset _nextCleanup;

    /// <summary>Leases currently held and unexpired.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                Maintain(_clock.GetUtcNow(), force: true);
                return _leases.Count;
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
            Maintain(now);
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
        Maintain(now);
        return _leases.TryGetValue(key, out var current)
            && current.ExpiresAt > now
            && string.Equals(current.Owner, owner, StringComparison.Ordinal);
    }

    private void Maintain(DateTimeOffset now, bool force = false)
    {
        if (!force && now < _nextCleanup)
            return;
        _nextCleanup = now.AddSeconds(1);
        List<string>? expired = null;
        foreach (var pair in _leases)
            if (pair.Value.ExpiresAt <= now)
                (expired ??= []).Add(pair.Key);
        if (expired is not null)
            foreach (var key in expired)
                _leases.Remove(key);
    }

    private readonly record struct Lease(string Owner, DateTimeOffset ExpiresAt);
}
