using HostLoom.RateLimiting.Internal;

namespace HostLoom.RateLimiting;

/// <summary>Process-local token buckets with the same atomic decision contract as remote stores.</summary>
public sealed class InMemoryRateLimitStore : IRateLimitStore
{
    private readonly Dictionary<string, TokenBucketState> _buckets = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    private readonly TimeProvider _timeProvider;
    private long _lastSweepMilliseconds;

    /// <summary>Creates an empty store using <paramref name="timeProvider"/> for refill and expiry.</summary>
    public InMemoryRateLimitStore(TimeProvider? timeProvider = null) =>
        _timeProvider = timeProvider ?? TimeProvider.System;

    /// <summary>Current bucket count; reading it reclaims expired entries.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                SweepExpired(NowMilliseconds(), force: true);
                return _buckets.Count;
            }
        }
    }

    /// <inheritdoc />
    public ValueTask<RateLimitDecision> AcquireAsync(
        string key,
        TokenBucketRateLimitPolicy policy,
        int permitCount,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(policy);
        policy.ThrowIfInvalid();
        ArgumentOutOfRangeException.ThrowIfNegative(permitCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(permitCount, policy.TokenLimit);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            var now = NowMilliseconds();
            SweepExpired(now);
            _buckets.TryGetValue(key, out var current);
            var (state, decision) = TokenBucketAlgorithm.Evaluate(
                current,
                now,
                policy,
                permitCount
            );
            _buckets[key] = state;
            return ValueTask.FromResult(decision);
        }
    }

    private long NowMilliseconds() => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    private void SweepExpired(long now, bool force = false)
    {
        if (!force && now - _lastSweepMilliseconds < 1_000)
        {
            return;
        }

        _lastSweepMilliseconds = now;
        foreach (var pair in _buckets)
        {
            if (now >= pair.Value.ExpiresAtMilliseconds)
            {
                _buckets.Remove(pair.Key);
            }
        }
    }
}
