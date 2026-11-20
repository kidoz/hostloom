namespace HostLoom;

/// <summary>
/// A per-process <see cref="IInboxStore"/> with real expiry on a <see cref="TimeProvider"/>. It
/// deduplicates redeliveries to this process only; a replacement instance starts empty, which is
/// the guarantee a shared backend adds.
/// </summary>
public sealed class InMemoryInboxStore(TimeProvider? timeProvider = null) : IInboxStore
{
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, (Guid Token, DateTimeOffset ExpiresAt)> _seen = new(
        StringComparer.Ordinal
    );

    /// <summary>Keys currently remembered and unexpired.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                var now = _clock.GetUtcNow();
                return _seen.Values.Count(entry => entry.ExpiresAt > now);
            }
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> TryRecordAsync(
        string key,
        Guid claimToken,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var now = _clock.GetUtcNow();
            if (_seen.TryGetValue(key, out var entry) && entry.ExpiresAt > now)
            {
                return ValueTask.FromResult(false);
            }

            // Expired keys are dropped as they are met, and swept when the table grows, so a busy
            // topic never holds every key it ever saw.
            if (_seen.Count >= 1024 && _seen.Count % 1024 == 0)
            {
                foreach (
                    var stale in _seen
                        .Where(pair => pair.Value.ExpiresAt <= now)
                        .Select(pair => pair.Key)
                        .ToArray()
                )
                {
                    _seen.Remove(stale);
                }
            }

            _seen[key] = (claimToken, now + window);
            return ValueTask.FromResult(true);
        }
    }

    /// <inheritdoc />
    public ValueTask ReleaseAsync(
        string key,
        Guid claimToken,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_seen.TryGetValue(key, out var entry) && entry.Token == claimToken)
                _seen.Remove(key);
        }

        return ValueTask.CompletedTask;
    }
}
