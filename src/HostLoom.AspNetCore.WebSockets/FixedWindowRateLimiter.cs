namespace HostLoom.AspNetCore.WebSockets;

/// <summary>
/// Counts acquisitions in fixed one-second windows measured by the injected
/// <see cref="TimeProvider"/>. Each session owns one instance per budget and only its receive
/// loop calls it, so no synchronization is needed.
/// </summary>
internal sealed class FixedWindowRateLimiter(TimeProvider timeProvider, int limit)
{
    private long _windowStart = timeProvider.GetTimestamp();
    private int _count;

    public bool TryAcquire()
    {
        var now = timeProvider.GetTimestamp();
        if (timeProvider.GetElapsedTime(_windowStart, now) >= TimeSpan.FromSeconds(1))
        {
            _windowStart = now;
            _count = 0;
        }

        return ++_count <= limit;
    }
}
