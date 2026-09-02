namespace HostLoom.AspNetCore.WebSockets;

internal sealed class ControlFrameRateLimiter(TimeProvider timeProvider, int limit)
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
