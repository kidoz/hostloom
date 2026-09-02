using System.Collections.Concurrent;

namespace HostLoom.Caching.Internal;

/// <summary>
/// One warning per key per interval. A store outage otherwise turns every cache call into a log
/// line, which is exactly when the log is least able to absorb it.
/// </summary>
internal sealed class DegradedLogThrottle(TimeProvider time, TimeSpan interval)
{
    private const int MaxTrackedKeys = 10_000;
    private readonly ConcurrentDictionary<string, long> _lastLogged = new(StringComparer.Ordinal);

    public bool ShouldLog(string key)
    {
        var now = time.GetUtcNow().UtcTicks;
        if (_lastLogged.TryGetValue(key, out var last) && now - last < interval.Ticks)
        {
            return false;
        }

        if (_lastLogged.Count >= MaxTrackedKeys)
        {
            _lastLogged.Clear();
        }

        _lastLogged[key] = now;
        return true;
    }
}
