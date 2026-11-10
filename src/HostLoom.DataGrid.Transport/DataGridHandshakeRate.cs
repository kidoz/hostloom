namespace HostLoom.DataGrid.Transport;

/// <summary>Fixed burst/rate budget shared by both peer directions; rejected starts consume no token.</summary>
internal sealed class DataGridHandshakeRate
{
    private readonly object _sync = new();
    private readonly int _limit;
    private readonly DataGridTransportRole _role;
    private readonly TimeProvider _clock;
    private long _last;
    private double _tokens;

    internal DataGridHandshakeRate(
        int limit,
        DataGridTransportRole role,
        TimeProvider? clock = null
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, 32);
        _limit = limit;
        _role = role;
        _clock = clock ?? TimeProvider.System;
        _last = _clock.GetTimestamp();
        _tokens = limit;
    }

    internal bool TryAcquire(DataGridTransportDirection direction)
    {
        lock (_sync)
        {
            var now = _clock.GetTimestamp();
            _tokens = Math.Min(
                _limit,
                _tokens + _clock.GetElapsedTime(_last, now).TotalSeconds * _limit
            );
            _last = now;
            if (_tokens < 1)
            {
                DataGridTransportDiagnostics.Reject(
                    _role,
                    direction,
                    DataGridTransportResource.Handshake,
                    DataGridTransportRejection.Rate
                );
                return false;
            }
            _tokens--;
            return true;
        }
    }
}
