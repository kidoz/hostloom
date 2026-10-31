namespace HostLoom;

/// <summary>One receive destination whose failure ratio can pause consumption.</summary>
/// <param name="Address">Request endpoint, or event topic.</param>
/// <param name="Subscription">Event subscription name. Null for a request endpoint.</param>
public readonly record struct ReceiveDestination(string Address, string? Subscription)
{
    public static ReceiveDestination For(ReceiveContext context) =>
        context is EventReceiveContext ev
            ? new(ev.Destination.Value, ev.Subscription)
            : new(context.Destination.Value, null);
}

/// <summary>
/// In-memory record of destinations whose consumers should stop taking work until a resume time.
/// Process-local: a restart forgets every pause.
/// </summary>
public sealed class ReceiveAdmission
{
    private readonly Lock _gate = new();
    private readonly Dictionary<ReceiveDestination, DateTimeOffset> _resumeAt = [];

    /// <summary>Marks <paramref name="destination"/> paused until <paramref name="resumeAt"/>.</summary>
    /// <remarks>A later pause that ends sooner does not shorten one already in effect.</remarks>
    public void Suspend(ReceiveDestination destination, DateTimeOffset resumeAt)
    {
        lock (_gate)
        {
            if (_resumeAt.TryGetValue(destination, out var current) && current > resumeAt)
            {
                return;
            }

            _resumeAt[destination] = resumeAt;
        }
    }

    /// <summary>Whether <paramref name="destination"/> is still paused at <paramref name="now"/>.</summary>
    public bool TryGetSuspension(
        ReceiveDestination destination,
        DateTimeOffset now,
        out DateTimeOffset resumeAt
    )
    {
        lock (_gate)
        {
            if (!_resumeAt.TryGetValue(destination, out resumeAt) || resumeAt <= now)
            {
                _resumeAt.Remove(destination);
                resumeAt = default;
                return false;
            }

            return true;
        }
    }

    /// <summary>How many destinations are still paused at <paramref name="now"/>.</summary>
    public int CountSuspended(DateTimeOffset now)
    {
        lock (_gate)
        {
            var expired = new List<ReceiveDestination>();
            foreach (var pair in _resumeAt)
            {
                if (pair.Value <= now)
                {
                    expired.Add(pair.Key);
                }
            }

            foreach (var destination in expired)
            {
                _resumeAt.Remove(destination);
            }

            return _resumeAt.Count;
        }
    }
}
