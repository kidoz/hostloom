namespace HostLoom.RateLimiting.HttpClient;

/// <summary>Bounded local waiting for one outgoing HTTP handler pipeline.</summary>
public sealed class HttpRateLimitOptions
{
    /// <summary>Total time a request may wait for permits. Zero rejects immediately.</summary>
    public TimeSpan MaxWait { get; set; }

    /// <summary>Maximum requests in the wait loop for this handler pipeline. Zero disables waiting.</summary>
    public int QueueLimit { get; set; }

    internal void ThrowIfInvalid()
    {
        if (MaxWait < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxWait), "MaxWait must not be negative.");
        }

        if (QueueLimit < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(QueueLimit),
                "QueueLimit must not be negative."
            );
        }

        if (MaxWait > TimeSpan.Zero && QueueLimit == 0)
        {
            throw new ArgumentException("QueueLimit must be positive when MaxWait is positive.");
        }
    }

    internal HttpRateLimitOptions Snapshot() =>
        new() { MaxWait = MaxWait, QueueLimit = QueueLimit };
}
