namespace HostLoom.RateLimiting;

/// <summary>Atomic token-bucket decisions supplied by a backend.</summary>
public interface IRateLimitStore
{
    /// <summary>
    /// Refills and consumes permits in one atomic backend operation. The key is already fully
    /// prefixed. A rejection caused by insufficient permits is a decision, not an exception.
    /// </summary>
    ValueTask<RateLimitDecision> AcquireAsync(
        string key,
        TokenBucketRateLimitPolicy policy,
        int permitCount,
        CancellationToken cancellationToken = default
    );
}

/// <summary>The result of one atomic token-bucket operation.</summary>
/// <param name="IsAcquired">Whether the requested permits were consumed.</param>
/// <param name="RemainingPermits">Whole permits remaining after this decision.</param>
/// <param name="RetryAfter">Delay until enough permits may be available, or zero when not applicable.</param>
public readonly record struct RateLimitDecision(
    bool IsAcquired,
    int RemainingPermits,
    TimeSpan RetryAfter
)
{
    /// <summary>Creates an acquired decision.</summary>
    public static RateLimitDecision Acquired(int remainingPermits) =>
        new(true, remainingPermits, TimeSpan.Zero);

    /// <summary>Creates a rejected decision.</summary>
    public static RateLimitDecision Rejected(int remainingPermits, TimeSpan retryAfter) =>
        new(false, remainingPermits, retryAfter);
}
