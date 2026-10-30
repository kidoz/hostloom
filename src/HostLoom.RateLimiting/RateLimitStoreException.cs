namespace HostLoom.RateLimiting;

/// <summary>Backend-neutral store failure classification.</summary>
public enum RateLimitFailureKind
{
    /// <summary>The backend could not be reached.</summary>
    Unavailable,

    /// <summary>The backend operation exceeded its deadline; its outcome may be unknown.</summary>
    Timeout,

    /// <summary>Another backend or protocol failure.</summary>
    Other,
}

/// <summary>A store operation failed without producing a usable rate-limit decision.</summary>
public sealed class RateLimitStoreException : Exception
{
    /// <summary>Creates the exception for <paramref name="kind"/>.</summary>
    public RateLimitStoreException(RateLimitFailureKind kind, string message)
        : base(message) => Kind = kind;

    /// <summary>Creates the exception for <paramref name="kind"/> wrapping the backend exception.</summary>
    public RateLimitStoreException(
        RateLimitFailureKind kind,
        string message,
        Exception? innerException
    )
        : base(message, innerException) => Kind = kind;

    /// <summary>What went wrong, without naming the backend.</summary>
    public RateLimitFailureKind Kind { get; }
}
