namespace HostLoom.RateLimiting.HttpClient;

/// <summary>An outgoing request could not acquire a permit within its local wait budget.</summary>
public sealed class RateLimitExceededException : Exception
{
    /// <summary>Creates an exception without a known retry delay.</summary>
    public RateLimitExceededException()
        : base("The outgoing request was rejected by its local rate limiter.") { }

    /// <summary>Creates an exception with an optional retry delay.</summary>
    public RateLimitExceededException(TimeSpan? retryAfter)
        : base("The outgoing request was rejected by its local rate limiter.") =>
        RetryAfter = retryAfter;

    /// <summary>Creates an exception with a message and inner exception.</summary>
    public RateLimitExceededException(string message, Exception? innerException)
        : base(message, innerException) { }

    /// <summary>Estimated delay before permits may be available, when the limiter supplied one.</summary>
    public TimeSpan? RetryAfter { get; }
}
