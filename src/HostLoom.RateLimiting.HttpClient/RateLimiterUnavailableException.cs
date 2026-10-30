namespace HostLoom.RateLimiting.HttpClient;

/// <summary>The rate-limit store could not decide and the limiter is configured to deny work.</summary>
public sealed class RateLimiterUnavailableException : Exception
{
    /// <summary>Creates the exception.</summary>
    public RateLimiterUnavailableException()
        : base("The rate-limit store is unavailable and the limiter denied the outgoing request.")
    { }

    /// <summary>Creates the exception with a message and inner exception.</summary>
    public RateLimiterUnavailableException(string message, Exception? innerException)
        : base(message, innerException) { }
}
