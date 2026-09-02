namespace HostLoom.Caching;

/// <summary>Why a distributed store could not complete an operation.</summary>
public enum CacheFailureKind
{
    /// <summary>The backend is unreachable or refused the connection.</summary>
    Unavailable,

    /// <summary>The backend did not answer in time.</summary>
    Timeout,

    /// <summary>Anything else: a protocol error, a rejected command, an unexpected exception.</summary>
    Other,
}

/// <summary>
/// The one exception type an <see cref="IDistributedCacheStore"/> raises for a backend failure.
/// The composed cache maps <see cref="Kind"/> to fail-open behaviour; a consumer never sees it.
/// </summary>
public sealed class CacheStoreException : Exception
{
    /// <summary>Creates the exception for <paramref name="kind"/>.</summary>
    public CacheStoreException(CacheFailureKind kind, string message)
        : base(message) => Kind = kind;

    /// <summary>Creates the exception for <paramref name="kind"/> wrapping the backend's exception.</summary>
    public CacheStoreException(CacheFailureKind kind, string message, Exception innerException)
        : base(message, innerException) => Kind = kind;

    /// <summary>What went wrong, in backend-neutral terms.</summary>
    public CacheFailureKind Kind { get; }
}

/// <summary>
/// Raised by <see cref="ICache.SetIfAbsentAsync{T}(string, T, CacheEntryOptions, CancellationToken)"/>
/// when the distributed store is unavailable and the caller chose
/// <see cref="UnavailableBehavior.Throw"/>. No other read or write throws for a store failure.
/// </summary>
public sealed class CacheUnavailableException : Exception
{
    /// <summary>Creates the exception for <paramref name="key"/>.</summary>
    public CacheUnavailableException(string key, CacheFailureKind kind, Exception? innerException)
        : base(
            $"The distributed cache store is unavailable ({kind}), so '{key}' could not be written.",
            innerException
        )
    {
        Key = key;
        Kind = kind;
    }

    /// <summary>The consumer key that could not be written.</summary>
    public string Key { get; }

    /// <summary>Why the store could not answer.</summary>
    public CacheFailureKind Kind { get; }
}
