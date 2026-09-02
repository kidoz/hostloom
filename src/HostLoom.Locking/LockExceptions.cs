using System.Globalization;

namespace HostLoom.Locking;

/// <summary>The key stayed held by another owner past the retry policy or the wait bound.</summary>
public sealed class LockNotAcquiredException : Exception
{
    /// <summary>Creates the exception for <paramref name="key"/>.</summary>
    public LockNotAcquiredException(string key, TimeSpan waited, int attempts)
        : base(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Lock '{key}' was not acquired after {attempts} attempt(s) over {waited.TotalMilliseconds:0} ms; another owner holds it."
            )
        )
    {
        Key = key;
        Waited = waited;
        Attempts = attempts;
    }

    /// <summary>The consumer key.</summary>
    public string Key { get; }

    /// <summary>How long the caller waited before giving up.</summary>
    public TimeSpan Waited { get; }

    /// <summary>Provider calls made, including the first.</summary>
    public int Attempts { get; }
}

/// <summary>The provider failed while acquiring, classified by <see cref="Kind"/>.</summary>
public sealed class LockProviderUnavailableException : Exception
{
    /// <summary>Creates the exception for <paramref name="key"/>.</summary>
    public LockProviderUnavailableException(
        string key,
        TimeSpan waited,
        int attempts,
        LockFailureKind kind,
        Exception? innerException
    )
        : base(
            string.Create(
                CultureInfo.InvariantCulture,
                $"The lock provider failed ({kind}) while acquiring '{key}' on attempt {attempts} after {waited.TotalMilliseconds:0} ms."
            ),
            innerException
        )
    {
        Key = key;
        Waited = waited;
        Attempts = attempts;
        Kind = kind;
    }

    /// <summary>The consumer key.</summary>
    public string Key { get; }

    /// <summary>How long the caller had waited when the failure happened.</summary>
    public TimeSpan Waited { get; }

    /// <summary>Provider calls made, including the failing one.</summary>
    public int Attempts { get; }

    /// <summary>What went wrong, without naming the backend.</summary>
    public LockFailureKind Kind { get; }
}

/// <summary>
/// The current asynchronous flow already holds the key. Waiting would only run out the lease,
/// because the holder is the caller itself.
/// </summary>
public sealed class LockReentrancyException : Exception
{
    /// <summary>Creates the exception for <paramref name="key"/>.</summary>
    public LockReentrancyException(string key)
        : base(
            $"Lock '{key}' is already held by the current asynchronous flow. Locks are not "
                + "re-entrant; release it before acquiring it again, or restructure the call."
        ) => Key = key;

    /// <summary>The consumer key.</summary>
    public string Key { get; }
}
