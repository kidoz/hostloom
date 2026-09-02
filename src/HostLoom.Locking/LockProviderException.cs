namespace HostLoom.Locking;

/// <summary>
/// Thrown by an <see cref="ILockProvider"/> for a backend failure. The composed lock maps it to
/// <see cref="LockProviderUnavailableException"/>, so consumers never see a backend exception type.
/// </summary>
public sealed class LockProviderException : Exception
{
    /// <summary>Creates the exception for <paramref name="kind"/>.</summary>
    public LockProviderException(LockFailureKind kind, string message)
        : base(message) => Kind = kind;

    /// <summary>Creates the exception for <paramref name="kind"/> wrapping the backend's own.</summary>
    public LockProviderException(LockFailureKind kind, string message, Exception? innerException)
        : base(message, innerException) => Kind = kind;

    /// <summary>What went wrong, without naming the backend.</summary>
    public LockFailureKind Kind { get; }
}
