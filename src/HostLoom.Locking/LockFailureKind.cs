namespace HostLoom.Locking;

/// <summary>Provider-neutral classification of a lock backend failure.</summary>
public enum LockFailureKind
{
    /// <summary>The backend could not be reached or refused the connection.</summary>
    Unavailable,

    /// <summary>The backend did not answer within its command timeout.</summary>
    Timeout,

    /// <summary>Any other backend failure.</summary>
    Other,
}
