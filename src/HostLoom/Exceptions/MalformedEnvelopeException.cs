namespace HostLoom;

/// <summary>
/// The received HostLoom envelope or one of its serialized payloads is malformed.
/// </summary>
/// <remarks>
/// Transports use this exception to distinguish poison records from application failures.
/// It is distinct from <see cref="InvalidDataException"/> so a transport cannot confuse an
/// application exception with malformed wire data.
/// </remarks>
public sealed class MalformedEnvelopeException : Exception
{
    public MalformedEnvelopeException(string message)
        : base(message) { }

    public MalformedEnvelopeException(string message, Exception innerException)
        : base(message, innerException) { }
}
