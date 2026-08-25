namespace HostLoom;

internal enum MessageKind
{
    Request,
    Response,
    Fault,

    /// <summary>Published to a topic with no reply. Carries an empty <c>ResponseType</c>.</summary>
    Event,
}

/// <summary>
/// Sanitized fault detail. Deliberately carries no stack trace: server-side implementation
/// detail must not cross the transport boundary.
/// </summary>
internal sealed record RemoteFault(string ErrorType, string Message);

internal sealed record MessageEnvelope
{
    public required Guid MessageId { get; init; }

    public Guid? CorrelationId { get; init; }

    public required MessageKind Kind { get; init; }

    public required string MessageType { get; init; }

    public required string ResponseType { get; init; }

    public required DateTimeOffset SentAt { get; init; }

    public required byte[] Body { get; init; }

    public RemoteFault? Fault { get; init; }
}
