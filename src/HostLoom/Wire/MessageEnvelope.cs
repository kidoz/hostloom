using System.Text.Json.Serialization;

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

    /// <summary>Stable id of the chain this message belongs to. Absent on a frame written before flow fields existed.</summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Guid? ConversationId { get; init; }

    /// <summary>Id of the message that caused this one. Absent when this message starts a flow.</summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Guid? InitiatorId { get; init; }

    /// <summary>
    /// Absolute UTC time after which a request must not run. Absent means the request does not expire
    /// on its own. Events leave it unset.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? ExpiresAt { get; init; }

    public required MessageKind Kind { get; init; }

    public required string MessageType { get; init; }

    public required string ResponseType { get; init; }

    public required DateTimeOffset SentAt { get; init; }

    public required byte[] Body { get; init; }

    public RemoteFault? Fault { get; init; }
}
