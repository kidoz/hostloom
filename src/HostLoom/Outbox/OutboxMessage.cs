namespace HostLoom;

/// <summary>
/// One event waiting in the outbox: the encoded wire frame the relay hands to the transport
/// unchanged, plus what an operator needs to see about it. The frame is the same bytes a direct
/// publish would send, so a subscriber cannot tell the two apart.
/// </summary>
public sealed record OutboxMessage
{
    /// <summary>The envelope's message id, which is also the outbox key.</summary>
    public required Guid MessageId { get; init; }

    /// <summary>The topic the event is published to.</summary>
    public required string Topic { get; init; }

    /// <summary>The logical message type name carried by the envelope, for diagnostics.</summary>
    public required string MessageType { get; init; }

    /// <summary>The encoded envelope, published as-is.</summary>
    public required ReadOnlyMemory<byte> Frame { get; init; }

    /// <summary>When the event was appended.</summary>
    public required DateTimeOffset EnqueuedAt { get; init; }

    /// <summary>Publish attempts that failed so far, as the store reports them.</summary>
    public int Attempts { get; init; }

    /// <summary>
    /// The earliest time a claim may return the message again after a failed attempt, as the
    /// store reports it; <see langword="null"/> when it is due at once.
    /// </summary>
    public DateTimeOffset? NextAttemptAt { get; init; }
}
