using MessagePack;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>A version-one HostLoom WebSocket application frame.</summary>
[MessagePackObject]
public sealed class HubFrame
{
    [Key(0)]
    public HubFrameKind Kind { get; init; }

    /// <summary>The client-chosen stream identifier. <see cref="Guid.Empty"/> addresses the session itself.</summary>
    [Key(1)]
    public Guid StreamId { get; init; }

    [Key(2)]
    public Guid? SessionId { get; init; }

    [Key(3)]
    public string? Operation { get; init; }

    [Key(4)]
    public string? Topic { get; init; }

    [Key(5)]
    public string? Key { get; init; }

    [Key(6)]
    public int? TimeoutMilliseconds { get; init; }

    [Key(7)]
    public int? Credit { get; init; }

    [Key(8)]
    public long? Sequence { get; init; }

    [Key(9)]
    public Guid? EventId { get; init; }

    [Key(10)]
    public string? Code { get; init; }

    [Key(11)]
    public string? Message { get; init; }

    [Key(12)]
    public ReadOnlyMemory<byte>? Payload { get; init; }

    [Key(13)]
    public int? MaximumMessageSize { get; init; }

    [Key(14)]
    public int? MaximumConcurrentRequests { get; init; }
}
