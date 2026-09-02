using System.Text.Json;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class JsonHubFrame
{
    public required string Kind { get; init; }

    public ulong StreamId { get; init; }

    public string? SessionId { get; init; }

    public string? Operation { get; init; }

    public string? Topic { get; init; }

    public string? Key { get; init; }

    public int? TimeoutMilliseconds { get; init; }

    public int? Credit { get; init; }

    public long? Sequence { get; init; }

    public string? EventId { get; init; }

    public string? Code { get; init; }

    public string? Message { get; init; }

    public ReadOnlyMemory<byte>? Payload { get; init; }

    public int? MaximumMessageSize { get; init; }

    public int? MaximumConcurrentRequests { get; init; }

    public static JsonHubFrame FromHubFrame(HubFrame frame, bool camelCaseKind) =>
        new()
        {
            Kind = camelCaseKind
                ? JsonNamingPolicy.CamelCase.ConvertName(frame.Kind.ToString())
                : frame.Kind.ToString(),
            StreamId = frame.StreamId,
            SessionId = frame.SessionId,
            Operation = frame.Operation,
            Topic = frame.Topic,
            Key = frame.Key,
            TimeoutMilliseconds = frame.TimeoutMilliseconds,
            Credit = frame.Credit,
            Sequence = frame.Sequence,
            EventId = frame.EventId,
            Code = frame.Code,
            Message = frame.Message,
            Payload = frame.Payload,
            MaximumMessageSize = frame.MaximumMessageSize,
            MaximumConcurrentRequests = frame.MaximumConcurrentRequests,
        };

    public HubFrame ToHubFrame()
    {
        if (
            !Enum.TryParse<HubFrameKind>(Kind, ignoreCase: true, out var kind)
            || kind is HubFrameKind.None
            || !Enum.IsDefined(kind)
            || !string.Equals(Kind, kind.ToString(), StringComparison.OrdinalIgnoreCase)
        )
        {
            throw new InvalidDataException($"The JSON frame kind '{Kind}' is not recognized.");
        }

        return new HubFrame
        {
            Kind = kind,
            StreamId = StreamId,
            SessionId = SessionId,
            Operation = Operation,
            Topic = Topic,
            Key = Key,
            TimeoutMilliseconds = TimeoutMilliseconds,
            Credit = Credit,
            Sequence = Sequence,
            EventId = EventId,
            Code = Code,
            Message = Message,
            Payload = Payload,
            MaximumMessageSize = MaximumMessageSize,
            MaximumConcurrentRequests = MaximumConcurrentRequests,
        };
    }
}
