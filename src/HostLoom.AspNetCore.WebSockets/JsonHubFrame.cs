using System.Text.Json;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class JsonHubFrame
{
    internal static HubFrame Parse(JsonElement json)
    {
        if (json.ValueKind is not JsonValueKind.Object)
        {
            throw new InvalidDataException("The JSON frame must be an object.");
        }

        foreach (var property in json.EnumerateObject())
        {
            switch (property.Name)
            {
                case "kind":
                case "operation":
                case "topic":
                case "key":
                case "code":
                case "message":
                    String(property.Name);
                    break;
                case "streamId":
                case "sessionId":
                case "eventId":
                    Identifier(property.Name);
                    break;
                case "timeoutMilliseconds":
                case "maximumMessageSize":
                case "maximumConcurrentRequests":
                    Integer(property.Name, 1);
                    break;
                case "credit":
                case "sequence":
                    Integer(property.Name, 0);
                    break;
                case "payload":
                    var encoded = String(property.Name)!;
                    if (
                        encoded.Length % 4 != 0
                        || encoded.Any(character =>
                            !(
                                character
                                is >= 'A'
                                    and <= 'Z'
                                    or >= 'a'
                                    and <= 'z'
                                    or >= '0'
                                    and <= '9'
                                    or '+'
                                    or '/'
                                    or '='
                            )
                        )
                    )
                    {
                        throw new InvalidDataException(
                            "The payload must be Base64 without whitespace."
                        );
                    }
                    try
                    {
                        Convert.FromBase64String(encoded);
                    }
                    catch (FormatException exception)
                    {
                        throw new InvalidDataException("The payload must be Base64.", exception);
                    }
                    break;
                default:
                    throw new InvalidDataException(
                        $"Unknown JSON frame property '{property.Name}'."
                    );
            }
        }

        var kindName = String("kind", required: true)!;
        if (
            !Enum.TryParse<HubFrameKind>(kindName, true, out var kind)
            || kind is HubFrameKind.None
            || !Enum.IsDefined(kind)
            || JsonNamingPolicy.CamelCase.ConvertName(kind.ToString()) != kindName
        )
        {
            throw new InvalidDataException(
                "The JSON frame kind must be a recognized camel-case name."
            );
        }
        var stream = Identifier("streamId", required: true)!.Value;
        if (
            (kind is HubFrameKind.Welcome && stream != Guid.Empty)
            || (kind is not (HubFrameKind.Welcome or HubFrameKind.Fault) && stream == Guid.Empty)
        )
        {
            throw new InvalidDataException(
                "The JSON frame uses an invalid session or stream identifier."
            );
        }
        string[] required = kind switch
        {
            HubFrameKind.Welcome =>
            [
                "sessionId",
                "credit",
                "maximumMessageSize",
                "maximumConcurrentRequests",
            ],
            HubFrameKind.Request => ["operation", "payload"],
            HubFrameKind.Response => ["payload"],
            HubFrameKind.Fault => ["code", "message"],
            HubFrameKind.Subscribe or HubFrameKind.Subscribed => ["topic", "credit"],
            HubFrameKind.Event => ["sequence", "eventId", "payload"],
            HubFrameKind.Credit => ["credit"],
            HubFrameKind.Ack => ["sequence"],
            _ => [],
        };
        foreach (var name in required)
        {
            if (!json.TryGetProperty(name, out _))
                throw new InvalidDataException($"Missing JSON frame property '{name}'.");
        }
        return new HubFrame
        {
            Kind = kind,
            StreamId = stream,
            SessionId = Identifier("sessionId"),
            EventId = Identifier("eventId"),
            Operation = String("operation"),
            Topic = String("topic"),
            Key = String("key"),
            Code = String("code"),
            Message = String("message"),
            Credit = (int?)Integer("credit", 0),
            Sequence = Integer("sequence", 0),
            TimeoutMilliseconds = (int?)Integer("timeoutMilliseconds", 1),
            MaximumMessageSize = (int?)Integer("maximumMessageSize", 1),
            MaximumConcurrentRequests = (int?)Integer("maximumConcurrentRequests", 1),
            Payload = json.TryGetProperty("payload", out var bytes)
                ? Convert.FromBase64String(bytes.GetString()!)
                : (ReadOnlyMemory<byte>?)null,
        };

        string? String(string name, bool required = false)
        {
            if (!json.TryGetProperty(name, out var value))
            {
                return required
                    ? throw new InvalidDataException($"Missing JSON frame property '{name}'.")
                    : null;
            }
            return value.ValueKind is JsonValueKind.String
                ? value.GetString()
                : throw new InvalidDataException($"JSON frame property '{name}' must be a string.");
        }

        Guid? Identifier(string name, bool required = false)
        {
            var value = String(name, required);
            if (value is null)
                return null;
            if (
                value.Length != 32
                || value.Any(character => !(character is >= '0' and <= '9' or >= 'a' and <= 'f'))
                || !Guid.TryParseExact(value, "N", out var identifier)
            )
            {
                throw new InvalidDataException(
                    $"JSON frame property '{name}' must be 32 lowercase hexadecimal digits."
                );
            }
            return identifier;
        }

        long? Integer(string name, int minimum)
        {
            if (!json.TryGetProperty(name, out var value))
                return null;
            var maximum = name == "sequence" ? 9_007_199_254_740_991L : int.MaxValue;
            if (
                value.ValueKind is not JsonValueKind.Number
                || !value.TryGetDouble(out var number)
                || !double.IsFinite(number)
                || number < minimum
                || number > maximum
                || Math.Truncate(number) != number
            )
            {
                throw new InvalidDataException(
                    $"JSON frame property '{name}' must be an integer from {minimum} to {maximum}."
                );
            }
            return (long)number;
        }
    }

    public required string Kind { get; init; }

    public Guid StreamId { get; init; }

    public Guid? SessionId { get; init; }

    public string? Operation { get; init; }

    public string? Topic { get; init; }

    public string? Key { get; init; }

    public int? TimeoutMilliseconds { get; init; }

    public int? Credit { get; init; }

    public long? Sequence { get; init; }

    public Guid? EventId { get; init; }

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
