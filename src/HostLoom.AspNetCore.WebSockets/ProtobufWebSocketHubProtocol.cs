using System.Buffers;
using System.Net.WebSockets;
using ProtoBuf;

namespace HostLoom.AspNetCore.WebSockets;

public sealed class ProtobufWebSocketHubProtocol : IWebSocketHubProtocol
{
    public const string ProtocolName = "hostloom.protobuf.v1";

    public string SubProtocol => ProtocolName;

    public WebSocketMessageType MessageType => WebSocketMessageType.Binary;

    public HubFrame Decode(ReadOnlySpan<byte> payload)
    {
        try
        {
            return ProtobufHubFrame.ToHubFrame(Serializer.Deserialize<ProtobufHubFrame>(payload));
        }
        catch (Exception exception)
            when (exception is ProtoException or EndOfStreamException or OverflowException)
        {
            throw new InvalidDataException("The Protocol Buffers frame was invalid.", exception);
        }
    }

    public byte[] Encode(HubFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        var writer = new ArrayBufferWriter<byte>();
        Serializer.Serialize(writer, ProtobufHubFrame.FromHubFrame(frame));
        return writer.WrittenSpan.ToArray();
    }

    [ProtoContract]
    private sealed class ProtobufHubFrame
    {
        [ProtoMember(1)]
        public HubFrameKind Kind { get; set; }

        [ProtoMember(2)]
        public byte[]? StreamId { get; set; }

        [ProtoMember(3)]
        public byte[]? SessionId { get; set; }

        [ProtoMember(4)]
        public string? Operation { get; set; }

        [ProtoMember(5)]
        public string? Topic { get; set; }

        [ProtoMember(6)]
        public string? Key { get; set; }

        [ProtoMember(7)]
        public int? TimeoutMilliseconds { get; set; }

        [ProtoMember(8)]
        public int? Credit { get; set; }

        [ProtoMember(9)]
        public long? Sequence { get; set; }

        [ProtoMember(10)]
        public byte[]? EventId { get; set; }

        [ProtoMember(11)]
        public string? Code { get; set; }

        [ProtoMember(12)]
        public string? Message { get; set; }

        [ProtoMember(13)]
        public byte[]? Payload { get; set; }

        [ProtoMember(14)]
        public int? MaximumMessageSize { get; set; }

        [ProtoMember(15)]
        public int? MaximumConcurrentRequests { get; set; }

        public static ProtobufHubFrame FromHubFrame(HubFrame frame) =>
            new()
            {
                Kind = frame.Kind,
                StreamId = ToBytes(frame.StreamId),
                SessionId = ToOptionalBytes(frame.SessionId),
                Operation = frame.Operation,
                Topic = frame.Topic,
                Key = frame.Key,
                TimeoutMilliseconds = frame.TimeoutMilliseconds,
                Credit = frame.Credit,
                Sequence = frame.Sequence,
                EventId = ToOptionalBytes(frame.EventId),
                Code = frame.Code,
                Message = frame.Message,
                Payload = frame.Payload?.ToArray(),
                MaximumMessageSize = frame.MaximumMessageSize,
                MaximumConcurrentRequests = frame.MaximumConcurrentRequests,
            };

        public static HubFrame ToHubFrame(ProtobufHubFrame frame) =>
            new()
            {
                Kind = frame.Kind,
                StreamId = ToIdentifier(frame.StreamId) ?? Guid.Empty,
                SessionId = ToIdentifier(frame.SessionId),
                Operation = frame.Operation,
                Topic = frame.Topic,
                Key = frame.Key,
                TimeoutMilliseconds = frame.TimeoutMilliseconds,
                Credit = frame.Credit,
                Sequence = frame.Sequence,
                EventId = ToIdentifier(frame.EventId),
                Code = frame.Code,
                Message = frame.Message,
                Payload = frame.Payload,
                MaximumMessageSize = frame.MaximumMessageSize,
                MaximumConcurrentRequests = frame.MaximumConcurrentRequests,
            };

        // The wire form is the 16 big-endian bytes of RFC 4122, so a non-.NET client reads the
        // same identifier the JSON contract spells as 32 hex digits.
        private static byte[] ToBytes(Guid identifier)
        {
            var bytes = new byte[16];
            _ = identifier.TryWriteBytes(bytes, bigEndian: true, out _);
            return bytes;
        }

        private static byte[]? ToOptionalBytes(Guid? identifier) =>
            identifier is { } value ? ToBytes(value) : null;

        private static Guid? ToIdentifier(byte[]? bytes) =>
            bytes switch
            {
                null => null,
                { Length: 16 } => new Guid(bytes, bigEndian: true),
                _ => throw new InvalidDataException("A frame identifier must be 16 bytes."),
            };
    }
}
