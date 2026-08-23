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
        public ulong StreamId { get; set; }

        [ProtoMember(3)]
        public string? SessionId { get; set; }

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
        public string? EventId { get; set; }

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
                Payload = frame.Payload?.ToArray(),
                MaximumMessageSize = frame.MaximumMessageSize,
                MaximumConcurrentRequests = frame.MaximumConcurrentRequests,
            };

        public static HubFrame ToHubFrame(ProtobufHubFrame frame) =>
            new()
            {
                Kind = frame.Kind,
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
    }
}
