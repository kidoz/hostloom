using BenchmarkDotNet.Attributes;
using HostLoom.AspNetCore.WebSockets;

namespace HostLoom.Benchmarks;

[MemoryDiagnoser]
public class WebSocketProtocolEncodeBenchmarks
{
    private readonly JsonWebSocketHubProtocol _json = new();
    private readonly MessagePackWebSocketHubProtocol _messagePack = new();
    private readonly ProtobufWebSocketHubProtocol _protobuf = new();
    private HubFrame _frame = null!;

    [Params(0, 256, 4096)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var payload = GC.AllocateUninitializedArray<byte>(PayloadSize);
        for (var index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(index % 251);
        }

        _frame = WebSocketBenchmarkFrame.Create(payload);
    }

    [Benchmark(Baseline = true)]
    public byte[] Json() => _json.Encode(_frame);

    [Benchmark]
    public byte[] MessagePack() => _messagePack.Encode(_frame);

    [Benchmark]
    public byte[] Protobuf() => _protobuf.Encode(_frame);
}

[MemoryDiagnoser]
public class WebSocketProtocolDecodeBenchmarks
{
    private readonly JsonWebSocketHubProtocol _json = new();
    private readonly MessagePackWebSocketHubProtocol _messagePack = new();
    private readonly ProtobufWebSocketHubProtocol _protobuf = new();
    private byte[] _jsonPayload = null!;
    private byte[] _messagePackPayload = null!;
    private byte[] _protobufPayload = null!;

    [Params(0, 256, 4096)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var payload = GC.AllocateUninitializedArray<byte>(PayloadSize);
        for (var index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(index % 251);
        }

        var frame = WebSocketBenchmarkFrame.Create(payload);
        _jsonPayload = _json.Encode(frame);
        _messagePackPayload = _messagePack.Encode(frame);
        _protobufPayload = _protobuf.Encode(frame);
    }

    [Benchmark(Baseline = true)]
    public HubFrame Json() => _json.Decode(_jsonPayload);

    [Benchmark]
    public HubFrame MessagePack() => _messagePack.Decode(_messagePackPayload);

    [Benchmark]
    public HubFrame Protobuf() => _protobuf.Decode(_protobufPayload);
}

internal static class WebSocketBenchmarkFrame
{
    public static HubFrame Create(byte[] payload) =>
        new()
        {
            Kind = HubFrameKind.Event,
            StreamId = new("22222222222222222222222222222222"),
            Topic = "orders.changed",
            Key = "customer-12345",
            Sequence = 987654321,
            EventId = new("55555555555555555555555555555555"),
            Payload = payload,
        };
}
