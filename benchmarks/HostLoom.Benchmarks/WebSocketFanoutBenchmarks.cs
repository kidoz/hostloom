using System.Net.WebSockets;
using BenchmarkDotNet.Attributes;
using HostLoom.AspNetCore.WebSockets;

namespace HostLoom.Benchmarks;

[MemoryDiagnoser]
public class WebSocketFanoutBenchmarks
{
    private const int OperationsPerInvoke = 256;
    private const string Topic = "orders.changed";
    private const string EventKey = "customer-12345";
    private WebSocketSessionRegistry _registry = null!;
    private BenchmarkSession[] _sessions = null!;
    private byte[] _payload = null!;

    [Params(1, 100, 500)]
    public int SessionCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _payload = GC.AllocateUninitializedArray<byte>(256);
        for (var index = 0; index < _payload.Length; index++)
        {
            _payload[index] = (byte)(index % 251);
        }

        _registry = new WebSocketSessionRegistry();
        _sessions = new BenchmarkSession[SessionCount];
        for (var index = 0; index < _sessions.Length; index++)
        {
            var session = new BenchmarkSession(index);
            _sessions[index] = session;
            _registry.Subscribe(session, Topic, key: null);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (var session in _sessions)
        {
            _registry.Unsubscribe(session, Topic, key: null);
        }
    }

    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void PublishToReadySessions()
    {
        for (var operation = 0; operation < OperationsPerInvoke; operation++)
        {
            _registry.Publish(Topic, EventKey, _payload);
            foreach (var session in _sessions)
            {
                session.Drain();
            }
        }
    }

    private sealed class BenchmarkSession : IWebSocketEventSink
    {
        private const int MaximumMessageSize = 65_536;
        private readonly JsonWebSocketHubProtocol _protocol = new();
        private readonly ByteBoundedOutboundQueue _outbound = new(
            maximumBytes: MaximumMessageSize,
            maximumFrames: 1
        );
        private readonly SubscriptionState _subscription;

        public BenchmarkSession(int index)
        {
            Id = $"session-{index}";
            _subscription = new SubscriptionState(
                streamId: (ulong)index + 1,
                Topic,
                key: null,
                initialCredit: 1
            );
            if (
                !_subscription.CompleteInitialization(_outbound.TryWriteReserved, _outbound.Release)
            )
            {
                throw new InvalidOperationException("The benchmark subscription could not start.");
            }
        }

        public string Id { get; }

        public bool TryQueueEvent(
            string topic,
            string? subscriptionKey,
            string? eventKey,
            ReadOnlyMemory<byte> payload,
            string eventId,
            long sequence
        )
        {
            if (
                !string.Equals(topic, Topic, StringComparison.Ordinal)
                || subscriptionKey is not null
            )
            {
                return true;
            }

            var encoded = _protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Event,
                    StreamId = _subscription.StreamId,
                    Topic = topic,
                    Key = eventKey,
                    EventId = eventId,
                    Sequence = sequence,
                    Payload = payload,
                }
            );
            if (encoded.Length > MaximumMessageSize)
            {
                return false;
            }

            return _subscription.AcceptLiveEvent(
                _outbound,
                encoded,
                _protocol.MessageType,
                out var frame
            ) switch
            {
                LiveEventDisposition.Active => _outbound.TryWriteReserved(frame),
                LiveEventDisposition.Dropped or LiveEventDisposition.Stopped => true,
                LiveEventDisposition.Buffered or LiveEventDisposition.CapacityExceeded => false,
                _ => throw new InvalidOperationException(
                    "The benchmark subscription delivery state is invalid."
                ),
            };
        }

        public void Abort() =>
            throw new InvalidOperationException("The bounded benchmark queue overflowed.");

        public void Drain()
        {
            if (!_outbound.Reader.TryRead(out var frame))
            {
                return;
            }

            _outbound.Release(frame);
            if (!_subscription.TryAddCredit(1, maximum: 1))
            {
                throw new InvalidOperationException("The benchmark credit could not be restored.");
            }

            if (_outbound.Reader.TryRead(out _))
            {
                throw new InvalidOperationException(
                    "The benchmark produced more than one frame per subscriber."
                );
            }
        }
    }
}
