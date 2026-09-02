using System.Net.WebSockets;
using System.Threading.Channels;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class ByteBoundedOutboundQueue(int maximumBytes, int maximumFrames)
{
    private readonly Channel<OutboundFrame> _channel = Channel.CreateBounded<OutboundFrame>(
        new BoundedChannelOptions(maximumFrames)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        }
    );
    private long _queuedBytes;
    private int _queuedFrames;

    public ChannelReader<OutboundFrame> Reader => _channel.Reader;

    public bool TryWrite(byte[] payload, WebSocketMessageType messageType)
    {
        return TryReserve(payload, messageType, out var frame) && TryWriteReserved(frame);
    }

    public bool TryReserve(
        byte[] payload,
        WebSocketMessageType messageType,
        out OutboundFrame frame
    )
    {
        ArgumentNullException.ThrowIfNull(payload);
        frame = default;
        var frames = Interlocked.Increment(ref _queuedFrames);
        if (frames > maximumFrames)
        {
            Interlocked.Decrement(ref _queuedFrames);
            return false;
        }

        var queued = Interlocked.Add(ref _queuedBytes, payload.Length);
        if (queued > maximumBytes)
        {
            Interlocked.Add(ref _queuedBytes, -payload.Length);
            Interlocked.Decrement(ref _queuedFrames);
            return false;
        }

        frame = new OutboundFrame(payload, messageType);
        return true;
    }

    public bool TryWriteReserved(OutboundFrame frame)
    {
        if (_channel.Writer.TryWrite(frame))
        {
            return true;
        }

        Release(frame);
        return false;
    }

    public void Release(OutboundFrame frame)
    {
        Interlocked.Add(ref _queuedBytes, -frame.Payload.Length);
        Interlocked.Decrement(ref _queuedFrames);
    }

    public void Complete() => _channel.Writer.TryComplete();
}

internal readonly record struct OutboundFrame(byte[] Payload, WebSocketMessageType MessageType);
