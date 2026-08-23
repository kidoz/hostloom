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

    public ChannelReader<OutboundFrame> Reader => _channel.Reader;

    public bool TryWrite(byte[] payload, WebSocketMessageType messageType)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var queued = Interlocked.Add(ref _queuedBytes, payload.Length);
        if (queued > maximumBytes)
        {
            Interlocked.Add(ref _queuedBytes, -payload.Length);
            return false;
        }

        if (_channel.Writer.TryWrite(new OutboundFrame(payload, messageType)))
        {
            return true;
        }

        Interlocked.Add(ref _queuedBytes, -payload.Length);
        return false;
    }

    public void Release(OutboundFrame frame) =>
        Interlocked.Add(ref _queuedBytes, -frame.Payload.Length);

    public void Complete() => _channel.Writer.TryComplete();
}

internal readonly record struct OutboundFrame(byte[] Payload, WebSocketMessageType MessageType);
