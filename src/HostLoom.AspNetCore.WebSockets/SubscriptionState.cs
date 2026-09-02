using System.Net.WebSockets;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class SubscriptionState(
    ulong streamId,
    string topic,
    string? key,
    int initialCredit,
    CancellationToken cancellationToken = default
)
{
    private readonly Lock _gate = new();
    private readonly Queue<OutboundFrame> _bufferedLiveEvents = [];
    private readonly SemaphoreSlim _creditAvailable = new(0, 1);
    private readonly CancellationTokenSource _snapshotCancellation =
        CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    private int _credit = initialCredit;
    private long _lastAcknowledged;
    private bool _initializing = true;
    private bool _stopped;

    public ulong StreamId { get; } = streamId;

    public string Topic { get; } = topic;

    public string? Key { get; } = key;

    public long LastAcknowledged => Interlocked.Read(ref _lastAcknowledged);

    public CancellationToken SnapshotCancellationToken => _snapshotCancellation.Token;

    public LiveEventDisposition AcceptLiveEvent(
        ByteBoundedOutboundQueue outbound,
        byte[] payload,
        WebSocketMessageType messageType,
        out OutboundFrame frame
    )
    {
        ArgumentNullException.ThrowIfNull(outbound);
        ArgumentNullException.ThrowIfNull(payload);
        frame = default;
        lock (_gate)
        {
            if (_stopped)
            {
                return LiveEventDisposition.Stopped;
            }

            if (!_initializing && !TryConsumeCredit())
            {
                return LiveEventDisposition.Dropped;
            }

            if (!outbound.TryReserve(payload, messageType, out frame))
            {
                return LiveEventDisposition.CapacityExceeded;
            }

            if (!_initializing)
            {
                return LiveEventDisposition.Active;
            }

            _bufferedLiveEvents.Enqueue(frame);
            return LiveEventDisposition.Buffered;
        }
    }

    public bool CompleteInitialization(
        Func<OutboundFrame, bool> write,
        Action<OutboundFrame> release
    )
    {
        lock (_gate)
        {
            if (_stopped)
            {
                ReleaseBuffered(release);
                return true;
            }

            while (_bufferedLiveEvents.TryDequeue(out var frame))
            {
                if (!TryConsumeCredit())
                {
                    release(frame);
                    continue;
                }

                if (!write(frame))
                {
                    ReleaseBuffered(release);
                    _initializing = false;
                    return false;
                }
            }

            _initializing = false;
            return true;
        }
    }

    public SnapshotWriteDisposition WriteSnapshot(Func<bool> write)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return SnapshotWriteDisposition.Stopped;
            }

            return write() ? SnapshotWriteDisposition.Written : SnapshotWriteDisposition.Failed;
        }
    }

    public void Stop(Action<OutboundFrame> release)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            ReleaseBuffered(release);
        }

        try
        {
            _snapshotCancellation.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // A completed subscription raced with session cleanup.
        }
    }

    public async ValueTask WaitForCreditAsync(CancellationToken cancellationToken)
    {
        while (!TryConsumeCredit())
        {
            await _creditAvailable.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public bool TryConsumeCredit()
    {
        while (true)
        {
            var current = Volatile.Read(ref _credit);
            if (current <= 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _credit, current - 1, current) == current)
            {
                return true;
            }
        }
    }

    public bool TryAddCredit(int amount, int maximum)
    {
        if (amount <= 0)
        {
            return false;
        }

        while (true)
        {
            var current = Volatile.Read(ref _credit);
            if (amount > maximum - current)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _credit, current + amount, current) == current)
            {
                if (current == 0)
                {
                    try
                    {
                        _creditAvailable.Release();
                    }
                    catch (SemaphoreFullException)
                    {
                        // A prior zero-to-positive transition already left a wake-up pending.
                    }
                }

                return true;
            }
        }
    }

    public void Acknowledge(long sequence)
    {
        while (true)
        {
            var current = Interlocked.Read(ref _lastAcknowledged);
            if (
                sequence <= current
                || Interlocked.CompareExchange(ref _lastAcknowledged, sequence, current) == current
            )
            {
                return;
            }
        }
    }

    private void ReleaseBuffered(Action<OutboundFrame> release)
    {
        while (_bufferedLiveEvents.TryDequeue(out var frame))
        {
            release(frame);
        }
    }
}

internal enum LiveEventDisposition
{
    Buffered,
    Active,
    Dropped,
    Stopped,
    CapacityExceeded,
}

internal enum SnapshotWriteDisposition
{
    Written,
    Stopped,
    Failed,
}
