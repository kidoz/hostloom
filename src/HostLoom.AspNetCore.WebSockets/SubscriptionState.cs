using System.Net.WebSockets;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class SubscriptionState
{
    public SubscriptionState(
        Guid streamId,
        string topic,
        string? key,
        int initialCredit,
        CancellationToken cancellationToken = default
    )
    {
        StreamId = streamId;
        Topic = topic;
        Key = key;
        _credit = initialCredit;
        _snapshotCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        SnapshotCancellationToken = _snapshotCancellation.Token;
    }

    private readonly Lock _gate = new();
    private readonly Queue<OutboundFrame> _bufferedLiveEvents = [];
    private readonly SemaphoreSlim _creditAvailable = new(0, 1);
    private readonly CancellationTokenSource _snapshotCancellation;
    private int _credit;
    private bool _initializationFinished;
    private bool _canceling;
    private bool _sourceDisposed;
    private bool _creditDisposed;
    private long _lastAcknowledged;
    private bool _initializing = true;
    private bool _stopped;

    public Guid StreamId { get; }

    public string Topic { get; }

    public string? Key { get; }

    public long LastAcknowledged => Interlocked.Read(ref _lastAcknowledged);

    public CancellationToken SnapshotCancellationToken { get; }

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

            if (!outbound.TryReserve(payload, messageType, Topic, out frame))
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
                ReleaseBuffered(release, "subscription_stopped");
                return true;
            }

            while (_bufferedLiveEvents.TryDequeue(out var frame))
            {
                if (!TryConsumeCredit())
                {
                    release(frame);
                    WebSocketDiagnostics.EventDropped(Topic, "no_credit");
                    continue;
                }

                if (!write(frame))
                {
                    WebSocketDiagnostics.EventDropped(Topic, "queue_unavailable");
                    ReleaseBuffered(release, "queue_unavailable");
                    FinishInitialization();
                    return false;
                }
            }

            FinishInitialization();
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
            ReleaseBuffered(release, "subscription_stopped");
            if (_initializationFinished)
            {
                DisposeFinishedResources();
                return;
            }
            _canceling = true;
        }

        try
        {
            _snapshotCancellation.Cancel();
        }
        finally
        {
            lock (_gate)
            {
                _canceling = false;
                DisposeFinishedResources();
            }
        }
    }

    // Called by initialization's finally, or by the rejecting path when it never started.
    public void InitializationFinished()
    {
        lock (_gate)
        {
            _initializationFinished = true;
            DisposeFinishedResources();
        }
    }

    private void DisposeFinishedResources()
    {
        if (!_initializationFinished || _canceling)
            return;
        if (!_sourceDisposed)
        {
            _snapshotCancellation.Dispose();
            _sourceDisposed = true;
        }
        if (_stopped && !_creditDisposed)
        {
            _creditAvailable.Dispose();
            _creditDisposed = true;
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

        lock (_gate)
        {
            if (_stopped)
                return false;
            while (true)
            {
                var current = Volatile.Read(ref _credit);
                if (amount > maximum - current)
                {
                    return false;
                }

                if (Interlocked.CompareExchange(ref _credit, current + amount, current) == current)
                {
                    if (current == 0 && Volatile.Read(ref _initializing))
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

    private void ReleaseBuffered(Action<OutboundFrame> release, string reason)
    {
        while (_bufferedLiveEvents.TryDequeue(out var frame))
        {
            release(frame);
            WebSocketDiagnostics.EventDropped(Topic, reason);
        }
    }

    private void FinishInitialization()
    {
        Volatile.Write(ref _initializing, false);
        _creditAvailable.Wait(0);
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
