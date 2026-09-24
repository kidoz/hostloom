using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace HostLoom.Transport.RabbitMq;

/// <summary>
/// The confirmed publisher channels of one broker. At most <c>capacity</c> publications hold a
/// channel at once, each exclusively. A channel whose publication completed goes back to the
/// idle queue; any other is never reused, because a late confirmation or return may still arrive
/// on it, and is closed in the background instead of by the publication that gave it up.
/// </summary>
/// <remarks>
/// Closing a channel waits for the broker's close-ok, up to the client library's continuation
/// timeout of 20 seconds, and the broker that just failed a publication is the one least likely
/// to answer promptly. Awaiting that close before the caller returned made a one-second
/// <see cref="RabbitMqOptions.PublishTimeout"/> end after twenty-one. So a discarded channel's
/// permit is released at once and its close runs as a tracked task that logs its failure.
/// Channels still closing are capped at <c>capacity</c>: while that many are closing, a
/// publication that needs a new channel waits for one to finish, within its own deadline, so a
/// broker that never answers cannot make the pool open channels without bound.
/// </remarks>
internal sealed class PublisherChannelPool
{
    private readonly int _capacity;
    private readonly object _owner;
    private readonly ChannelCloser _closer;

    // Waiters observe shutdown through their tokens; the semaphore is never disposed.
    private readonly SemaphoreSlim _permits;
    private readonly ConcurrentQueue<IChannel> _idle = new();
    private readonly Lock _gate = new();
    private bool _shutDown;

    public PublisherChannelPool(int capacity, object owner, ILogger logger)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _capacity = capacity;
        _owner = owner;
        _closer = new ChannelCloser("publisher", logger);
        _permits = new SemaphoreSlim(capacity, capacity);
    }

    /// <summary>Channels whose close was started and has not finished; read by the closing-channels gauge.</summary>
    public int ClosingCount => _closer.ClosingCount;

    /// <summary>
    /// Waits for a permit, then hands out an open idle channel or, once fewer than
    /// <c>capacity</c> channels are closing, a new one from <paramref name="open"/>. The caller
    /// owns the channel exclusively until it passes it to <see cref="Return"/> or
    /// <see cref="Discard"/>. Every wait observes <paramref name="cancellationToken"/>.
    /// </summary>
    public async ValueTask<IChannel> RentAsync(
        Func<CancellationToken, ValueTask<IChannel>> open,
        CancellationToken cancellationToken
    )
    {
        await _permits.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_shutDown, _owner);
            }

            while (_idle.TryDequeue(out var idle))
            {
                if (idle.IsOpen)
                {
                    return idle;
                }

                // The connection or the broker closed it while it was idle.
                _ = _closer.Start(idle);
            }

            await _closer
                .WaitUntilFewerThanAsync(_capacity, cancellationToken)
                .ConfigureAwait(false);
            return await open(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _permits.Release();
            throw;
        }
    }

    /// <summary>
    /// Takes back a channel whose publication completed, for the next publication; once the
    /// pool is shutting down it is closed instead. Releases the caller's permit.
    /// </summary>
    public void Return(IChannel channel)
    {
        bool pooled;
        lock (_gate)
        {
            pooled = !_shutDown;
            if (pooled)
            {
                _idle.Enqueue(channel);
            }
        }

        if (!pooled)
        {
            _ = _closer.Start(channel);
        }

        _permits.Release();
    }

    /// <summary>
    /// Gives up a channel whose publication did not complete: it is closed in the background and
    /// never reused, and the caller's permit is released without waiting for the close.
    /// </summary>
    public void Discard(IChannel channel)
    {
        _ = _closer.Start(channel);
        _permits.Release();
    }

    /// <summary>
    /// Stops renting and joins every publication still holding a channel, then starts closing
    /// the idle channels and <paramref name="alsoClose"/>. Returns those closes, which never
    /// fault: each completes with its failure, or with none.
    /// </summary>
    public async Task<IReadOnlyList<Task<Exception?>>> ShutdownAsync(IChannel? alsoClose)
    {
        lock (_gate)
        {
            _shutDown = true;
        }

        // A publication holding a channel observes the owner's shutdown token, so it ends
        // promptly and hands the channel to Return or Discard, which never wait for a close.
        for (var i = 0; i < _capacity; i++)
        {
            await _permits.WaitAsync().ConfigureAwait(false);
        }

        try
        {
            List<Task<Exception?>> closes = [];
            while (_idle.TryDequeue(out var idle))
            {
                closes.Add(_closer.Start(idle, logFailure: false));
            }

            if (alsoClose is not null)
            {
                closes.Add(_closer.Start(alsoClose, logFailure: false));
            }

            return closes;
        }
        finally
        {
            // A caller that raced shutdown takes a permit and is refused as disposed, not left waiting.
            _permits.Release(_capacity);
        }
    }

    /// <summary>
    /// Whether every publisher channel close still running, discarded channels included, finished
    /// before <paramref name="deadline"/> was cancelled.
    /// </summary>
    public ValueTask<bool> WaitForClosesAsync(CancellationToken deadline) =>
        _closer.WaitForAllAsync(deadline);
}
