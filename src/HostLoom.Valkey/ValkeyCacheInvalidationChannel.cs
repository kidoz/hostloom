using System.Globalization;
using HostLoom.Caching;
using HostLoom.Valkey.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>
/// Explicit cache invalidation over a dedicated standalone Pub/Sub connection. Subscription failure
/// restarts with bounded backoff; missing or dropped messages leave staleness bounded by L1 expiry.
/// Tracking and keyspace-notification modes are not supported by this adapter.
/// </summary>
public sealed class ValkeyCacheInvalidationChannel : ICacheInvalidationChannel, IAsyncDisposable
{
    private readonly ValkeyConnection _connection;
    private readonly string _namespace;
    private readonly ILogger _logger;
    private readonly Lock _gate = new();
    private readonly List<Action<CacheInvalidation>> _handlers = [];
    private readonly CancellationTokenSource _shutdown = new();
    private readonly TaskCompletionSource _ready = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private Task? _worker;
    private ValkeySubscriber? _subscriber;
    private bool _disposed;
    private int _subscribed;
    private long _dropped;
    private long _lastWarning;

    /// <summary>Borrows the command connection; owns only its subscriber socket and worker.</summary>
    public ValkeyCacheInvalidationChannel(
        ValkeyConnection connection,
        CachingOptions options,
        ILogger<ValkeyCacheInvalidationChannel>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Namespace);
        if (options.Invalidation.Mode != CacheInvalidationMode.Auto)
            throw new NotSupportedException(
                "HostLoom.Valkey supports Auto with explicit-channel invalidation only; tracking and keyspace notifications are not implemented."
            );
        _connection = connection;
        _namespace = options.Namespace;
        // Pub/Sub ignores SELECT; isolate namespaces that happen to use different logical databases.
        ChannelName =
            options.Namespace
            + ":cache:invalidate:db:"
            + connection.Settings.Connection.Database.ToString(CultureInfo.InvariantCulture);
        _logger = logger ?? NullLogger<ValkeyCacheInvalidationChannel>.Instance;
    }

    /// <summary>Database-qualified channel; values are encoded as versioned JSON without reflection.</summary>
    public string ChannelName { get; }

    /// <summary>Whether the current subscriber has acknowledged its subscription and remains connected.</summary>
    public bool IsSubscribed =>
        Volatile.Read(ref _subscribed) != 0 && Volatile.Read(ref _subscriber)?.IsConnected == true;

    /// <summary>Incoming messages dropped due to local subscriber queue overflow.</summary>
    public long DroppedMessages => Interlocked.Read(ref _dropped);

    /// <summary>Starts recovery and waits for the first acknowledged subscription. Caller cancellation only ends its wait.</summary>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            StartWorker();
        }
        return _ready.Task.WaitAsync(cancellationToken);
    }

    /// <inheritdoc />
    public IDisposable Subscribe(Action<CacheInvalidation> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (_gate)
        {
            StartWorker();
            _handlers.Add(handler);
        }
        return new Subscription(this, handler);
    }

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken = default
    )
    {
        lock (_gate)
            ObjectDisposedException.ThrowIf(_disposed, this);
        var payload = ValkeyInvalidationCodec.Encode(invalidation);
        try
        {
            await _connection
                .ExecuteAsync(new ValkeyCommand("PUBLISH", ChannelName, payload), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.Cache(exception, "publish invalidation");
        }
    }

    private void StartWorker()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _worker ??= Task.Run(RunAsync, CancellationToken.None);
    }

    private async Task RunAsync()
    {
        var token = _shutdown.Token;
        var delay = TimeSpan.FromMilliseconds(100);
        var established = false;
        while (!token.IsCancellationRequested)
        {
            try
            {
                var subscriber = await ValkeySubscriber
                    .ConnectAsync(
                        new ValkeySubscriberOptions
                        {
                            Connection = _connection.Settings.Connection,
                            QueueCapacity = _connection.Settings.InvalidationQueueCapacity,
                            MaxSubscriptions = 1,
                            OperationTimeout = _connection.Settings.CommandTimeout,
                            // This worker owns recovery, including retry after SDK recovery would be exhausted.
                            EnableReconnect = false,
                        },
                        token
                    )
                    .ConfigureAwait(false);
                await using var subscriberLifetime = subscriber.ConfigureAwait(false);
                Volatile.Write(ref _subscriber, subscriber);
                var subscription = await subscriber
                    .SubscribeAsync(ChannelName, token)
                    .ConfigureAwait(false);
                await using var subscriptionLifetime = subscription.ConfigureAwait(false);
                Volatile.Write(ref _subscribed, 1);
                if (established)
                    CachingDiagnostics.InvalidationResubscribed(_namespace);
                established = true;
                _ready.TrySetResult();
                delay = TimeSpan.FromMilliseconds(100);
                long drops = 0;
                try
                {
                    await foreach (
                        var message in subscription.ReadAllAsync(token).ConfigureAwait(false)
                    )
                    {
                        RecordDrops(subscription, ref drops);
                        if (ValkeyInvalidationCodec.Decode(message.Payload) is { } invalidation)
                            Dispatch(invalidation);
                        else
                            ValkeyDiagnostics.Malformed.Add(1);
                    }
                }
                finally
                {
                    RecordDrops(subscription, ref drops);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                ValkeyDiagnostics.Failures.Add(1);
                Warn(
                    $"Subscription failed ({exception.GetType().Name}); retrying. Missed invalidations rely on L1 expiry."
                );
            }
            finally
            {
                Volatile.Write(ref _subscribed, 0);
                Volatile.Write(ref _subscriber, null);
            }
            try
            {
                await Task.Delay(delay, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 30_000));
        }
    }

    private void RecordDrops(ValkeySubscription subscription, ref long observed)
    {
        var total = subscription.DroppedMessages;
        var delta = total - observed;
        if (delta <= 0)
            return;
        observed = total;
        Interlocked.Add(ref _dropped, delta);
        ValkeyDiagnostics.Dropped.Add(delta);
        Warn("Subscriber queue overflow; missed invalidations rely on L1 expiry.");
    }

    private void Dispatch(CacheInvalidation invalidation)
    {
        Action<CacheInvalidation>[] handlers;
        lock (_gate)
            handlers = [.. _handlers];
        foreach (var handler in handlers)
        {
            try
            {
                handler(invalidation);
            }
            catch (Exception)
            {
                ValkeyDiagnostics.HandlerFailures.Add(1);
                Warn("An invalidation handler failed; remaining handlers continue.");
            }
        }
    }

    private void Warn(string reason)
    {
        var now = Environment.TickCount64;
        if (_lastWarning != 0 && now - _lastWarning < 30_000)
            return;
        _lastWarning = now;
        _logger.LogWarning("Valkey invalidation degraded: {Reason}", reason);
    }

    /// <inheritdoc />
    public override string ToString() =>
        "Valkey explicit channel only (best-effort; gaps rely on L1 expiry)";

    /// <summary>Stops and joins the worker and disposes the dedicated subscriber.</summary>
    public async ValueTask DisposeAsync()
    {
        Task? worker;
        lock (_gate)
        {
            if (_disposed)
                return;
            _disposed = true;
            _handlers.Clear();
            worker = _worker;
        }
        await _shutdown.CancelAsync().ConfigureAwait(false);
        _ready.TrySetCanceled(_shutdown.Token);
        if (worker is not null)
            await worker.ConfigureAwait(false);
        _shutdown.Dispose();
    }

    private sealed class Subscription(
        ValkeyCacheInvalidationChannel channel,
        Action<CacheInvalidation> handler
    ) : IDisposable
    {
        public void Dispose()
        {
            lock (channel._gate)
                channel._handlers.Remove(handler);
        }
    }
}
