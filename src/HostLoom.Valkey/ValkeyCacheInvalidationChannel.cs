using System.Globalization;
using HostLoom.Caching;
using HostLoom.Valkey.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>
/// Explicit cache invalidation over a dedicated standalone Pub/Sub connection. Subscription failure
/// restarts with bounded backoff; each acknowledged subscription, including the first, delivers <see cref="CacheInvalidation.Flush"/> to its subscribers when
/// <see cref="CacheInvalidationOptions.FlushLocalOnReconnect"/> is set, and otherwise missing or
/// dropped messages leave staleness bounded by L1 expiry. A periodic probe published to the
/// socket through a private subscription proves it still delivers; one that dies without a close is
/// replaced when its probe does not arrive. Tracking and keyspace-notification modes are not
/// supported by this adapter.
/// </summary>
public sealed class ValkeyCacheInvalidationChannel : ICacheInvalidationChannel, IAsyncDisposable
{
    private readonly ValkeyConnection _connection;
    private readonly string _namespace;
    private readonly bool _flushOnReconnect;
    private readonly TimeSpan _probeInterval;
    private readonly TimeSpan _probeTimeout;
    private readonly string _probeChannelName;
    private readonly TimeProvider _clock;
    private readonly int _maxKeyLength;
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
    private long _probesReceived;
    private long _subscriberResets;
    private TaskCompletionSource? _probeWaiter;

    /// <summary>The shortest gap between two degraded warnings from one channel.</summary>
    internal static readonly TimeSpan WarningInterval = TimeSpan.FromSeconds(30);

    // A clock timestamp can be zero, so "never warned" needs a value no clock returns.
    private const long NeverWarned = long.MinValue;
    private long _lastWarning = NeverWarned;

    /// <summary>An invalidation naming nothing: what the channel publishes to itself as a probe.</summary>
    private static readonly byte[] ProbePayload = ValkeyInvalidationCodec.Encode(
        new CacheInvalidation([], [])
    );

    /// <summary>Borrows the command connection; owns only its subscriber socket and worker.</summary>
    public ValkeyCacheInvalidationChannel(
        ValkeyConnection connection,
        CachingOptions options,
        ILogger<ValkeyCacheInvalidationChannel>? logger = null
    )
        : this(connection, options, logger, TimeProvider.System) { }

    internal ValkeyCacheInvalidationChannel(
        ValkeyConnection connection,
        CachingOptions options,
        ILogger<ValkeyCacheInvalidationChannel>? logger,
        TimeProvider clock
    )
    {
        _clock = clock;
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Namespace);
        if (options.Invalidation.Mode != CacheInvalidationMode.Auto)
            throw new NotSupportedException(
                "HostLoom.Valkey supports Auto with explicit-channel invalidation only; tracking and keyspace notifications are not implemented."
            );
        _connection = connection;
        _namespace = options.Namespace;
        _flushOnReconnect = options.Invalidation.FlushLocalOnReconnect;
        _probeInterval = connection.Settings.InvalidationProbeInterval;
        _probeTimeout = connection.Settings.CommandTimeout;
        _maxKeyLength = options.MaxKeyLength;
        // Pub/Sub ignores SELECT; isolate namespaces that happen to use different logical databases.
        ChannelName =
            options.Namespace
            + ":cache:invalidate:db:"
            + connection.Settings.Connection.Database.ToString(CultureInfo.InvariantCulture);
        _probeChannelName = ChannelName + ":probe:" + Guid.NewGuid().ToString("N");
        _logger = logger ?? NullLogger<ValkeyCacheInvalidationChannel>.Instance;
    }

    /// <summary>Database-qualified channel; values are encoded as versioned JSON without reflection.</summary>
    public string ChannelName { get; }

    /// <summary>Whether the current subscriber has acknowledged its subscription and remains connected.</summary>
    public bool IsSubscribed =>
        Volatile.Read(ref _subscribed) != 0 && Volatile.Read(ref _subscriber)?.IsConnected == true;

    /// <summary>Incoming messages dropped due to local subscriber queue overflow.</summary>
    public long DroppedMessages => Interlocked.Read(ref _dropped);

    /// <summary>Probes this channel published to itself and received back.</summary>
    public long ProbesReceived => Interlocked.Read(ref _probesReceived);

    /// <summary>Subscribers replaced because a probe did not arrive in time.</summary>
    public long SubscriberResets => Interlocked.Read(ref _subscriberResets);

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
            long? acknowledgedAt = null;
            try
            {
                var subscriber = await ValkeySubscriber
                    .ConnectAsync(
                        new ValkeySubscriberOptions
                        {
                            Connection = _connection.Settings.Connection,
                            QueueCapacity = _connection.Settings.InvalidationQueueCapacity,
                            MaxSubscriptions = 2,
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
                if (established)
                    CachingDiagnostics.InvalidationResubscribed(_namespace);
                // Even the first successful subscription has a gap while its acknowledgement
                // is pending. Clear entries filled in that gap before reporting readiness.
                if (_flushOnReconnect)
                    Dispatch(CacheInvalidation.Flush);
                Volatile.Write(ref _subscribed, 1);
                established = true;
                _ready.TrySetResult();
                acknowledgedAt = _clock.GetTimestamp();
                long drops = 0;
                using var probeStop = CancellationTokenSource.CreateLinkedTokenSource(token);
                var probeSubscription =
                    _probeInterval > TimeSpan.Zero
                        ? await subscriber
                            .SubscribeAsync(_probeChannelName, token)
                            .ConfigureAwait(false)
                        : null;
                var probeReader = probeSubscription is not null
                    ? ReadProbesAsync(probeSubscription, probeStop.Token)
                    : Task.CompletedTask;
                var probe =
                    _probeInterval > TimeSpan.Zero
                        ? ProbeAsync(subscriber, probeStop.Token)
                        : Task.CompletedTask;
                try
                {
                    await foreach (
                        var message in subscription.ReadAllAsync(token).ConfigureAwait(false)
                    )
                    {
                        RecordDrops(subscription, ref drops);
                        var invalidation = ValkeyInvalidationCodec.Decode(
                            message.Payload,
                            _maxKeyLength
                        );
                        if (invalidation is null)
                            ValkeyDiagnostics.Malformed.Add(1);
                        else if (IsProbe(invalidation))
                        {
                            // Empty peer invalidations have no effect.
                        }
                        else
                            Dispatch(invalidation);
                    }
                }
                finally
                {
                    RecordDrops(subscription, ref drops);
                    await probeStop.CancelAsync().ConfigureAwait(false);
                    await probe.ConfigureAwait(false);
                    await probeReader.ConfigureAwait(false);
                    if (probeSubscription is not null)
                        await probeSubscription.DisposeAsync().ConfigureAwait(false);
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
            // An acknowledgement alone is not recovery: flapping sockets retain their backoff.
            if (
                acknowledgedAt is { } since
                && _clock.GetElapsedTime(since) >= TimeSpan.FromSeconds(30)
            )
                delay = TimeSpan.FromMilliseconds(100);
            try
            {
                await Task.Delay(delay, _clock, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 30_000));
        }
    }

    /// <summary>A message naming no key, no tag, and no flush: a probe, from this or another instance.</summary>
    private static bool IsProbe(CacheInvalidation invalidation) =>
        !invalidation.FlushAll && invalidation.Keys.Count == 0 && invalidation.Tags.Count == 0;

    /// <summary>
    /// Publishes a probe every <see cref="ValkeyOptions.InvalidationProbeInterval"/> over the
    /// command connection and waits for the independent probe reader on the same socket. A publish that fails
    /// proves nothing about the subscriber and is skipped; a published probe that does not
    /// arrive within <see cref="ValkeyOptions.CommandTimeout"/> means the subscriber socket is
    /// dead without having been closed, so the subscriber is disposed, which ends the read loop
    /// and starts recovery with its flush.
    /// </summary>
    private async Task ReadProbesAsync(ValkeySubscription subscription, CancellationToken token)
    {
        try
        {
            await foreach (var message in subscription.ReadAllAsync(token).ConfigureAwait(false))
            {
                if (!message.Payload.Span.SequenceEqual(ProbePayload))
                    continue;
                Interlocked.Increment(ref _probesReceived);
                Volatile.Read(ref _probeWaiter)?.TrySetResult();
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }

    private async Task ProbeAsync(ValkeySubscriber subscriber, CancellationToken token)
    {
        try
        {
            while (true)
            {
                await Task.Delay(_probeInterval, token).ConfigureAwait(false);
                var waiter = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously
                );
                Volatile.Write(ref _probeWaiter, waiter);
                try
                {
                    await _connection
                        .ExecuteAsync(
                            new ValkeyCommand("PUBLISH", _probeChannelName, ProbePayload),
                            token
                        )
                        .ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    continue;
                }

                var arrived = await Task.WhenAny(waiter.Task, Task.Delay(_probeTimeout, token))
                    .ConfigureAwait(false);
                if (arrived == waiter.Task)
                    continue;

                token.ThrowIfCancellationRequested();
                Interlocked.Increment(ref _subscriberResets);
                ValkeyDiagnostics.Failures.Add(1);
                Warn(
                    "A subscriber probe was not delivered in time; replacing the subscriber. Missed invalidations rely on L1 expiry."
                );
                await subscriber.DisposeAsync().ConfigureAwait(false);
                return;
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // The read loop ended or the channel is shutting down.
        }
        finally
        {
            Volatile.Write(ref _probeWaiter, null);
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

    /// <summary>
    /// Logs at most one warning per <see cref="WarningInterval"/> on the injected clock. The
    /// worker, the probe loop and handler failures warn concurrently; the compare-and-swap lets
    /// exactly one caller claim each interval.
    /// </summary>
    internal void Warn(string reason)
    {
        var now = _clock.GetTimestamp();
        var last = Interlocked.Read(ref _lastWarning);
        if (last != NeverWarned && _clock.GetElapsedTime(last, now) < WarningInterval)
            return;
        if (Interlocked.CompareExchange(ref _lastWarning, now, last) != last)
            return;
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
