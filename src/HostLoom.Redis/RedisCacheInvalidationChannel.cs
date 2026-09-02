using System.Text;
using HostLoom.Caching;
using HostLoom.Redis.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace HostLoom.Redis;

/// <summary>
/// Cross-instance invalidation on Redis. The explicit pub/sub channel
/// <c>{namespace}:cache:invalidate</c> is always subscribed and carries what
/// <c>RemoveAsync</c> and <c>RemoveByTagAsync</c> publish. On top of it,
/// <see cref="CacheInvalidationOptions.Mode"/> adds one server-side transport: client tracking
/// (<c>CLIENT TRACKING ON REDIRECT</c> to this process's subscriber connection, so the server
/// reports every tracked entry another client modifies or expires) or keyspace notifications for
/// the filtered prefixes (which need <c>notify-keyspace-events Kxe</c> on the server).
/// <c>Auto</c> picks tracking on Redis 6.0 or later and broadcast below that.
/// </summary>
/// <remarks>
/// StackExchange.Redis re-establishes pub/sub subscriptions on its own after a reconnect; the
/// tracking registration is per connection and is re-issued here on <c>ConnectionRestored</c>.
/// Both are counted on <c>hostloom.cache.invalidation.resubscribed</c>. The subscription is
/// retried with exponential backoff while Redis is unreachable, and a mode that cannot be enabled
/// after <see cref="RedisOptions.MaxClientCommandRetries"/> attempts leaves the explicit channel
/// as the only fan-out, logged once.
/// </remarks>
public sealed class RedisCacheInvalidationChannel : ICacheInvalidationChannel, IAsyncDisposable
{
    private const string FormatMarker = "v1";
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    private readonly RedisConnection _connection;
    private readonly CachingOptions _options;
    private readonly RedisKeyLayout _layout;
    private readonly RedisChannel _channel;
    private readonly ILogger _logger;
    private readonly Lock _gate = new();
    private readonly List<Action<CacheInvalidation>> _handlers = [];
    private readonly CancellationTokenSource _disposal = new();
    private Task? _subscribing;
    private Task? _reinitialising;
    private long _trackingInitialisations;
    private int _disposed;

    /// <summary>Creates the channel for the namespace in <paramref name="options"/> over the shared connection.</summary>
    public RedisCacheInvalidationChannel(
        RedisConnection connection,
        CachingOptions options,
        ILogger<RedisCacheInvalidationChannel>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Namespace);
        _connection = connection;
        _options = options;
        _layout = new RedisKeyLayout(
            options.Namespace,
            connection.Options.UseHashTags,
            options.PayloadVersion
        );
        _channel = RedisChannel.Literal(options.Namespace + ":cache:invalidate");
        _logger = logger ?? NullLogger<RedisCacheInvalidationChannel>.Instance;
        _connection.Restored += OnRestored;
    }

    /// <summary>The explicit channel name every instance of the namespace subscribes to.</summary>
    public string ChannelName => _channel.ToString();

    /// <summary>Whether the explicit subscription is currently established.</summary>
    public bool IsSubscribed { get; private set; }

    /// <summary>The server-side transport in effect, once the subscription exists.</summary>
    public RedisInvalidationTransport Transport { get; private set; } =
        RedisInvalidationTransport.Pending;

    /// <summary>Times tracking was enabled, including re-initialisations after a reconnect.</summary>
    public long TrackingInitialisations => Interlocked.Read(ref _trackingInitialisations);

    /// <inheritdoc />
    public async ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(invalidation);
        var multiplexer = await _connection
            .GetMultiplexerAsync(cancellationToken)
            .ConfigureAwait(false);
        await multiplexer
            .GetSubscriber()
            .PublishAsync(_channel, Encode(invalidation))
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public IDisposable Subscribe(Action<CacheInvalidation> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        lock (_gate)
        {
            _handlers.Add(handler);
            _subscribing ??= Task.Run(
                () => SubscribeWithRetryAsync(_disposal.Token),
                CancellationToken.None
            );
        }

        return new Subscription(this, handler);
    }

    /// <summary>The type and the transport in effect, for the cache probe.</summary>
    public override string ToString() =>
        $"{nameof(RedisCacheInvalidationChannel)} ({Transport switch
        {
            RedisInvalidationTransport.Tracking => "tracking",
            RedisInvalidationTransport.Broadcast => "broadcast",
            RedisInvalidationTransport.ExplicitOnly => "explicit channel only",
            _ => "pending",
        }})";

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _connection.Restored -= OnRestored;
        await _disposal.CancelAsync().ConfigureAwait(false);
        foreach (var pending in new[] { _subscribing, _reinitialising })
        {
            if (pending is null)
            {
                continue;
            }

            try
            {
                await pending.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected while a retry delay was pending.
            }
        }

        if (IsSubscribed && _connection.IsConnected)
        {
            try
            {
                var multiplexer = await _connection
                    .GetMultiplexerAsync(CancellationToken.None)
                    .ConfigureAwait(false);
                var subscriber = multiplexer.GetSubscriber();
                await subscriber.UnsubscribeAsync(_channel).ConfigureAwait(false);
                if (Transport == RedisInvalidationTransport.Tracking)
                {
                    await subscriber
                        .UnsubscribeAsync(
                            RedisChannel.Literal(RedisInvalidationDecoder.TrackingChannel)
                        )
                        .ConfigureAwait(false);
                }
                else if (Transport == RedisInvalidationTransport.Broadcast)
                {
                    foreach (var pattern in KeyspacePatterns())
                    {
                        await subscriber
                            .UnsubscribeAsync(RedisChannel.Pattern(pattern))
                            .ConfigureAwait(false);
                    }
                }
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                // The subscription dies with the connection either way.
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug(
                        exception,
                        "Unsubscribe from {Channel} failed during disposal.",
                        ChannelName
                    );
                }
            }
        }

        _disposal.Dispose();
    }

    internal static string Encode(CacheInvalidation invalidation)
    {
        var builder = new StringBuilder(FormatMarker);
        foreach (var key in invalidation.Keys)
        {
            builder.Append('\n').Append('k').Append(key);
        }

        foreach (var tag in invalidation.Tags)
        {
            builder.Append('\n').Append('t').Append(tag);
        }

        return builder.ToString();
    }

    internal static CacheInvalidation? Decode(string? message)
    {
        if (message is null)
        {
            return null;
        }

        var lines = message.Split('\n');
        if (lines.Length == 0 || !string.Equals(lines[0], FormatMarker, StringComparison.Ordinal))
        {
            return null;
        }

        var keys = new List<string>();
        var tags = new List<string>();
        for (var i = 1; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.Length < 2)
            {
                continue;
            }

            switch (line[0])
            {
                case 'k':
                    keys.Add(line[1..]);
                    break;
                case 't':
                    tags.Add(line[1..]);
                    break;
                default:
                    break;
            }
        }

        return new CacheInvalidation(keys, tags);
    }

    private async Task SubscribeWithRetryAsync(CancellationToken cancellationToken)
    {
        var delay = _connection.Options.InitialRetryDelay;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var multiplexer = await _connection
                    .GetMultiplexerAsync(cancellationToken)
                    .ConfigureAwait(false);
                var queue = await multiplexer
                    .GetSubscriber()
                    .SubscribeAsync(_channel)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                queue.OnMessage(OnExplicitMessage);
                IsSubscribed = true;
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(
                        new EventId(1311, "RedisInvalidationSubscribed"),
                        "Subscribed to invalidation channel {Channel}.",
                        ChannelName
                    );
                }

                await InitialiseTransportAsync(multiplexer, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    new EventId(1312, "RedisInvalidationSubscribeFailed"),
                    exception,
                    "Could not subscribe to invalidation channel {Channel}; retrying in {Delay}. Until then the in-process tier relies on expiry.",
                    ChannelName,
                    delay
                );
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = delay * 2 > MaxRetryDelay ? MaxRetryDelay : delay * 2;
            }
        }
    }

    private async Task InitialiseTransportAsync(
        IConnectionMultiplexer multiplexer,
        CancellationToken cancellationToken
    )
    {
        Version? version = null;
        try
        {
            var endpoints = multiplexer.GetEndPoints();
            if (endpoints.Length > 0)
            {
                version = multiplexer.GetServer(endpoints[0]).Version;
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogDebug(exception, "Server version unavailable; assuming pre-6.0.");
        }

        var transport = RedisInvalidationDecoder.Resolve(_options.Invalidation.Mode, version);
        var enabled = transport switch
        {
            RedisInvalidationTransport.Tracking => await EnableTrackingAsync(
                    multiplexer,
                    cancellationToken
                )
                .ConfigureAwait(false),
            RedisInvalidationTransport.Broadcast => await EnableBroadcastAsync(
                    multiplexer,
                    cancellationToken
                )
                .ConfigureAwait(false),
            _ => false,
        };
        Transport = enabled ? transport : RedisInvalidationTransport.ExplicitOnly;
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                new EventId(1314, "RedisInvalidationTransport"),
                "Invalidation for namespace {Namespace} uses {Transport} (Caching:Invalidation:Mode = {Mode}, server {Version}).",
                _options.Namespace,
                Transport,
                _options.Invalidation.Mode,
                version?.ToString() ?? "unknown"
            );
        }
    }

    private async Task<bool> EnableTrackingAsync(
        IConnectionMultiplexer multiplexer,
        CancellationToken cancellationToken
    )
    {
        var delay = _connection.Options.InitialRetryDelay;
        var attempts = _connection.Options.MaxClientCommandRetries + 1;
        for (var attempt = 1; attempt <= attempts; attempt++)
        {
            try
            {
                var subscriber = multiplexer.GetSubscriber();
                var queue = await subscriber
                    .SubscribeAsync(RedisChannel.Literal(RedisInvalidationDecoder.TrackingChannel))
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                queue.OnMessage(OnTrackingMessage);

                var db = multiplexer.GetDatabase(_connection.Options.DatabaseIndex);
                var list = await db.ExecuteAsync("CLIENT", "LIST", "TYPE", "pubsub")
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                var subscriberId = RedisInvalidationDecoder.FindSubscriberClientId(
                    (string?)list,
                    _connection.ClientName
                );
                if (subscriberId is null)
                {
                    throw new InvalidOperationException(
                        $"No pub/sub client named '{_connection.ClientName}' in CLIENT LIST yet."
                    );
                }

                // NOLOOP: this connection's own writes must not evict the in-process entry it
                // has just written; other clients' writes and server-side expiry still do.
                await db.ExecuteAsync(
                        "CLIENT",
                        "TRACKING",
                        "ON",
                        "REDIRECT",
                        subscriberId.Value,
                        "NOLOOP"
                    )
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                Interlocked.Increment(ref _trackingInitialisations);
                return true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                if (attempt == attempts)
                {
                    _logger.LogWarning(
                        new EventId(1315, "RedisTrackingUnavailable"),
                        exception,
                        "Could not enable client tracking for namespace {Namespace} after {Attempts} attempts; the explicit invalidation channel is the only fan-out. An externally supplied multiplexer needs allowAdmin=true for the CLIENT commands.",
                        _options.Namespace,
                        attempts
                    );
                    return false;
                }

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = delay * 2 > MaxRetryDelay ? MaxRetryDelay : delay * 2;
            }
        }

        return false;
    }

    private async Task<bool> EnableBroadcastAsync(
        IConnectionMultiplexer multiplexer,
        CancellationToken cancellationToken
    )
    {
        try
        {
            var subscriber = multiplexer.GetSubscriber();
            foreach (var pattern in KeyspacePatterns())
            {
                var queue = await subscriber
                    .SubscribeAsync(RedisChannel.Pattern(pattern))
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                queue.OnMessage(OnKeyspaceMessage);
            }

            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                new EventId(1316, "RedisBroadcastUnavailable"),
                exception,
                "Could not subscribe to keyspace notifications for namespace {Namespace}; the explicit invalidation channel is the only fan-out. The server needs notify-keyspace-events Kxe.",
                _options.Namespace
            );
            return false;
        }
    }

    private IReadOnlyList<string> KeyspacePatterns() =>
        RedisInvalidationDecoder.KeyspacePatterns(
            _layout,
            _connection.Options.DatabaseIndex,
            [.. _options.Invalidation.KeyPrefixFilters]
        );

    private void OnExplicitMessage(ChannelMessage message)
    {
        if (Decode(message.Message) is { } invalidation)
        {
            Dispatch(invalidation);
        }
    }

    private void OnTrackingMessage(ChannelMessage message)
    {
        if (RedisInvalidationDecoder.TryParseTrackingKey(_layout, message.Message, out var key))
        {
            Dispatch(new CacheInvalidation([key], []));
        }
    }

    private void OnKeyspaceMessage(ChannelMessage message)
    {
        if (
            RedisInvalidationDecoder.TryParseKeyspaceEvent(
                _layout,
                message.Channel,
                message.Message,
                out var key
            )
        )
        {
            Dispatch(new CacheInvalidation([key], []));
        }
    }

    private void Dispatch(CacheInvalidation invalidation)
    {
        Action<CacheInvalidation>[] handlers;
        lock (_gate)
        {
            handlers = [.. _handlers];
        }

        foreach (var handler in handlers)
        {
            handler(invalidation);
        }
    }

    private void OnRestored(object? sender, EventArgs args)
    {
        if (!IsSubscribed)
        {
            return;
        }

        // StackExchange.Redis re-establishes every pub/sub subscription itself; tracking is
        // per connection and has to be registered again on the new one.
        CachingDiagnostics.InvalidationResubscribed(_options.Namespace);
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                new EventId(1313, "RedisInvalidationResubscribed"),
                "Invalidation channel {Channel} re-established after a reconnect.",
                ChannelName
            );
        }

        if (Transport == RedisInvalidationTransport.Tracking)
        {
            lock (_gate)
            {
                _reinitialising = Task.Run(
                    async () =>
                    {
                        var multiplexer = await _connection
                            .GetMultiplexerAsync(_disposal.Token)
                            .ConfigureAwait(false);
                        if (
                            !await EnableTrackingAsync(multiplexer, _disposal.Token)
                                .ConfigureAwait(false)
                        )
                        {
                            Transport = RedisInvalidationTransport.ExplicitOnly;
                        }
                    },
                    CancellationToken.None
                );
            }
        }
    }

    private sealed class Subscription(
        RedisCacheInvalidationChannel owner,
        Action<CacheInvalidation> handler
    ) : IDisposable
    {
        public void Dispose()
        {
            lock (owner._gate)
            {
                owner._handlers.Remove(handler);
            }
        }
    }
}
