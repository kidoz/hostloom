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
/// (<c>CLIENT TRACKING ON REDIRECT … BCAST PREFIX</c> to this process's subscriber connection,
/// covering namespace entries populated by reads or writes) or keyspace notifications for
/// the filtered prefixes (which need <c>notify-keyspace-events Kg$xe</c> on the server).
/// <c>Auto</c> picks tracking on Redis 6.0 or later and broadcast below that.
/// </summary>
/// <remarks>
/// StackExchange.Redis re-establishes pub/sub subscriptions on its own after a reconnect; the
/// tracking registration is local to each server connection and is re-issued on reconnects and
/// topology changes, including on replicas before promotion. A reconnect also delivers
/// <see cref="CacheInvalidation.Flush"/> to this process's subscribers when
/// <see cref="CacheInvalidationOptions.FlushLocalOnReconnect"/> is set, because nothing published
/// during the outage was received.
/// Reconnects are counted on <c>hostloom.cache.invalidation.resubscribed</c>. The subscription is
/// retried with exponential backoff while Redis is unreachable, and a mode that cannot be enabled
/// after <see cref="RedisOptions.MaxClientCommandRetries"/> attempts leaves the explicit channel
/// as the only fan-out until a later reconnect or topology refresh retries registration.
/// </remarks>
public sealed class RedisCacheInvalidationChannel : ICacheInvalidationChannel, IAsyncDisposable
{
    private const string FormatMarker = "v1";
    private const char FlushLine = '*';

    /// <summary>Largest explicit message accepted, in bytes as published; larger ones are dropped unread.</summary>
    internal const int MaxPayloadBytes = 1_048_576;

    /// <summary>Most keys and tags together in one explicit message.</summary>
    internal const int MaxItems = 10_000;

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
    private Task? _disposing;
    private readonly SemaphoreSlim _trackingRefresh = new(0, 1);
    private readonly List<ChannelMessageQueue> _queues = [];
    private ChannelMessageQueue? _trackingQueue;
    private long _trackingInitialisations;
    private long _malformed;
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
        _connection.SubscriptionRestored += OnSubscriptionRestored;
        _connection.TopologyChanged += OnTopologyChanged;
    }

    /// <summary>The explicit channel name every instance of the namespace subscribes to.</summary>
    public string ChannelName => _channel.ToString();

    /// <summary>Whether the explicit subscription is currently established.</summary>
    public bool IsSubscribed { get; private set; }

    /// <summary>The server-side transport in effect, once the subscription exists.</summary>
    public RedisInvalidationTransport Transport { get; private set; } =
        RedisInvalidationTransport.Pending;

    /// <summary>Successful tracking registration passes, including reconnects and topology refreshes.</summary>
    public long TrackingInitialisations => Interlocked.Read(ref _trackingInitialisations);

    /// <summary>
    /// Explicit messages dropped without dispatch because they exceeded
    /// <see cref="MaxPayloadBytes"/> or <see cref="MaxItems"/>, or carried a key or tag the kernel
    /// would reject. Also counted on <c>hostloom.redis.invalidation.malformed</c>.
    /// </summary>
    public long MalformedMessages => Interlocked.Read(ref _malformed);

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
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
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
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _disposed = 1;
            _handlers.Clear();
            return new ValueTask(_disposing ??= DisposeCoreAsync());
        }
    }

    private async Task DisposeCoreAsync()
    {
        _connection.Restored -= OnRestored;
        _connection.SubscriptionRestored -= OnSubscriptionRestored;
        _connection.TopologyChanged -= OnTopologyChanged;
        await _disposal.CancelAsync().ConfigureAwait(false);
        if (_subscribing is { } pending)
        {
            try
            {
                await pending.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected while a retry delay was pending.
            }
        }

        // Queues belong to this channel even while the externally owned multiplexer
        // is disconnected. Detach them so the SDK cannot restore them after disposal.
        foreach (var queue in _queues)
        {
            try
            {
                await queue.UnsubscribeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
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
        _queues.Clear();
        IsSubscribed = false;

        lock (_gate)
        {
            _trackingRefresh.Dispose();
        }
        _disposal.Dispose();
    }

    /// <summary>Serialises a message; one every subscriber would drop is rejected here instead.</summary>
    /// <exception cref="ArgumentException">More than <see cref="MaxItems"/> keys and tags, or over <see cref="MaxPayloadBytes"/>.</exception>
    internal static string Encode(CacheInvalidation invalidation)
    {
        if (invalidation.Keys.Count + invalidation.Tags.Count > MaxItems)
        {
            throw new ArgumentException("Too many invalidation items.", nameof(invalidation));
        }

        var builder = new StringBuilder(FormatMarker);
        if (invalidation.FlushAll)
        {
            // A one-character line: earlier decoders skip lines shorter than two characters,
            // so an instance that predates the flush ignores it rather than misreading it.
            builder.Append('\n').Append(FlushLine);
        }

        foreach (var key in invalidation.Keys)
        {
            builder.Append('\n').Append('k').Append(key);
        }

        foreach (var tag in invalidation.Tags)
        {
            builder.Append('\n').Append('t').Append(tag);
        }

        var message = builder.ToString();
        if (Encoding.UTF8.GetByteCount(message) > MaxPayloadBytes)
        {
            throw new ArgumentException("Invalidation payload is too large.", nameof(invalidation));
        }

        return message;
    }

    /// <summary>
    /// Parses a message from the explicit channel, or returns null for one that is not
    /// <c>v1</c>, exceeds <see cref="MaxPayloadBytes"/> characters or <see cref="MaxItems"/> keys
    /// and tags, or names a key or tag the kernel itself would reject (empty, longer than
    /// <paramref name="maxKeyLength"/>, or containing whitespace or control characters). The
    /// whole message is dropped on any violation so a publisher cannot smuggle one bad item in
    /// among good ones. Unknown line kinds are skipped for forward compatibility.
    /// </summary>
    internal static CacheInvalidation? Decode(string? message, int maxKeyLength = 512)
    {
        if (message is null || message.Length > MaxPayloadBytes)
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
        var flush = false;
        var items = 0;
        for (var i = 1; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.Length == 1 && line[0] == FlushLine)
            {
                flush = true;
                continue;
            }

            if (line.Length < 2)
            {
                continue;
            }

            var kind = line[0];
            if (kind is not ('k' or 't'))
            {
                continue;
            }

            var item = line[1..];
            if (++items > MaxItems || !CacheKey.IsValid(item, maxKeyLength))
            {
                return null;
            }

            (kind == 'k' ? keys : tags).Add(item);
        }

        return new CacheInvalidation(keys, tags) { FlushAll = flush };
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
                await SubscribeOwnedAsync(
                        multiplexer.GetSubscriber(),
                        _channel,
                        OnExplicitMessage,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
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
                // One worker owns registration and every retry. Coalesce simultaneous
                // socket and topology events, including events during initialisation.
                while (true)
                {
                    await _trackingRefresh.WaitAsync(cancellationToken).ConfigureAwait(false);
                    await InitialiseTransportAsync(multiplexer, cancellationToken)
                        .ConfigureAwait(false);
                }
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
            // The SDK reports a placeholder version for an endpoint it has not connected to,
            // so read it from a node that answered rather than from whichever came first.
            var servers = multiplexer
                .GetEndPoints()
                .Select(endpoint => multiplexer.GetServer(endpoint))
                .ToArray();
            version = (
                servers.FirstOrDefault(server => server.IsConnected) ?? servers.FirstOrDefault()
            )?.Version;
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
            RedisInvalidationTransport.Broadcast
                when Transport == RedisInvalidationTransport.Broadcast => true,
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
                if (_trackingQueue is null)
                {
                    _trackingQueue = await SubscribeOwnedAsync(
                            multiplexer.GetSubscriber(),
                            RedisChannel.Literal(RedisInvalidationDecoder.TrackingChannel),
                            OnTrackingMessage,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                }

                var servers = multiplexer
                    .GetEndPoints()
                    .Select(endpoint => multiplexer.GetServer(endpoint))
                    .Where(server => server.IsConnected && server.ServerType != ServerType.Sentinel)
                    .ToArray();
                if (servers.Length == 0)
                {
                    throw new InvalidOperationException("No connected Redis servers for tracking.");
                }

                await _connection
                    .TrackingRegistrationGate.WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                try
                {
                    await Task.WhenAll(
                            servers.Select(async server =>
                            {
                                // Client IDs and tracking state are local to a server. Register replicas
                                // too: promotion can happen without either of our sockets reconnecting.
                                var list = await server
                                    .ExecuteAsync("CLIENT", "LIST", "TYPE", "pubsub")
                                    .WaitAsync(cancellationToken)
                                    .ConfigureAwait(false);
                                var subscriberId = RedisInvalidationDecoder.FindSubscriberClientId(
                                    (string?)list,
                                    _connection.ClientName,
                                    out var matches
                                );
                                if (subscriberId is null)
                                {
                                    throw new InvalidOperationException(
                                        $"No pub/sub client named '{_connection.ClientName}' on {server.EndPoint} yet."
                                    );
                                }

                                if (matches > 1)
                                {
                                    // Redirecting to the wrong one would send this process's
                                    // invalidations elsewhere while reporting tracking as
                                    // enabled; the explicit channel is the honest fallback.
                                    throw new InvalidOperationException(
                                        $"{matches} pub/sub clients on {server.EndPoint} are named '{_connection.ClientName}'; tracking needs a client name unique to this process (Redis:ClientName, or the ClientName of an externally supplied multiplexer)."
                                    );
                                }

                                await RegisterTrackingAsync(
                                        server,
                                        subscriberId.Value,
                                        cancellationToken
                                    )
                                    .ConfigureAwait(false);
                            })
                        )
                        .ConfigureAwait(false);
                }
                finally
                {
                    _connection.TrackingRegistrationGate.Release();
                }
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

    private async Task RegisterTrackingAsync(
        IServer server,
        long subscriberId,
        CancellationToken token
    )
    {
        var info = (RedisResult[])
            (
                await server
                    .ExecuteAsync("CLIENT", "TRACKINGINFO")
                    .WaitAsync(token)
                    .ConfigureAwait(false)
            )!;
        string[] flags = [];
        string[] prefixes = [];
        long redirect = -1;
        for (var i = 0; i + 1 < info.Length; i += 2)
        {
            switch ((string?)info[i])
            {
                case "flags":
                    flags = (string[])info[i + 1]!;
                    break;
                case "prefixes":
                    prefixes = (string[])info[i + 1]!;
                    break;
                case "redirect":
                    redirect = (long)info[i + 1];
                    break;
            }
        }
        var configured =
            flags.Contains("on")
            && flags.Contains("bcast")
            && flags.Contains("noloop")
            && !flags.Contains("broken_redirect")
            && redirect == subscriberId;
        var covered = prefixes.Any(prefix =>
            _layout.DataPrefix.StartsWith(prefix, StringComparison.Ordinal)
        );
        if (configured && covered)
        {
            return;
        }

        // Redis rejects duplicate PREFIX arguments. Add only a missing prefix when
        // the redirect is healthy. A replaced subscriber requires resetting tracking;
        // preserve every prefix owned by other channels on the same connection.
        var wanted =
            configured ? new[] { _layout.DataPrefix }
            : covered ? prefixes
            : prefixes.Append(_layout.DataPrefix).Distinct().ToArray();
        if (!configured && flags.Contains("on"))
        {
            await server
                .ExecuteAsync("CLIENT", "TRACKING", "OFF")
                .WaitAsync(token)
                .ConfigureAwait(false);
        }
        var arguments = new List<object>
        {
            "TRACKING",
            "ON",
            "REDIRECT",
            subscriberId,
            "BCAST",
            "NOLOOP",
        };
        foreach (var prefix in wanted)
        {
            arguments.Add("PREFIX");
            arguments.Add(prefix);
        }
        // RESP2 redirects invalidations directly to this node's pub/sub socket;
        // no per-node SUBSCRIBE is needed. BCAST covers local write populations.
        await server
            .ExecuteAsync(_connection.Options.DatabaseIndex, "CLIENT", arguments)
            .WaitAsync(token)
            .ConfigureAwait(false);
    }

    private async Task<bool> EnableBroadcastAsync(
        IConnectionMultiplexer multiplexer,
        CancellationToken cancellationToken
    )
    {
        try
        {
            if (
                !await KeyspaceNotificationsEnabledAsync(multiplexer, cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                _logger.LogWarning(
                    new EventId(1319, "RedisKeyspaceNotificationsOff"),
                    "Keyspace notifications are not configured on the Redis server for namespace {Namespace}; the explicit invalidation channel is the only fan-out. The server needs notify-keyspace-events Kg$xe.",
                    _options.Namespace
                );
                return false;
            }

            var subscriber = multiplexer.GetSubscriber();
            foreach (var pattern in KeyspacePatterns())
            {
                await SubscribeOwnedAsync(
                        subscriber,
                        RedisChannel.Pattern(pattern),
                        OnKeyspaceMessage,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
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
                "Could not subscribe to keyspace notifications for namespace {Namespace}; the explicit invalidation channel is the only fan-out. The server needs notify-keyspace-events Kg$xe.",
                _options.Namespace
            );
            return false;
        }
    }

    /// <summary>
    /// Whether the server's <c>notify-keyspace-events</c> covers what broadcast mode listens
    /// for. A subscription alone proves nothing: the server accepts it and simply never
    /// publishes. A server that refuses <c>CONFIG GET</c> (a managed offering, an ACL) cannot be
    /// checked and is trusted, because the setting may well be applied out of band.
    /// </summary>
    private async Task<bool> KeyspaceNotificationsEnabledAsync(
        IConnectionMultiplexer multiplexer,
        CancellationToken cancellationToken
    )
    {
        var server = multiplexer
            .GetEndPoints()
            .Select(endpoint => multiplexer.GetServer(endpoint))
            .FirstOrDefault(candidate =>
                candidate.IsConnected && candidate.ServerType != ServerType.Sentinel
            );
        if (server is null)
        {
            return true;
        }

        string? flags;
        try
        {
            var result = await server
                .ExecuteAsync("CONFIG", "GET", "notify-keyspace-events")
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            flags = RedisInvalidationDecoder.ReadConfigValue(result);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    exception,
                    "notify-keyspace-events could not be read for namespace {Namespace}; keyspace subscriptions proceed unverified.",
                    _options.Namespace
                );
            }

            return true;
        }

        return flags is null || RedisInvalidationDecoder.KeyspaceNotificationsCover(flags);
    }

    private async Task<ChannelMessageQueue> SubscribeOwnedAsync(
        ISubscriber subscriber,
        RedisChannel channel,
        Action<ChannelMessage> handler,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        // The SDK call cannot be cancelled. Join it before the worker stops so a late
        // result is still owned and disposed instead of abandoning its subscription.
        var queue = await subscriber.SubscribeAsync(channel).ConfigureAwait(false);
        _queues.Add(queue);
        cancellationToken.ThrowIfCancellationRequested();
        queue.OnMessage(handler);
        return queue;
    }

    private IReadOnlyList<string> KeyspacePatterns() =>
        RedisInvalidationDecoder.KeyspacePatterns(
            _layout,
            _connection.Options.DatabaseIndex,
            [.. _options.Invalidation.KeyPrefixFilters]
        );

    private void OnExplicitMessage(ChannelMessage message) =>
        HandleExplicitMessage(message.Message);

    /// <summary>
    /// Applies one explicit-channel payload. Anything over <see cref="MaxPayloadBytes"/> is
    /// dropped before it is decoded, so an oversized publish never costs more than a length
    /// check on the single reader that serves every subscriber; a message that decodes to
    /// nothing valid is dropped as a whole. Both are counted, and the content is never logged.
    /// </summary>
    internal void HandleExplicitMessage(RedisValue message)
    {
        var length = message.IsNull ? 0 : message.Length();
        if (
            length > MaxPayloadBytes
            || Decode((string?)message, _options.MaxKeyLength) is not { } invalidation
        )
        {
            Interlocked.Increment(ref _malformed);
            RedisDiagnostics.InvalidationMalformed.Add(
                1,
                new KeyValuePair<string, object?>(RedisDiagnostics.NamespaceTag, _options.Namespace)
            );
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    new EventId(1317, "RedisInvalidationMalformed"),
                    "Dropped a malformed or oversized message of {Length} bytes on {Channel}.",
                    length,
                    ChannelName
                );
            }

            return;
        }

        Dispatch(invalidation);
    }

    private void OnTrackingMessage(ChannelMessage message) =>
        HandleTrackingMessage(message.Message);

    internal void HandleTrackingMessage(RedisValue message)
    {
        // Redis sends a null payload for FLUSHDB and FLUSHALL, without any keys.
        if (message.IsNull)
        {
            Dispatch(CacheInvalidation.Flush);
            return;
        }
        if (RedisInvalidationDecoder.TryParseTrackingKey(_layout, message, out var key))
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
            if (_disposed != 0)
            {
                return;
            }
            handlers = [.. _handlers];
        }

        foreach (var handler in handlers)
        {
            lock (_gate)
            {
                if (_disposed != 0)
                {
                    return;
                }
                try
                {
                    handler(invalidation);
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        new EventId(1318, "RedisInvalidationHandlerFailed"),
                        exception,
                        "An invalidation subscriber failed for {Channel}; continuing with the remaining subscribers.",
                        ChannelName
                    );
                }
            }
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

        RequestTrackingRefresh();
    }

    private void OnSubscriptionRestored(object? sender, EventArgs args)
    {
        // Whatever was published, tracked, or broadcast while the subscription connection was
        // down never arrived. Every in-process entry could be stale, so the subscribers drop
        // them all. Interactive-connection blips lose no invalidation and do not flush.
        if (IsSubscribed && _options.Invalidation.FlushLocalOnReconnect)
        {
            Dispatch(CacheInvalidation.Flush);
        }
    }

    private void OnTopologyChanged(object? sender, EventArgs args) => RequestTrackingRefresh();

    private void RequestTrackingRefresh()
    {
        lock (_gate)
        {
            if (_disposed == 0 && _trackingRefresh.CurrentCount == 0)
            {
                _trackingRefresh.Release();
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
