using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace HostLoom.Transport.RabbitMq;

public sealed class RabbitMqRequestBroker : IRequestBroker, IEventBroker
{
    private const string ContentType = "application/vnd.hostloom.envelope+json";

    /// <summary>
    /// How long disposal waits for channels that are still closing before it disposes the
    /// connection, which closes whatever is left.
    /// </summary>
    internal static readonly TimeSpan ChannelCloseBound = TimeSpan.FromSeconds(5);

    private readonly RabbitMqOptions _options;
    private readonly RabbitMqQueueNaming _queueNaming;
    private readonly Func<CancellationToken, ValueTask<IConnection>> _connectionFactory;
#pragma warning disable CA2213
    private readonly SemaphoreSlim _initializationGate = new(1, 1);
#pragma warning restore CA2213
    private readonly CancellationTokenSource _shutdown = new();
    private readonly CancellationToken _shutdownToken;
    private Task? _disposing;
    private readonly PublisherChannelPool _publishers;
    private readonly Lock _disposalGate = new();
    private readonly ILogger<RabbitMqRequestBroker> _logger;
    private readonly TimeProvider _clock;
    private readonly KeyValuePair<string, object?> _clientTag;
    private readonly ConcurrentDictionary<
        Guid,
        TaskCompletionSource<ReadOnlyMemory<byte>>
    > _pending = new();
    private readonly ConcurrentDictionary<RequestAddress, bool> _declaredTopics = new();
    private IConnection? _connection;
    private IChannel? _clientChannel;
    private string? _replyQueue;
    private bool _disposed;

    public RabbitMqRequestBroker(IOptions<RabbitMqOptions> options)
        : this(options, logger: null) { }

    public RabbitMqRequestBroker(
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqRequestBroker>? logger
    )
        : this(options, connectionFactory: null, logger) { }

    /// <summary>
    /// Takes a connection factory so the request/reply correlation can be driven by fake
    /// <see cref="IConnection"/> and <see cref="IChannel"/> instances in tests, without a broker,
    /// and the clock that runs request and publication deadlines and the disposal bound.
    /// </summary>
    internal RabbitMqRequestBroker(
        IOptions<RabbitMqOptions> options,
        Func<CancellationToken, ValueTask<IConnection>>? connectionFactory,
        ILogger<RabbitMqRequestBroker>? logger = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.MaxConcurrentPublishes, 1);
        if (
            _options.PublishTimeout <= TimeSpan.Zero
            || _options.PublishTimeout.TotalMilliseconds > uint.MaxValue - 1
        )
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "PublishTimeout must be a finite positive timer duration."
            );
        ValidateDispatchConcurrency(
            _options.RequestDispatchConcurrency,
            nameof(RabbitMqOptions.RequestDispatchConcurrency)
        );
        ValidateDispatchConcurrency(
            _options.EventDispatchConcurrency,
            nameof(RabbitMqOptions.EventDispatchConcurrency)
        );
        _shutdownToken = _shutdown.Token;
        _logger = logger ?? NullLogger<RabbitMqRequestBroker>.Instance;
        _clock = timeProvider ?? TimeProvider.System;
        _publishers = new PublisherChannelPool(
            _options.MaxConcurrentPublishes,
            this,
            _logger,
            _clock
        );
        if (!Enum.IsDefined(_options.QueueNaming))
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "RabbitMqOptions.QueueNaming must be a defined naming mode."
            );
        _queueNaming = _options.QueueNaming;
        _connectionFactory = connectionFactory ?? ConnectAsync;
        _clientTag = new(RabbitMqDiagnostics.ClientTag, _options.ClientProvidedName);
        RabbitMqDiagnostics.Register(this);
    }

    /// <summary>Requests published and still awaiting a reply; read by the pending-requests gauge.</summary>
    internal int PendingRequestCount => _pending.Count;

    /// <summary>Channels closing in the background; read by the closing-channels gauge.</summary>
    internal int ClosingChannelCount => _publishers.ClosingCount;

    /// <summary>The configured client name every measurement of this broker is tagged with.</summary>
    internal string ClientName => _options.ClientProvidedName;

    /// <summary>
    /// A consumer channel dispatches at most this many deliveries at once, so a value above the
    /// prefetch window could never be reached and would only misstate the real bound.
    /// </summary>
    private void ValidateDispatchConcurrency(ushort concurrency, string name)
    {
        var limit = _options.PrefetchCount == 0 ? ushort.MaxValue : _options.PrefetchCount;
        if (concurrency < 1 || concurrency > limit)
            throw new ArgumentOutOfRangeException(
                nameof(concurrency),
                concurrency,
                $"RabbitMqOptions.{name} must be between 1 and PrefetchCount ({limit})."
            );
    }

    /// <summary>
    /// Consumer channels never publish with confirmations: a listener's reply is answered on the
    /// same channel, and the tracking mode would make every reply await a broker round trip.
    /// </summary>
    private static CreateChannelOptions ConsumerChannelOptions(ushort dispatchConcurrency) =>
        new(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            consumerDispatchConcurrency: dispatchConcurrency
        );

    public async ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        var connection = await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection
            .CreateChannelAsync(
                ConsumerChannelOptions(_options.RequestDispatchConcurrency),
                cancellationToken
            )
            .ConfigureAwait(false);

        var stopping = new CancellationTokenSource();
        try
        {
            await channel
                .QueueDeclareAsync(
                    queue: RabbitMqQueueNames.Request(address.Value, _queueNaming),
                    durable: _options.DurableRequestQueues,
                    exclusive: false,
                    autoDelete: false,
                    arguments: QueueArguments(),
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            await channel
                .BasicQosAsync(0, _options.PrefetchCount, global: false, cancellationToken)
                .ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, delivery) =>
            {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                    delivery.CancellationToken,
                    stopping.Token
                );
                try
                {
                    // Checked before the handler runs: the property is caller-controlled and
                    // becomes a default-exchange routing key, so a request naming any other
                    // queue must not be executed and then answered into it.
                    var replyTo = delivery.BasicProperties.ReplyTo;
                    if (!IsAcceptableReplyQueue(replyTo, _options.AllowNamedReplyQueues))
                    {
                        throw new MalformedEnvelopeException(
                            "RabbitMQ request did not name a server-named reply queue; set RabbitMqOptions.AllowNamedReplyQueues to answer declared queues."
                        );
                    }

                    var response = await handler(delivery.Body, linked.Token).ConfigureAwait(false);

                    var properties = new BasicProperties
                    {
                        ContentType = ContentType,
                        CorrelationId = delivery.BasicProperties.CorrelationId,
                    };
                    await channel
                        .BasicPublishAsync(
                            exchange: string.Empty,
                            routingKey: replyTo,
                            mandatory: true,
                            basicProperties: properties,
                            body: response,
                            cancellationToken: linked.Token
                        )
                        .ConfigureAwait(false);
                    await channel
                        .BasicAckAsync(delivery.DeliveryTag, multiple: false, linked.Token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (linked.IsCancellationRequested)
                {
                    await RequeueAsync(channel, delivery.DeliveryTag).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    RecordRejection(exception);
                    _logger.LogError(
                        new EventId(1401, "RabbitMqDeliveryRejected"),
                        exception,
                        "RabbitMQ delivery {DeliveryTag} failed and is rejected without requeue. Dead-letter exchange configured: {HasDeadLetterExchange}.",
                        delivery.DeliveryTag,
                        !string.IsNullOrEmpty(_options.DeadLetterExchange)
                    );
                    await channel
                        .BasicRejectAsync(
                            delivery.DeliveryTag,
                            requeue: false,
                            CancellationToken.None
                        )
                        .ConfigureAwait(false);
                }
            };

            await channel
                .BasicConsumeAsync(
                    RabbitMqQueueNames.Request(address.Value, _queueNaming),
                    autoAck: false,
                    consumer,
                    cancellationToken
                )
                .ConfigureAwait(false);
            return new ChannelSubscription(channel, stopping);
        }
        catch
        {
            stopping.Dispose();
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Whether a request's <c>ReplyTo</c> may be answered. By default only a server-named queue
    /// (<c>amq.gen-…</c>) or the direct reply-to pseudo-queue qualifies, which is what this
    /// broker's own client sends; with named queues allowed, any well-formed AMQP queue name does.
    /// </summary>
    internal static bool IsAcceptableReplyQueue(
        [NotNullWhen(true)] string? replyTo,
        bool allowNamedQueues
    )
    {
        if (string.IsNullOrEmpty(replyTo) || replyTo.Length > 255)
        {
            return false;
        }

        foreach (var character in replyTo)
        {
            if (char.IsWhiteSpace(character) || char.IsControl(character))
            {
                return false;
            }
        }

        return allowNamedQueues
            || replyTo.StartsWith("amq.gen-", StringComparison.Ordinal)
            || string.Equals(replyTo, "amq.rabbitmq.reply-to", StringComparison.Ordinal);
    }

    /// <summary>
    /// Hands a delivery back to the queue when this process, not the message, is the reason it
    /// was not handled: the listener is stopping or the delivery was cancelled. The nack itself
    /// may fail on a closing channel, in which case the broker requeues the unacknowledged
    /// delivery when the channel goes, so the outcome is the same.
    /// </summary>
    private async ValueTask RequeueAsync(IChannel channel, ulong deliveryTag)
    {
        RabbitMqDiagnostics.DeliveriesRequeued.Add(1, _clientTag);
        try
        {
            await channel
                .BasicNackAsync(deliveryTag, multiple: false, requeue: true, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Closing channel: the broker requeues the unacknowledged delivery itself.
        }
    }

    /// <summary>
    /// Counts a delivery rejected without requeue. A malformed frame or an unacceptable reply
    /// queue is the message's fault; anything else, including a reply or acknowledgement the
    /// channel refused, is counted against the handler side.
    /// </summary>
    private void RecordRejection(Exception exception) =>
        RabbitMqDiagnostics.DeliveriesRejected.Add(
            1,
            _clientTag,
            new(
                RabbitMqDiagnostics.ReasonTag,
                exception is MalformedEnvelopeException ? "malformed" : "handler_failed"
            )
        );

    /// <summary>Queue arguments shared by request and subscription queues; <see langword="null"/> when none apply.</summary>
    private Dictionary<string, object?>? QueueArguments() =>
        string.IsNullOrEmpty(_options.DeadLetterExchange)
            ? null
            : new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["x-dead-letter-exchange"] = _options.DeadLetterExchange,
            };

    /// <summary>
    /// A topic is a fanout exchange; a subscription is a versioned, role-qualified queue bound
    /// to it. Fanout because every subscription must receive every event, and a durable named queue
    /// because a subscription's backlog has to survive the consumer being away.
    /// </summary>
    public async ValueTask<IAsyncDisposable> SubscribeAsync(
        RequestAddress topic,
        string subscription,
        EventFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        ArgumentNullException.ThrowIfNull(handler);

        var connection = await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection
            .CreateChannelAsync(
                ConsumerChannelOptions(_options.EventDispatchConcurrency),
                cancellationToken
            )
            .ConfigureAwait(false);

        var stopping = new CancellationTokenSource();
        try
        {
            await DeclareTopicAsync(channel, topic, cancellationToken).ConfigureAwait(false);

            var queue = RabbitMqQueueNames.Subscription(topic.Value, subscription, _queueNaming);
            await channel
                .QueueDeclareAsync(
                    queue: queue,
                    durable: _options.DurableTopics,
                    exclusive: false,
                    autoDelete: false,
                    arguments: QueueArguments(),
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            await channel
                .QueueBindAsync(
                    queue: queue,
                    exchange: topic.Value,
                    routingKey: string.Empty,
                    arguments: null,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            await channel
                .BasicQosAsync(0, _options.PrefetchCount, global: false, cancellationToken)
                .ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, delivery) =>
            {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                    delivery.CancellationToken,
                    stopping.Token
                );
                try
                {
                    await handler(delivery.Body, linked.Token).ConfigureAwait(false);
                    await channel
                        .BasicAckAsync(delivery.DeliveryTag, multiple: false, linked.Token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (linked.IsCancellationRequested)
                {
                    await RequeueAsync(channel, delivery.DeliveryTag).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    RecordRejection(exception);
                    _logger.LogError(
                        new EventId(1401, "RabbitMqDeliveryRejected"),
                        exception,
                        "RabbitMQ delivery {DeliveryTag} failed and is rejected without requeue. Dead-letter exchange configured: {HasDeadLetterExchange}.",
                        delivery.DeliveryTag,
                        !string.IsNullOrEmpty(_options.DeadLetterExchange)
                    );
                    await channel
                        .BasicRejectAsync(
                            delivery.DeliveryTag,
                            requeue: false,
                            CancellationToken.None
                        )
                        .ConfigureAwait(false);
                }
            };

            await channel
                .BasicConsumeAsync(queue, autoAck: false, consumer, cancellationToken)
                .ConfigureAwait(false);
            return new ChannelSubscription(channel, stopping);
        }
        catch
        {
            stopping.Dispose();
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask PublishAsync(
        RequestAddress topic,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    )
    {
        using var deadline = new CancellationTokenSource(_options.PublishTimeout, _clock);
        using var operation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            deadline.Token,
            _shutdownToken
        );
        try
        {
            await EnsureConnectionAsync(operation.Token).ConfigureAwait(false);
            await PublishFrameAsync(
                    topic,
                    new BasicProperties
                    {
                        ContentType = ContentType,
                        Persistent = _options.DurableTopics,
                    },
                    frame,
                    isEvent: true,
                    operation.Token,
                    deadline.Token
                )
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (_disposed && !cancellationToken.IsCancellationRequested)
        {
            throw new ObjectDisposedException(nameof(RabbitMqRequestBroker));
        }
        catch (OperationCanceledException exception)
            when (deadline.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                "The RabbitMQ event publication deadline elapsed.",
                exception
            );
        }
    }

    /// <summary>
    /// Publishes one frame and records the outcome the broker gave it. A caller that walked away
    /// or a broker being disposed is neither an outcome nor a failure, so neither is counted; the
    /// deadline elapsing is, because that is what an operator watching the broker needs to see.
    /// </summary>
    private async ValueTask PublishFrameAsync(
        RequestAddress address,
        BasicProperties properties,
        ReadOnlyMemory<byte> frame,
        bool isEvent,
        CancellationToken cancellationToken,
        CancellationToken deadline
    )
    {
        var started = Stopwatch.GetTimestamp();
        string? outcome = "failed";
        try
        {
            await PublishFrameCoreAsync(address, properties, frame, isEvent, cancellationToken)
                .ConfigureAwait(false);
            outcome = "confirmed";
        }
        catch (PublishException exception) when (exception.IsReturn)
        {
            outcome = "returned";
            throw;
        }
        catch (OperationCanceledException)
        {
            outcome = deadline.IsCancellationRequested ? "timed_out" : null;
            throw;
        }
        catch (ObjectDisposedException)
        {
            outcome = null;
            throw;
        }
        finally
        {
            if (outcome is not null)
            {
                var tags = new TagList { _clientTag, new(RabbitMqDiagnostics.OutcomeTag, outcome) };
                RabbitMqDiagnostics.Publishes.Add(1, tags);
                RabbitMqDiagnostics.PublishDuration.Record(
                    Stopwatch.GetElapsedTime(started).TotalSeconds,
                    tags
                );
            }
        }
    }

    /// <summary>
    /// Publishes on a channel rented exclusively from the pool. Only a publication that completed
    /// returns its channel; anything else, a return or nack included, discards it, because a late
    /// confirmation may still arrive on it. Neither waits for a channel to close, so the caller's
    /// deadline bounds this whole call.
    /// </summary>
    private async ValueTask PublishFrameCoreAsync(
        RequestAddress address,
        BasicProperties properties,
        ReadOnlyMemory<byte> frame,
        bool isEvent,
        CancellationToken cancellationToken
    )
    {
        var channel = await _publishers
            .RentAsync(OpenPublisherAsync, cancellationToken)
            .ConfigureAwait(false);
        var published = false;
        try
        {
            if (isEvent)
                await DeclareTopicAsync(channel, address, cancellationToken).ConfigureAwait(false);
            await channel
                .BasicPublishAsync(
                    exchange: isEvent ? address.Value : string.Empty,
                    routingKey: isEvent
                        ? string.Empty
                        : RabbitMqQueueNames.Request(address.Value, _queueNaming),
                    mandatory: !isEvent,
                    basicProperties: properties,
                    body: frame,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            published = true;
        }
        finally
        {
            if (published)
                _publishers.Return(channel);
            else
                _publishers.Discard(channel);
        }
    }

    private async ValueTask<IChannel> OpenPublisherAsync(CancellationToken cancellationToken)
    {
        var connection = await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection
            .CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true
                ),
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        using var deadline = new CancellationTokenSource(timeout, _clock);
        using var operation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            deadline.Token,
            _shutdownToken
        );
        var completion = new TaskCompletionSource<ReadOnlyMemory<byte>>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var registered = false;
        try
        {
            await EnsureClientAsync(operation.Token).ConfigureAwait(false);
            if (!_pending.TryAdd(requestId, completion))
                throw new InvalidOperationException(
                    $"Request id '{requestId}' is already pending."
                );
            registered = true;
            var properties = new BasicProperties
            {
                ContentType = ContentType,
                CorrelationId = requestId.ToString("N"),
                ReplyTo = _replyQueue,
            };

            try
            {
                await PublishFrameAsync(
                        address,
                        properties,
                        request,
                        isEvent: false,
                        operation.Token,
                        deadline.Token
                    )
                    .ConfigureAwait(false);
            }
            catch (PublishException exception) when (exception.IsReturn)
            {
                // An unroutable request follows the request/response timeout contract.
            }

            return await completion.Task.WaitAsync(operation.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (_disposed && !cancellationToken.IsCancellationRequested)
        {
            throw new ObjectDisposedException(nameof(RabbitMqRequestBroker));
        }
        catch (OperationCanceledException exception)
            when (deadline.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new RequestTimeoutException(address, timeout, exception);
        }
        finally
        {
            if (registered)
                _pending.TryRemove(requestId, out _);
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_disposalGate)
        {
            _disposed = true;
            return new ValueTask(_disposing ??= DisposeCoreAsync());
        }
    }

    /// <summary>
    /// Cancels every operation, joins the publications still holding a channel, closes the idle
    /// publisher channels and the reply channel, and waits a bounded time for them and for any
    /// discarded channel still closing before disposing the connection, which closes the rest.
    /// </summary>
    private async Task DisposeCoreAsync()
    {
        await _shutdown.CancelAsync().ConfigureAwait(false);
        foreach (var completion in _pending.Values)
            completion.TrySetException(new ObjectDisposedException(nameof(RabbitMqRequestBroker)));
        await _initializationGate.WaitAsync().ConfigureAwait(false);
        try
        {
            List<Exception> failures =
            [
                .. await _publishers
                    .ShutdownAsync(_clientChannel, ChannelCloseBound)
                    .ConfigureAwait(false),
            ];
            if (_connection is not null)
            {
                try
                {
                    await _connection.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    failures.Add(exception);
                }
            }
            if (failures.Count > 0)
                throw new AggregateException(failures);
        }
        finally
        {
            RabbitMqDiagnostics.Unregister(this);
            _initializationGate.Release();
            _shutdown.Dispose();
        }
    }

    private async ValueTask<IConnection> EnsureConnectionAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _initializationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null && IsRecovering(_connection))
            {
                // Automatic recovery owns this connection: it reopens this same object and
                // restores the channels, queues, and consumers created on it. Replacing it here
                // would cancel that and leave every listener and subscription silently dead while
                // publishing carried on against a fresh connection — the worst pairing, because
                // nothing reports it. Handing the closed connection back instead fails this
                // operation loudly and transiently, which a caller can retry.
                return _connection;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
            }

            _connection = await _connectionFactory(cancellationToken).ConfigureAwait(false);
            _connection.QueueNameChangedAfterRecoveryAsync += OnQueueNameChangedAfterRecoveryAsync;
            _connection.RecoverySucceededAsync += OnRecoverySucceededAsync;
            RabbitMqDiagnostics.Connections.Add(
                1,
                _clientTag,
                new(RabbitMqDiagnostics.EventTag, "opened")
            );
            return _connection;
        }
        finally
        {
            _initializationGate.Release();
        }
    }

    /// <summary>
    /// Follows the reply queue across a recovery that renames it.
    /// </summary>
    /// <remarks>
    /// The reply queue is server-named and exclusive, so topology recovery re-declares it under a
    /// new name. Keeping the connection through a drop is what makes this reachable: the recovered
    /// channel reports itself open, so nothing re-declares the reply path, and the cached name
    /// would keep addressing a queue that no longer exists — every request timing out afterwards,
    /// permanently, on a connection that looks healthy.
    /// </remarks>
    private Task OnQueueNameChangedAfterRecoveryAsync(
        object? sender,
        QueueNameChangedAfterRecoveryEventArgs eventArgs
    )
    {
        if (string.Equals(_replyQueue, eventArgs.NameBefore, StringComparison.Ordinal))
        {
            _replyQueue = eventArgs.NameAfter;
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Counts a recovery the client library completed on the connection it owns, which is the
    /// only evidence in this process that a drop happened and was survived.
    /// </summary>
    private Task OnRecoverySucceededAsync(object? sender, AsyncEventArgs eventArgs)
    {
        RabbitMqDiagnostics.Connections.Add(
            1,
            _clientTag,
            new(RabbitMqDiagnostics.EventTag, "recovered")
        );
        return Task.CompletedTask;
    }

    /// <summary>
    /// Reports whether the client library will restore this connection rather than the broker
    /// needing a new one. Only an application-initiated close is final; a peer or library shutdown
    /// is a dropped broker or network, which is exactly what recovery exists for.
    /// </summary>
    private static bool IsRecovering(IConnection connection) =>
        connection.CloseReason is not { Initiator: ShutdownInitiator.Application };

    private async ValueTask EnsureClientAsync(CancellationToken cancellationToken)
    {
        if (_clientChannel is { IsOpen: true } && _replyQueue is not null)
        {
            return;
        }

        var connection = await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false);
        await _initializationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_clientChannel is { IsOpen: true } && _replyQueue is not null)
            {
                return;
            }

            // Build the reply path locally and publish it to the fields only once the consumer
            // is actually running. Assigning earlier lets a failure here leave the client
            // looking initialized while no reply consumer exists, so every request times out.
            var channel = await connection
                .CreateChannelAsync(
                    new CreateChannelOptions(
                        publisherConfirmationsEnabled: true,
                        publisherConfirmationTrackingEnabled: true
                    ),
                    cancellationToken
                )
                .ConfigureAwait(false);
            try
            {
                var queue = await channel
                    .QueueDeclareAsync(
                        queue: string.Empty,
                        durable: false,
                        exclusive: true,
                        autoDelete: true,
                        arguments: null,
                        cancellationToken: cancellationToken
                    )
                    .ConfigureAwait(false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += (_, delivery) =>
                {
                    if (
                        Guid.TryParseExact(delivery.BasicProperties.CorrelationId, "N", out var id)
                        && _pending.TryGetValue(id, out var completion)
                    )
                    {
                        completion.TrySetResult(delivery.Body.ToArray());
                    }

                    return Task.CompletedTask;
                };
                await channel
                    .BasicConsumeAsync(queue.QueueName, autoAck: true, consumer, cancellationToken)
                    .ConfigureAwait(false);

                _clientChannel = channel;
                _replyQueue = queue.QueueName;
            }
            catch
            {
                await channel.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _initializationGate.Release();
        }
    }

    /// <summary>
    /// Declares the topic exchange once per broker instance. Declaration is idempotent, so a race
    /// between two publishers costs a redundant frame and nothing else.
    /// </summary>
    private async ValueTask DeclareTopicAsync(
        IChannel channel,
        RequestAddress topic,
        CancellationToken cancellationToken
    )
    {
        if (_declaredTopics.ContainsKey(topic))
        {
            return;
        }

        await channel
            .ExchangeDeclareAsync(
                exchange: topic.Value,
                type: ExchangeType.Fanout,
                durable: _options.DurableTopics,
                autoDelete: false,
                arguments: null,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        _declaredTopics[topic] = true;
    }

    private async ValueTask<IConnection> ConnectAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            Uri = _options.Uri,
            ClientProvidedName = _options.ClientProvidedName,
            AutomaticRecoveryEnabled = true,
            // Stated rather than left to the default, because the listeners depend on it: it is
            // what re-declares the queues and re-registers the consumers after a broker drop.
            TopologyRecoveryEnabled = true,
        };
        return await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// One listener or subscription. Disposal first cancels the deliveries in flight, so their
    /// handlers stop and the deliveries are requeued rather than rejected, then closes the channel.
    /// </summary>
    private sealed class ChannelSubscription : IAsyncDisposable
    {
        private readonly IChannel _channel;
        private readonly CancellationTokenSource _stopping;
        private int _disposed;

        public ChannelSubscription(IChannel channel, CancellationTokenSource stopping)
        {
            _channel = channel;
            _stopping = stopping;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                await _stopping.CancelAsync().ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    await _channel.DisposeAsync().ConfigureAwait(false);
                }
                finally
                {
                    _stopping.Dispose();
                }
            }
        }
    }
}
