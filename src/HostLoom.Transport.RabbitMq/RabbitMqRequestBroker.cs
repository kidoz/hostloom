using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HostLoom.Transport.RabbitMq;

public sealed class RabbitMqRequestBroker : IRequestBroker, IEventBroker
{
    private const string ContentType = "application/vnd.hostloom.envelope+json";
    private readonly RabbitMqOptions _options;
    private readonly Func<CancellationToken, ValueTask<IConnection>> _connectionFactory;
    private readonly SemaphoreSlim _initializationGate = new(1, 1);
    private readonly SemaphoreSlim _publishGate = new(1, 1);
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
        : this(options, connectionFactory: null) { }

    /// <summary>
    /// Takes a connection factory so the request/reply correlation can be driven by fake
    /// <see cref="IConnection"/> and <see cref="IChannel"/> instances in tests, without a broker.
    /// </summary>
    internal RabbitMqRequestBroker(
        IOptions<RabbitMqOptions> options,
        Func<CancellationToken, ValueTask<IConnection>>? connectionFactory
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
        _connectionFactory = connectionFactory ?? ConnectAsync;
    }

    public async ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        var connection = await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection
            .CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        try
        {
            await channel
                .QueueDeclareAsync(
                    queue: address.Value,
                    durable: _options.DurableRequestQueues,
                    exclusive: false,
                    autoDelete: false,
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
                try
                {
                    var response = await handler(delivery.Body, delivery.CancellationToken)
                        .ConfigureAwait(false);
                    if (string.IsNullOrWhiteSpace(delivery.BasicProperties.ReplyTo))
                    {
                        throw new MalformedEnvelopeException(
                            "RabbitMQ request did not specify a reply queue."
                        );
                    }

                    var properties = new BasicProperties
                    {
                        ContentType = ContentType,
                        CorrelationId = delivery.BasicProperties.CorrelationId,
                    };
                    await channel
                        .BasicPublishAsync(
                            exchange: string.Empty,
                            routingKey: delivery.BasicProperties.ReplyTo,
                            mandatory: true,
                            basicProperties: properties,
                            body: response,
                            cancellationToken: delivery.CancellationToken
                        )
                        .ConfigureAwait(false);
                    await channel
                        .BasicAckAsync(
                            delivery.DeliveryTag,
                            multiple: false,
                            delivery.CancellationToken
                        )
                        .ConfigureAwait(false);
                }
                catch
                {
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
                .BasicConsumeAsync(address.Value, autoAck: false, consumer, cancellationToken)
                .ConfigureAwait(false);
            return new ChannelSubscription(channel);
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// A topic is a fanout exchange; a subscription is a queue named <c>topic.subscription</c> bound
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
            .CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        try
        {
            await DeclareTopicAsync(channel, topic, cancellationToken).ConfigureAwait(false);

            var queue = $"{topic.Value}.{subscription}";
            await channel
                .QueueDeclareAsync(
                    queue: queue,
                    durable: _options.DurableTopics,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
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
                try
                {
                    await handler(delivery.Body, delivery.CancellationToken).ConfigureAwait(false);
                    await channel
                        .BasicAckAsync(
                            delivery.DeliveryTag,
                            multiple: false,
                            delivery.CancellationToken
                        )
                        .ConfigureAwait(false);
                }
                catch
                {
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
            return new ChannelSubscription(channel);
        }
        catch
        {
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
        await EnsureClientAsync(cancellationToken).ConfigureAwait(false);

        var properties = new BasicProperties { ContentType = ContentType };

        await _publishGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Declared inside the gate: an IChannel must not be used concurrently, and this
            // shares _clientChannel with every request and event publish. Declaring outside let
            // an exchange declaration interleave frames with a publish, which closes the
            // connection rather than failing the one operation.
            await DeclareTopicAsync(_clientChannel!, topic, cancellationToken)
                .ConfigureAwait(false);

            // Not mandatory: an event with no subscriptions is dropped, which is ordinary
            // publish/subscribe. Returning it unrouted would make publishing fail whenever
            // nobody happens to be listening.
            await _clientChannel!
                .BasicPublishAsync(
                    exchange: topic.Value,
                    routingKey: string.Empty,
                    mandatory: false,
                    basicProperties: properties,
                    body: frame,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }
        finally
        {
            _publishGate.Release();
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        await EnsureClientAsync(cancellationToken).ConfigureAwait(false);
        var completion = new TaskCompletionSource<ReadOnlyMemory<byte>>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        if (!_pending.TryAdd(requestId, completion))
        {
            throw new InvalidOperationException($"Request id '{requestId}' is already pending.");
        }

        try
        {
            var properties = new BasicProperties
            {
                ContentType = ContentType,
                CorrelationId = requestId.ToString("N"),
                ReplyTo = _replyQueue,
            };

            await _publishGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _clientChannel!
                    .BasicPublishAsync(
                        exchange: string.Empty,
                        routingKey: address.Value,
                        mandatory: true,
                        basicProperties: properties,
                        body: request,
                        cancellationToken: cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            finally
            {
                _publishGate.Release();
            }

            try
            {
                return await completion
                    .Task.WaitAsync(timeout, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (TimeoutException exception)
            {
                throw new RequestTimeoutException(address, timeout, exception);
            }
        }
        finally
        {
            _pending.TryRemove(requestId, out _);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var completion in _pending.Values)
        {
            completion.TrySetException(new ObjectDisposedException(nameof(RabbitMqRequestBroker)));
        }

        _pending.Clear();
        if (_clientChannel is not null)
        {
            await _clientChannel.DisposeAsync().ConfigureAwait(false);
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        _initializationGate.Dispose();
        _publishGate.Dispose();
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
            if (_clientChannel is { IsOpen: true } && _replyQueue is not null)
            {
                return;
            }

            // Build the reply path locally and publish it to the fields only once the consumer
            // is actually running. Assigning earlier lets a failure here leave the client
            // looking initialized while no reply consumer exists, so every request times out.
            var channel = await connection
                .CreateChannelAsync(cancellationToken: cancellationToken)
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

    private sealed class ChannelSubscription(IChannel channel) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => channel.DisposeAsync();
    }
}
