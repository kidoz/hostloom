using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HostLoom.Transport.RabbitMq;

public sealed class RabbitMqRequestBroker(IOptions<RabbitMqOptions> options) : IRequestBroker
{
    private const string ContentType = "application/vnd.hostloom.envelope+json";
    private readonly RabbitMqOptions _options = options.Value;
    private readonly SemaphoreSlim _initializationGate = new(1, 1);
    private readonly SemaphoreSlim _publishGate = new(1, 1);
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ReadOnlyMemory<byte>>> _pending = new();
    private IConnection? _connection;
    private IChannel? _clientChannel;
    private string? _replyQueue;
    private bool _disposed;

    public async ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken)
    {
        var connection = await EnsureConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        try
        {
            await channel.QueueDeclareAsync(
                queue: address.Value,
                durable: _options.DurableRequestQueues,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await channel.BasicQosAsync(0, _options.PrefetchCount, global: false, cancellationToken).ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, delivery) =>
            {
                try
                {
                    var response = await handler(delivery.Body, delivery.CancellationToken).ConfigureAwait(false);
                    if (string.IsNullOrWhiteSpace(delivery.BasicProperties.ReplyTo))
                    {
                        throw new InvalidDataException("RabbitMQ request did not specify a reply queue.");
                    }

                    var properties = new BasicProperties
                    {
                        ContentType = ContentType,
                        CorrelationId = delivery.BasicProperties.CorrelationId
                    };
                    await channel.BasicPublishAsync(
                        exchange: string.Empty,
                        routingKey: delivery.BasicProperties.ReplyTo,
                        mandatory: true,
                        basicProperties: properties,
                        body: response,
                        cancellationToken: delivery.CancellationToken).ConfigureAwait(false);
                    await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, delivery.CancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    await channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, CancellationToken.None).ConfigureAwait(false);
                }
            };

            await channel.BasicConsumeAsync(address.Value, autoAck: false, consumer, cancellationToken).ConfigureAwait(false);
            return new ChannelSubscription(channel);
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        await EnsureClientAsync(cancellationToken).ConfigureAwait(false);
        var completion = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                ReplyTo = _replyQueue
            };

            await _publishGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _clientChannel!.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: address.Value,
                    mandatory: true,
                    basicProperties: properties,
                    body: request,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _publishGate.Release();
            }

            try
            {
                return await completion.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
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
            if (_connection is not { IsOpen: true })
            {
                if (_connection is not null)
                {
                    await _connection.DisposeAsync().ConfigureAwait(false);
                }

                var factory = new ConnectionFactory
                {
                    Uri = _options.Uri,
                    ClientProvidedName = _options.ClientProvidedName,
                    AutomaticRecoveryEnabled = true
                };
                _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            }

            return _connection;
        }
        finally
        {
            _initializationGate.Release();
        }
    }

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
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            try
            {
                var queue = await channel.QueueDeclareAsync(
                    queue: string.Empty,
                    durable: false,
                    exclusive: true,
                    autoDelete: true,
                    arguments: null,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += (_, delivery) =>
                {
                    if (Guid.TryParseExact(delivery.BasicProperties.CorrelationId, "N", out var id)
                        && _pending.TryGetValue(id, out var completion))
                    {
                        completion.TrySetResult(delivery.Body.ToArray());
                    }

                    return Task.CompletedTask;
                };
                await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer, cancellationToken).ConfigureAwait(false);

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

    private sealed class ChannelSubscription(IChannel channel) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => channel.DisposeAsync();
    }
}
