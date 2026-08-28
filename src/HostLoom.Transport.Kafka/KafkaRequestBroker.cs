using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace HostLoom.Transport.Kafka;

public sealed class KafkaRequestBroker : IRequestBroker, IEventBroker
{
    private const string CorrelationHeader = "hostloom-correlation-id";
    private const string ReplyToHeader = "hostloom-reply-to";
    private readonly KafkaOptions _options;
    private readonly IProducer<string, byte[]> _producer;
    private readonly ConcurrentDictionary<
        Guid,
        TaskCompletionSource<ReadOnlyMemory<byte>>
    > _pending = new();
    private readonly ConcurrentBag<ConsumerSubscription> _subscriptions = [];
    private readonly SemaphoreSlim _replyConsumerGate = new(1, 1);
    private readonly ILogger<KafkaRequestBroker> _logger;
    private readonly Func<ConsumerConfig, IConsumer<string, byte[]>> _consumerFactory;
    private ConsumerSubscription? _replySubscription;
    private bool _disposed;

    public KafkaRequestBroker(
        IOptions<KafkaOptions> options,
        ILogger<KafkaRequestBroker>? logger = null
    )
        : this(options, logger, producer: null, consumerFactory: null) { }

    /// <summary>
    /// Takes the producer and a consumer factory so topology and delivery can be driven by fakes in
    /// tests, without a broker.
    /// </summary>
    internal KafkaRequestBroker(
        IOptions<KafkaOptions> options,
        ILogger<KafkaRequestBroker>? logger,
        IProducer<string, byte[]>? producer,
        Func<ConsumerConfig, IConsumer<string, byte[]>>? consumerFactory
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger ?? NullLogger<KafkaRequestBroker>.Instance;
        _options = options.Value;
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.BootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ConsumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ResponseTopic);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ClientId);

        _consumerFactory =
            consumerFactory ?? (config => new ConsumerBuilder<string, byte[]>(config).Build());
        _producer =
            producer
            ?? new ProducerBuilder<string, byte[]>(
                new ProducerConfig
                {
                    BootstrapServers = _options.BootstrapServers,
                    ClientId = _options.ClientId,
                    EnableIdempotence = _options.EnableIdempotence,
                    Acks = Acks.All,
                }
            ).Build();
    }

    public ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        var consumer = _consumerFactory(
            new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                ClientId = $"{_options.ClientId}-{address.Value}",
                GroupId = $"{_options.ConsumerGroup}.{address.Value}",
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            }
        );
        consumer.Subscribe(address.Value);

        var subscription = ConsumerSubscription.Start(
            consumer,
            address.Value,
            async (record, token) =>
            {
                var correlationId = GetRequiredHeader(record.Message.Headers, CorrelationHeader);
                var replyTo = GetRequiredHeader(record.Message.Headers, ReplyToHeader);
                var response = await handler(record.Message.Value, token).ConfigureAwait(false);

                await _producer
                    .ProduceAsync(
                        replyTo,
                        new Message<string, byte[]>
                        {
                            Key = correlationId,
                            Value = response.ToArray(),
                            Headers = new Headers
                            {
                                { CorrelationHeader, Encoding.UTF8.GetBytes(correlationId) },
                            },
                        },
                        token
                    )
                    .ConfigureAwait(false);
                consumer.Commit(record);
            },
            _logger
        );
        _subscriptions.Add(subscription);
        return ValueTask.FromResult<IAsyncDisposable>(subscription);
    }

    /// <summary>
    /// A topic is a Kafka topic; a subscription is a consumer group. Distinct groups each receive
    /// every record, which is the fan-out, while instances sharing a group divide the partitions.
    /// </summary>
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        RequestAddress topic,
        string subscription,
        EventFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        ArgumentNullException.ThrowIfNull(handler);
        cancellationToken.ThrowIfCancellationRequested();

        var consumer = _consumerFactory(
            new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                ClientId = $"{_options.ClientId}-{topic.Value}-{subscription}",
                GroupId = SubscriptionGroup(_options.ConsumerGroup, topic, subscription),
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            }
        );
        consumer.Subscribe(topic.Value);

        var handled = ConsumerSubscription.Start(
            consumer,
            topic.Value,
            async (record, token) =>
            {
                await handler(record.Message.Value, token).ConfigureAwait(false);
                consumer.Commit(record);
            },
            _logger
        );
        _subscriptions.Add(handled);
        return ValueTask.FromResult<IAsyncDisposable>(handled);
    }

    public async ValueTask PublishAsync(
        RequestAddress topic,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // No key, so records round-robin across partitions. Ordering therefore holds within a
        // partition only; key-based partitioning is a contract-level concern the broker cannot infer.
        await _producer
            .ProduceAsync(
                topic.Value,
                new Message<string, byte[]> { Value = frame.ToArray() },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <summary>Consumer group backing one subscription, scoped by the service's group prefix.</summary>
    internal static string SubscriptionGroup(
        string prefix,
        RequestAddress topic,
        string subscription
    ) => $"{prefix}.{topic.Value}.{subscription}";

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await EnsureReplyConsumerAsync(cancellationToken).ConfigureAwait(false);

        var completion = new TaskCompletionSource<ReadOnlyMemory<byte>>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        if (!_pending.TryAdd(requestId, completion))
        {
            throw new InvalidOperationException($"Request id '{requestId}' is already pending.");
        }

        try
        {
            var id = requestId.ToString("N");
            await _producer
                .ProduceAsync(
                    address.Value,
                    new Message<string, byte[]>
                    {
                        Key = id,
                        Value = request.ToArray(),
                        Headers = new Headers
                        {
                            { CorrelationHeader, Encoding.UTF8.GetBytes(id) },
                            { ReplyToHeader, Encoding.UTF8.GetBytes(_options.ResponseTopic) },
                        },
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

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
            completion.TrySetException(new ObjectDisposedException(nameof(KafkaRequestBroker)));
        }

        if (_replySubscription is not null)
        {
            await _replySubscription.DisposeAsync().ConfigureAwait(false);
        }

        while (_subscriptions.TryTake(out var subscription))
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
        }

        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        _replyConsumerGate.Dispose();
    }

    private async ValueTask EnsureReplyConsumerAsync(CancellationToken cancellationToken)
    {
        if (_replySubscription is not null)
        {
            return;
        }

        await _replyConsumerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // CA1508: the second read is the inner half of a double-checked initialisation.
            // Another caller can win the gate between the two reads; dataflow cannot see that.
#pragma warning disable CA1508
            if (_replySubscription is not null)
#pragma warning restore CA1508
            {
                return;
            }

            var consumer = _consumerFactory(
                new ConsumerConfig
                {
                    BootstrapServers = _options.BootstrapServers,
                    ClientId = $"{_options.ClientId}-replies",
                    GroupId = $"{_options.ConsumerGroup}.replies.{_options.ClientId}",
                    EnableAutoCommit = true,
                    // The unique group may not be assigned before the first response is produced.
                    // Earliest avoids losing that race; retained unrelated responses are filtered below.
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                }
            );
            consumer.Subscribe(_options.ResponseTopic);
            _replySubscription = ConsumerSubscription.Start(
                consumer,
                _options.ResponseTopic,
                (record, _) =>
                {
                    var value = GetRequiredHeader(record.Message.Headers, CorrelationHeader);
                    if (
                        Guid.TryParseExact(value, "N", out var id)
                        && _pending.TryGetValue(id, out var completion)
                    )
                    {
                        completion.TrySetResult(record.Message.Value);
                    }

                    return ValueTask.CompletedTask;
                },
                _logger
            );
        }
        finally
        {
            _replyConsumerGate.Release();
        }
    }

    /// <summary>
    /// Reads one required header, classifying both ways it can be absent as a malformed envelope.
    /// </summary>
    /// <remarks>
    /// A record produced without any headers — by an operations tool or a replay — carries a null
    /// collection rather than an empty one. A record with headers but not this one makes
    /// <see cref="Headers.GetLastBytes"/> throw <see cref="KeyNotFoundException"/>; it does not
    /// return null, so <c>TryGetLastBytes</c> is the only way to ask without throwing. Either
    /// escaping as something other than <see cref="MalformedEnvelopeException"/> classifies the
    /// record as a transient fault, costing it a full redelivery and backoff budget before being
    /// discarded, where the consumer loop commits and skips a malformed envelope immediately.
    /// </remarks>
    internal static string GetRequiredHeader(Headers? headers, string name)
    {
        if (headers is null)
        {
            throw new MalformedEnvelopeException(
                $"Kafka message carries no headers, so required header '{name}' is missing."
            );
        }

        return headers.TryGetLastBytes(name, out var value)
            ? Encoding.UTF8.GetString(value)
            : throw new MalformedEnvelopeException(
                $"Kafka message is missing required header '{name}'."
            );
    }
}
