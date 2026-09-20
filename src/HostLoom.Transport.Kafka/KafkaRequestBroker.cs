using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace HostLoom.Transport.Kafka;

/// <summary>Decides the offsets a newly assigned set of partitions starts from; the client library's rebalance hook.</summary>
internal delegate IEnumerable<TopicPartitionOffset> PartitionsAssignedHandler(
    IConsumer<string, byte[]> consumer,
    List<TopicPartition> partitions
);

/// <summary>Builds a consumer for <paramref name="config"/>, wiring <paramref name="partitionsAssigned"/> when given.</summary>
internal delegate IConsumer<string, byte[]> KafkaConsumerFactory(
    ConsumerConfig config,
    PartitionsAssignedHandler? partitionsAssigned
);

public sealed class KafkaRequestBroker : IRequestBroker, IEventBroker
{
    private const string CorrelationHeader = "hostloom-correlation-id";
    private const string ReplyToHeader = "hostloom-reply-to";
    private static readonly TimeSpan WatermarkQueryTimeout = TimeSpan.FromSeconds(5);
    private readonly KafkaOptions _options;
    private readonly IProducer<string, byte[]> _producer;
    private readonly ConcurrentDictionary<
        Guid,
        TaskCompletionSource<ReadOnlyMemory<byte>>
    > _pending = new();
    private readonly ConcurrentBag<ConsumerSubscription> _subscriptions = [];
    private readonly ConcurrentDictionary<TopicPartition, Offset> _replyOffsets = new();
    private readonly SemaphoreSlim _replyConsumerGate = new(1, 1);
    private readonly TaskCompletionSource _replyConsumerReady = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly ILogger<KafkaRequestBroker> _logger;
    private readonly KafkaConsumerFactory _consumerFactory;
    private readonly TimeProvider _clock;
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
        KafkaConsumerFactory? consumerFactory,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger ?? NullLogger<KafkaRequestBroker>.Instance;
        _options = options.Value;
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.BootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ConsumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ResponseTopic);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ClientId);
        ValidateSecurity(_options);
        _clock = timeProvider ?? TimeProvider.System;

        _consumerFactory = consumerFactory ?? BuildConsumer;
        _producer =
            producer ?? new ProducerBuilder<string, byte[]>(CreateProducerConfig(_options)).Build();
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
            CreateConsumerConfig(
                _options,
                clientId: $"{_options.ClientId}-{address.Value}",
                groupId: $"{_options.ConsumerGroup}.{address.Value}",
                enableAutoCommit: false,
                autoOffsetReset: AutoOffsetReset.Earliest
            ),
            partitionsAssigned: null
        );
        consumer.Subscribe(address.Value);

        var subscription = ConsumerSubscription.Start(
            consumer,
            address.Value,
            async (record, token) =>
            {
                // Everything a caller controls is checked before the handler runs, so a bad
                // request is skipped as malformed rather than executed and then found unanswerable.
                var correlationId = GetRequiredHeader(record.Message.Headers, CorrelationHeader);
                var replyTo = KafkaReplyTopic.Require(
                    GetRequiredHeader(record.Message.Headers, ReplyToHeader),
                    _options.AllowedReplyTopics
                );
                RejectIfStale(record.Message.Timestamp, _options.MaxRequestAge, _clock.GetUtcNow());

                var response = await handler(record.Message.Value, token).ConfigureAwait(false);

                try
                {
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
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    // The handler already ran. Surfacing this as a transient failure would rewind
                    // and run it again for a reply that still cannot be delivered.
                    throw UnroutableReplyException.For(replyTo, exception);
                }

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
            CreateConsumerConfig(
                _options,
                clientId: $"{_options.ClientId}-{topic.Value}-{subscription}",
                groupId: SubscriptionGroup(_options.ConsumerGroup, topic, subscription),
                enableAutoCommit: false,
                autoOffsetReset: AutoOffsetReset.Earliest
            ),
            partitionsAssigned: null
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
        await EnsureReplyConsumerAsync(address, timeout, cancellationToken).ConfigureAwait(false);

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

        _replyConsumerReady.TrySetException(
            new ObjectDisposedException(nameof(KafkaRequestBroker))
        );

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

    /// <summary>
    /// Starts the reply consumer on first use and waits until it has been assigned partitions of
    /// the response topic, bounded by the request timeout. The consumer starts at the end of the
    /// topic, so a request produced before the assignment could have its reply land before the
    /// consumer's start position and be lost; waiting is what makes "latest" safe.
    /// </summary>
    private async ValueTask EnsureReplyConsumerAsync(
        RequestAddress address,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        if (_replySubscription is null)
        {
            await StartReplyConsumerAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_replyConsumerReady.Task.IsCompletedSuccessfully)
        {
            return;
        }

        try
        {
            await _replyConsumerReady
                .Task.WaitAsync(timeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new RequestTimeoutException(
                address,
                timeout,
                new TimeoutException(
                    $"The reply consumer was not assigned any partition of response topic '{_options.ResponseTopic}' within {timeout}; check that the topic exists and the client may read it.",
                    exception
                )
            );
        }
    }

    private async ValueTask StartReplyConsumerAsync(CancellationToken cancellationToken)
    {
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
                CreateConsumerConfig(
                    _options,
                    clientId: $"{_options.ClientId}-replies",
                    groupId: $"{_options.ConsumerGroup}.replies.{_options.ClientId}",
                    enableAutoCommit: true,
                    // The group is unique to this process, so there is never a committed offset
                    // to resume from. Earliest would replay every retained reply on the topic on
                    // each restart; Latest starts at the end, and RequestAsync waits for the
                    // assignment below before producing, so no reply can precede the start.
                    autoOffsetReset: AutoOffsetReset.Latest
                ),
                partitionsAssigned: OnReplyPartitionsAssigned
            );
            consumer.Subscribe(_options.ResponseTopic);
            _replySubscription = ConsumerSubscription.Start(
                consumer,
                _options.ResponseTopic,
                (record, _) =>
                {
                    // This loop and assignment callbacks own reply progress. Even an unrelated
                    // or malformed reply is consumed, so resume at the next record on reassignment.
                    _replyOffsets[record.TopicPartition] = record.Offset + 1;
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
    /// Resolves initial offsets before permitting requests. Reassignments retain local progress;
    /// a partition added after startup is read from the beginning so pending replies survive.
    /// </summary>
    private IEnumerable<TopicPartitionOffset> OnReplyPartitionsAssigned(
        IConsumer<string, byte[]> consumer,
        List<TopicPartition> partitions
    )
    {
        var offsets = new List<TopicPartitionOffset>(partitions.Count);
        try
        {
            foreach (var partition in partitions)
            {
                if (!_replyOffsets.TryGetValue(partition, out var offset))
                {
                    offset = _replyConsumerReady.Task.IsCompletedSuccessfully
                        ? Offset.Beginning
                        : consumer.QueryWatermarkOffsets(partition, WatermarkQueryTimeout).High;
                    if (!_replyConsumerReady.Task.IsCompletedSuccessfully && offset.Value < 0)
                    {
                        throw new InvalidOperationException(
                            "The reply partition has no resolved high watermark."
                        );
                    }
                }

                offsets.Add(new TopicPartitionOffset(partition, offset));
            }
        }
        catch (Exception exception)
        {
            // Never fall back to a deferred End offset: a reply could arrive before it resolves.
            // Initialization fails for callers; recreating the broker starts a fresh attempt.
            _replyConsumerReady.TrySetException(exception);
            throw;
        }

        foreach (var offset in offsets)
        {
            _replyOffsets[offset.TopicPartition] = offset.Offset;
        }

        if (partitions.Count > 0)
        {
            _replyConsumerReady.TrySetResult();
        }

        return offsets;
    }

    private IConsumer<string, byte[]> BuildConsumer(
        ConsumerConfig config,
        PartitionsAssignedHandler? partitionsAssigned
    )
    {
        var builder = new ConsumerBuilder<string, byte[]>(config);
        if (partitionsAssigned is not null)
        {
            builder.SetPartitionsAssignedHandler(
                (consumer, partitions) => partitionsAssigned(consumer, partitions)
            );
        }

        return builder.Build();
    }

    /// <summary>The producer configuration: HostLoom's settings, then the typed security options, then <see cref="KafkaOptions.ConfigureClient"/>.</summary>
    internal static ProducerConfig CreateProducerConfig(KafkaOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId,
            EnableIdempotence = options.EnableIdempotence,
            Acks = Acks.All,
        };
        ApplyClientOptions(config, options);
        return config;
    }

    /// <summary>A consumer configuration in the same layering as <see cref="CreateProducerConfig"/>.</summary>
    internal static ConsumerConfig CreateConsumerConfig(
        KafkaOptions options,
        string clientId,
        string groupId,
        bool enableAutoCommit,
        AutoOffsetReset autoOffsetReset
    )
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = clientId,
            GroupId = groupId,
            EnableAutoCommit = enableAutoCommit,
            AutoOffsetReset = autoOffsetReset,
        };
        ApplyClientOptions(config, options);
        return config;
    }

    /// <summary>
    /// Refuses credentials that would be silently ignored: the client library only sends SASL
    /// settings over a SASL protocol, so a user name without one connects unauthenticated. The
    /// password value never appears in the message.
    /// </summary>
    internal static void ValidateSecurity(KafkaOptions options)
    {
        var hasCredentials =
            !string.IsNullOrEmpty(options.SaslUsername)
            || !string.IsNullOrEmpty(options.SaslPassword)
            || options.SaslMechanism is not null;
        var saslProtocol =
            options.SecurityProtocol is SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl;
        if (hasCredentials && !saslProtocol)
        {
            throw new ArgumentException(
                "KafkaOptions.SaslUsername, SaslPassword, and SaslMechanism require SecurityProtocol SaslPlaintext or SaslSsl; the client library ignores them otherwise.",
                nameof(options)
            );
        }
    }

    private static void ApplyClientOptions(ClientConfig config, KafkaOptions options)
    {
        if (options.SecurityProtocol is { } protocol)
        {
            config.SecurityProtocol = protocol;
        }

        if (options.SaslMechanism is { } mechanism)
        {
            config.SaslMechanism = mechanism;
        }

        if (!string.IsNullOrEmpty(options.SaslUsername))
        {
            config.SaslUsername = options.SaslUsername;
        }

        if (!string.IsNullOrEmpty(options.SaslPassword))
        {
            config.SaslPassword = options.SaslPassword;
        }

        if (!string.IsNullOrEmpty(options.SslCaLocation))
        {
            config.SslCaLocation = options.SslCaLocation;
        }

        options.ConfigureClient?.Invoke(config);
    }

    /// <summary>
    /// Classifies a request record older than <paramref name="maxAge"/> as malformed. Uses the
    /// record's Kafka timestamp because the frame is opaque to the transport; a record without
    /// one is not judged.
    /// </summary>
    internal static void RejectIfStale(Timestamp timestamp, TimeSpan? maxAge, DateTimeOffset now)
    {
        if (maxAge is not { } limit || timestamp.Type == TimestampType.NotAvailable)
        {
            return;
        }

        if (now - timestamp.UtcDateTime > limit)
        {
            throw new MalformedEnvelopeException(
                $"Kafka request record is older than KafkaOptions.MaxRequestAge ({limit}); it is not handled."
            );
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
