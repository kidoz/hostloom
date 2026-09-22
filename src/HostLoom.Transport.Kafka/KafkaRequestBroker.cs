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

/// <summary>
/// Kafka transport: requests and events are records on topics, and each client reads its
/// replies from <see cref="KafkaOptions.ResponseTopic"/> in a consumer group of its own.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="RequestAsync"/> reports a reply consumer that could not start, or a request the
/// producer could not deliver, as <see cref="MessagingTransportException"/> with the client
/// library's exception inside; a failed start is retried by the next request.
/// <see cref="PublishAsync"/> reports a failed delivery the same way. Publication has no
/// deadline of its own beyond the producer's <c>message.timeout.ms</c>.
/// </para>
/// <para>
/// The health probe answers from the reply consumer's local state and never contacts the
/// broker: unhealthy while a start awaits its assignment or after one failed, healthy otherwise.
/// </para>
/// </remarks>
public sealed class KafkaRequestBroker : IRequestBroker, IEventBroker, IBrokerHealthProbe
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
    // No wait handle is used; pending callers must be able to observe disposal safely.
#pragma warning disable CA2213
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
#pragma warning restore CA2213
    private readonly ILogger<KafkaRequestBroker> _logger;
    private readonly KafkaConsumerFactory _consumerFactory;
    private readonly TimeProvider _clock;
    private readonly KafkaReplyConsumer _replies;
    private volatile bool _disposed;
    private readonly Lock _disposalGate = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly CancellationToken _shutdownToken;
    private Task? _disposing;
    private readonly KeyValuePair<string, object?> _clientTag;

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
        _shutdownToken = _shutdown.Token;

        _consumerFactory = consumerFactory ?? BuildConsumer;
        _producer =
            producer ?? new ProducerBuilder<string, byte[]>(CreateProducerConfig(_options)).Build();
        _clientTag = new(KafkaDiagnostics.ClientTag, _options.ClientId);
        _replies = new KafkaReplyConsumer(
            _options,
            _consumerFactory,
            CompleteRequest,
            _logger,
            _clientTag,
            this
        );
        KafkaDiagnostics.Register(this);
    }

    /// <summary>Requests produced and still awaiting a reply; read by the pending-requests gauge.</summary>
    internal int PendingRequestCount => _pending.Count;

    /// <summary>The configured client id every producer-side measurement is tagged with.</summary>
    internal string ClientId => _options.ClientId;

    public async ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
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
            SubscribeOwned(consumer, address.Value, () => _disposed, this, _logger);

            var subscription = ConsumerSubscription.Start(
                consumer,
                address.Value,
                async (record, token) =>
                {
                    // Everything a caller controls is checked before the handler runs, so a bad
                    // request is skipped as malformed rather than executed and then found unanswerable.
                    var correlationId = GetRequiredHeader(
                        record.Message.Headers,
                        CorrelationHeader
                    );
                    var replyTo = KafkaReplyTopic.Require(
                        GetRequiredHeader(record.Message.Headers, ReplyToHeader),
                        _options.AllowedReplyTopics
                    );
                    RejectIfStale(
                        record.Message.Timestamp,
                        _options.MaxRequestAge,
                        _clock.GetUtcNow()
                    );

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
                                        {
                                            CorrelationHeader,
                                            Encoding.UTF8.GetBytes(correlationId)
                                        },
                                    },
                                },
                                token
                            )
                            .ConfigureAwait(false);
                        RecordProduced("reply");
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
                },
                _logger,
                commitOnSuccess: true
            );
            _subscriptions.Add(subscription);
            return subscription;
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    /// <summary>
    /// A topic is a Kafka topic; a subscription is a consumer group. Distinct groups each receive
    /// every record, which is the fan-out, while instances sharing a group divide the partitions.
    /// </summary>
    public async ValueTask<IAsyncDisposable> SubscribeAsync(
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

        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
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
            SubscribeOwned(consumer, topic.Value, () => _disposed, this, _logger);

            var handled = ConsumerSubscription.Start(
                consumer,
                topic.Value,
                async (record, token) =>
                {
                    await handler(record.Message.Value, token).ConfigureAwait(false);
                },
                _logger,
                commitOnSuccess: true,
                retryIndefinitely: true
            );
            _subscriptions.Add(handled);
            return handled;
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async ValueTask PublishAsync(
        RequestAddress topic,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            // No key, so records round-robin across partitions. Ordering therefore holds within
            // a partition only; key-based partitioning is a contract-level concern the broker
            // cannot infer.
            await _producer
                .ProduceAsync(
                    topic.Value,
                    new Message<string, byte[]> { Value = frame.ToArray() },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The client library may cancel its task without the caller's token; this carries it.
            cancellationToken.ThrowIfCancellationRequested();
            throw;
        }
        catch (Exception exception)
            when (_disposed && exception is KafkaException or ObjectDisposedException)
        {
            throw new ObjectDisposedException(nameof(KafkaRequestBroker));
        }
        catch (KafkaException exception)
        {
            throw new MessagingTransportException(
                topic,
                $"Kafka could not deliver the event to '{topic}': {exception.Error.Reason}",
                exception
            );
        }
        RecordProduced("event");
    }

    private void RecordProduced(string kind) =>
        KafkaDiagnostics.Produced.Add(1, _clientTag, new(KafkaDiagnostics.KindTag, kind));

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
        cancellationToken.ThrowIfCancellationRequested();
        using var deadline = new CancellationTokenSource(timeout, _clock);
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            deadline.Token,
            _shutdownToken
        );
        var token = lifetime.Token;
        var registered = false;
        var replyReady = false;
        try
        {
            try
            {
                await _replies.EnsureStartedAsync(token).ConfigureAwait(false);
            }
            catch (Exception exception)
                when (exception is not (OperationCanceledException or ObjectDisposedException))
            {
                throw new MessagingTransportException(
                    address,
                    $"The Kafka reply consumer for response topic '{_options.ResponseTopic}' could not start, so the request was not sent; the next request retries it.",
                    exception
                );
            }
            replyReady = true;
            token.ThrowIfCancellationRequested();
            var completion = new TaskCompletionSource<ReadOnlyMemory<byte>>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            if (!_pending.TryAdd(requestId, completion))
            {
                throw new InvalidOperationException(
                    $"Request id '{requestId}' is already pending."
                );
            }
            registered = true;
            var id = requestId.ToString("N");
            try
            {
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
                        token
                    )
                    .WaitAsync(token)
                    .ConfigureAwait(false);
            }
            catch (KafkaException exception)
            {
                throw new MessagingTransportException(
                    address,
                    $"Kafka could not deliver the request to '{address}': {exception.Error.Reason}",
                    exception
                );
            }
            RecordProduced("request");
            return await completion.Task.WaitAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw;
        }
        catch (Exception exception)
            when (_disposed
                && exception is OperationCanceledException or MessagingTransportException
            )
        {
            throw new ObjectDisposedException(nameof(KafkaRequestBroker));
        }
        catch (OperationCanceledException exception) when (deadline.IsCancellationRequested)
        {
            var detail = replyReady
                ? "The Kafka request deadline elapsed while producing the request or waiting for its reply."
                : $"The reply consumer was not assigned any partition of response topic '{_options.ResponseTopic}' within {timeout}; check that the topic exists and the client may read it.";
            throw new RequestTimeoutException(
                address,
                timeout,
                new TimeoutException(detail, exception)
            );
        }
        finally
        {
            if (registered)
            {
                _pending.TryRemove(requestId, out _);
            }
        }
    }

    /// <summary>Completes the pending request a reply record is correlated to, if it is still waiting.</summary>
    private void CompleteRequest(ConsumeResult<string, byte[]> record)
    {
        var value = GetRequiredHeader(record.Message.Headers, CorrelationHeader);
        if (
            Guid.TryParseExact(value, "N", out var id)
            && _pending.TryGetValue(id, out var completion)
        )
        {
            completion.TrySetResult(record.Message.Value);
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

    private async Task DisposeCoreAsync()
    {
        // Started before anything yields, so the reply consumer is marked disposed at once and a
        // consumer its start is still building is released instead of subscribed.
        var replies = _replies.DisposeAsync().AsTask();
        await _shutdown.CancelAsync().ConfigureAwait(false);
        List<Exception> failures = [];
        try
        {
            await replies.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(exception);
        }
        await _lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            foreach (var completion in _pending.Values)
            {
                completion.TrySetException(new ObjectDisposedException(nameof(KafkaRequestBroker)));
            }
            while (_subscriptions.TryTake(out var subscription))
            {
                try
                {
                    await subscription.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    failures.Add(exception);
                }
            }
            try
            {
                _producer.Flush(TimeSpan.FromSeconds(5));
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
            try
            {
                _producer.Dispose();
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
            if (failures.Count > 0)
            {
                throw new AggregateException(failures);
            }
        }
        finally
        {
            // Waiters may still enter and observe disposal; do not dispose their semaphore.
            KafkaDiagnostics.Unregister(this);
            _lifecycleGate.Release();
            _shutdown.Dispose();
        }
    }

    /// <summary>
    /// Reports the reply consumer's local state and never contacts the broker, so an outage that
    /// begins after the consumer started does not make it unhealthy.
    /// </summary>
    public ValueTask<BrokerHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(
            _disposed
                ? BrokerHealth.Unhealthy(
                    "Kafka reply consumer is unavailable; the next request retries initialization."
                )
                : _replies.Health
        );
    }

    /// <summary>
    /// Subscribes a consumer the caller has just built, disposing it if that fails or its owner
    /// was disposed meanwhile, so a failed start never leaks a group member.
    /// </summary>
    internal static void SubscribeOwned(
        IConsumer<string, byte[]> consumer,
        string topic,
        Func<bool> disposed,
        object owner,
        ILogger logger
    )
    {
        try
        {
            ObjectDisposedException.ThrowIf(disposed(), owner);
            consumer.Subscribe(topic);
        }
        catch
        {
            // Preserve the startup failure even if native cleanup also fails.
            try
            {
                consumer.Dispose();
            }
            catch (Exception exception)
            {
                logger.LogDebug(exception, "Consumer cleanup failed during startup.");
            }
            throw;
        }
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
