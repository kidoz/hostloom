using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace HostLoom.Transport.Kafka;

public sealed class KafkaRequestBroker : IRequestBroker
{
    private const string CorrelationHeader = "hostloom-correlation-id";
    private const string ReplyToHeader = "hostloom-reply-to";
    private readonly KafkaOptions _options;
    private readonly IProducer<string, byte[]> _producer;
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ReadOnlyMemory<byte>>> _pending = new();
    private readonly ConcurrentBag<Subscription> _subscriptions = [];
    private readonly SemaphoreSlim _replyConsumerGate = new(1, 1);
    private readonly ILogger<KafkaRequestBroker> _logger;
    private Subscription? _replySubscription;
    private bool _disposed;

    public KafkaRequestBroker(IOptions<KafkaOptions> options, ILogger<KafkaRequestBroker>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger ?? NullLogger<KafkaRequestBroker>.Instance;
        _options = options.Value;
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.BootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ConsumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ResponseTopic);
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.ClientId);

        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.ClientId,
            EnableIdempotence = _options.EnableIdempotence,
            Acks = Acks.All
        }).Build();
    }

    public ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = $"{_options.ClientId}-{address.Value}",
            GroupId = $"{_options.ConsumerGroup}.{address.Value}",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(address.Value);

        var subscription = Subscription.Start(consumer, address.Value, async (record, token) =>
        {
            var correlationId = GetRequiredHeader(record.Message.Headers, CorrelationHeader);
            var replyTo = GetRequiredHeader(record.Message.Headers, ReplyToHeader);
            var response = await handler(record.Message.Value, token).ConfigureAwait(false);

            await _producer.ProduceAsync(replyTo, new Message<string, byte[]>
            {
                Key = correlationId,
                Value = response.ToArray(),
                Headers = new Headers { { CorrelationHeader, Encoding.UTF8.GetBytes(correlationId) } }
            }, token).ConfigureAwait(false);
            consumer.Commit(record);
        }, _logger);
        _subscriptions.Add(subscription);
        return ValueTask.FromResult<IAsyncDisposable>(subscription);
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await EnsureReplyConsumerAsync(cancellationToken).ConfigureAwait(false);

        var completion = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(requestId, completion))
        {
            throw new InvalidOperationException($"Request id '{requestId}' is already pending.");
        }

        try
        {
            var id = requestId.ToString("N");
            await _producer.ProduceAsync(address.Value, new Message<string, byte[]>
            {
                Key = id,
                Value = request.ToArray(),
                Headers = new Headers
                {
                    { CorrelationHeader, Encoding.UTF8.GetBytes(id) },
                    { ReplyToHeader, Encoding.UTF8.GetBytes(_options.ResponseTopic) }
                }
            }, cancellationToken).ConfigureAwait(false);

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

            var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                ClientId = $"{_options.ClientId}-replies",
                GroupId = $"{_options.ConsumerGroup}.replies.{_options.ClientId}",
                EnableAutoCommit = true,
                // The unique group may not be assigned before the first response is produced.
                // Earliest avoids losing that race; retained unrelated responses are filtered below.
                AutoOffsetReset = AutoOffsetReset.Earliest
            }).Build();
            consumer.Subscribe(_options.ResponseTopic);
            _replySubscription = Subscription.Start(consumer, _options.ResponseTopic, (record, _) =>
            {
                var value = GetRequiredHeader(record.Message.Headers, CorrelationHeader);
                if (Guid.TryParseExact(value, "N", out var id) && _pending.TryGetValue(id, out var completion))
                {
                    completion.TrySetResult(record.Message.Value);
                }

                return ValueTask.CompletedTask;
            }, _logger);
        }
        finally
        {
            _replyConsumerGate.Release();
        }
    }

    private static string GetRequiredHeader(Headers headers, string name)
    {
        var value = headers.GetLastBytes(name)
            ?? throw new InvalidDataException($"Kafka message is missing required header '{name}'.");
        return Encoding.UTF8.GetString(value);
    }

    /// <summary>
    /// Owns one long-running consumer loop. The loop survives per-record failures: a single bad
    /// record, handler fault, produce failure, or commit failure must never take the consumer
    /// down permanently, because nothing restarts it and every later request would time out.
    /// A failed record is rewound and retried rather than consumed past, because the partition
    /// commits a single position and any later commit would silently skip it.
    /// </summary>
    private sealed class Subscription : IAsyncDisposable
    {
        private static readonly TimeSpan ConsumeFailureBackoff = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// How many times one record is delivered to the handler before it is skipped. A partition
        /// tracks a single committed position, so a record that keeps failing blocks every later
        /// request behind it; the cap trades that stall for an explicit, logged drop.
        /// </summary>
        private const int MaxDeliveryAttempts = 5;

        private readonly IConsumer<string, byte[]> _consumer;
        private readonly string _topic;
        private readonly ILogger _logger;
        private readonly CancellationTokenSource _stopping = new();
        private readonly Task _loop;

        private Subscription(
            IConsumer<string, byte[]> consumer,
            string topic,
            Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler,
            ILogger logger)
        {
            _consumer = consumer;
            _topic = topic;
            _logger = logger;
            _loop = Task.Factory.StartNew(
                () => RunAsync(handler),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }

        public static Subscription Start(
            IConsumer<string, byte[]> consumer,
            string topic,
            Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler,
            ILogger logger) => new(consumer, topic, handler, logger);

        public async ValueTask DisposeAsync()
        {
            if (_stopping.IsCancellationRequested)
            {
                return;
            }

            await _stopping.CancelAsync().ConfigureAwait(false);
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "HostLoom Kafka consumer loop for '{Topic}' faulted before shutdown.", _topic);
            }
            finally
            {
                // Must run even when the loop faulted, or the consumer leaks its group membership
                // and the broker waits out the session timeout before rebalancing.
                try
                {
                    _consumer.Close();
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "HostLoom Kafka consumer for '{Topic}' failed to close cleanly.", _topic);
                }

                _consumer.Dispose();
                _stopping.Dispose();
            }
        }

        private async Task RunAsync(Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler)
        {
            // Delivery attempts for the record currently being retried on each partition. Keyed by
            // partition because an assignment spans several, and each has its own committed offset.
            var retries = new Dictionary<TopicPartition, (long Offset, int Attempts)>();

            while (!_stopping.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> record;
                try
                {
                    record = _consumer.Consume(_stopping.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "HostLoom Kafka consume failed on '{Topic}'; retrying.", _topic);
                    if (!await DelayAsync().ConfigureAwait(false))
                    {
                        break;
                    }

                    continue;
                }

                if (record is null || record.IsPartitionEOF)
                {
                    continue;
                }

                try
                {
                    await handler(record, _stopping.Token).ConfigureAwait(false);
                    retries.Remove(record.TopicPartition);
                }
                catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
                {
                    break;
                }
                catch (InvalidDataException exception)
                {
                    // Poison record: it can never be decoded, so committing past it keeps the
                    // partition moving instead of blocking every later request behind it.
                    _logger.LogError(
                        exception,
                        "HostLoom Kafka record at {Offset} on '{Topic}' is malformed; skipping it.",
                        record.TopicPartitionOffset,
                        _topic);
                    TryCommit(record);
                    retries.Remove(record.TopicPartition);
                }
                catch (Exception exception)
                {
                    // Transient. A partition carries one committed position, and Commit(result)
                    // commits result.Offset + 1, so committing any later record here would advance
                    // the group past this offset and drop it for good. Rewind to this record and
                    // retry it instead of consuming on.
                    var attempts =
                        retries.TryGetValue(record.TopicPartition, out var state) && state.Offset == record.Offset.Value
                            ? state.Attempts + 1
                            : 1;

                    if (attempts >= MaxDeliveryAttempts)
                    {
                        _logger.LogError(
                            exception,
                            "HostLoom Kafka record at {Offset} on '{Topic}' failed {Attempts} times; skipping it.",
                            record.TopicPartitionOffset,
                            _topic,
                            attempts);
                        TryCommit(record);
                        retries.Remove(record.TopicPartition);
                        continue;
                    }

                    _logger.LogError(
                        exception,
                        "HostLoom Kafka record at {Offset} on '{Topic}' failed on attempt {Attempts}; rewinding to retry it.",
                        record.TopicPartitionOffset,
                        _topic,
                        attempts);

                    if (TrySeek(record))
                    {
                        retries[record.TopicPartition] = (record.Offset.Value, attempts);
                    }
                    else
                    {
                        // The partition was most likely revoked. Its offset is still uncommitted,
                        // so whoever is assigned it next redelivers the record.
                        retries.Remove(record.TopicPartition);
                    }

                    if (!await DelayAsync().ConfigureAwait(false))
                    {
                        break;
                    }
                }
            }
        }

        private bool TrySeek(ConsumeResult<string, byte[]> record)
        {
            try
            {
                _consumer.Seek(record.TopicPartitionOffset);
                return true;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "HostLoom Kafka consumer could not rewind to {Offset} on '{Topic}'.",
                    record.TopicPartitionOffset,
                    _topic);
                return false;
            }
        }

        private void TryCommit(ConsumeResult<string, byte[]> record)
        {
            try
            {
                _consumer.Commit(record);
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "HostLoom Kafka commit failed at {Offset} on '{Topic}'.",
                    record.TopicPartitionOffset,
                    _topic);
            }
        }

        private async Task<bool> DelayAsync()
        {
            try
            {
                await Task.Delay(ConsumeFailureBackoff, _stopping.Token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }
    }
}
