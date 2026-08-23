using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace HostLoom.Transport.Kafka;

/// <summary>
/// Owns one long-running consumer loop. The loop survives per-record failures: a single bad
/// record, handler fault, produce failure, or commit failure must never take the consumer
/// down permanently, because nothing restarts it and every later request would time out.
/// A failed record is rewound and retried rather than consumed past, because the partition
/// commits a single position and any later commit would silently skip it.
/// </summary>
/// <remarks>
/// <para>
/// Commit responsibility is split. The <c>handler</c> commits on success, because the offset must
/// not advance until the reply has actually been produced. This loop commits only when it gives up
/// on a record — a malformed one, or one past the redelivery cap — so that a skip is durable.
/// </para>
/// <para>
/// Takes <see cref="IConsumer{TKey,TValue}"/> rather than building one, so the loop can be
/// driven by a fake consumer in tests without a broker.
/// </para>
/// </remarks>
internal sealed class ConsumerSubscription : IAsyncDisposable
{
    internal static readonly TimeSpan DefaultConsumeFailureBackoff = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// How many times one record is rewound and re-consumed before it is skipped. A partition
    /// tracks a single committed position, so a record that keeps failing blocks every later
    /// request behind it; the cap trades that stall for an explicit, logged drop.
    /// This is broker redelivery, a separate layer from the in-process retry that
    /// <c>ConfigureReceivePipeline</c> applies within a single delivery.
    /// </summary>
    internal const int MaxRedeliveryAttempts = 5;

    private readonly IConsumer<string, byte[]> _consumer;
    private readonly string _topic;
    private readonly ILogger _logger;
    private readonly TimeSpan _backoff;
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _loop;

    private ConsumerSubscription(
        IConsumer<string, byte[]> consumer,
        string topic,
        Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler,
        ILogger logger,
        TimeSpan backoff)
    {
        _consumer = consumer;
        _topic = topic;
        _logger = logger;
        _backoff = backoff;
        _loop = Task.Factory.StartNew(
            () => RunAsync(handler),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();
    }

    public static ConsumerSubscription Start(
        IConsumer<string, byte[]> consumer,
        string topic,
        Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler,
        ILogger logger,
        TimeSpan? backoff = null) =>
        new(consumer, topic, handler, logger, backoff ?? DefaultConsumeFailureBackoff);

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

                if (attempts >= MaxRedeliveryAttempts)
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
            await Task.Delay(_backoff, _stopping.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
