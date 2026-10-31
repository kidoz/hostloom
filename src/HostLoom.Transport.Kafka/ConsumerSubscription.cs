using System.Diagnostics.CodeAnalysis;
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
/// The loop commits after successful handling when configured, separately from handler retries.
/// Poison records and unanswerable requests are skipped. Event application failures retain their
/// offset and retry until shutdown rather than being discarded after a fixed attempt count.
/// </para>
/// <para>
/// Takes <see cref="IConsumer{TKey,TValue}"/> rather than building one, so the loop can be
/// driven by a fake consumer in tests without a broker.
/// </para>
/// </remarks>
internal sealed class ConsumerSubscription : IAsyncDisposable
{
    internal static readonly TimeSpan DefaultConsumeFailureBackoff = TimeSpan.FromMilliseconds(500);
    internal static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(5);

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

    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "CleanupAsync disposes this source in a finally block on the background cleanup task, after the consumer loop ends."
    )]
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _loop;
    private readonly bool _commitOnSuccess;
    private readonly bool _retryIndefinitely;
    private readonly KeyValuePair<string, object?> _destinationTag;
    private readonly Lock _disposalGate = new();
    private readonly TimeProvider _clock;
    private readonly TimeSpan _shutdownTimeout;
    private Task? _disposing;

    private ConsumerSubscription(
        IConsumer<string, byte[]> consumer,
        string topic,
        Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler,
        ILogger logger,
        TimeSpan backoff,
        bool commitOnSuccess,
        bool retryIndefinitely,
        TimeSpan shutdownTimeout,
        TimeProvider clock
    )
    {
        _consumer = consumer;
        _topic = topic;
        _logger = logger;
        _backoff = backoff;
        _commitOnSuccess = commitOnSuccess;
        _retryIndefinitely = retryIndefinitely;
        _shutdownTimeout = shutdownTimeout;
        _clock = clock;
        _destinationTag = new(KafkaDiagnostics.DestinationTag, topic);
        _loop = Task
            .Factory.StartNew(
                () => RunAsync(handler),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
            )
            .Unwrap();
    }

    public static ConsumerSubscription Start(
        IConsumer<string, byte[]> consumer,
        string topic,
        Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler,
        ILogger logger,
        TimeSpan? backoff = null,
        bool commitOnSuccess = false,
        bool retryIndefinitely = false,
        TimeSpan? shutdownTimeout = null,
        TimeProvider? timeProvider = null
    ) =>
        new(
            consumer,
            topic,
            handler,
            logger,
            backoff ?? DefaultConsumeFailureBackoff,
            commitOnSuccess,
            retryIndefinitely,
            shutdownTimeout ?? DefaultShutdownTimeout,
            timeProvider ?? TimeProvider.System
        );

    public ValueTask DisposeAsync()
    {
        lock (_disposalGate)
        {
            // Native close and synchronous handler/cancellation callbacks can block too. Keep
            // them off the disposing caller and give every caller the same bounded wait.
            return new ValueTask(_disposing ??= WaitForCleanupAsync(Task.Run(CleanupAsync)));
        }
    }

    private async Task WaitForCleanupAsync(Task cleanup)
    {
        try
        {
            await cleanup.WaitAsync(_shutdownTimeout, _clock).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            if (cleanup.IsCompleted)
            {
                await cleanup.ConfigureAwait(false);
                return;
            }
            _logger.LogWarning(
                KafkaEvents.ConsumerCleanupDeferred,
                "HostLoom Kafka consumer cleanup for '{Topic}' exceeded {Timeout}; cleanup continues in the background.",
                _topic,
                _shutdownTimeout
            );
            ObserveLateFailure(cleanup);
        }
    }

    private async Task CleanupAsync()
    {
        try
        {
            try
            {
                await _stopping.CancelAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                RecordFault("cancel");
                _logger.LogError(
                    KafkaEvents.ConsumerCancellationFailed,
                    exception,
                    "HostLoom Kafka consumer cancellation for '{Topic}' failed; cleanup continues.",
                    _topic
                );
            }
            await _loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        catch (Exception exception)
        {
            RecordFault("loop");
            _logger.LogError(
                KafkaEvents.LoopFaulted,
                exception,
                "HostLoom Kafka consumer loop for '{Topic}' faulted before shutdown.",
                _topic
            );
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
                RecordFault("close");
                _logger.LogError(
                    KafkaEvents.ConsumerCloseFailed,
                    exception,
                    "HostLoom Kafka consumer for '{Topic}' failed to close cleanly.",
                    _topic
                );
            }

            try
            {
                _consumer.Dispose();
            }
            finally
            {
                _stopping.Dispose();
            }
        }
    }

    private static void ObserveLateFailure(Task task) =>
        _ = task.ContinueWith(
            static failed => _ = failed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        );

    private async Task RunAsync(
        Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler
    )
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
                RecordFault("consume");
                _logger.LogError(
                    KafkaEvents.ConsumeFailed,
                    exception,
                    "HostLoom Kafka consume failed on '{Topic}'; retrying.",
                    _topic
                );
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

            KafkaDiagnostics.Consumed.Add(1, _destinationTag);
            try
            {
                var handling = handler(record, _stopping.Token);
                if (handling.IsCompletedSuccessfully)
                    await handling.ConfigureAwait(false);
                else
                {
                    Task task = handling.AsTask();
                    try
                    {
                        await task.WaitAsync(_stopping.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
                    {
                        // The handler may ignore cancellation. It owns no consumer calls; once
                        // detached it cannot commit, rewind, or consume against the closed client.
                        ObserveLateFailure(task);
                        break;
                    }
                }
                _stopping.Token.ThrowIfCancellationRequested();
                retries.Remove(record.TopicPartition);
                // A failed commit must not enter the handler retry path. The record was handled;
                // later commits can advance it, or Kafka can redeliver after reassignment.
                if (_commitOnSuccess)
                    TryCommit(record);
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                break;
            }
            catch (Exception) when (_stopping.IsCancellationRequested)
            {
                // Shutdown never commits or retries a delivery abandoned during handling.
                break;
            }
            catch (MalformedEnvelopeException exception)
            {
                // Poison record: it can never be decoded, so committing past it keeps the
                // partition moving instead of blocking every later request behind it.
                _logger.LogError(
                    KafkaEvents.RecordMalformed,
                    exception,
                    "HostLoom Kafka record at {Offset} on '{Topic}' is malformed; skipping it.",
                    record.TopicPartitionOffset,
                    _topic
                );
                RecordSkipped("malformed");
                TryCommit(record);
                retries.Remove(record.TopicPartition);
            }
            catch (RequestExpiredException exception)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(
                        KafkaEvents.RecordMalformed,
                        exception,
                        "HostLoom Kafka request at {Offset} on '{Topic}' expired; skipping it.",
                        record.TopicPartitionOffset,
                        _topic
                    );
                }

                RecordSkipped("expired");
                TryCommit(record);
                retries.Remove(record.TopicPartition);
            }
            catch (ReceiveSuspendedException exception)
            {
                // Keep the record. Pause so the loop does not spin, then read the same offset again.
                var assigned = _consumer.Assignment;
                if (assigned is { Count: > 0 })
                {
                    _consumer.Pause(assigned);
                }

                var delay = exception.ResumeAt - _clock.GetUtcNow();
                if (delay > TimeSpan.Zero)
                {
                    try
                    {
                        await Task.Delay(delay, _clock, _stopping.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }

                if (assigned is { Count: > 0 })
                {
                    _consumer.Resume(assigned);
                }

                _consumer.Seek(record.TopicPartitionOffset);
            }
            catch (UnroutableReplyException exception)
            {
                // The handler completed; only the reply failed to leave. Rewinding would run the
                // handler again for an answer that still has nowhere to go, so the record is
                // committed past like a poison one and the caller's timeout reports the loss.
                _logger.LogError(
                    KafkaEvents.ReplyUnroutable,
                    exception,
                    "HostLoom Kafka record at {Offset} on '{Topic}' was handled but its reply to '{ReplyTopic}' could not be produced ({ExceptionType}); skipping it without re-running the handler.",
                    record.TopicPartitionOffset,
                    _topic,
                    exception.ReplyTopic,
                    exception.InnerException?.GetType().FullName
                );
                RecordSkipped("unroutable_reply");
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
                    retries.TryGetValue(record.TopicPartition, out var state)
                    && state.Offset == record.Offset.Value
                        ? state.Attempts == int.MaxValue
                            ? int.MaxValue
                            : state.Attempts + 1
                        : 1;

                if (!_retryIndefinitely && attempts >= MaxRedeliveryAttempts)
                {
                    _logger.LogError(
                        KafkaEvents.RecordAttemptsExhausted,
                        exception,
                        "HostLoom Kafka record at {Offset} on '{Topic}' failed {Attempts} times; skipping it.",
                        record.TopicPartitionOffset,
                        _topic,
                        attempts
                    );
                    RecordSkipped("attempts_exhausted");
                    TryCommit(record);
                    retries.Remove(record.TopicPartition);
                    continue;
                }

                _logger.LogError(
                    KafkaEvents.RecordRewound,
                    exception,
                    "HostLoom Kafka record at {Offset} on '{Topic}' failed on attempt {Attempts}; rewinding to retry it.",
                    record.TopicPartitionOffset,
                    _topic,
                    attempts
                );

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
            KafkaDiagnostics.RecordsRewound.Add(1, _destinationTag);
            return true;
        }
        catch (Exception exception)
        {
            RecordFault("seek");
            _logger.LogError(
                KafkaEvents.SeekFailed,
                exception,
                "HostLoom Kafka consumer could not rewind to {Offset} on '{Topic}'.",
                record.TopicPartitionOffset,
                _topic
            );
            return false;
        }
    }

    private void TryCommit(ConsumeResult<string, byte[]> record)
    {
        try
        {
            _consumer.Commit(record);
            KafkaDiagnostics.Committed.Add(1, _destinationTag);
        }
        catch (Exception exception)
        {
            RecordFault("commit");
            _logger.LogError(
                KafkaEvents.CommitFailed,
                exception,
                "HostLoom Kafka commit failed at {Offset} on '{Topic}'.",
                record.TopicPartitionOffset,
                _topic
            );
        }
    }

    private void RecordSkipped(string reason) =>
        KafkaDiagnostics.RecordsSkipped.Add(
            1,
            _destinationTag,
            new(KafkaDiagnostics.ReasonTag, reason)
        );

    private void RecordFault(string stage) =>
        KafkaDiagnostics.LoopFaults.Add(1, _destinationTag, new(KafkaDiagnostics.StageTag, stage));

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
