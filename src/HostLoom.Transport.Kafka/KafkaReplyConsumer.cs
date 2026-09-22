using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace HostLoom.Transport.Kafka;

/// <summary>
/// The reply path of one <see cref="KafkaRequestBroker"/>: a consumer of
/// <see cref="KafkaOptions.ResponseTopic"/> in a group unique to this client, started by the
/// first request and, after a failed start, again by the next one. It owns the consumer's
/// startup, the high-watermark query that pins each initially assigned partition, the reply
/// offsets that survive a reassignment, and the local state the broker's health probe reports.
/// Each record is handed to the broker, which completes the request it correlates to.
/// </summary>
internal sealed class KafkaReplyConsumer : IAsyncDisposable
{
    private static readonly TimeSpan WatermarkQueryTimeout = TimeSpan.FromSeconds(5);

    private readonly KafkaOptions _options;
    private readonly KafkaConsumerFactory _consumerFactory;
    private readonly Action<ConsumeResult<string, byte[]>> _deliver;
    private readonly ILogger _logger;
    private readonly KeyValuePair<string, object?> _clientTag;
    private readonly object _owner;
    private readonly ConcurrentDictionary<TopicPartition, Offset> _offsets = new();

    // No wait handle is used; a start racing disposal must be able to observe it safely.
#pragma warning disable CA2213
    private readonly SemaphoreSlim _consumerGate = new(1, 1);
#pragma warning restore CA2213
    private readonly Lock _startGate = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly CancellationToken _shutdownToken;
    private TaskCompletionSource _assigned = NewSignal();
    private ConsumerSubscription? _subscription;
    private Task? _starting;
    private Task? _disposing;
    private volatile bool _failed;
    private volatile bool _disposed;

    /// <param name="deliver">Receives every reply record, on the consumer loop's thread.</param>
    /// <param name="owner">Named by the <see cref="ObjectDisposedException"/> a start after disposal throws.</param>
    public KafkaReplyConsumer(
        KafkaOptions options,
        KafkaConsumerFactory consumerFactory,
        Action<ConsumeResult<string, byte[]>> deliver,
        ILogger logger,
        KeyValuePair<string, object?> clientTag,
        object owner
    )
    {
        _options = options;
        _consumerFactory = consumerFactory;
        _deliver = deliver;
        _logger = logger;
        _clientTag = clientTag;
        _owner = owner;
        _shutdownToken = _shutdown.Token;
    }

    /// <summary>
    /// What this consumer knows about itself, never asking the broker: unavailable after a failed
    /// start until the next request retries it, waiting while a start awaits its assignment, and
    /// healthy otherwise, including before the first request and during a later outage.
    /// </summary>
    public BrokerHealth Health =>
        _disposed || _failed
            ? BrokerHealth.Unhealthy(
                "Kafka reply consumer is unavailable; the next request retries initialization."
            )
        : _starting is { IsCompleted: false }
            ? BrokerHealth.Unhealthy("Kafka reply consumer is awaiting assignment.")
        : BrokerHealth.Healthy(
            "Kafka local consumer state has no reported startup failure; broker reachability is not probed."
        );

    /// <summary>
    /// Completes once the consumer has its initial assignment, starting it if no start is under
    /// way. The start is shared and is not cancelled by <paramref name="cancellationToken"/>,
    /// which bounds only this caller's wait; a start that failed is retried by the next caller.
    /// </summary>
    public async ValueTask EnsureStartedAsync(CancellationToken cancellationToken)
    {
        Task starting;
        lock (_startGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, _owner);
            // SDK construction is synchronous. A task of its own lets each request bound its
            // wait without cancelling shared initialization or blocking on construction. A
            // previous caller may have timed out before the shared start failed.
            if (_starting is { IsFaulted: true } or { IsCanceled: true })
                _starting = null;
            starting = _starting ??= Task.Run(InitializeAsync, CancellationToken.None);
        }

        try
        {
            await starting.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch when (starting.IsFaulted)
        {
            lock (_startGate)
            {
                if (ReferenceEquals(_starting, starting))
                {
                    _starting = null;
                }
            }

            throw;
        }
    }

    /// <summary>
    /// Stops a start in progress, waits for it, and closes the consumer. A start failure is left
    /// to the callers that waited for it; a failure to close the consumer is thrown.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        lock (_startGate)
        {
            _disposed = true;
            return new ValueTask(_disposing ??= DisposeCoreAsync());
        }
    }

    private async Task DisposeCoreAsync()
    {
        await _shutdown.CancelAsync().ConfigureAwait(false);
        if (_starting is { } starting)
        {
            try
            {
                await starting.ConfigureAwait(false);
            }
            catch (Exception)
            { /* Initialization failure is reported to its callers. */
            }
        }

        await _consumerGate.WaitAsync().ConfigureAwait(false);
        try
        {
            _assigned.TrySetException(new ObjectDisposedException(nameof(KafkaRequestBroker)));
            if (_subscription is not null)
            {
                await _subscription.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _consumerGate.Release();
            _shutdown.Dispose();
        }
    }

    private async Task InitializeAsync()
    {
        try
        {
            await StartAsync(_shutdownToken).ConfigureAwait(false);
            await _assigned.Task.WaitAsync(_shutdownToken).ConfigureAwait(false);
            _failed = false;
            RecordInitialization("succeeded");
        }
        catch
        {
            _failed = true;
            RecordInitialization("failed");
            await _consumerGate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                    _subscription = null;
                }
                _offsets.Clear();
                _assigned = NewSignal();
            }
            finally
            {
                _consumerGate.Release();
            }
            throw;
        }
    }

    private async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        await _consumerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, _owner);
            // A successfully registered consumer remains owned until disposal.
            if (_subscription is not null)
            {
                return;
            }

            var consumer = _consumerFactory(
                KafkaRequestBroker.CreateConsumerConfig(
                    _options,
                    clientId: $"{_options.ClientId}-replies",
                    groupId: $"{_options.ConsumerGroup}.replies.{_options.ClientId}",
                    enableAutoCommit: true,
                    // The group is unique to this process, so there is never a committed offset
                    // to resume from. Earliest would replay every retained reply on the topic on
                    // each restart; Latest starts at the end, and a request waits for the
                    // assignment below before producing, so no reply can precede the start.
                    autoOffsetReset: AutoOffsetReset.Latest
                ),
                partitionsAssigned: OnPartitionsAssigned
            );
            KafkaRequestBroker.SubscribeOwned(
                consumer,
                _options.ResponseTopic,
                () => _disposed,
                _owner,
                _logger
            );
            _subscription = ConsumerSubscription.Start(
                consumer,
                _options.ResponseTopic,
                (record, _) =>
                {
                    // This loop and assignment callbacks own reply progress. Even an unrelated
                    // or malformed reply is consumed, so resume at the next record on reassignment.
                    _offsets[record.TopicPartition] = record.Offset + 1;
                    _deliver(record);
                    return ValueTask.CompletedTask;
                },
                _logger
            );
        }
        finally
        {
            _consumerGate.Release();
        }
    }

    /// <summary>
    /// Resolves initial offsets before permitting requests. Reassignments retain local progress;
    /// a partition added after startup is read from the beginning so pending replies survive.
    /// </summary>
    private IEnumerable<TopicPartitionOffset> OnPartitionsAssigned(
        IConsumer<string, byte[]> consumer,
        List<TopicPartition> partitions
    )
    {
        var offsets = new List<TopicPartitionOffset>(partitions.Count);
        try
        {
            foreach (var partition in partitions)
            {
                if (!_offsets.TryGetValue(partition, out var offset))
                {
                    offset = _assigned.Task.IsCompletedSuccessfully
                        ? Offset.Beginning
                        : consumer.QueryWatermarkOffsets(partition, WatermarkQueryTimeout).High;
                    if (!_assigned.Task.IsCompletedSuccessfully && offset.Value < 0)
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
            // Initialization fails for callers; the next request starts a fresh consumer.
            _assigned.TrySetException(exception);
            throw;
        }

        foreach (var offset in offsets)
        {
            _offsets[offset.TopicPartition] = offset.Offset;
        }

        if (partitions.Count > 0)
        {
            _assigned.TrySetResult();
        }

        return offsets;
    }

    private void RecordInitialization(string outcome) =>
        KafkaDiagnostics.ReplyConsumerInitializations.Add(
            1,
            _clientTag,
            new(KafkaDiagnostics.OutcomeTag, outcome)
        );

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
