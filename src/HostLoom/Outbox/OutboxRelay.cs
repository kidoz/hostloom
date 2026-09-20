using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom;

/// <summary>
/// Moves events from an <see cref="IOutboxStore"/> to the transport: claims a batch, publishes
/// each frame unchanged through the <see cref="IEventBroker"/>, and marks it published, or marks
/// it failed with a backoff and leaves it for a later claim. A message that fails
/// <see cref="OutboxOptions.MaxAttempts"/> times is dead-lettered and never claimed again.
/// Drains when woken by a publish and every <see cref="OutboxOptions.PollInterval"/> regardless,
/// so a message appended by another process or committed after the wake is still relayed.
/// Composes without a container: <c>new OutboxRelay(store, broker, options)</c>, then
/// <see cref="StartAsync"/>.
/// </summary>
public sealed class OutboxRelay : IAsyncDisposable
{
    private readonly IOutboxStore _store;
    private readonly IEventBroker _broker;
    private readonly OutboxOptions _options;
    private readonly TimeProvider _clock;
    private readonly ILogger<OutboxRelay> _logger;
    private readonly CancellationTokenSource _stopping = new();
    private readonly Lock _gate = new();
    private TaskCompletionSource _wake = NewSignal();
    private Task? _loop;
    private bool _started;
    private bool _disposed;
    private long _published;
    private long _failed;
    private long _deadLettered;

    /// <summary>Composes the relay.</summary>
    /// <exception cref="ArgumentException"><see cref="OutboxOptions.Validate"/> reported a violation.</exception>
    public OutboxRelay(
        IOutboxStore store,
        IEventBroker broker,
        OutboxOptions options,
        TimeProvider? timeProvider = null,
        ILogger<OutboxRelay>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(broker);
        ArgumentNullException.ThrowIfNull(options);
        var problems = options.Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "OutboxOptions are not usable: " + string.Join(" ", problems),
                nameof(options)
            );
        }

        _store = store;
        _broker = broker;
        _options = options;
        _clock = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<OutboxRelay>.Instance;
    }

    /// <summary>Messages this relay published since it was created.</summary>
    public long Published => Interlocked.Read(ref _published);

    /// <summary>Publish attempts this relay recorded as failed since it was created, dead-lettering attempts included.</summary>
    public long Failed => Interlocked.Read(ref _failed);

    /// <summary>Messages this relay dead-lettered since it was created.</summary>
    public long DeadLettered => Interlocked.Read(ref _deadLettered);

    /// <summary>Starts the drain loop on the thread pool and returns at once. Repeated calls do nothing.</summary>
    /// <exception cref="ObjectDisposedException">The relay was disposed.</exception>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_started)
            {
                return Task.CompletedTask;
            }

            _started = true;
            var token = _stopping.Token;
            _loop = Task.Run(() => RunAsync(token), CancellationToken.None);
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                OutboxEvents.RelayStarted,
                "The outbox relay drains every {PollInterval} in batches of {BatchSize} with a {ClaimLease} claim lease and dead-letters after {MaxAttempts} attempts.",
                _options.PollInterval,
                _options.BatchSize,
                _options.ClaimLease,
                _options.MaxAttempts
            );
        }

        return Task.CompletedTask;
    }

    /// <summary>Stops the loop and waits for the drain in progress, bounded by <paramref name="cancellationToken"/>.</summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task? loop;
        lock (_gate)
        {
            loop = _loop;
        }

        await _stopping.CancelAsync().ConfigureAwait(false);
        if (loop is null)
        {
            return;
        }

        try
        {
            await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host gave up waiting; the loop observes the stop token on its own.
        }
    }

    /// <summary>Asks the loop to drain now instead of at the next poll interval.</summary>
    public void Wake()
    {
        lock (_gate)
        {
            _wake.TrySetResult();
        }
    }

    /// <summary>
    /// Drains the store once: claims batches and publishes them until a batch comes back short or
    /// a publish fails. Returns how many messages were published. The loop calls this; a test or
    /// a manual relay may call it directly.
    /// </summary>
    public async ValueTask<int> DrainAsync(CancellationToken cancellationToken = default)
    {
        var published = 0;
        while (true)
        {
            var batch = await _store
                .ClaimAsync(_options.BatchSize, _options.ClaimLease, cancellationToken)
                .ConfigureAwait(false);
            if (batch.Count == 0)
            {
                return published;
            }

            var failed = false;
            foreach (var message in batch)
            {
                if (await PublishAsync(message, cancellationToken).ConfigureAwait(false))
                {
                    published++;
                }
                else
                {
                    failed = true;
                }
            }

            // A short batch means the store is drained; a failure means the transport or the
            // store is unwell, and hammering either from a tight loop helps nobody.
            if (failed || batch.Count < _options.BatchSize)
            {
                return published;
            }
        }
    }

    /// <summary>Stops the relay and waits for its loop without a bound.</summary>
    public async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _stopping.Dispose();
    }

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>What the store keeps about a failure: the type, never the message, which can carry connection strings or payload fragments.</summary>
    private static string Describe(Exception exception) =>
        exception.GetType().FullName ?? exception.GetType().Name;

    private async Task<bool> PublishAsync(
        OutboxMessage message,
        CancellationToken cancellationToken
    )
    {
        var tags = new TagList { { "messaging.destination.name", message.Topic } };
        try
        {
            await _broker
                .PublishAsync(new RequestAddress(message.Topic), message.Frame, cancellationToken)
                .ConfigureAwait(false);
            await _store
                .MarkPublishedAsync(message.MessageId, cancellationToken)
                .ConfigureAwait(false);
            Interlocked.Increment(ref _published);
            HostLoomDiagnostics.OutboxPublished.Add(1, tags);
            HostLoomDiagnostics.OutboxLag.Record(
                (_clock.GetUtcNow() - message.EnqueuedAt).TotalSeconds,
                tags
            );
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            Interlocked.Increment(ref _failed);
            HostLoomDiagnostics.OutboxFailed.Add(1, tags);
            var attempts = message.Attempts + 1;
            try
            {
                if (attempts >= _options.MaxAttempts)
                {
                    await DeadLetterAsync(message, attempts, exception, tags, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await HoldBackAsync(message, attempts, exception, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception storeException)
            {
                // The lease expires on its own; the message is retried then.
                _logger.LogWarning(
                    OutboxEvents.StoreFailed,
                    storeException,
                    "The outbox store failed to record the failure of message {MessageId}.",
                    message.MessageId
                );
            }

            return false;
        }
    }

    private async ValueTask HoldBackAsync(
        OutboxMessage message,
        int attempts,
        Exception exception,
        CancellationToken cancellationToken
    )
    {
        var delay = _options.GetRetryDelay(attempts);
        var nextAttemptAt = _clock.GetUtcNow() + delay;
        _logger.LogWarning(
            OutboxEvents.PublishFailed,
            exception,
            "Publishing outbox message {MessageId} to '{Topic}' failed on attempt {Attempt} of {MaxAttempts}; it is retried after {Delay}.",
            message.MessageId,
            message.Topic,
            attempts,
            _options.MaxAttempts,
            delay
        );
        await _store
            .MarkFailedAsync(
                message.MessageId,
                Describe(exception),
                nextAttemptAt,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    private async ValueTask DeadLetterAsync(
        OutboxMessage message,
        int attempts,
        Exception exception,
        TagList tags,
        CancellationToken cancellationToken
    )
    {
        _logger.LogError(
            OutboxEvents.DeadLettered,
            exception,
            "Publishing outbox message {MessageId} to '{Topic}' failed on attempt {Attempt} of {MaxAttempts}; it is dead-lettered and will not be claimed again.",
            message.MessageId,
            message.Topic,
            attempts,
            _options.MaxAttempts
        );
        await _store
            .MarkDeadLetteredAsync(message.MessageId, Describe(exception), cancellationToken)
            .ConfigureAwait(false);
        Interlocked.Increment(ref _deadLettered);
        HostLoomDiagnostics.OutboxDeadLettered.Add(1, tags);
    }

    private async Task RunAsync(CancellationToken stopping)
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                TaskCompletionSource signal;
                lock (_gate)
                {
                    _wake = NewSignal();
                    signal = _wake;
                }

                try
                {
                    await DrainAsync(stopping).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        OutboxEvents.StoreFailed,
                        exception,
                        "The outbox store failed during a drain; the relay retries after {PollInterval}.",
                        _options.PollInterval
                    );
                }

                await WaitAsync(signal.Task, stopping).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            // Stopping.
        }
        finally
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation(
                    OutboxEvents.RelayStopped,
                    "The outbox relay stopped after publishing {Published} message(s).",
                    Published
                );
            }
        }
    }

    private async Task WaitAsync(Task wake, CancellationToken stopping)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        var delay = Task.Delay(_options.PollInterval, _clock, linked.Token);
        await Task.WhenAny(wake, delay).ConfigureAwait(false);
        await linked.CancelAsync().ConfigureAwait(false);
        try
        {
            await delay.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Either the wake won or the relay is stopping; both are expected.
        }

        stopping.ThrowIfCancellationRequested();
    }
}
