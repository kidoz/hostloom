using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom;

/// <summary>
/// Moves events from an <see cref="IOutboxStore"/> to the transport: claims a batch, publishes
/// each frame unchanged through the <see cref="IEventBroker"/>, and marks it published, or marks
/// it failed and leaves it for a later claim. Drains when woken by a publish and every
/// <see cref="OutboxOptions.PollInterval"/> regardless, so a message appended by another process
/// or committed after the wake is still relayed. Composes without a container:
/// <c>new OutboxRelay(store, broker, options)</c>, then <see cref="StartAsync"/>.
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

    /// <summary>Publish attempts this relay recorded as failed since it was created.</summary>
    public long Failed => Interlocked.Read(ref _failed);

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
                "The outbox relay drains every {PollInterval} in batches of {BatchSize} with a {ClaimLease} claim lease.",
                _options.PollInterval,
                _options.BatchSize,
                _options.ClaimLease
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
            _logger.LogWarning(
                OutboxEvents.PublishFailed,
                exception,
                "Publishing outbox message {MessageId} to '{Topic}' failed on attempt {Attempt}; it stays pending.",
                message.MessageId,
                message.Topic,
                message.Attempts + 1
            );
            try
            {
                await _store
                    .MarkFailedAsync(message.MessageId, exception.Message, cancellationToken)
                    .ConfigureAwait(false);
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
