using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom;

/// <summary>
/// Moves events from an <see cref="IOutboxStore"/> to the transport: claims a batch, publishes
/// each frame unchanged through the <see cref="IEventBroker"/>, and marks it published.
/// Drains when woken by a publish and every <see cref="OutboxOptions.PollInterval"/> regardless,
/// so a message appended by another process or committed after the wake is still relayed.
/// Composes without a container: <c>new OutboxRelay(store, broker, options)</c>, then
/// <see cref="StartAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// A publish that fails marks that message failed with a backoff, which leaves it for a later
/// claim, and ends the drain. The rest of the claimed batch is neither tried nor charged an
/// attempt. The relay holds it under its claim lease, up to a batch of it, and publishes it, oldest
/// first, as soon as a later publish succeeds, provided less than half of
/// <see cref="OutboxOptions.ClaimLease"/> has run since the claim; after that, lease expiry returns
/// it to the store for any relay to claim. Stopping the relay forgets what it holds. A message that
/// fails <see cref="OutboxOptions.MaxAttempts"/> times is dead-lettered and never claimed again.
/// </para>
/// <para>
/// After a drain that ended on a failed publish, the loop pauses before it drains again and
/// ignores wakes and the poll interval meanwhile. The pause is <see cref="OutboxOptions.RetryDelay"/>
/// grown by <see cref="OutboxOptions.RetryBackoffFactor"/> per consecutive failed drain and clamped
/// to <see cref="OutboxOptions.MaxRetryDelay"/>; a zero retry delay turns it off. The drain after a
/// failure first tries a single message, and not the one whose failure it follows when another is
/// due; the first publish that succeeds ends the backoff, and the drain goes on in full batches. An
/// outage therefore costs one attempt per failed drain, not one per pending message, and a message
/// that keeps failing while others publish is backed off and dead-lettered on its own without
/// holding the others up.
/// </para>
/// <para>
/// A message published but not marked, because the store failed, is not a failed attempt: it stays
/// under its claim lease, and the next claim after the lease publishes it again.
/// </para>
/// </remarks>
public sealed class OutboxRelay : IAsyncDisposable
{
    /// <summary>The longest pause a timer accepts.</summary>
    private static readonly TimeSpan MaxPause = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly IOutboxStore _store;
    private readonly IEventBroker _broker;
    private readonly OutboxOptions _options;
    private readonly TimeProvider _clock;
    private readonly ILogger<OutboxRelay> _logger;
    private readonly CancellationTokenSource _stopping = new();
    private readonly Lock _gate = new();
    private readonly List<Claimed> _held = [];
    private TaskCompletionSource _wake = NewSignal();
    private Task? _loop;
    private bool _started;
    private bool _disposed;
    private long _published;
    private long _failed;
    private long _deadLettered;
    private int _failedDrains;
    private Guid? _lastFailure;
    private long _backoffStarted;
    private int _heldCount;

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

    /// <summary>Messages this relay published and marked since it was created.</summary>
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
        try
        {
            if (loop is not null)
            {
                await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host gave up waiting; the loop observes the stop token on its own.
        }
        finally
        {
            // Lease expiry returns what the relay still holds to the store.
            ForgetHeld();
        }
    }

    /// <summary>
    /// Asks the loop to drain now instead of at the next poll interval. Ignored while the loop
    /// pauses after a failed publish.
    /// </summary>
    public void Wake()
    {
        lock (_gate)
        {
            _wake.TrySetResult();
        }
    }

    /// <summary>
    /// Drains the store once: claims batches and publishes them until a batch comes back short, a
    /// publish fails, or the store fails to mark one. After a failed publish, the first claim of
    /// the next drain is a single message, and once a publish succeeds the messages an earlier
    /// drain left untried and the relay still holds go out next. Returns how many messages were
    /// published and marked.
    /// The loop calls this; a test or a manual relay may call it directly, and a direct call does
    /// not wait out the loop's pause.
    /// </summary>
    public async ValueTask<int> DrainAsync(CancellationToken cancellationToken = default) =>
        (await DrainCoreAsync(cancellationToken).ConfigureAwait(false)).Published;

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

    /// <summary>
    /// The drain behind <see cref="DrainAsync"/>. Also returns the pause the loop takes before the
    /// next drain, or <see langword="null"/> when the drain did not end on a failed publish.
    /// </summary>
    private async ValueTask<(int Published, TimeSpan? Pause)> DrainCoreAsync(
        CancellationToken cancellationToken
    )
    {
        var published = 0;
        var probing = Volatile.Read(ref _failedDrains) > 0;
        while (true)
        {
            // Taken before the claim, so the relay's view of each lease never outlasts the store's.
            var claimedAt = _clock.GetTimestamp();
            var batch = probing
                ? await ClaimProbeAsync(cancellationToken).ConfigureAwait(false)
                : await _store
                    .ClaimAsync(_options.BatchSize, _options.ClaimLease, cancellationToken)
                    .ConfigureAwait(false);
            var queue = new List<Claimed>(batch.Count);
            foreach (var message in batch)
            {
                queue.Add(new Claimed(message, claimedAt, Carried: false));
            }

            Unhold(queue);

            // Nothing new is due, so what this relay still holds goes out instead; while the
            // relay backs off, the first of it is the probe.
            if (queue.Count == 0)
            {
                queue.AddRange(TakeHeld());
                if (queue.Count == 0)
                {
                    return (published, null);
                }
            }

            var unmarked = false;
            for (var index = 0; index < queue.Count; index++)
            {
                var entry = queue[index];
                if (entry.Carried && !MayStillPublish(entry))
                {
                    // Too little of its lease is left to publish it safely; lease expiry returns
                    // it to the store for whichever relay claims it next.
                    continue;
                }

                switch (await PublishAsync(entry.Message, cancellationToken).ConfigureAwait(false))
                {
                    case PublishOutcome.Published:
                        published++;
                        if (!unmarked)
                        {
                            // The transport works, so what a stopped drain left untried goes out
                            // now, oldest first, instead of waiting for its lease to expire.
                            queue.InsertRange(index + 1, TakeHeld());
                        }

                        break;
                    case PublishOutcome.NotMarked:
                        unmarked = true;
                        break;
                    default:
                        // Whatever made this publish fail most likely fails the next one too, and
                        // trying the rest would charge each of them an attempt for it. The relay
                        // holds them, untried, for the next drain whose publish succeeds.
                        Hold(queue, index + 1);
                        return (published, BackOff(entry.Message));
                }
            }

            // A short batch means the store is drained; a mark that failed means the store is
            // unwell, and hammering it from a tight loop helps nobody.
            if (unmarked || (!probing && batch.Count < _options.BatchSize))
            {
                return (published, null);
            }

            probing = false;
        }
    }

    /// <summary>
    /// Claims what the first drain after a failed publish tries: one message, so a transport that
    /// is still down costs one attempt and leaves at most one other message leased untried. The
    /// message that failed last is due again just as the pause ends, because both back off by the
    /// same arithmetic, so when the claim returns it and another message is due, the other is
    /// claimed too and tried first; the one that failed goes next if that publish succeeds, and
    /// otherwise is held with the rest. Without this the same message would absorb every failed
    /// drain of an outage and be dead-lettered for it, and a message that can never be published
    /// would hold up every message behind it until it was dead-lettered.
    /// </summary>
    private async ValueTask<IReadOnlyList<OutboxMessage>> ClaimProbeAsync(
        CancellationToken cancellationToken
    )
    {
        var claimed = await _store
            .ClaimAsync(1, _options.ClaimLease, cancellationToken)
            .ConfigureAwait(false);
        Guid? lastFailure;
        lock (_gate)
        {
            lastFailure = _lastFailure;
        }

        if (claimed.Count != 1 || claimed[0].MessageId != lastFailure)
        {
            return claimed;
        }

        var other = await _store
            .ClaimAsync(1, _options.ClaimLease, cancellationToken)
            .ConfigureAwait(false);
        return other.Count == 0 ? claimed : [.. other, claimed[0]];
    }

    /// <summary>
    /// Whether a message this relay claimed may still be published under that claim: less than
    /// half of <see cref="OutboxOptions.ClaimLease"/> has run since just before the claim. The
    /// other half is left for the publish and the mark to finish before another relay may claim
    /// the message; with the default one-minute lease that is thirty seconds, as long as the
    /// RabbitMQ transport's default publish timeout. The relay cannot know a transport's own
    /// bound, so the margin scales with the lease the operator sized for claim, publish, and mark.
    /// </summary>
    private bool MayStillPublish(Claimed entry) =>
        _clock.GetElapsedTime(entry.ClaimedAt) < _options.ClaimLease / 2;

    /// <summary>
    /// Keeps the entries of <paramref name="queue"/> from <paramref name="start"/> on, which a
    /// drain claimed and did not try, for the next drain whose publish succeeds. Keeps at most
    /// <see cref="OutboxOptions.BatchSize"/> of them, dropping those claimed longest ago, and none
    /// that may no longer be published; lease expiry returns whatever is dropped to the store.
    /// </summary>
    private void Hold(List<Claimed> queue, int start)
    {
        if (start >= queue.Count)
        {
            return;
        }

        lock (_gate)
        {
            _held.RemoveAll(held => !MayStillPublish(held));
            for (var index = start; index < queue.Count; index++)
            {
                var entry = queue[index];
                _held.RemoveAll(held => held.Message.MessageId == entry.Message.MessageId);
                if (MayStillPublish(entry))
                {
                    _held.Add(entry with { Carried = true });
                }
            }

            var excess = _held.Count - _options.BatchSize;
            if (excess > 0)
            {
                // Those claimed longest ago have the least lease left; the rest keep their order.
                var oldest = _held
                    .OrderBy(static held => held.ClaimedAt)
                    .Take(excess)
                    .Select(static held => held.Message.MessageId)
                    .ToHashSet();
                _held.RemoveAll(held => oldest.Contains(held.Message.MessageId));
            }

            Volatile.Write(ref _heldCount, _held.Count);
        }
    }

    /// <summary>
    /// Takes every held message that may still be published, oldest first, and forgets the rest.
    /// Each is handed out once: a drain that takes it either publishes it or holds it again.
    /// </summary>
    private Claimed[] TakeHeld()
    {
        if (Volatile.Read(ref _heldCount) == 0)
        {
            return [];
        }

        lock (_gate)
        {
            Claimed[] held =
            [
                .. _held.Where(MayStillPublish).OrderBy(static entry => entry.Message.EnqueuedAt),
            ];
            _held.Clear();
            Volatile.Write(ref _heldCount, 0);
            return held;
        }
    }

    /// <summary>
    /// Forgets held messages a fresh claim returned again, which a store only does once their
    /// lease has expired by its own clock: the fresh claim supersedes the old one, so the relay
    /// never has one message queued twice.
    /// </summary>
    private void Unhold(List<Claimed> claimed)
    {
        if (claimed.Count == 0 || Volatile.Read(ref _heldCount) == 0)
        {
            return;
        }

        var ids = claimed.Select(static entry => entry.Message.MessageId).ToHashSet();
        lock (_gate)
        {
            _held.RemoveAll(held => ids.Contains(held.Message.MessageId));
            Volatile.Write(ref _heldCount, _held.Count);
        }
    }

    private void ForgetHeld()
    {
        lock (_gate)
        {
            _held.Clear();
            Volatile.Write(ref _heldCount, 0);
        }
    }

    /// <summary>
    /// Publishes one claimed message and marks it. A publish the transport accepted ends a
    /// backoff even when the store then fails to mark the message.
    /// </summary>
    private async Task<PublishOutcome> PublishAsync(
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
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            await RecordFailureAsync(message, exception, tags, cancellationToken)
                .ConfigureAwait(false);
            return PublishOutcome.Failed;
        }

        Recover();
        try
        {
            await _store
                .MarkPublishedAsync(message.MessageId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            // The transport has the frame, so this is not a failed attempt: counting it would
            // back the message off and in the end dead-letter a message that was delivered.
            // Nothing is recorded; the claim lease expires and the next claim publishes the
            // message again, a duplicate an inbox on the receiving side absorbs.
            _logger.LogWarning(
                OutboxEvents.MarkPublishedFailed,
                exception,
                "Outbox message {MessageId} was published to '{Topic}', but the store failed to mark it; it is published again when its claim lease expires.",
                message.MessageId,
                message.Topic
            );
            return PublishOutcome.NotMarked;
        }

        Interlocked.Increment(ref _published);
        HostLoomDiagnostics.OutboxPublished.Add(1, tags);
        HostLoomDiagnostics.OutboxLag.Record(
            (_clock.GetUtcNow() - message.EnqueuedAt).TotalSeconds,
            tags
        );
        return PublishOutcome.Published;
    }

    /// <summary>
    /// Counts a failed drain, remembers the message it failed on, and returns how long the loop
    /// pauses before the next drain. Logs only the first failed drain of a run of them.
    /// </summary>
    private TimeSpan BackOff(OutboxMessage message)
    {
        int failedDrains;
        lock (_gate)
        {
            if (_failedDrains == 0)
            {
                _backoffStarted = _clock.GetTimestamp();
            }

            if (_failedDrains < int.MaxValue)
            {
                _failedDrains++;
            }

            failedDrains = _failedDrains;
            _lastFailure = message.MessageId;
        }

        var pause = _options.GetRetryDelay(failedDrains);
        if (failedDrains == 1)
        {
            _logger.LogWarning(
                OutboxEvents.RelayBackingOff,
                "Publishing to '{Topic}' failed, so the outbox relay stopped draining; it tries one message again after {Pause}, backing off up to {MaxRetryDelay} until a publish succeeds.",
                message.Topic,
                pause,
                _options.MaxRetryDelay
            );
        }

        return pause;
    }

    /// <summary>Ends a backoff after a publish the transport accepted, logging the recovery once.</summary>
    private void Recover()
    {
        if (Volatile.Read(ref _failedDrains) == 0)
        {
            return;
        }

        int failedDrains;
        long started;
        lock (_gate)
        {
            failedDrains = _failedDrains;
            if (failedDrains == 0)
            {
                return;
            }

            started = _backoffStarted;
            _failedDrains = 0;
            _lastFailure = null;
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                OutboxEvents.RelayRecovered,
                "The outbox relay published again after {FailedDrains} failed drain(s) over {Duration} and drains normally.",
                failedDrains,
                _clock.GetElapsedTime(started)
            );
        }
    }

    /// <summary>Counts a failed publish attempt and backs the message off or dead-letters it.</summary>
    private async ValueTask RecordFailureAsync(
        OutboxMessage message,
        Exception exception,
        TagList tags,
        CancellationToken cancellationToken
    )
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

                TimeSpan? pause = null;
                try
                {
                    pause = (await DrainCoreAsync(stopping).ConfigureAwait(false)).Pause;
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

                if (pause > TimeSpan.Zero)
                {
                    // Wakes are ignored: during an outage every publish would otherwise start a
                    // drain that fails and charges another message an attempt.
                    await Task.Delay(
                            pause.Value < MaxPause ? pause.Value : MaxPause,
                            _clock,
                            stopping
                        )
                        .ConfigureAwait(false);
                }
                else
                {
                    await WaitAsync(signal.Task, stopping).ConfigureAwait(false);
                }
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

    /// <summary>How a claimed message fared.</summary>
    private enum PublishOutcome
    {
        /// <summary>Published and marked.</summary>
        Published,

        /// <summary>Published, but the store failed to mark it.</summary>
        NotMarked,

        /// <summary>The transport refused it; the failure was recorded against the message.</summary>
        Failed,
    }

    /// <summary>
    /// A message this relay claimed, with a timestamp taken just before the claim and whether a
    /// stopped drain carried it over, untried, to a later one.
    /// </summary>
    private readonly record struct Claimed(OutboxMessage Message, long ClaimedAt, bool Carried);
}
