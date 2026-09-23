using System.Diagnostics;
using HostLoom.Pipelines;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom;

/// <summary>Left on the context when a redelivered event was recognised and its handlers were not run.</summary>
/// <param name="Key">The inbox key: topic, subscription, and message id.</param>
public sealed record InboxDuplicate(string Key);

/// <summary>
/// Left on the context when the store could not say whether the delivery was seen, so the
/// handlers ran anyway. Processing twice is recoverable; dropping a delivery on an outage is not.
/// </summary>
/// <param name="Key">The inbox key.</param>
/// <param name="Failure">What the store threw.</param>
public sealed record InboxSkipped(string Key, Exception Failure);

/// <summary>
/// The idempotent consumer: once an event's handlers have completed for a (topic, subscription,
/// message id), skips every redelivery of it inside a window. The key is recorded with the
/// <see cref="IInboxStore"/> before the handlers run, so a concurrent redelivery is skipped too.
/// Requests pass through untouched, because a request that is not answered leaves its caller
/// waiting for a timeout.
/// </summary>
/// <remarks>
/// <para>
/// A run that throws or is cancelled releases its key through
/// <see cref="IInboxStore.ReleaseAsync"/> before the original exception continues to the
/// transport, so the transport's redelivery runs the handlers again: delivery stays
/// at-least-once. The key covers the whole subscription, so a redelivery after one handler
/// failed also runs the handlers that had completed. An in-process retry works on either side
/// of this filter, but <c>UseRetry</c> registered after it retries inside one recorded run and
/// does not depend on the release succeeding.
/// </para>
/// <para>
/// What remains is a process that stops between recording the key and completing the
/// handlers, such as a crash or a kill. Nothing releases the key, so it stays until the window
/// ends, and a redelivery inside the window is acknowledged as a duplicate without running the
/// handlers: that event is lost. A release that fails, and a store that does not implement one,
/// lose a failed run's redeliveries the same way. The window also bounds how long a redelivery
/// is recognised and how much the store holds; a day covers a broker's redelivery horizon in
/// most deployments.
/// </para>
/// </remarks>
public sealed class InboxFilter : IFilter<ReceiveContext>
{
    // The delivery's token cannot bound the release: it may be the very cancellation being
    // handled. A few seconds covers a remote store's round trip under load, while keeping a slow
    // store from holding the transport's requeue or rewind, and on Kafka the partition, for long.
    private static readonly TimeSpan ReleaseTimeout = TimeSpan.FromSeconds(5);

    private readonly IInboxStore _store;
    private readonly TimeSpan _window;
    private readonly ILogger<InboxFilter> _logger;

    /// <summary>Creates the filter over <paramref name="store"/> with <paramref name="window"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The window is not positive.</exception>
    public InboxFilter(IInboxStore store, TimeSpan window, ILogger<InboxFilter>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        _store = store;
        _window = window;
        _logger = logger ?? NullLogger<InboxFilter>.Instance;
    }

    /// <summary>
    /// The inbox key for one delivery: <c>{topic.Length}:{topic}:{subscription.Length}:{subscription}:{messageId}</c>.
    /// </summary>
    /// <remarks>
    /// The topic and subscription are length-prefixed rather than joined with a bare separator,
    /// because both may contain <c>:</c>; without the prefix, <c>("a:b", "c")</c> and
    /// <c>("a", "b:c")</c> would share a key and one subscription's delivery would silence the
    /// other's. The message id is the sender's, so the framework rejects an empty one at decoding.
    /// </remarks>
    public static string KeyFor(EventReceiveContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return KeyFor(context.Destination.Value, context.Subscription, context.MessageId);
    }

    /// <summary>Builds the key <see cref="KeyFor(EventReceiveContext)"/> builds, from its parts.</summary>
    public static string KeyFor(string topic, string subscription, Guid messageId)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(subscription);
        return $"{topic.Length}:{topic}:{subscription.Length}:{subscription}:{messageId:N}";
    }

    /// <inheritdoc />
    public async ValueTask SendAsync(ReceiveContext context, IPipe<ReceiveContext> next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);
        if (context is not EventReceiveContext eventContext)
        {
            await next.SendAsync(context).ConfigureAwait(false);
            return;
        }

        var key = KeyFor(eventContext);
        bool first;
        try
        {
            first = await _store
                .TryRecordAsync(key, _window, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            var skipped = new InboxSkipped(key, exception);
            context.AddOrUpdatePayload(() => skipped, _ => skipped);
            _logger.LogWarning(
                InboxEvents.StoreUnavailable,
                exception,
                "The inbox store could not record delivery {Key}; the handlers run without deduplication.",
                key
            );
            await next.SendAsync(context).ConfigureAwait(false);
            return;
        }

        if (!first)
        {
            var duplicate = new InboxDuplicate(key);
            context.AddOrUpdatePayload(() => duplicate, _ => duplicate);
            HostLoomDiagnostics.InboxDuplicates.Add(
                1,
                new TagList
                {
                    { "messaging.destination.name", eventContext.Destination.Value },
                    { "messaging.consumer.group.name", eventContext.Subscription },
                }
            );
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug(
                    InboxEvents.Duplicate,
                    "Delivery {Key} was already handled inside the inbox window; its handlers were not run.",
                    key
                );
            }

            return;
        }

        try
        {
            await next.SendAsync(context).ConfigureAwait(false);
        }
        catch
        {
            // Every failure, cancellation included: the key was recorded for a run that did not
            // complete, and keeping it would turn the transport's redelivery into a duplicate.
            await ReleaseAsync(key).ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public void Probe(IProbeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var scope = context.CreateScope("inbox");
        scope.Set("window", _window);
        scope.Set("store", _store.GetType().Name);
        scope.Set("onUnavailable", "run");
    }

    /// <summary>
    /// Forgets <paramref name="key"/> after a run that did not complete, best-effort. A failure
    /// here is logged and swallowed, so the handlers' own exception is the one the transport sees.
    /// </summary>
    private async ValueTask ReleaseAsync(string key)
    {
        using var timeout = new CancellationTokenSource(ReleaseTimeout);
        try
        {
            await _store.ReleaseAsync(key, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                InboxEvents.ReleaseFailed,
                exception,
                "The inbox store could not release delivery {Key} after its handlers failed or were cancelled; a redelivery inside the window will be treated as a duplicate.",
                key
            );
        }
    }
}
