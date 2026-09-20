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
/// The idempotent consumer: runs an event's handlers at most once per (topic, subscription,
/// message id) inside a window, by recording the key with the <see cref="IInboxStore"/> before
/// the handlers run. Requests pass through untouched, because a request that is not answered
/// leaves its caller waiting for a timeout.
/// </summary>
/// <remarks>
/// The key is recorded before processing, so a run that fails after recording is not repeated
/// by a later redelivery inside the window; put <c>UseRetry</c> after this filter when a failed
/// run should be retried in process. The window bounds how long a redelivery is recognised and
/// how much the store holds; a day covers a broker's redelivery horizon in most deployments.
/// </remarks>
public sealed class InboxFilter : IFilter<ReceiveContext>
{
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

        await next.SendAsync(context).ConfigureAwait(false);
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
}
