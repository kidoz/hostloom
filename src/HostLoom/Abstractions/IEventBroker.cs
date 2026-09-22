namespace HostLoom;

/// <summary>Delivers one published frame to a subscription. Unlike a request, it returns nothing.</summary>
public delegate ValueTask EventFrameHandler(
    ReadOnlyMemory<byte> frame,
    CancellationToken cancellationToken
);

/// <summary>
/// Optional transport capability for publish/subscribe, kept separate from
/// <see cref="IRequestBroker"/> so a transport can support request/response alone. Publishing
/// through a transport that does not implement this throws rather than silently dropping.
/// </summary>
public interface IEventBroker
{
    /// <summary>
    /// Attaches <paramref name="subscription"/> to <paramref name="topic"/>. Distinct subscription
    /// names on one topic each receive every event; the transport maps the name onto its own
    /// primitive — a bound queue, a consumer group, and so on.
    /// </summary>
    /// <remarks>
    /// The token passed to <paramref name="handler"/> is cancelled only by disposing the returned
    /// handle or by the transport itself, never by a publisher. The handler may be invoked for
    /// several events at once where the transport allows it; their order, redelivery after a
    /// failure, and how instances sharing a subscription name divide its events are defined by
    /// each transport.
    /// </remarks>
    ValueTask<IAsyncDisposable> SubscribeAsync(
        RequestAddress topic,
        string subscription,
        EventFrameHandler handler,
        CancellationToken cancellationToken
    );

    /// <summary>Publishes <paramref name="frame"/> to every subscription on <paramref name="topic"/>.</summary>
    /// <remarks>
    /// Completion means the transport accepted the event, as that transport defines acceptance,
    /// not that any subscriber handled it. An exception means acceptance is uncertain: the event
    /// may still be delivered, so publishing it again can deliver it twice. Cancelling
    /// <paramref name="cancellationToken"/> ends the wait with an
    /// <see cref="OperationCanceledException"/> carrying that token, and the event may still be
    /// accepted. Any publication deadline is the transport's own.
    /// </remarks>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <exception cref="ObjectDisposedException">The transport was disposed before or during the call.</exception>
    /// <exception cref="TimeoutException">The transport has a publication deadline and it elapsed.</exception>
    /// <exception cref="MessagingTransportException">Any other transport failure.</exception>
    ValueTask PublishAsync(
        RequestAddress topic,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    );
}
