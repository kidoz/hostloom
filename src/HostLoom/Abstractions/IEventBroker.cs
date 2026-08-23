namespace HostLoom;

/// <summary>Delivers one published frame to a subscription. Unlike a request, it returns nothing.</summary>
public delegate ValueTask EventFrameHandler(ReadOnlyMemory<byte> frame, CancellationToken cancellationToken);

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
    ValueTask<IAsyncDisposable> SubscribeAsync(
        RequestAddress topic,
        string subscription,
        EventFrameHandler handler,
        CancellationToken cancellationToken);

    ValueTask PublishAsync(RequestAddress topic, ReadOnlyMemory<byte> frame, CancellationToken cancellationToken);
}
