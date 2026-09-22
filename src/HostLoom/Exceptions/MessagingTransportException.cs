namespace HostLoom;

/// <summary>
/// A transport could not carry a request or an event: the broker refused or lost the
/// connection, rejected the publication, or its client library failed. The client library's
/// exception, when there is one, is the <see cref="Exception.InnerException"/>.
/// </summary>
/// <remarks>
/// <see cref="IRequestBroker.RequestAsync"/> and <see cref="IEventBroker.PublishAsync"/> throw it
/// for every failure other than those with their own documented type: a timeout, the caller's
/// cancellation, and a disposed transport. Whether the broker accepted the message is not known
/// when it is thrown, so retrying a publication can deliver the event twice and retrying a
/// request can run its handler twice.
/// </remarks>
public sealed class MessagingTransportException : Exception
{
    public MessagingTransportException(
        RequestAddress address,
        string message,
        Exception? innerException = null
    )
        : base(message, innerException)
    {
        Address = address;
    }

    /// <summary>The request address or event topic the operation was for.</summary>
    public RequestAddress Address { get; }
}
