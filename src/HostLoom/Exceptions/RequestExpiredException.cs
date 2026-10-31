namespace HostLoom;

/// <summary>
/// A request arrived with <see cref="ExpiresAt"/> already in the past, so the handler did not run
/// and no fault was sent. The caller observes its own <see cref="RequestTimeoutException"/>.
/// </summary>
/// <remarks>
/// Transports treat this like a poison frame: Kafka commits past the record, and RabbitMQ rejects
/// it without requeue. An in-memory requester is left waiting for the timeout it already set.
/// </remarks>
public sealed class RequestExpiredException : Exception
{
    public RequestExpiredException(RequestAddress destination, DateTimeOffset expiresAt)
        : base(
            $"The request to '{destination.Value}' expired at {expiresAt:O} and was not handled."
        )
    {
        Destination = destination;
        ExpiresAt = expiresAt;
    }

    public RequestAddress Destination { get; }

    public DateTimeOffset ExpiresAt { get; }
}
