namespace HostLoom;

/// <summary>
/// A delivery arrived for a destination whose kill switch has paused consumption. The handler
/// did not start. Transports keep the delivery and stop reading until <see cref="ResumeAt"/>.
/// </summary>
/// <remarks>
/// This is not a handler failure and not a <see cref="MessagingTransportException"/>: the work
/// was refused before it began, so acceptance of a side effect is known to be false. A transport
/// that lets the exception escape follows its ordinary failure path.
/// </remarks>
public sealed class ReceiveSuspendedException : Exception
{
    public ReceiveSuspendedException(
        RequestAddress destination,
        string? subscription,
        DateTimeOffset resumeAt
    )
        : base(
            subscription is null
                ? $"Consumption of '{destination.Value}' is paused until {resumeAt:O}."
                : $"Consumption of '{destination.Value}' subscription '{subscription}' is paused until {resumeAt:O}."
        )
    {
        Destination = destination;
        Subscription = subscription;
        ResumeAt = resumeAt;
    }

    public RequestAddress Destination { get; }

    public string? Subscription { get; }

    public DateTimeOffset ResumeAt { get; }
}
