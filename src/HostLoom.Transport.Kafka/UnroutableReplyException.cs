namespace HostLoom.Transport.Kafka;

/// <summary>
/// The handler ran and its reply could not be produced to the caller's reply topic. The consumer
/// loop treats it like a malformed record: the request is committed and skipped, not rewound,
/// because re-running the handler would repeat its side effects for a reply that still has
/// nowhere to go, and the caller's own timeout already covers the missing answer.
/// </summary>
public sealed class UnroutableReplyException : Exception
{
    public UnroutableReplyException()
        : this("The reply could not be produced.") { }

    public UnroutableReplyException(string message)
        : base(message) { }

    public UnroutableReplyException(string message, Exception innerException)
        : base(message, innerException) { }

    private UnroutableReplyException(string replyTopic, Exception cause, string message)
        : base(message, cause)
    {
        ReplyTopic = replyTopic;
    }

    /// <summary>The topic the reply was addressed to, when known.</summary>
    public string? ReplyTopic { get; }

    internal static UnroutableReplyException For(string replyTopic, Exception cause) =>
        new(
            replyTopic,
            cause,
            $"The reply could not be produced to '{replyTopic}' ({cause.GetType().Name})."
        );
}
