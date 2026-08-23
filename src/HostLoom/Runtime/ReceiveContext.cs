using HostLoom.Pipelines;

namespace HostLoom;

/// <summary>
/// Pipeline context for one inbound request delivery. Filters registered through
/// <see cref="HostLoomBuilder.ConfigureReceivePipeline"/> observe the decoded message before the
/// handler runs, and observe handler faults as exceptions rather than as encoded fault envelopes.
/// </summary>
public sealed class ReceiveContext : PipeContext
{
    internal ReceiveContext(
        RequestAddress endpoint,
        Guid messageId,
        string messageType,
        Type executorType,
        object message,
        CancellationToken cancellationToken)
        : base(cancellationToken)
    {
        Endpoint = endpoint;
        MessageId = messageId;
        MessageType = messageType;
        ExecutorType = executorType;
        Message = message;
    }

    /// <summary>Endpoint that received the frame.</summary>
    public RequestAddress Endpoint { get; }

    /// <summary>Wire message id of the request envelope.</summary>
    public Guid MessageId { get; }

    /// <summary>Logical message type name carried by the envelope.</summary>
    public string MessageType { get; }

    /// <summary>Deserialized request instance.</summary>
    public object Message { get; }

    internal Type ExecutorType { get; }

    internal object? Response { get; set; }
}
