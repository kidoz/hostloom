using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

/// <summary>
/// Pipeline context for one inbound delivery, whether a request or an event. Filters registered
/// through <see cref="HostLoomBuilder.ConfigureReceivePipeline"/> observe the decoded message before
/// the handler runs, and observe handler faults as exceptions rather than as encoded fault envelopes.
/// </summary>
public abstract class ReceiveContext : PipeContext
{
    private protected ReceiveContext(
        RequestAddress destination,
        Guid messageId,
        string messageType,
        object message,
        CancellationToken cancellationToken)
        : base(cancellationToken)
    {
        Destination = destination;
        MessageId = messageId;
        MessageType = messageType;
        Message = message;
    }

    /// <summary>Endpoint for a request, topic for an event.</summary>
    public RequestAddress Destination { get; }

    /// <summary>Wire message id of the envelope.</summary>
    public Guid MessageId { get; }

    /// <summary>Logical message type name carried by the envelope.</summary>
    public string MessageType { get; }

    /// <summary>Deserialized message instance.</summary>
    public object Message { get; }

    /// <summary>Runs the handlers this delivery targets. Called by the pipeline's terminal filter.</summary>
    private protected abstract ValueTask ExecuteAsync(IServiceProvider provider, CancellationToken cancellationToken);

    internal ValueTask InvokeAsync(IServiceProvider provider, CancellationToken cancellationToken) =>
        ExecuteAsync(provider, cancellationToken);
}

/// <summary>One inbound request, which produces exactly one response.</summary>
public sealed class RequestReceiveContext : ReceiveContext
{
    private readonly Type _executorType;

    internal RequestReceiveContext(
        RequestAddress endpoint,
        Guid messageId,
        string messageType,
        Type executorType,
        object message,
        CancellationToken cancellationToken)
        : base(endpoint, messageId, messageType, message, cancellationToken) =>
        _executorType = executorType;

    internal object? Response { get; private set; }

    private protected override async ValueTask ExecuteAsync(IServiceProvider provider, CancellationToken cancellationToken)
    {
        var executor = (IRequestExecutor)provider.GetRequiredService(_executorType);
        Response = await executor.ExecuteAsync(Message, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>One event delivered to one subscription, which may hold several handlers.</summary>
public sealed class EventReceiveContext : ReceiveContext
{
    private readonly Type _executorType;
    private readonly IReadOnlyList<Type> _handlerTypes;

    internal EventReceiveContext(
        RequestAddress topic,
        string subscription,
        Guid messageId,
        string messageType,
        Type executorType,
        IReadOnlyList<Type> handlerTypes,
        object message,
        CancellationToken cancellationToken)
        : base(topic, messageId, messageType, message, cancellationToken)
    {
        Subscription = subscription;
        _executorType = executorType;
        _handlerTypes = handlerTypes;
    }

    /// <summary>Name of the subscription this delivery belongs to.</summary>
    public string Subscription { get; }

    private protected override ValueTask ExecuteAsync(IServiceProvider provider, CancellationToken cancellationToken)
    {
        var executor = (IEventExecutor)provider.GetRequiredService(_executorType);
        return executor.ExecuteAsync(Message, _handlerTypes, cancellationToken);
    }
}
