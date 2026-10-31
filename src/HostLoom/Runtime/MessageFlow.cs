namespace HostLoom;

/// <summary>
/// The flow of the delivery currently executing on this asynchronous context.
/// </summary>
/// <remarks>
/// Resolved from the delivery scope. Reading it outside a delivery throws, because there is no
/// inbound message to describe. A publish or request made on another thread, without this
/// execution context, starts its own flow.
/// </remarks>
public interface IMessageFlow
{
    /// <summary>Stable identifier of the chain this delivery belongs to.</summary>
    Guid ConversationId { get; }

    /// <summary>Identifier of the message that caused this one, when this delivery is a continuation.</summary>
    Guid? InitiatorId { get; }

    /// <summary>Wire message id of this delivery.</summary>
    Guid MessageId { get; }
}

/// <summary>
/// Ambient delivery identity. The dispatcher pushes one for each request or event, and outbound
/// envelopes read it when they are built.
/// </summary>
internal static class MessageFlow
{
    private static readonly AsyncLocal<State?> Ambient = new();

    internal readonly record struct State(
        Guid ConversationId,
        Guid? InitiatorId,
        Guid MessageId,
        Guid OutboundInitiatorId
    );

    internal static State? Current => Ambient.Value;

    /// <summary>Pushes the inbound envelope for the rest of this delivery, including its pipeline.</summary>
    public static IDisposable Begin(MessageEnvelope inbound)
    {
        var previous = Ambient.Value;
        Ambient.Value = new State(
            inbound.ConversationId ?? inbound.MessageId,
            inbound.InitiatorId,
            inbound.MessageId,
            inbound.CorrelationId ?? inbound.MessageId
        );
        return new Pop(previous);
    }

    /// <summary>
    /// Conversation and initiator to stamp on a message sent from the current execution context.
    /// A context with no delivery starts a new conversation and names no initiator.
    /// </summary>
    public static (Guid ConversationId, Guid? InitiatorId) StampOutbound()
    {
        if (Ambient.Value is not { } current)
        {
            return (Guid.NewGuid(), null);
        }

        return (current.ConversationId, current.OutboundInitiatorId);
    }

    internal static void Tag(System.Diagnostics.Activity? activity)
    {
        if (activity is null || Ambient.Value is not { } current)
        {
            return;
        }

        activity.SetTag("hostloom.conversation.id", current.ConversationId);
        if (current.InitiatorId is { } initiator)
        {
            activity.SetTag("hostloom.initiator.id", initiator);
        }
    }

    private sealed class Pop(State? previous) : IDisposable
    {
        public void Dispose() => Ambient.Value = previous;
    }
}

internal sealed class AmbientMessageFlow : IMessageFlow
{
    public Guid ConversationId =>
        MessageFlow.Current?.ConversationId
        ?? throw new InvalidOperationException("No message delivery is active on this context.");

    public Guid? InitiatorId => MessageFlow.Current?.InitiatorId;

    public Guid MessageId =>
        MessageFlow.Current?.MessageId
        ?? throw new InvalidOperationException("No message delivery is active on this context.");
}
