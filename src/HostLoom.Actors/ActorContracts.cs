namespace HostLoom.Actors;

/// <summary>Owns local state for one incarnation. Each asynchronous turn finishes before the next begins.</summary>
public interface IActor<in TMessage, TReply>
{
    ValueTask<TReply> ReceiveAsync(TMessage message, CancellationToken stopping);
}

/// <summary>Sends immutable messages to one live or terminated incarnation.</summary>
public interface IActorRef<in TMessage, TReply>
{
    ActorAdmission TryTell(TMessage message);
    ValueTask<TReply> AskAsync(
        TMessage message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    );
}

public enum ActorAdmission
{
    Accepted,
    Full,
    Stopped,
}

public sealed class ActorAdmissionException(ActorAdmission reason)
    : InvalidOperationException($"Actor admission failed ({reason}).")
{
    public ActorAdmission Reason { get; } = reason;
}

/// <summary>The handler began execution and failed. Its external effects may be partial.</summary>
public sealed class ActorFaultException(Exception cause)
    : Exception("The actor handler failed.", cause);

/// <summary>The actor terminated before this admitted message started execution.</summary>
public sealed class ActorTerminatedException() : Exception("The queued message did not start.");

/// <summary>The owner can stop this incarnation or observe its failure after cleanup.</summary>
public sealed class ActorHandle<TMessage, TReply>
{
    private readonly ActorCell<TMessage, TReply> _cell;

    internal ActorHandle(ActorCell<TMessage, TReply> cell) => _cell = cell;

    public IActorRef<TMessage, TReply> Ref => _cell;
    public Task Completion => _cell.Completion;

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        _cell.Close();
        return Completion.WaitAsync(cancellationToken);
    }
}
