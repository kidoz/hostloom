using System.Threading.Channels;

namespace HostLoom.Actors;

internal sealed class ActorCell<TMessage, TReply> : IActorCell, IActorRef<TMessage, TReply>
{
    private readonly record struct Envelope(
        TMessage Message,
        long Admitted,
        TaskCompletionSource<TReply>? Reply
    );

    private readonly Channel<Envelope> _mailbox;
    private readonly Lock _gate = new();
    private readonly TimeProvider _clock;
    private readonly ActorDiagnostics _diagnostics;
    private readonly CancellationTokenSource _stopping = new();
    private readonly TaskCompletionSource _completion = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private Action<IActorCell>? _release;
    private Task? _abortCallbacks;
    private bool _closed;
    private bool _finishing;

    internal ActorCell(
        int capacity,
        TimeProvider clock,
        ActorDiagnostics diagnostics,
        Action<IActorCell> release
    )
    {
        _mailbox = Channel.CreateBounded<Envelope>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false,
            }
        );
        _clock = clock;
        _diagnostics = diagnostics;
        _release = release;
    }

    public Task Completion => _completion.Task;

    internal void Start(IActor<TMessage, TReply> actor)
    {
        // The actor must not inherit a spawning request's AsyncLocal state.
        ThreadPool.UnsafeQueueUserWorkItem(
            static state =>
            {
                _ = state.Cell.RunAsync(state.Actor);
            },
            (Cell: this, Actor: actor),
            preferLocal: false
        );
    }

    public ActorAdmission TryTell(TMessage message)
    {
        lock (_gate)
            return Admit(new(message, _clock.GetTimestamp(), null));
    }

    public ValueTask<TReply> AskAsync(
        TMessage message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        var start = _clock.GetTimestamp();
        ActorSystem.ValidateTimer(timeout, nameof(timeout));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var admitted = _clock.GetTimestamp();
            var remaining = timeout - _clock.GetElapsedTime(start, admitted);
            if (remaining <= TimeSpan.Zero)
            {
                _diagnostics.Timeout();
                throw new TimeoutException();
            }

            var reply = new TaskCompletionSource<TReply>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var admission = Admit(new(message, admitted, reply));
            if (admission != ActorAdmission.Accepted)
                throw new ActorAdmissionException(admission);
            return WaitForReplyAsync(reply.Task, remaining, cancellationToken);
        }
    }

    private async ValueTask<TReply> WaitForReplyAsync(
        Task<TReply> reply,
        TimeSpan remaining,
        CancellationToken cancellationToken
    )
    {
        try
        {
            return await reply
                .WaitAsync(remaining, _clock, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _diagnostics.Timeout();
            throw;
        }
    }

    private ActorAdmission Admit(Envelope envelope)
    {
        if (_closed)
        {
            _diagnostics.Stopped();
            return ActorAdmission.Stopped;
        }
        if (!_mailbox.Writer.TryWrite(envelope))
        {
            _diagnostics.Full();
            return ActorAdmission.Full;
        }
        _diagnostics.Enqueue();
        return ActorAdmission.Accepted;
    }

    public void Close()
    {
        lock (_gate)
        {
            _closed = true;
            _mailbox.Writer.TryComplete();
        }
    }

    public void Abort()
    {
        lock (_gate)
        {
            if (_finishing || _abortCallbacks is not null)
                return;
            // Shutdown closed admission first. Serialize discard with the consumer's take.
            DiscardQueued();
            _abortCallbacks = _stopping.CancelAsync();
        }
    }

    private void DiscardQueued()
    {
        while (_mailbox.Reader.TryRead(out var pending))
        {
            _diagnostics.Discard();
            if (pending.Reply is not null)
                FailReply(pending.Reply, new ActorTerminatedException());
        }
    }

    private static void FailReply(TaskCompletionSource<TReply> reply, Exception error)
    {
        reply.SetException(error);
        // A caller may already have abandoned its wait.
        _ = reply.Task.Exception;
    }

    private async Task RunAsync(IActor<TMessage, TReply> actor)
    {
        Exception? failure = null;
        try
        {
            var batch = 0;
            while (await _mailbox.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                Envelope current;
                lock (_gate)
                {
                    if (!_mailbox.Reader.TryRead(out current))
                        continue;
                    _diagnostics.BeginTurn();
                }
                var started = _clock.GetTimestamp();
                try
                {
                    var value = await actor
                        .ReceiveAsync(current.Message, _stopping.Token)
                        .ConfigureAwait(false);
                    current.Reply?.SetResult(value);
                }
                catch (OperationCanceledException error)
                    when (_stopping.IsCancellationRequested
                        && error.CancellationToken == _stopping.Token
                    )
                {
                    current.Reply?.SetCanceled(_stopping.Token);
                }
                catch (Exception error)
                {
                    Close();
                    var fault = new ActorFaultException(error);
                    if (current.Reply is not null)
                        FailReply(current.Reply, fault);
                    _diagnostics.Fault();
                    throw fault;
                }
                finally
                {
                    _diagnostics.EndTurn();
                    _diagnostics.RecordTurn(
                        _clock.GetElapsedTime(current.Admitted, started),
                        _clock.GetElapsedTime(started)
                    );
                }
                if (++batch == 64)
                {
                    batch = 0;
                    await Task.Yield();
                }
            }
        }
        catch (Exception error)
        {
            failure = error;
        }

        Task? callbacks;
        lock (_gate)
        {
            Close();
            DiscardQueued();
            _finishing = true;
            callbacks = _abortCallbacks;
        }
        try
        {
            if (callbacks is not null)
                await callbacks.ConfigureAwait(false);
        }
        catch (Exception error)
        {
            failure = Combine(failure, error);
        }
        try
        {
            if (actor is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            else if (actor is IDisposable disposable)
                disposable.Dispose();
        }
        catch (Exception error)
        {
            failure = Combine(failure, error);
        }
        Finish(failure);
    }

    private static Exception Combine(Exception? first, Exception second) =>
        first is null ? second : new AggregateException(first, second);

    internal void Finish(Exception? failure)
    {
        lock (_gate)
        {
            _finishing = true;
            _stopping.Dispose();
        }
        var release = _release!;
        _release = null;
        release(this);
        if (failure is null)
            _completion.SetResult();
        else
        {
            _completion.SetException(failure);
            _ = Completion.Exception;
        }
    }
}
