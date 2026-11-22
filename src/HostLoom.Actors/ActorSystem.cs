namespace HostLoom.Actors;

internal interface IActorCell
{
    Task Completion { get; }
    void Close();
    void Abort();
}

/// <summary>Owns a bounded population of explicit, process-local actors, including actors being constructed or disposed.</summary>
public sealed class ActorSystem : IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly HashSet<IActorCell> _actors = [];
    private readonly int _maxActors;
    private readonly int _mailboxCapacity;
    private readonly TimeSpan _shutdownGracePeriod;
    private readonly TimeProvider _clock;
    private readonly TaskCompletionSource _terminated = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private bool _closed;
    private readonly ActorDiagnostics _diagnostics;

    public ActorSystem(
        int maxActors,
        int mailboxCapacity,
        TimeSpan shutdownGracePeriod,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxActors, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(mailboxCapacity, 1);
        ValidateTimer(shutdownGracePeriod, nameof(shutdownGracePeriod));
        _maxActors = maxActors;
        _mailboxCapacity = mailboxCapacity;
        _shutdownGracePeriod = shutdownGracePeriod;
        _clock = timeProvider ?? TimeProvider.System;
        _diagnostics = new();
    }

    public ActorSnapshot Snapshot => _diagnostics.Snapshot;

    internal static void ValidateTimer(TimeSpan duration, string parameter)
    {
        if (duration < TimeSpan.Zero || duration.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(parameter);
    }

    public ActorHandle<TMessage, TReply> Spawn<TMessage, TReply>(
        Func<IActor<TMessage, TReply>> create
    )
    {
        ArgumentNullException.ThrowIfNull(create);
        var cell = Reserve<TMessage, TReply>();
        IActor<TMessage, TReply> actor;
        try
        {
            actor =
                create() ?? throw new InvalidOperationException("The actor factory returned null.");
        }
        catch
        {
            // The spawn caller owns the construction error. Shutdown only joins the reservation.
            cell.Finish(null);
            throw;
        }

        return StartReserved(cell, actor);
    }

    /// <summary>Retains the population reservation until asynchronous construction or failure cleanup finishes.</summary>
    public async ValueTask<ActorHandle<TMessage, TReply>> SpawnAsync<TMessage, TReply>(
        Func<ValueTask<IActor<TMessage, TReply>>> create
    )
    {
        ArgumentNullException.ThrowIfNull(create);
        var cell = Reserve<TMessage, TReply>();
        IActor<TMessage, TReply> actor;
        try
        {
            actor =
                await create().ConfigureAwait(false)
                ?? throw new InvalidOperationException("The actor factory returned null.");
        }
        catch
        {
            cell.Finish(null);
            throw;
        }
        return StartReserved(cell, actor);
    }

    private ActorCell<TMessage, TReply> Reserve<TMessage, TReply>()
    {
        ActorCell<TMessage, TReply> cell;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            if (_actors.Count >= _maxActors)
                throw new InvalidOperationException("The actor population limit has been reached.");
            cell = new(_mailboxCapacity, _clock, _diagnostics, Release);
            _actors.Add(cell);
            _diagnostics.Reserve();
            return cell;
        }
    }

    private ActorHandle<TMessage, TReply> StartReserved<TMessage, TReply>(
        ActorCell<TMessage, TReply> cell,
        IActor<TMessage, TReply> actor
    )
    {
        lock (_gate)
        {
            // Shutdown has already closed a reserved cell. Its reader will dispose the
            // constructed actor even when spawn can no longer hand it to the caller.
            cell.Start(actor);
            ObjectDisposedException.ThrowIf(_closed, this);
            return new(cell);
        }
    }

    private void Release(IActorCell cell)
    {
        lock (_gate)
        {
            _actors.Remove(cell);
            _diagnostics.Release();
        }
    }

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        IActorCell[] cells;
        lock (_gate)
        {
            if (_closed)
                return _terminated.Task.WaitAsync(cancellationToken);
            _closed = true;
            cells = _actors.ToArray();
            foreach (var cell in cells)
                cell.Close();
        }
        _ = ShutdownAsync(cells);
        return _terminated.Task.WaitAsync(cancellationToken);
    }

    private async Task ShutdownAsync(IActorCell[] cells)
    {
        var completed = Task.WhenAll(cells.Select(cell => cell.Completion));
        try
        {
            if (!completed.IsCompleted)
            {
                using var timer = new CancellationTokenSource();
                var deadline = Task.Delay(_shutdownGracePeriod, _clock, timer.Token);
                if (await Task.WhenAny(completed, deadline).ConfigureAwait(false) == deadline)
                {
                    foreach (var cell in cells)
                        cell.Abort();
                }
                await timer.CancelAsync().ConfigureAwait(false);
            }
            await completed.ConfigureAwait(false);
        }
        catch (Exception error)
        {
            _diagnostics.Dispose();
            _terminated.SetException(
                completed.Exception?.InnerExceptions ?? new[] { error }.AsReadOnly()
            );
            _ = _terminated.Task.Exception;
            return;
        }
        _diagnostics.Dispose();
        _terminated.SetResult();
    }

    public ValueTask DisposeAsync() => new(StopAsync());
}
