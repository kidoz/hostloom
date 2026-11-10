using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Threading.Channels;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;

[assembly: InternalsVisibleTo("HostLoom.Tests")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.ProcessProbe")]

namespace HostLoom.DataGrid.Runtime;

internal sealed record DataGridPeerRuntimeStatus(
    RaftRole Role,
    long Term,
    long CommitIndex,
    long AppliedIndex,
    long SnapshotIndex,
    bool Ready,
    bool Stopped,
    int? LeaderId = null
);

/// <summary>Experimental single actor. Local calls are test seams, not the public network client.</summary>
internal sealed class DataGridPeerRuntime
    : IDataGridPeerReceiver,
        IDataGridClientReceiver,
        IAsyncDisposable
{
    private readonly DataGridFileStore _store;
    private readonly DataGridBootstrapConfiguration _configuration;
    private readonly RaftNode _node;

    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "The actor finally block awaits transport disposal; DisposeAsync joins that owner task."
    )]
    private readonly DataGridPeerTransport _transport;
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _stop = new();
    private readonly DataGridRuntimeQueue _queue = new();
    private readonly object _admission = new();
    private readonly Dictionary<
        Guid,
        (TaskCompletionSource<RaftCompletion> Result, bool Read)
    > _pending = [];
    private readonly Dictionary<RaftToken, (CancellationTokenSource Cancel, Task Task)> _timers =
    [];
    private readonly Dictionary<int, SendSlot> _sends = [];
    private readonly List<Watcher> _watchers = [];
    private readonly Task _loop;
    private DataGridPeerRuntimeStatus _status;
    private Exception? _failure;
    private Task? _shutdown;

    // Caller transfers store/listener ownership; the transport captures owned credential material.
    internal DataGridPeerRuntime(
        DataGridFileStore store,
        DataGridPeerCredentials credentials,
        TcpListener listener,
        IReadOnlyList<IPAddress> peerAddresses,
        long processEpoch,
        TimeProvider? timeProvider = null,
        DataGridSecurityContext? security = null
    )
    {
        _store = store;
        _configuration = credentials.Configuration;
        _time = timeProvider ?? TimeProvider.System;
        DataGridPeerTransport? transport = null;
        try
        {
            store.ValidateOwner(credentials.Configuration, credentials.LocalMember);
            _node = new(
                credentials.Configuration,
                credentials.LocalMember,
                processEpoch,
                store.Image
            );
            _status = Capture(false);
            if (
                security is not null
                && (
                    security.LocalMember != credentials.LocalMember
                    || !security
                        .Configuration.ComputeDigest()
                        .AsSpan()
                        .SequenceEqual(credentials.Configuration.ComputeDigest())
                )
            )
                throw new ArgumentException("Security owner identity mismatch.", nameof(security));
            transport = security is null
                ? new(credentials, listener, peerAddresses, this)
                : new(security, listener, peerAddresses, this);
            transport.Start();
            _transport = transport;
            _loop = Task.Run(Run);
        }
        catch
        {
            try
            {
                transport?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                _store.Dispose();
                _queue.Dispose();
                _stop.Dispose();
            }
            throw;
        }
    }

    internal DataGridPeerRuntimeStatus Status => Volatile.Read(ref _status);
    internal Exception? Failure => Volatile.Read(ref _failure) ?? _transport.FatalError;
    internal int ActiveConnections => _transport.ActiveConnections;
    internal int RejectedConnections => _transport.RejectedConnections;

    internal Task<RaftCompletion> ProposeAsync(
        DataGridCommittedCommand command,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(command);
        return Submit(command, null, null, cancellationToken);
    }

    internal Task<RaftCompletion> ReadAsync(
        Guid map,
        DataGridKey key,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(key);
        return Submit(null, map, key, cancellationToken);
    }

    internal Task<RaftCompletion> ReadManyAsync(
        Guid map,
        IReadOnlyList<DataGridKey> keys,
        CancellationToken token
    )
    {
        var request = DataGridClientRequest.Read(map, keys);
        return Submit(null, map, null, token, request.Keys);
    }

    private async Task<RaftCompletion> Submit(
        DataGridCommittedCommand? command,
        Guid? map,
        DataGridKey? key,
        CancellationToken token,
        IReadOnlyList<DataGridKey>? keys = null
    )
    {
        token.ThrowIfCancellationRequested();
        var operation = DataGridRuntimeDiagnostics.Operation(command);
        var started = Stopwatch.GetTimestamp();
        var outcome = "faulted";
        DataGridRuntimeDiagnostics.Begin(operation);
        try
        {
            var result = await SubmitCore(command, map, key, token, keys).ConfigureAwait(false);
            outcome = DataGridRuntimeDiagnostics.Outcome(result.Kind);
            return result;
        }
        catch (OperationCanceledException)
        {
            outcome = "canceled";
            throw;
        }
        finally
        {
            DataGridRuntimeDiagnostics.End(
                operation,
                outcome,
                Stopwatch.GetElapsedTime(started).TotalSeconds
            );
        }
    }

    private async Task<RaftCompletion> SubmitCore(
        DataGridCommittedCommand? command,
        Guid? map,
        DataGridKey? key,
        CancellationToken token,
        IReadOnlyList<DataGridKey>? keys = null
    )
    {
        token.ThrowIfCancellationRequested();
        var callerToken = token;
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5), _time);
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(
            callerToken,
            deadline.Token
        );
        token = lifetime.Token;
        var result = new TaskCompletionSource<RaftCompletion>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var id = Guid.NewGuid();
        var bytes = command is null
            ? 128 + (keys?.Sum(key => key.ByteCount + 2) ?? 0)
            : DataGridCommittedCommandCodec.Encode(command).Length + 128;
        Post(
            () =>
            {
                if (token.IsCancellationRequested)
                {
                    result.TrySetCanceled(token);
                    return;
                }
                if (_pending.Count >= 128)
                {
                    result.TrySetResult(new(id, RaftCompletionKind.Busy));
                    return;
                }
                _pending.Add(id, (result, command is null));
                Apply(
                    command is null
                        ? keys is null
                            ? _node.Read(id, map!.Value, key!, Now())
                            : _node.ReadMany(id, map!.Value, keys, Now())
                        : _node.Propose(id, command)
                );
            },
            exception => result.TrySetException(exception),
            bytes,
            control: false
        );
        // Cancellation after posting may leave an admitted mutation; logical identity stays caller-owned.
        try
        {
            return await result.Task.WaitAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (deadline.IsCancellationRequested && !callerToken.IsCancellationRequested)
        {
            return new(
                id,
                command is null ? RaftCompletionKind.Unavailable : RaftCompletionKind.OutcomeUnknown
            );
        }
    }

    public async Task<DataGridClientReply> ExecuteAsync(
        Guid principal,
        DataGridClientRequest request,
        CancellationToken cancellationToken
    )
    {
        RaftCompletion completion;
        try
        {
            completion = request.Kind switch
            {
                DataGridMessageKind.Read => await ReadManyAsync(
                        request.MapId,
                        request.Keys,
                        cancellationToken
                    )
                    .ConfigureAwait(false),
                DataGridMessageKind.Mutation => await ProposeAsync(
                        DataGridCommittedCommand.Mutate(principal, request.Mutation!, Now()),
                        cancellationToken
                    )
                    .ConfigureAwait(false),
                DataGridMessageKind.OpenSession => await ProposeAsync(
                        DataGridCommittedCommand.OpenSession(
                            principal,
                            request.OpenNonce,
                            request.OpenNotAfter,
                            Now()
                        ),
                        cancellationToken
                    )
                    .ConfigureAwait(false),
                DataGridMessageKind.CloseSession => await ProposeAsync(
                        DataGridCommittedCommand.CloseSession(
                            principal,
                            request.SessionNumber,
                            request.RequestNumber,
                            Now()
                        ),
                        cancellationToken
                    )
                    .ConfigureAwait(false),
                _ => throw new InvalidDataException("Invalid client operation."),
            };
        }
        catch (Exception exception) when (exception is IOException or OperationCanceledException)
        {
            return DataGridClientReply.Failed(
                new(
                    request.Kind == DataGridMessageKind.Read
                        ? DataGridClientFailureCode.QuorumUnavailable
                        : DataGridClientFailureCode.OutcomeUnknown,
                    true
                )
            );
        }
        if (completion.Kind == RaftCompletionKind.Read)
            return DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: completion.ReadResult!
            );
        if (completion.Kind == RaftCompletionKind.Applied)
        {
            var result = completion.Result!;
            if (result.Status == DataGridApplyStatus.Completed)
                return DataGridClientReply.Completed(
                    request.Kind switch
                    {
                        DataGridMessageKind.Mutation => DataGridMessageKind.MutationResult,
                        DataGridMessageKind.OpenSession => DataGridMessageKind.OpenSessionResult,
                        _ => DataGridMessageKind.CloseSessionResult,
                    },
                    result
                );
            return DataGridClientReply.Failed(
                new(
                    result.Status switch
                    {
                        DataGridApplyStatus.SessionExpired =>
                            DataGridClientFailureCode.SessionExpired,
                        DataGridApplyStatus.RequestRetired =>
                            DataGridClientFailureCode.RequestRetired,
                        DataGridApplyStatus.RequestIdentityMismatch =>
                            DataGridClientFailureCode.RequestIdentityMismatch,
                        DataGridApplyStatus.CapacityExceeded =>
                            DataGridClientFailureCode.CapacityExceeded,
                        DataGridApplyStatus.MissingMap => DataGridClientFailureCode.MissingMap,
                        _ => DataGridClientFailureCode.InvalidRequest,
                    },
                    true
                )
            );
        }
        if (completion.Kind == RaftCompletionKind.NotLeader && Status.LeaderId is { } leader)
            return DataGridClientReply.Failed(
                new(
                    DataGridClientFailureCode.Redirect,
                    false,
                    _configuration.GetMember(leader).MemberId
                )
            );
        return DataGridClientReply.Failed(
            new(
                completion.Kind switch
                {
                    RaftCompletionKind.OutcomeUnknown => DataGridClientFailureCode.OutcomeUnknown,
                    RaftCompletionKind.Busy or RaftCompletionKind.CapacityExceeded =>
                        DataGridClientFailureCode.CapacityExceeded,
                    RaftCompletionKind.InvalidRequest => DataGridClientFailureCode.InvalidRequest,
                    _ => DataGridClientFailureCode.QuorumUnavailable,
                },
                completion.Kind
                    is RaftCompletionKind.OutcomeUnknown
                        or RaftCompletionKind.Unavailable
                        or RaftCompletionKind.CapacityExceeded
            )
        );
    }

    internal Task CompactAsync(CancellationToken token) =>
        Invoke(
            () =>
            {
                Apply(_node.Compact(Guid.NewGuid()));
                return true;
            },
            128,
            token
        );

    internal Task WaitForLeaderAsync(CancellationToken token) => Watch(0, leader: true, token);

    internal Task WaitForAppliedAsync(long index, CancellationToken token) =>
        Watch(index, leader: false, token);

    private async Task Watch(long index, bool leader, CancellationToken token)
    {
        var result = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await Invoke(
                () =>
                {
                    _watchers.RemoveAll(watcher => watcher.Completion.Task.IsCompleted);
                    if (_watchers.Count >= 64)
                        throw new IOException("Runtime watcher capacity.");
                    _watchers.Add(new(index, leader, result));
                    Publish();
                    return true;
                },
                128,
                token
            )
            .ConfigureAwait(false);
        try
        {
            await result.Task.WaitAsync(token).ConfigureAwait(false);
        }
        finally
        {
            result.TrySetCanceled(token);
        }
    }

    public Task<RaftMessage?> ReceiveAsync(
        RaftMessage message,
        CancellationToken cancellationToken
    ) =>
        Invoke(
            () => Apply(_node.Receive(message, Now()), message),
            checked((int)message.ByteCount),
            cancellationToken
        );

    public Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken) =>
        Invoke(
            () =>
            {
                Apply(_node.ObserveLeader(term));
                return _node.CurrentTerm;
            },
            128,
            cancellationToken
        );

    private async Task<T> Invoke<T>(Func<T> action, int bytes, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var result = new TaskCompletionSource<T>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        Post(
            () =>
            {
                if (token.IsCancellationRequested)
                {
                    result.TrySetCanceled(token);
                    return;
                }
                try
                {
                    result.TrySetResult(action());
                }
                catch (Exception exception)
                {
                    result.TrySetException(exception);
                    throw;
                }
            },
            exception => result.TrySetException(exception),
            bytes,
            control: true
        );
        return await result.Task.WaitAsync(token).ConfigureAwait(false);
    }

    private void Post(Action action, Action<Exception> reject, int bytes, bool control)
    {
        if (!TryPost(new(action, reject, bytes, control)))
            reject(new IOException("Runtime stopped or admission capacity exceeded."));
    }

    private bool TryPost(DataGridRuntimeWork work) =>
        _queue.TryWrite(work, _stop.IsCancellationRequested);

    private async Task Run()
    {
        try
        {
            Apply(_node.Start());
            Publish();
            while (!_stop.IsCancellationRequested)
            {
                var work = await _queue.ReadAsync(_stop.Token).ConfigureAwait(false);
                work.Action();
                Publish();
                if (_node.Role == RaftRole.Faulted)
                    throw new IOException("Raft owner faulted.");
                if (_transport.FatalError is { } fatal)
                    throw new IOException("Peer transport faulted.", fatal);
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (Exception exception)
        {
            Interlocked.CompareExchange(ref _failure, exception, null);
            await _stop.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            var error = new IOException(
                "Runtime stopped; admitted mutations may have committed.",
                Failure
            );
            foreach (var work in _queue.Complete())
                work.Reject(error);
            foreach (var pending in _pending)
                pending.Value.Result.TrySetResult(
                    new(
                        pending.Key,
                        pending.Value.Read
                            ? RaftCompletionKind.Unavailable
                            : RaftCompletionKind.OutcomeUnknown
                    )
                );
            foreach (var watcher in _watchers)
                watcher.Completion.TrySetException(error);
            var tasks = _timers
                .Values.Select(item => item.Task)
                .Concat(_sends.Values.Select(item => item.Task))
                .ToArray();
            foreach (var timer in _timers.Values)
                await timer.Cancel.CancelAsync().ConfigureAwait(false);
            foreach (var send in _sends.Values)
            {
                Task cancelled;
                lock (send.Sync)
                    cancelled = send.Current?.CancelAsync() ?? Task.CompletedTask;
                await cancelled.ConfigureAwait(false);
                send.Queue.Writer.TryComplete();
            }
            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    await _transport.DisposeAsync().ConfigureAwait(false);
                }
                finally
                {
                    foreach (var timer in _timers.Values)
                        timer.Cancel.Dispose();
                    try
                    {
                        _store.Dispose();
                    }
                    finally
                    {
                        Volatile.Write(ref _status, Capture(true));
                    }
                }
            }
        }
    }

    private RaftMessage? Apply(IReadOnlyList<RaftEffect> effects, RaftMessage? request = null)
    {
        RaftMessage? reply = null;
        var queue = new Queue<RaftEffect>(effects);
        while (queue.TryDequeue(out var effect))
        {
            switch (effect)
            {
                case RaftWrite write:
                    IReadOnlyList<RaftEffect> completed;
                    try
                    {
                        Persist(write.Image);
                        completed = _node.Persisted(write.Token, true);
                    }
                    catch (Exception exception)
                        when (exception
                                is IOException
                                    or OperationCanceledException
                                    or InvalidDataException
                        )
                    {
                        Interlocked.CompareExchange(ref _failure, exception, null);
                        completed = _node.Persisted(write.Token, false);
                    }
                    foreach (var item in completed)
                        queue.Enqueue(item);
                    break;
                case RaftSend send:
                    if (
                        send.Message.Kind
                        is RaftMessageKind.VoteResponse
                            or RaftMessageKind.AppendResponse
                            or RaftMessageKind.SnapshotResponse
                    )
                    {
                        if (
                            request is not null
                            && request.From == send.Message.To
                            && request.Token == send.Message.Token
                        )
                            reply = send.Message;
                    }
                    else
                        Send(send.Message);
                    break;
                case RaftTimer timer:
                    Schedule(timer);
                    break;
                case RaftCancelTimer cancel:
                    CancelTimer(cancel.Token);
                    break;
                case RaftCompletion completion:
                    if (_pending.Remove(completion.RequestId, out var result))
                        result.Result.TrySetResult(completion);
                    break;
                case RaftFault:
                    break;
            }
        }
        return reply;
    }

    private void Persist(RaftDurableImage image)
    {
        var started = Stopwatch.GetTimestamp();
        var outcome = "faulted";
        try
        {
            _store.Persist(image, _stop.Token);
            outcome = "persisted";
        }
        catch (OperationCanceledException)
        {
            outcome = "canceled";
            throw;
        }
        finally
        {
            DataGridRuntimeDiagnostics.Persisted(
                outcome,
                Stopwatch.GetElapsedTime(started).TotalSeconds
            );
        }
    }

    private void Schedule(RaftTimer timer)
    {
        if (_timers.Count >= 256)
            throw new IOException("Runtime timer capacity.");
        var cancel = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        var token = cancel.Token;
        var delay =
            timer.DelayMilliseconds == 0
                ? RandomNumberGenerator.GetInt32(500, 1001)
                : timer.DelayMilliseconds;
        var task = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(delay), _time, token)
                    .ConfigureAwait(false);
                if (
                    !TryPost(
                        new(
                            () =>
                            {
                                CancelTimer(timer.Token);
                                Apply(_node.Timeout(timer.Token, Now()));
                            },
                            _ => { },
                            128,
                            true
                        )
                    )
                )
                    StopForCapacity();
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        });
        _timers.Add(timer.Token, (cancel, task));
    }

    private void CancelTimer(RaftToken token)
    {
        if (_timers.Remove(token, out var timer))
        {
            timer.Cancel.Cancel();
            timer.Cancel.Dispose();
        }
        foreach (var send in _sends.Values)
        {
            lock (send.Sync)
            {
                if (send.Token == token)
                    send.Current?.Cancel();
                if (send.QueuedToken == token)
                    send.DiscardedToken = token;
            }
        }
    }

    private void Send(RaftMessage message)
    {
        if (!_sends.TryGetValue(message.To, out var slot))
        {
            slot = new SendSlot();
            _sends.Add(message.To, slot);
            slot.Task = SendWorker(slot);
        }
        lock (slot.Sync)
        {
            slot.Current?.Cancel();
            slot.QueuedToken = message.Token;
            while (slot.Queue.Reader.TryRead(out _)) { }
            if (!slot.Queue.Writer.TryWrite(message))
                throw new IOException("Peer sender stopped.");
        }
    }

    private async Task SendWorker(SendSlot slot)
    {
        try
        {
            await foreach (
                var message in slot.Queue.Reader.ReadAllAsync(_stop.Token).ConfigureAwait(false)
            )
            {
                using var cancel = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                lock (slot.Sync)
                {
                    if (slot.DiscardedToken == message.Token)
                        continue;
                    slot.Current = cancel;
                    slot.Token = message.Token;
                }
                try
                {
                    await Exchange(message, cancel.Token).ConfigureAwait(false);
                }
                finally
                {
                    lock (slot.Sync)
                        slot.Current = null;
                }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (Exception exception)
        {
            Interlocked.CompareExchange(ref _failure, exception, null);
            await _stop.CancelAsync().ConfigureAwait(false);
        }
    }

    private async Task Exchange(RaftMessage message, CancellationToken token)
    {
        try
        {
            var response = await _transport.SendAsync(message, token).ConfigureAwait(false);
            if (
                !token.IsCancellationRequested
                && !TryPost(
                    new(
                        () => Apply(_node.Receive(response, Now())),
                        _ => { },
                        checked((int)response.ByteCount),
                        true
                    )
                )
            )
                StopForCapacity();
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or AuthenticationException
                        or SocketException
                        or OperationCanceledException
                        or InvalidDataException
            ) { }
    }

    private void StopForCapacity()
    {
        if (_stop.IsCancellationRequested)
            return;
        Interlocked.CompareExchange(
            ref _failure,
            new IOException("Reserved control lane exhausted."),
            null
        );
        _stop.Cancel();
    }

    private DataGridTimestamp Now() => DataGridTimestamp.FromUtc(_time.GetUtcNow());

    private DataGridPeerRuntimeStatus Capture(bool stopped) =>
        new(
            _node.Role,
            _node.CurrentTerm,
            _node.Durable.CommitIndex,
            _node.Machine.LastAppliedIndex,
            _node.Durable.SnapshotIndex,
            !stopped
                && _node.Role == RaftRole.Leader
                && !_node.IsBusy
                && _node.Durable.CommitIndex > 0
                && _node.Durable.TermAt(_node.Durable.CommitIndex) == _node.CurrentTerm,
            stopped,
            _node.LeaderId
        );

    private void Publish()
    {
        var state = Capture(false);
        Volatile.Write(ref _status, state);
        foreach (
            var watcher in _watchers
                .Where(watcher =>
                    (watcher.Leader ? state.Ready : state.AppliedIndex >= watcher.Index)
                )
                .ToArray()
        )
            watcher.Completion.TrySetResult();
        _watchers.RemoveAll(watcher => watcher.Completion.Task.IsCompleted);
    }

    public ValueTask DisposeAsync()
    {
        lock (_admission)
            return new(_shutdown ??= ShutdownAsync());
    }

    private async Task ShutdownAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        // A timed-out synchronous filesystem operation retains ownership until the actor actually exits.
        await _loop.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        _queue.Dispose();
        _stop.Dispose();
    }

    private sealed class SendSlot
    {
        internal object Sync { get; } = new();
        internal Channel<RaftMessage> Queue { get; } = Channel.CreateBounded<RaftMessage>(1);
        internal CancellationTokenSource? Current { get; set; }
        internal RaftToken Token { get; set; }
        internal RaftToken QueuedToken { get; set; }
        internal RaftToken DiscardedToken { get; set; }
        internal Task Task { get; set; } = Task.CompletedTask;
    }

    private sealed record Watcher(long Index, bool Leader, TaskCompletionSource Completion);
}
