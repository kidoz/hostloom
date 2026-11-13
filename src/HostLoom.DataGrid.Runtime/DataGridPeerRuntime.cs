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
    private long _lastMaintenance;

    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "RunOwned finally disposes the source after actual owner cleanup, including when the bounded disposal wait times out. Completion joins that owner task."
    )]
    private readonly CancellationTokenSource _stop = new();

    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "RunOwned finally disposes the drained queue independently of the bounded disposal wait; Completion joins actual cleanup."
    )]
    private readonly DataGridRuntimeQueue _queue = new();
    private readonly DataGridRuntimeOperationBudget _operations = new();
    private readonly DataGridRuntimeOperationBudget _control = new(control: true);
    private readonly object _admission = new();
    private readonly Dictionary<
        Guid,
        (
            TaskCompletionSource<RaftCompletion> Result,
            bool Read,
            DataGridRuntimeOperationBudget.Lease Reservation
        )
    > _pending = [];
    private readonly Dictionary<RaftToken, (CancellationTokenSource Cancel, Task Task)> _timers =
    [];
    private readonly Dictionary<int, SendSlot> _sends = [];
    private readonly List<Watcher> _watchers = [];
    private readonly Task _loop;
    private DataGridPeerRuntimeStatus _status;
    private DataGridRuntimeSnapshot _diagnostics;
    private readonly int _memberOrdinal;
    private long _lastReadBarrierIndex,
        _lastReadBarrierTerm;
    private Exception? _failure;
    private Task? _shutdown;
    private Task? _cancellation;

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
        _memberOrdinal = credentials.LocalMember;
        _time = timeProvider ?? TimeProvider.System;
        _lastMaintenance = _time.GetTimestamp();
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
            _diagnostics = CaptureDiagnostics(_status);
            _loop = Task.Run(RunOwned);
        }
        catch
        {
            try
            {
                transport?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                try
                {
                    // Ownership transfers even when validation fails before transport construction.
                    listener.Stop();
                }
                finally
                {
                    _store.Dispose();
                    _queue.Dispose();
                    _stop.Dispose();
                }
            }
            throw;
        }
    }

    internal DataGridPeerRuntimeStatus Status => Volatile.Read(ref _status);
    internal DataGridRuntimeSnapshot Diagnostics => Volatile.Read(ref _diagnostics);

    // Actual owner cleanup, independent of the bounded DisposeAsync wait.
    internal Task Completion => _loop;
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
        IReadOnlyList<DataGridKey>? keys = null,
        Guid principal = default
    )
    {
        token.ThrowIfCancellationRequested();
        var operation = DataGridRuntimeDiagnostics.Operation(command);
        var started = Stopwatch.GetTimestamp();
        var outcome = "faulted";
        DataGridRuntimeDiagnostics.Begin(operation);
        try
        {
            var result = await SubmitCore(command, map, key, token, keys, principal)
                .ConfigureAwait(false);
            outcome = DataGridRuntimeDiagnostics.Outcome(result.Kind);
            return result;
        }
        catch (OperationCanceledException)
        {
            outcome = "canceled";
            throw;
        }
        catch (DataGridRuntimeAdmissionException exception)
        {
            outcome =
                exception.Failure == DataGridRuntimeAdmissionFailure.Capacity
                    ? "busy"
                    : "unavailable";
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
        IReadOnlyList<DataGridKey>? keys = null,
        Guid principal = default
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
            ? 128 + (keys?.Sum(item => item.ByteCount + 2) ?? (key is null ? 0 : key.ByteCount + 2))
            : DataGridCommittedCommandCodec.Encode(command).Length + 128;
        DataGridRuntimeOperationBudget.Lease? acquired =
            _operations.TryAcquire(principal, bytes, out var failure, _stop.IsCancellationRequested)
            ?? throw new DataGridRuntimeAdmissionException(failure);
        try
        {
            var reservation = acquired;
            var work = new DataGridRuntimeWork(
                () =>
                {
                    if (token.IsCancellationRequested)
                    {
                        reservation.Dispose();
                        result.TrySetCanceled(token);
                        return;
                    }
                    if (_pending.Count >= 128)
                    {
                        reservation.Dispose();
                        result.TrySetResult(new(id, RaftCompletionKind.Busy));
                        return;
                    }
                    _pending.Add(id, (result, command is null, reservation));
                    Apply(
                        command is null
                            ? keys is null
                                ? _node.Read(id, map!.Value, key!, Now())
                                : _node.ReadMany(id, map!.Value, keys, Now())
                            : _node.Propose(id, command)
                    );
                },
                exception =>
                {
                    reservation.Dispose();
                    result.TrySetException(exception);
                },
                bytes,
                Control: false,
                Principal: principal
            );
            if (!_queue.TryWrite(work, out failure, _stop.IsCancellationRequested))
            {
                throw new DataGridRuntimeAdmissionException(failure);
            }
            acquired = null; // Queue/pending completion now owns the charge.
        }
        finally
        {
            acquired?.Dispose();
        }
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
        if (principal == Guid.Empty)
            throw new ArgumentException("Authenticated principal required.", nameof(principal));
        ArgumentNullException.ThrowIfNull(request);
        RaftCompletion completion;
        try
        {
            var command = request.Kind switch
            {
                DataGridMessageKind.Read => null,
                DataGridMessageKind.Mutation => DataGridCommittedCommand.Mutate(
                    principal,
                    request.Mutation!,
                    Now()
                ),
                DataGridMessageKind.OpenSession => DataGridCommittedCommand.OpenSession(
                    principal,
                    request.OpenNonce,
                    request.OpenNotAfter,
                    Now()
                ),
                DataGridMessageKind.CloseSession => DataGridCommittedCommand.CloseSession(
                    principal,
                    request.SessionNumber,
                    request.RequestNumber,
                    Now()
                ),
                _ => throw new InvalidDataException("Invalid client operation."),
            };
            completion = await Submit(
                    command,
                    request.MapId,
                    null,
                    cancellationToken,
                    request.Kind == DataGridMessageKind.Read ? request.Keys : null,
                    principal
                )
                .ConfigureAwait(false);
        }
        catch (DataGridRuntimeAdmissionException exception)
        {
            return DataGridClientReply.Failed(
                new(
                    exception.Failure == DataGridRuntimeAdmissionFailure.Capacity
                        ? DataGridClientFailureCode.CapacityExceeded
                        : DataGridClientFailureCode.QuorumUnavailable,
                    false
                )
            );
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

    internal Task<RaftCompactionStatus> CompactAsync(CancellationToken token) =>
        Invoke(CompactOwned, 128, token);

    private RaftCompactionStatus CompactOwned()
    {
        if (_store.CollectObsoleteFiles(_stop.Token) == 32)
            return RaftCompactionStatus.Busy;
        var compaction = _node.PrepareCompaction(Guid.NewGuid());
        Apply(compaction.Effects);
        if (_node.Role == RaftRole.Faulted)
            throw new IOException("Snapshot publication failed.", Failure);
        if (compaction.Status != RaftCompactionStatus.Prepared)
            return compaction.Status;
        return RaftCompactionStatus.Published;
    }

    private void MaintainOwned()
    {
        if (_node.Role == RaftRole.Faulted || _stop.IsCancellationRequested)
            return;
        var compactDue =
            _node.Durable.Entries.Count >= RaftDurableImage.CompactEntryThreshold
            || _node.Durable.Entries.Sum(entry => (long)entry.ByteCount)
                >= RaftDurableImage.CompactByteThreshold;
        if (compactDue)
        {
            if (CompactOwned() == RaftCompactionStatus.TooLarge)
                throw new IOException("Configured state exceeds snapshot capacity.");
        }
        var now = _time.GetTimestamp();
        if (_time.GetElapsedTime(_lastMaintenance, now) < TimeSpan.FromSeconds(1))
            return;
        _lastMaintenance = now;
        if (!compactDue)
            _store.CollectObsoleteFiles(_stop.Token);
        Apply(_node.Maintain(Now()));
    }

    internal Task WaitForLeaderAsync(CancellationToken token) => Watch(0, leader: true, token);

    internal Task WaitForAppliedAsync(long index, CancellationToken token) =>
        Watch(index, leader: false, token);

    private async Task Watch(long index, bool leader, CancellationToken token)
    {
        var result = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var registered = await Invoke(
                    () =>
                    {
                        _watchers.RemoveAll(watcher => watcher.Completion.Task.IsCompleted);
                        if (_watchers.Count >= 64)
                            return false;
                        _watchers.Add(new(index, leader, result));
                        Publish();
                        return true;
                    },
                    128,
                    token
                )
                .ConfigureAwait(false);
            // Expected exhaustion belongs to this caller, not the actor's fault boundary.
            if (!registered)
                throw new IOException("Runtime watcher capacity.");
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

    private bool TryPost(DataGridRuntimeWork work)
    {
        if (!work.Control)
            return _queue.TryWrite(work, _stop.IsCancellationRequested);
        DataGridRuntimeOperationBudget.Lease? acquired = _control.TryAcquire(
            Guid.Empty,
            work.Bytes,
            out _,
            _stop.IsCancellationRequested
        );
        try
        {
            if (acquired is null)
                return false;
            var reservation = acquired;
            var retained = work with
            {
                Action = () =>
                {
                    try
                    {
                        work.Action();
                    }
                    finally
                    {
                        reservation.Dispose();
                    }
                },
                Reject = exception =>
                {
                    try
                    {
                        work.Reject(exception);
                    }
                    finally
                    {
                        reservation.Dispose();
                    }
                },
            };
            if (!_queue.TryWrite(retained, _stop.IsCancellationRequested))
                return false;
            acquired = null; // Action completion or rejection now owns the charge.
            return true;
        }
        finally
        {
            acquired?.Dispose();
        }
    }

    private Task StopAsync()
    {
        lock (_admission)
        {
            _operations.Complete();
            var cancellation = _cancellation ??= _stop.CancelAsync();
            _control.Complete();
            return cancellation;
        }
    }

    private async Task RunOwned()
    {
        try
        {
            await Run().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Interlocked.CompareExchange(ref _failure, exception, null);
            var final = Capture(true);
            Volatile.Write(ref _status, final);
            PublishDiagnostics(final);
            throw;
        }
        finally
        {
            try
            {
                _queue.Dispose();
            }
            finally
            {
                _stop.Dispose();
            }
        }
    }

    private async Task Run()
    {
        try
        {
            _store.CollectObsoleteFiles(_stop.Token);
            Apply(_node.Start());
            Publish();
            while (!_stop.IsCancellationRequested)
            {
                var work = await _queue.ReadAsync(_stop.Token).ConfigureAwait(false);
                work.Action();
                MaintainOwned();
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
            await StopAsync().ConfigureAwait(false);
        }
        finally
        {
            await StopAsync().ConfigureAwait(false);
            var error = new IOException(
                "Runtime stopped; admitted mutations may have committed.",
                Failure
            );
            _operations.Complete();
            foreach (var work in _queue.Complete())
                work.Reject(error);
            foreach (var pending in _pending)
            {
                pending.Value.Reservation.Dispose();
                pending.Value.Result.TrySetResult(
                    new(
                        pending.Key,
                        pending.Value.Read
                            ? RaftCompletionKind.Unavailable
                            : RaftCompletionKind.OutcomeUnknown
                    )
                );
            }
            _pending.Clear();
            foreach (var watcher in _watchers)
                watcher.Completion.TrySetException(error);
            _watchers.Clear();
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
                    _timers.Clear();
                    foreach (var send in _sends.Values)
                        while (send.Queue.Reader.TryRead(out _)) { }
                    _sends.Clear();
                    try
                    {
                        _store.Dispose();
                    }
                    finally
                    {
                        var final = Capture(true);
                        Volatile.Write(ref _status, final);
                        PublishDiagnostics(final);
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
                    if (completion.Kind == RaftCompletionKind.Read)
                    {
                        _lastReadBarrierIndex = completion.Index;
                        _lastReadBarrierTerm = _node.CurrentTerm;
                    }
                    if (_pending.Remove(completion.RequestId, out var result))
                    {
                        result.Reservation.Dispose();
                        result.Result.TrySetResult(completion);
                    }
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
            await StopAsync().ConfigureAwait(false);
        }
        finally
        {
            lock (slot.Sync)
            {
                while (slot.Queue.Reader.TryRead(out _)) { }
                slot.Current = null;
            }
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
        _ = StopAsync();
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
        PublishDiagnostics(state);
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

    private DataGridRuntimeSnapshot CaptureDiagnostics(DataGridPeerRuntimeStatus state)
    {
        var faulted = Failure is not null || _store.IsFaulted || _node.Role == RaftRole.Faulted;
        var maps = Enumerable
            .Range(0, _configuration.MapCount)
            .Select(_node.Machine.GetMapUsage)
            .ToArray();
        var replicas = Enumerable
            .Range(0, 3)
            .Where(peer => peer != _memberOrdinal)
            .Select(peer =>
            {
                var progress = _node.ReplicationProgress(peer);
                var known = !state.Stopped && !faulted && progress.Acknowledged;
                return new DataGridReplicaProgress(
                    peer,
                    known,
                    known ? progress.MatchedIndex : null,
                    known ? Math.Max(0, _node.Durable.LastIndex - progress.MatchedIndex) : null
                );
            })
            .ToArray();
        return new(
            _memberOrdinal,
            Stopwatch.GetTimestamp(),
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            state,
            faulted,
            _node.IsBusy,
            state.Ready && !faulted,
            _node.Durable.LastIndex,
            _lastReadBarrierIndex,
            _lastReadBarrierTerm,
            Array.AsReadOnly(maps),
            Array.AsReadOnly(replicas),
            _store.Progress
        );
    }

    private void PublishDiagnostics(DataGridPeerRuntimeStatus state)
    {
        var snapshot = CaptureDiagnostics(state);
        Volatile.Write(ref _diagnostics, snapshot);
        DataGridRuntimeDiagnostics.RecordState(snapshot);
    }

    public ValueTask DisposeAsync()
    {
        lock (_admission)
            return new(_shutdown ??= ShutdownAsync());
    }

    private async Task ShutdownAsync()
    {
        _ = StopAsync();
        // A timed-out synchronous filesystem operation retains ownership until the actor actually exits.
        await _loop.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
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
