namespace HostLoom.DataGrid.Consensus;

/// <summary>Single-owner deterministic three-voter model. No clock, I/O, transport or lease authority.</summary>
internal sealed class RaftNode
{
    private readonly DataGridBootstrapConfiguration _configuration;
    private readonly int _id;
    private readonly long _epoch;
    private readonly List<RaftEffect> _effects = [];
    private readonly Dictionary<Guid, ClientWait> _clients = [];
    private readonly Dictionary<int, RpcWait> _rpcs = [];
    private readonly Dictionary<int, RaftToken> _votes = [];
    private readonly HashSet<int> _granted = [];
    private readonly long[] _matched = new long[3];
    private readonly long[] _next = new long[3];
    private long _sequence;
    private RaftToken _mainTimer;
    private RaftWrite? _pending;
    private Action? _afterWrite;
    private RaftMessage? _deferred;
    private DataGridTimestamp _eventTime;
    private RaftDurableImage _durable;
    private DataGridLocalStateMachine _machine;

    internal RaftNode(
        DataGridBootstrapConfiguration configuration,
        int id,
        long epoch,
        RaftDurableImage durable
    )
    {
        if (id is < 0 or > 2 || epoch <= 0)
            throw new ArgumentOutOfRangeException(nameof(id));
        _configuration = configuration;
        _id = id;
        _epoch = epoch;
        _durable = durable;
        CurrentTerm = durable.Term;
        _machine = durable.Snapshot is { } image
            ? DataGridLocalStateMachine.Restore(configuration, image.Descriptor, image.GetContent())
            : new(configuration);
        ApplyCommitted();
        _effects.Clear();
    }

    internal RaftRole Role { get; private set; }
    internal long CurrentTerm { get; private set; }
    internal bool IsBusy => _pending is not null;
    internal RaftDurableImage Durable => _durable;
    internal DataGridLocalStateMachine Machine => _machine;
    internal int? LeaderId { get; private set; }

    internal IReadOnlyList<RaftEffect> Start() => Run(() => ResetMainTimer());

    // Authenticated snapshot progress observes leader authority before the image is complete.
    internal IReadOnlyList<RaftEffect> ObserveLeader(long term) =>
        Run(() =>
        {
            if (term <= 0 || term < CurrentTerm)
                return;
            var changed = term > CurrentTerm;
            CurrentTerm = term;
            StepDown();
            ResetMainTimer();
            if (changed)
            {
                _deferred = null;
                if (!IsBusy)
                    PersistHigherTerm();
            }
        });

    internal IReadOnlyList<RaftEffect> Receive(RaftMessage message, DataGridTimestamp time) =>
        Run(() =>
        {
            _eventTime = time;
            if (
                message.To != _id
                || message.From is < 0 or > 2
                || message.From == _id
                || message.Term < 0
                || message.Token.Epoch <= 0
                || message.Token.Sequence <= 0
                || !message.Matches(_configuration)
            )
                return;
            if (message.Term > CurrentTerm)
            {
                CurrentTerm = message.Term;
                StepDown();
                _deferred = message;
                if (!IsBusy)
                    PersistHigherTerm();
                return;
            }
            if (IsBusy)
                return;
            Dispatch(message);
        });

    internal IReadOnlyList<RaftEffect> Timeout(RaftToken token, DataGridTimestamp time) =>
        Run(() =>
        {
            _eventTime = time;
            var client = _clients.Values.FirstOrDefault(wait => wait.Deadline == token);
            if (client is not null)
            {
                Complete(
                    client,
                    client.IsRead
                        ? RaftCompletionKind.Unavailable
                        : RaftCompletionKind.OutcomeUnknown
                );
                return;
            }
            var rpc = _rpcs.FirstOrDefault(pair => pair.Value.Token == token);
            if (rpc.Value is not null)
            {
                _rpcs.Remove(rpc.Key);
                if (!IsBusy && Role == RaftRole.Leader)
                    SendAppend(rpc.Key);
                return;
            }
            if (token != _mainTimer)
                return;
            _mainTimer = default;
            if (IsBusy)
                return;
            if (Role == RaftRole.Leader)
            {
                Replicate();
                ResetMainTimer();
                return;
            }
            CurrentTerm = checked(CurrentTerm + 1);
            Role = RaftRole.Candidate;
            LeaderId = null;
            _votes.Clear();
            _granted.Clear();
            _granted.Add(_id);
            Persist(
                Image(CurrentTerm, _id),
                () =>
                {
                    foreach (var peer in Peers())
                    {
                        var request = NewToken();
                        _votes[peer] = request;
                        Send(
                            new(
                                RaftMessageKind.VoteRequest,
                                _id,
                                peer,
                                CurrentTerm,
                                request,
                                _configuration.IncarnationId,
                                _configuration.ComputeDigest(),
                                index: _durable.LastIndex,
                                previousTerm: _durable.TermAt(_durable.LastIndex)
                            )
                        );
                    }
                }
            );
        });

    internal IReadOnlyList<RaftEffect> Persisted(RaftToken token, bool success) =>
        Run(() =>
        {
            if (_pending is null || _pending.Token != token)
                return;
            if (!success)
            {
                Fault("Uncertain durable write.");
                return;
            }
            var image = _pending.Image;
            var continuation = _afterWrite;
            var writtenTerm = image.Term;
            _pending = null;
            _afterWrite = null;
            _durable = image;
            ApplyCommitted();
            if (CurrentTerm > writtenTerm)
            {
                PersistHigherTerm();
                return;
            }
            continuation?.Invoke();
            if (!IsBusy)
                ResetMainTimer();
        });

    internal IReadOnlyList<RaftEffect> Propose(Guid requestId, DataGridCommittedCommand command) =>
        Run(() =>
        {
            ArgumentNullException.ThrowIfNull(command);
            Admit(requestId, command, null, null);
        });

    internal IReadOnlyList<RaftEffect> Read(
        Guid requestId,
        Guid mapId,
        DataGridKey key,
        DataGridTimestamp time
    ) =>
        Run(() =>
        {
            ArgumentNullException.ThrowIfNull(key);
            if (
                !Enumerable
                    .Range(0, _configuration.MapCount)
                    .Any(i => _configuration.GetMap(i).MapId == mapId)
            )
            {
                _effects.Add(new RaftCompletion(requestId, RaftCompletionKind.InvalidRequest));
                return;
            }
            Admit(requestId, DataGridCommittedCommand.AdvanceTime(time), mapId, key);
        });

    internal IReadOnlyList<RaftEffect> Compact(Guid snapshotId) =>
        Run(() =>
        {
            if (IsBusy || _durable.CommitIndex == _durable.SnapshotIndex)
                return;
            var image = _machine.SaveSnapshot(snapshotId, _durable.TermAt(_durable.CommitIndex));
            if (image.Descriptor.ByteCount > RaftDurableImage.MaxImageBytes)
                return;
            Persist(
                new(
                    _configuration,
                    _durable.Term,
                    _durable.Vote,
                    _durable.Entries.Where(entry => entry.Index > _durable.CommitIndex),
                    _durable.CommitIndex,
                    image
                ),
                () =>
                {
                    if (Role == RaftRole.Leader)
                        Replicate();
                }
            );
        });

    internal IReadOnlyList<RaftEffect> ReadMany(
        Guid requestId,
        Guid map,
        IReadOnlyList<DataGridKey> keys,
        DataGridTimestamp time
    ) =>
        Run(() =>
        {
            if (
                keys.Count is < 1 or > 128
                || !Enumerable
                    .Range(0, _configuration.MapCount)
                    .Any(i => _configuration.GetMap(i).MapId == map)
            )
            {
                _effects.Add(new RaftCompletion(requestId, RaftCompletionKind.InvalidRequest));
                return;
            }
            var owned = keys.ToArray();
            for (var i = 0; i < owned.Length; i++)
            {
                ArgumentNullException.ThrowIfNull(owned[i]);
                for (var j = 0; j < i; j++)
                    if (owned[i].Equals(owned[j]))
                        throw new ArgumentException("Duplicate read key.", nameof(keys));
            }
            Admit(
                requestId,
                DataGridCommittedCommand.AdvanceTime(time),
                map,
                null,
                Array.AsReadOnly(owned)
            );
        });

    private void Admit(
        Guid requestId,
        DataGridCommittedCommand command,
        Guid? map,
        DataGridKey? key,
        IReadOnlyList<DataGridKey>? keys = null
    )
    {
        if (requestId == Guid.Empty)
            throw new ArgumentException("A request identity is required.", nameof(requestId));
        if (_clients.ContainsKey(requestId))
            return;
        if (Role != RaftRole.Leader)
        {
            _effects.Add(new RaftCompletion(requestId, RaftCompletionKind.NotLeader));
            return;
        }
        var bytes = DataGridCommittedCommandCodec.Encode(command);
        if (
            IsBusy
            || _clients.Count >= 128
            || _durable.LastIndex == long.MaxValue
            || _durable.LastIndex - _durable.CommitIndex >= 128
            || _durable.Entries.Count >= RaftDurableImage.MaxEntries
            || _durable.Entries.Sum(entry => (long)entry.ByteCount) + bytes.Length
                > RaftDurableImage.MaxLogBytes
        )
        {
            _effects.Add(new RaftCompletion(requestId, RaftCompletionKind.Busy));
            return;
        }
        var index = _durable.LastIndex + 1;
        var deadline = NewToken();
        _clients.Add(requestId, new(requestId, index, deadline, map, key, keys));
        _effects.Add(new RaftTimer(deadline, 5000));
        AppendLocal(new(index, CurrentTerm, bytes));
    }

    private void Dispatch(RaftMessage message)
    {
        if (message.Term < CurrentTerm)
        {
            if (message.Kind == RaftMessageKind.VoteRequest)
                Reply(message, RaftMessageKind.VoteResponse);
            if (message.Kind == RaftMessageKind.AppendRequest)
                Reply(
                    message,
                    RaftMessageKind.AppendResponse,
                    conflict: Math.Max(1, _durable.LastIndex)
                );
            if (message.Kind == RaftMessageKind.SnapshotRequest)
                Reply(message, RaftMessageKind.SnapshotResponse);
            return;
        }
        switch (message.Kind)
        {
            case RaftMessageKind.VoteRequest:
                if (
                    message.Index < 0
                    || message.PreviousTerm < 0
                    || (message.Index == 0) != (message.PreviousTerm == 0)
                    || message.PreviousTerm > message.Term
                )
                    throw new InvalidDataException("Invalid vote log position.");
                var lastTerm = _durable.TermAt(_durable.LastIndex);
                var grant =
                    (_durable.Vote is null || _durable.Vote == message.From)
                    && (
                        message.PreviousTerm > lastTerm
                        || (message.PreviousTerm == lastTerm && message.Index >= _durable.LastIndex)
                    );
                if (!grant)
                {
                    Reply(message, RaftMessageKind.VoteResponse);
                    break;
                }
                if (_durable.Vote == message.From)
                {
                    Reply(message, RaftMessageKind.VoteResponse, true);
                    ResetMainTimer();
                }
                else
                    Persist(
                        Image(CurrentTerm, message.From),
                        () =>
                        {
                            Reply(message, RaftMessageKind.VoteResponse, true);
                        }
                    );
                break;
            case RaftMessageKind.VoteResponse:
                if (
                    Role == RaftRole.Candidate
                    && _votes.TryGetValue(message.From, out var vote)
                    && vote == message.Token
                    && message.Accepted
                )
                {
                    _granted.Add(message.From);
                    if (_granted.Count >= 2)
                        BecomeLeader();
                }
                break;
            case RaftMessageKind.AppendRequest:
                ReceiveAppend(message);
                break;
            case RaftMessageKind.SnapshotRequest:
                ReceiveSnapshot(message);
                break;
            case RaftMessageKind.AppendResponse:
            case RaftMessageKind.SnapshotResponse:
                ReceiveReplicationResponse(message);
                break;
            default:
                throw new InvalidDataException("Unknown modeled peer message.");
        }
    }

    private void BecomeLeader()
    {
        Role = RaftRole.Leader;
        LeaderId = _id;
        _votes.Clear();
        for (var i = 0; i < 3; i++)
        {
            _matched[i] = i == _id ? _durable.LastIndex : 0;
            _next[i] = checked(_durable.LastIndex + 1);
        }
        if (_durable.LastIndex == long.MaxValue)
        {
            Fault("Log position exhausted.");
            return;
        }
        AppendLocal(
            new(
                _durable.LastIndex + 1,
                CurrentTerm,
                DataGridCommittedCommandCodec.Encode(
                    DataGridCommittedCommand.AdvanceTime(_eventTime)
                )
            )
        );
    }

    private void AppendLocal(RaftEntry entry) =>
        Persist(
            new(
                _configuration,
                CurrentTerm,
                _durable.Vote,
                _durable.Entries.Append(entry),
                _durable.CommitIndex,
                _durable.Snapshot
            ),
            () =>
            {
                _matched[_id] = _durable.LastIndex;
                Replicate();
                TryCommit();
            }
        );

    private void ReceiveAppend(RaftMessage message)
    {
        if (
            message.Index < 0
            || message.PreviousTerm < 0
            || message.Commit < 0
            || message.Entries.Count > 128
            || (message.Index == 0) != (message.PreviousTerm == 0)
            || message.PreviousTerm > message.Term
        )
            throw new InvalidDataException("Invalid append prefix.");
        StepDown();
        ResetMainTimer();
        LeaderId = message.From;
        if (message.Index < _durable.SnapshotIndex)
        {
            Reply(message, RaftMessageKind.AppendResponse, conflict: _durable.SnapshotIndex + 1);
            return;
        }
        if (message.Index > _durable.LastIndex)
        {
            Reply(message, RaftMessageKind.AppendResponse, conflict: _durable.LastIndex + 1);
            return;
        }
        if (_durable.TermAt(message.Index) != message.PreviousTerm)
        {
            var conflict = message.Index;
            var term = _durable.TermAt(conflict);
            while (conflict > _durable.SnapshotIndex + 1 && _durable.TermAt(conflict - 1) == term)
                conflict--;
            Reply(message, RaftMessageKind.AppendResponse, conflict: Math.Max(1, conflict));
            return;
        }
        var log = _durable.Entries.ToList();
        var expected = message.Index;
        var termBefore = message.PreviousTerm;
        foreach (var entry in message.Entries)
        {
            if (
                expected == long.MaxValue
                || entry.Index != ++expected
                || entry.Term < termBefore
                || entry.Term > message.Term
            )
                throw new InvalidDataException("Invalid append entry order/term.");
            termBefore = entry.Term;
            if (entry.Index <= (log.Count == 0 ? _durable.SnapshotIndex : log[^1].Index))
            {
                var old = log[checked((int)(entry.Index - _durable.SnapshotIndex - 1))];
                if (old.Term == entry.Term)
                {
                    if (!old.SameAs(entry))
                        throw new InvalidDataException("Conflicting same-term command.");
                    continue;
                }
                if (entry.Index <= _durable.CommitIndex)
                    throw new InvalidDataException("Attempted committed-log truncation.");
                log.RemoveAll(existing => existing.Index >= entry.Index);
            }
            log.Add(entry);
        }
        var commit = Math.Max(_durable.CommitIndex, Math.Min(message.Commit, expected));
        var changed =
            commit != _durable.CommitIndex
            || log.Count != _durable.Entries.Count
            || log.Where((entry, i) => !entry.SameAs(_durable.Entries[i])).Any();
        if (!changed)
        {
            Reply(message, RaftMessageKind.AppendResponse, true, expected);
            return;
        }
        Persist(
            new(_configuration, CurrentTerm, _durable.Vote, log, commit, _durable.Snapshot),
            () => Reply(message, RaftMessageKind.AppendResponse, true, expected)
        );
    }

    private void ReceiveSnapshot(RaftMessage message)
    {
        var snapshot =
            message.Snapshot ?? throw new InvalidDataException("Missing modeled snapshot.");
        if (snapshot.Descriptor.LastIncludedTerm > message.Term)
            throw new InvalidDataException("Future snapshot term.");
        StepDown();
        ResetMainTimer();
        LeaderId = message.From;
        if (snapshot.Descriptor.ByteCount > RaftDurableImage.MaxImageBytes)
            throw new InvalidDataException("Modeled snapshot capacity exceeded.");
        DataGridLocalStateMachine.Restore(
            _configuration,
            snapshot.Descriptor,
            snapshot.GetContent()
        );
        var index = snapshot.Descriptor.LastIncludedIndex;
        if (index <= _durable.CommitIndex)
        {
            Reply(message, RaftMessageKind.SnapshotResponse, true, index);
            return;
        }
        var matches =
            index <= _durable.LastIndex
            && _durable.TermAt(index) == snapshot.Descriptor.LastIncludedTerm;
        var suffix = matches ? _durable.Entries.Where(entry => entry.Index > index) : [];
        Persist(
            new(_configuration, CurrentTerm, _durable.Vote, suffix, index, snapshot),
            () => Reply(message, RaftMessageKind.SnapshotResponse, true, index)
        );
    }

    private void ReceiveReplicationResponse(RaftMessage message)
    {
        if (
            Role != RaftRole.Leader
            || !_rpcs.TryGetValue(message.From, out var rpc)
            || rpc.Token != message.Token
            || (
                rpc.Snapshot
                    ? message.Kind != RaftMessageKind.SnapshotResponse
                    : message.Kind != RaftMessageKind.AppendResponse
            )
        )
            return;
        _rpcs.Remove(message.From);
        _effects.Add(new RaftCancelTimer(rpc.Token));
        if (message.Accepted)
        {
            if (message.Index != rpc.LastIndex)
                throw new InvalidDataException("Unproven append acknowledgement.");
            _matched[message.From] = Math.Max(_matched[message.From], message.Index);
            _next[message.From] = checked(_matched[message.From] + 1);
            TryCommit();
            if (!IsBusy && _next[message.From] <= _durable.LastIndex)
                SendAppend(message.From);
        }
        else
        {
            var hint = Math.Min(_durable.LastIndex + 1, message.Conflict);
            _next[message.From] =
                hint > 0 && hint != _next[message.From]
                    ? hint
                    : Math.Max(1, _next[message.From] - 1);
            SendAppend(message.From);
        }
    }

    private void TryCommit()
    {
        if (IsBusy || Role != RaftRole.Leader)
            return;
        for (var index = _durable.LastIndex; index > _durable.CommitIndex; index--)
            if (
                _durable.TermAt(index) == CurrentTerm
                && _matched.Count(matched => matched >= index) >= 2
            )
            {
                Persist(
                    new(
                        _configuration,
                        CurrentTerm,
                        _durable.Vote,
                        _durable.Entries,
                        index,
                        _durable.Snapshot
                    ),
                    Replicate
                );
                return;
            }
    }

    private void Replicate()
    {
        if (Role != RaftRole.Leader || IsBusy)
            return;
        foreach (var peer in Peers())
            SendAppend(peer);
    }

    private void SendAppend(int peer)
    {
        if (Role != RaftRole.Leader || IsBusy || _rpcs.ContainsKey(peer))
            return;
        var token = NewToken();
        if (_next[peer] <= _durable.SnapshotIndex)
        {
            _rpcs[peer] = new(token, _durable.SnapshotIndex, true);
            Send(
                new(
                    RaftMessageKind.SnapshotRequest,
                    _id,
                    peer,
                    CurrentTerm,
                    token,
                    _configuration.IncarnationId,
                    _configuration.ComputeDigest(),
                    snapshot: _durable.Snapshot
                )
            );
        }
        else
        {
            var previous = Math.Min(_durable.LastIndex, _next[peer] - 1);
            var entries = new List<RaftEntry>();
            var bytes = 34;
            foreach (var entry in _durable.Entries.Where(entry => entry.Index > previous))
            {
                if (
                    entries.Count == 128
                    || bytes + 20 + entry.ByteCount > DataGridProtocolFrame.MaxPayloadByteCount
                )
                    break;
                entries.Add(entry);
                bytes += 20 + entry.ByteCount;
            }
            var last = entries.Count == 0 ? previous : entries[^1].Index;
            _rpcs[peer] = new(token, last, false);
            Send(
                new(
                    RaftMessageKind.AppendRequest,
                    _id,
                    peer,
                    CurrentTerm,
                    token,
                    _configuration.IncarnationId,
                    _configuration.ComputeDigest(),
                    index: previous,
                    previousTerm: _durable.TermAt(previous),
                    commit: _durable.CommitIndex,
                    entries: entries
                )
            );
        }
        _effects.Add(new RaftTimer(token, _rpcs[peer].Snapshot ? 60000 : 200));
    }

    private void ApplyCommitted()
    {
        if (_machine.LastAppliedIndex < _durable.SnapshotIndex)
        {
            var snapshot = _durable.Snapshot!;
            _machine = DataGridLocalStateMachine.Restore(
                _configuration,
                snapshot.Descriptor,
                snapshot.GetContent()
            );
        }
        while (_machine.LastAppliedIndex < _durable.CommitIndex)
        {
            var index = _machine.LastAppliedIndex + 1;
            var result = _machine.Apply(
                index,
                DataGridCommittedCommandCodec.ReadFrom(_durable.EntryAt(index).GetCommand())
            );
            foreach (var client in _clients.Values.Where(client => client.Index == index).ToArray())
            {
                if (Role != RaftRole.Leader)
                {
                    Complete(
                        client,
                        client.IsRead
                            ? RaftCompletionKind.Unavailable
                            : RaftCompletionKind.OutcomeUnknown
                    );
                    continue;
                }
                DataGridReadResult? read = null;
                if (client.IsRead)
                {
                    try
                    {
                        read = new(
                            _machine.AppliedTime,
                            client.Keys is null
                                ? [_machine.Read(client.Map!.Value, client.Key!)]
                                : _machine.ReadMany(client.Map!.Value, client.Keys)
                        );
                    }
                    catch (InvalidOperationException)
                    {
                        Complete(client, RaftCompletionKind.CapacityExceeded, index);
                        continue;
                    }
                }
                Complete(
                    client,
                    client.IsRead ? RaftCompletionKind.Read : RaftCompletionKind.Applied,
                    index,
                    result,
                    read?.GetItem(0),
                    read
                );
            }
        }
    }

    private void Persist(RaftDurableImage image, Action afterWrite)
    {
        if (IsBusy)
            throw new InvalidOperationException("Overlapping modeled persistence.");
        _pending = new(NewToken(), image);
        _afterWrite = afterWrite;
        _effects.Add(_pending);
    }

    private void PersistHigherTerm()
    {
        var message = _deferred;
        _deferred = null;
        Persist(
            Image(CurrentTerm, null),
            () =>
            {
                if (message is not null && message.Term == CurrentTerm)
                    Dispatch(message);
            }
        );
    }

    private RaftDurableImage Image(long term, int? vote) =>
        new(_configuration, term, vote, _durable.Entries, _durable.CommitIndex, _durable.Snapshot);

    private void StepDown()
    {
        Role = RaftRole.Follower;
        LeaderId = null;
        _votes.Clear();
        _granted.Clear();
        foreach (var rpc in _rpcs.Values)
            _effects.Add(new RaftCancelTimer(rpc.Token));
        _rpcs.Clear();
        foreach (var client in _clients.Values.ToArray())
            Complete(
                client,
                client.IsRead ? RaftCompletionKind.Unavailable : RaftCompletionKind.OutcomeUnknown
            );
    }

    private void Complete(
        ClientWait client,
        RaftCompletionKind kind,
        long index = 0,
        DataGridApplyResult? result = null,
        DataGridLocalEntry? entry = null,
        DataGridReadResult? read = null
    )
    {
        _clients.Remove(client.RequestId);
        _effects.Add(new RaftCancelTimer(client.Deadline));
        _effects.Add(new RaftCompletion(client.RequestId, kind, index, result, entry, read));
    }

    private void Reply(
        RaftMessage request,
        RaftMessageKind kind,
        bool accepted = false,
        long index = 0,
        long conflict = 0
    ) =>
        Send(
            new(
                kind,
                _id,
                request.From,
                CurrentTerm,
                request.Token,
                _configuration.IncarnationId,
                _configuration.ComputeDigest(),
                index: index,
                accepted: accepted,
                conflict: conflict
            )
        );

    private void Send(RaftMessage message) => _effects.Add(new RaftSend(message));

    private IEnumerable<int> Peers() => Enumerable.Range(0, 3).Where(peer => peer != _id);

    private RaftToken NewToken() => new(_epoch, checked(++_sequence));

    private void ResetMainTimer()
    {
        if (_mainTimer != default)
            _effects.Add(new RaftCancelTimer(_mainTimer));
        _mainTimer = NewToken();
        _effects.Add(new RaftTimer(_mainTimer, Role == RaftRole.Leader ? 100 : 0));
    }

    private void Fault(string reason)
    {
        StepDown();
        Role = RaftRole.Faulted;
        _pending = null;
        _afterWrite = null;
        if (_mainTimer != default)
            _effects.Add(new RaftCancelTimer(_mainTimer));
        _effects.Add(new RaftFault(reason));
    }

    private RaftEffect[] Run(Action action)
    {
        _effects.Clear();
        if (Role == RaftRole.Faulted)
            return [];
        try
        {
            action();
        }
        catch (Exception exception)
            when (exception
                    is InvalidDataException
                        or InvalidOperationException
                        or OverflowException
            )
        {
            Fault(exception.Message);
        }
        return _effects.ToArray();
    }

    private sealed record RpcWait(RaftToken Token, long LastIndex, bool Snapshot);

    private sealed record ClientWait(
        Guid RequestId,
        long Index,
        RaftToken Deadline,
        Guid? Map,
        DataGridKey? Key,
        IReadOnlyList<DataGridKey>? Keys = null
    )
    {
        internal bool IsRead => Map is not null;
    }
}
