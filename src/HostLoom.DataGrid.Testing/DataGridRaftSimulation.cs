using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using HostLoom.DataGrid.Consensus;

[assembly: InternalsVisibleTo("HostLoom.Tests")]

namespace HostLoom.DataGrid.Testing;

public enum DataGridRaftRole
{
    Follower,
    Candidate,
    Leader,
    Faulted,
}

public enum DataGridRaftOutcomeKind
{
    Applied,
    Read,
    NotLeader,
    Busy,
    OutcomeUnknown,
    Unavailable,
    InvalidRequest,
    CapacityExceeded,
}

public sealed record DataGridRaftStatus(
    bool Online,
    DataGridRaftRole Role,
    long Term,
    long DurableTerm,
    int? VotedFor,
    long LastIndex,
    long CommitIndex,
    long AppliedIndex,
    bool Busy,
    bool Ready
);

public sealed record DataGridRaftOutcome(
    Guid RequestId,
    int Node,
    DataGridRaftOutcomeKind Kind,
    long InvocationTick,
    long CompletionTick,
    long Index,
    DataGridApplyResult? Result,
    DataGridLocalEntry? Entry
);

/// <summary>Three-node deterministic Raft experiment. Durable writes are atomic memory transactions.</summary>
/// <remarks>This is not the v1 TLS/WAL transport or evidence of filesystem durability.</remarks>
public sealed class DataGridRaftSimulation
{
    private readonly DataGridBootstrapConfiguration _configuration;
    private readonly RaftNode?[] _nodes = new RaftNode?[3];
    private readonly RaftDurableImage[] _stores = new RaftDurableImage[3];
    private readonly long[] _epochs = new long[3];
    private readonly bool[][] _links = [new bool[3], new bool[3], new bool[3]];
    private readonly bool[] _held = new bool[3];
    private readonly DataGridJournalFault[] _faults = new DataGridJournalFault[3];
    private readonly SortedDictionary<(long Due, long Order), Scheduled> _events = [];
    private readonly Dictionary<(int Node, RaftToken Token), (long Due, long Order)> _timers = [];
    private readonly Dictionary<Guid, Request> _requests = [];
    private readonly SortedDictionary<long, RaftEntry> _committed = [];
    private readonly Dictionary<long, byte[]> _prefixHashes = [];
    private readonly Dictionary<long, int> _leaders = [];
    private readonly List<string> _trace = [];
    private readonly Guid _snapshotId = new("eeeeeeee-0000-0000-0000-000000000001");
    private readonly DataGridLocalStateMachine _reference;
    private long _order;
    private long _packetBytes;
    private long _historyBytes;
    private int _traceBytes;
    private ulong _requestNumber;
    private uint _random;

    public DataGridRaftSimulation(DataGridBootstrapConfiguration configuration, uint seed)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        _configuration = configuration;
        Seed = seed;
        _random = seed;
        _reference = new(configuration);
        for (var i = 0; i < 3; i++)
        {
            _stores[i] = new(configuration);
            for (var j = 0; j < 3; j++)
                _links[i][j] = i != j;
            Restart(i);
        }
    }

    public uint Seed { get; }
    public long Tick { get; private set; }
    public bool TraceTruncated { get; private set; }
    public IReadOnlyList<string> Trace => _trace.ToArray();
    public int PendingEventCount => _events.Count;
    public int PendingMessageCount => _events.Values.Count(item => item.Message is not null);
    public DataGridTimestamp UtcTime => new(checked(Tick * TimeSpan.TicksPerMillisecond));

    public DataGridRaftStatus Status(int node)
    {
        CheckNode(node);
        var actor = _nodes[node];
        var store = _stores[node];
        return new(
            actor is not null && actor.Role != RaftRole.Faulted,
            (DataGridRaftRole)(actor?.Role ?? RaftRole.Faulted),
            actor?.CurrentTerm ?? store.Term,
            store.Term,
            store.Vote,
            store.LastIndex,
            store.CommitIndex,
            actor?.Machine.LastAppliedIndex ?? store.SnapshotIndex,
            actor?.IsBusy ?? false,
            actor is { Role: RaftRole.Leader, IsBusy: false }
                && actor.Durable.CommitIndex > 0
                && actor.Durable.TermAt(actor.Durable.CommitIndex) == actor.CurrentTerm
        );
    }

    public DataGridLocalEntry? ReadLocal(int node, Guid map, DataGridKey key) =>
        Actor(node).Machine.Read(map, key);

    public DataGridRaftOutcome? Outcome(Guid request) =>
        _requests.TryGetValue(request, out var state) ? state.Outcome : null;

    public Guid Propose(int node, DataGridCommittedCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        var id = Begin(node, command, null, null);
        Consume(node, Actor(node).Propose(id, command));
        return id;
    }

    public Guid Read(int node, Guid map, DataGridKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var id = Begin(node, null, map, key);
        Consume(node, Actor(node).Read(id, map, key, UtcTime));
        return id;
    }

    public void LoseReply(Guid request) => _requests[request].DropReply = true;

    public void HoldWrites(int node, bool hold)
    {
        CheckNode(node);
        _held[node] = hold;
        Record($"hold node={node} value={hold}");
    }

    public void FailNextWrite(int node, DataGridJournalFault fault)
    {
        CheckNode(node);
        _faults[node] = fault;
    }

    public void SetLink(int first, int second, bool enabled)
    {
        CheckNode(first);
        CheckNode(second);
        _links[first][second] = enabled;
        _links[second][first] = enabled;
        Record($"link {first}<->{second} enabled={enabled}");
    }

    public void Isolate(int node, bool isolated = true)
    {
        CheckNode(node);
        for (var other = 0; other < 3; other++)
            if (other != node)
                SetLink(node, other, !isolated);
    }

    public void Stop(int node, bool persistPendingWrite = false)
    {
        CheckNode(node);
        if (persistPendingWrite)
        {
            var write = _events
                .Values.FirstOrDefault(item =>
                    item.Node == node && item.Epoch == _epochs[node] && item.Write is not null
                )
                ?.Write;
            if (write is not null)
                Store(node, write.Image);
        }
        _nodes[node] = null;
        foreach (
            var item in _events
                .Where(pair =>
                    pair.Value.Node == node
                    && pair.Value.Message is null
                    && pair.Value.Client is null
                )
                .Select(pair => pair.Key)
                .ToArray()
        )
            Remove(item);
        foreach (var timer in _timers.Keys.Where(key => key.Node == node).ToArray())
            _timers.Remove(timer);
        foreach (
            var request in _requests.Values.Where(request =>
                request.Node == node && request.Outcome is null
            )
        )
            FinishUnavailable(request);
        Record($"stop node={node} persistedPending={persistPendingWrite}");
        CheckInvariants();
    }

    public void Restart(int node)
    {
        CheckNode(node);
        if (_nodes[node] is { Role: not RaftRole.Faulted })
            throw new InvalidOperationException("Stop before restarting a node.");
        if (_nodes[node] is { Role: RaftRole.Faulted })
            Stop(node);
        _nodes[node] = new(_configuration, node, checked(++_epochs[node]), _stores[node]);
        Record($"restart node={node} epoch={_epochs[node]}");
        Consume(node, _nodes[node]!.Start());
        CheckInvariants();
    }

    public void ForceElection(int node)
    {
        var timer = _events.FirstOrDefault(pair => pair.Value.Node == node && pair.Value.Election);
        if (timer.Value is null)
            throw new InvalidOperationException("No election timer for this node.");
        Remove(timer.Key);
        _timers.Remove((node, timer.Value.Token));
        Consume(node, Actor(node).Timeout(timer.Value.Token, UtcTime));
        CheckInvariants();
    }

    public void Compact(int node)
    {
        Consume(node, Actor(node).Compact(NewId()));
        CheckInvariants();
    }

    public void DropNextMessage()
    {
        var pair = NextMessage();
        Remove(pair.Key);
        Record("network-drop");
    }

    public void DuplicateNextMessage()
    {
        var pair = NextMessage();
        Add(pair.Key.Due, pair.Value);
        Record("network-duplicate");
    }

    public void DelayNextMessage(long milliseconds)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(milliseconds);
        var pair = NextMessage();
        var due = checked(pair.Key.Due + milliseconds);
        Remove(pair.Key);
        Add(due, pair.Value);
        Record($"network-delay={milliseconds}");
    }

    public bool Step()
    {
        if (_events.Count == 0)
            return false;
        var pair = _events.First();
        Remove(pair.Key);
        Tick = Math.Max(Tick, pair.Key.Due);
        var item = pair.Value;
        if (item.Client is { } client)
        {
            if (_requests[client].Outcome is null)
                FinishUnavailable(_requests[client]);
        }
        else if (item.Message is { } message)
        {
            if (_nodes[item.Node] is { } actor && _links[message.From][message.To])
                Consume(item.Node, actor.Receive(message, UtcTime));
            else
                Record($"drop from={message.From} to={message.To}");
        }
        else if (_nodes[item.Node] is { } actor && item.Epoch == _epochs[item.Node])
        {
            if (item.Write is { } write)
            {
                if (_held[item.Node])
                    Add(Tick + 10, item);
                else
                {
                    var fault = _faults[item.Node];
                    _faults[item.Node] = DataGridJournalFault.None;
                    if (fault != DataGridJournalFault.BeforeAppend)
                        Store(item.Node, write.Image);
                    Consume(
                        item.Node,
                        actor.Persisted(write.Token, fault == DataGridJournalFault.None)
                    );
                }
            }
            else
            {
                _timers.Remove((item.Node, item.Token));
                Consume(item.Node, actor.Timeout(item.Token, UtcTime));
            }
        }
        CheckInvariants();
        return true;
    }

    public void RunUntil(Func<DataGridRaftSimulation, bool> condition, int maximumEvents = 20000)
    {
        ArgumentNullException.ThrowIfNull(condition);
        for (var i = 0; i < maximumEvents; i++)
        {
            if (condition(this))
                return;
            if (!Step())
                break;
        }
        if (!condition(this))
            throw new TimeoutException($"Inconclusive simulation timeout seed={Seed} tick={Tick}.");
    }

    public void Advance(long milliseconds, int maximumEvents = 20000)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(milliseconds);
        var target = checked(Tick + milliseconds);
        var count = 0;
        while (_events.Count > 0 && _events.First().Key.Due <= target)
        {
            if (++count > maximumEvents)
                throw new TimeoutException("Inconclusive event-budget exhaustion.");
            Step();
        }
        Tick = target;
        CheckInvariants();
    }

    /// <summary>Checks committed-prefix consistency, election uniqueness and local application history.</summary>
    public void CheckInvariants()
    {
        for (var i = 0; i < 3; i++)
        {
            var actor = _nodes[i];
            if (actor is null || actor.Role == RaftRole.Faulted)
                continue;
            if (actor.Role == RaftRole.Leader)
            {
                if (_leaders.TryGetValue(actor.CurrentTerm, out var leader) && leader != i)
                    Fail("Conflicting leaders in one term.");
                if (_leaders.Count >= 65536 && !_leaders.ContainsKey(actor.CurrentTerm))
                    throw new InvalidOperationException("Election history capacity exceeded.");
                _leaders[actor.CurrentTerm] = i;
            }
            var applied = actor.Machine.LastAppliedIndex;
            if (applied != actor.Durable.CommitIndex || applied > _stores[i].CommitIndex)
                Fail("Application passed durable commitment.");
            if (applied > 0)
            {
                var hash = actor
                    .Machine.SaveSnapshot(_snapshotId, actor.Durable.TermAt(applied))
                    .Descriptor.GetDigest();
                if (
                    !_prefixHashes.TryGetValue(applied, out var expected)
                    || !hash.AsSpan().SequenceEqual(expected)
                )
                    Fail("Application differs from committed sequential history.");
            }
        }
    }

    internal RaftNode InternalNode(int node) => Actor(node);

    internal RaftDurableImage StoredImage(int node) => _stores[node];

    internal void Inject(RaftMessage message, long delay = 0) =>
        Add(checked(Tick + delay), new(message.To, 0, default, Message: message));

    internal void ValidateInjectedCompletion(int node, RaftCompletion completion) =>
        ObserveCompletion(node, completion);

    private Guid Begin(int node, DataGridCommittedCommand? command, Guid? map, DataGridKey? key)
    {
        Actor(node);
        if (_requests.Count >= 8192)
            throw new InvalidOperationException("Client history capacity exceeded.");
        var bytes = command is null ? null : DataGridCommittedCommandCodec.Encode(command);
        if (_historyBytes + (bytes?.Length ?? 0) > 64L * 1024 * 1024)
            throw new InvalidOperationException("Client history byte capacity exceeded.");
        var id = NewId();
        _historyBytes += bytes?.Length ?? 0;
        var request = new Request(id, node, Tick, _committed.Count, bytes, map, key);
        _requests.Add(id, request);
        request.Deadline = Add(checked(Tick + 5000), new(node, 0, default, Client: id));
        Record($"invoke node={node} request={id:N} read={map is not null}");
        return id;
    }

    private void Consume(int node, IReadOnlyList<RaftEffect> effects)
    {
        foreach (var effect in effects)
        {
            switch (effect)
            {
                case RaftWrite write:
                    Add(Tick + 1, new(node, _epochs[node], write.Token, Write: write));
                    break;
                case RaftSend send:
                    Add(
                        Tick + 1 + NextRandom() % 10,
                        new(send.Message.To, 0, default, Message: send.Message)
                    );
                    break;
                case RaftTimer timer:
                    var delay =
                        timer.DelayMilliseconds == 0
                            ? 500 + (int)(NextRandom() % 501)
                            : timer.DelayMilliseconds;
                    var scheduled = new Scheduled(
                        node,
                        _epochs[node],
                        timer.Token,
                        Election: timer.DelayMilliseconds == 0
                    );
                    _timers[(node, timer.Token)] = Add(checked(Tick + delay), scheduled);
                    break;
                case RaftCancelTimer cancel:
                    if (_timers.Remove((node, cancel.Token), out var key))
                        Remove(key);
                    break;
                case RaftCompletion completion:
                    ObserveCompletion(node, completion);
                    break;
                case RaftFault fault:
                    Record($"fault node={node} reason={fault.Reason}");
                    break;
            }
        }
    }

    private void Store(int node, RaftDurableImage image)
    {
        var old = _stores[node];
        if (
            image.Term < old.Term
            || (image.Term == old.Term && old.Vote is not null && image.Vote != old.Vote)
            || image.CommitIndex < old.CommitIndex
        )
            Fail("Persistent term/vote/commit moved backwards.");
        for (long index = 1; index <= image.CommitIndex; index++)
        {
            if (index <= image.SnapshotIndex)
            {
                if (!_committed.ContainsKey(index))
                    Fail("Snapshot has an unobserved committed prefix.");
                continue;
            }
            var entry = image.EntryAt(index);
            if (_committed.TryGetValue(index, out var known))
            {
                if (!known.SameAs(entry))
                    Fail("Conflicting committed entries.");
            }
            else
            {
                if (index != _committed.Count + 1 || _committed.Count >= 65536)
                    Fail("Committed history gap/capacity.");
                if (_historyBytes + entry.ByteCount > 64L * 1024 * 1024)
                    throw new InvalidOperationException("Committed history capacity exceeded.");
                _historyBytes += entry.ByteCount;
                _committed.Add(index, entry);
                _reference.Apply(index, DataGridCommittedCommandCodec.ReadFrom(entry.GetCommand()));
                _prefixHashes[index] = _reference
                    .SaveSnapshot(_snapshotId, entry.Term)
                    .Descriptor.GetDigest();
            }
        }
        if (
            image.Snapshot is { } snapshot
            && (
                !_prefixHashes.TryGetValue(image.SnapshotIndex, out var hash)
                || !hash.AsSpan().SequenceEqual(snapshot.Descriptor.GetDigest())
            )
        )
            Fail("Snapshot differs from committed prefix.");
        _stores[node] = image;
        Record(
            $"persist node={node} term={image.Term} vote={image.Vote} last={image.LastIndex} commit={image.CommitIndex}"
        );
    }

    private void ObserveCompletion(int node, RaftCompletion completion)
    {
        if (!_requests.TryGetValue(completion.RequestId, out var request))
            Fail("Completion has no invocation.");
        if (request!.ServerCompleted)
            Fail("Duplicate server completion.");
        request.ServerCompleted = true;
        if (completion.Kind is RaftCompletionKind.Applied or RaftCompletionKind.Read)
        {
            if (completion.Index <= request.StartedCommit)
                Fail("Success used a prefix committed before invocation.");
            if (
                !_committed.TryGetValue(completion.Index, out var entry)
                || !_stores.Where(store => Contains(store, entry)).Skip(1).Any()
            )
                Fail("Success preceded majority persistence.");
            var reference = Replay(completion.Index, out var expectedResult);
            if (completion.Kind == RaftCompletionKind.Applied)
            {
                if (
                    request.Command is null
                    || !entry!.GetCommand().AsSpan().SequenceEqual(request.Command)
                    || !SameResult(completion.Result, expectedResult)
                )
                    Fail("Applied reply differs from history.");
            }
            else
            {
                if (
                    request.Map is null
                    || !entry!
                        .GetCommand()
                        .AsSpan()
                        .SequenceEqual(
                            DataGridCommittedCommandCodec.Encode(
                                DataGridCommittedCommand.AdvanceTime(
                                    new(request.Started * TimeSpan.TicksPerMillisecond)
                                )
                            )
                        )
                    || completion.Entry is { RemainingTimeToLive: { } ttl } && ttl <= TimeSpan.Zero
                )
                    Fail("Invalid read completion.");
                var expected = reference.Read(request.Map!.Value, request.Key!);
                if (!SameEntry(completion.Entry, expected))
                    Fail("Read differs from captured history.");
            }
        }
        Record(
            $"server-complete node={node} request={completion.RequestId:N} kind={completion.Kind} index={completion.Index} lost={request.DropReply}"
        );
        if (!request.DropReply && request.Outcome is null)
        {
            Remove(request.Deadline);
            request.Outcome = new(
                request.Id,
                node,
                (DataGridRaftOutcomeKind)completion.Kind,
                request.Started,
                Tick,
                completion.Index,
                completion.Result,
                completion.Entry
            );
        }
    }

    private DataGridLocalStateMachine Replay(long index, out DataGridApplyResult? result)
    {
        var state = new DataGridLocalStateMachine(_configuration);
        result = null;
        for (long i = 1; i <= index; i++)
            result = state.Apply(
                i,
                DataGridCommittedCommandCodec.ReadFrom(_committed[i].GetCommand())
            );
        return state;
    }

    private static bool Contains(RaftDurableImage image, RaftEntry entry) =>
        image.SnapshotIndex >= entry.Index
        || image.LastIndex >= entry.Index && image.EntryAt(entry.Index).SameAs(entry);

    private static bool SameResult(DataGridApplyResult? left, DataGridApplyResult? right)
    {
        if (
            left is null
            || right is null
            || left.Status != right.Status
            || left.ItemCount != right.ItemCount
            || left.SessionNumber != right.SessionNumber
            || left.SessionExpiresAt != right.SessionExpiresAt
        )
            return false;
        for (var i = 0; i < left.ItemCount; i++)
            if (left.GetItem(i) != right.GetItem(i))
                return false;
        return true;
    }

    private static bool SameEntry(DataGridLocalEntry? left, DataGridLocalEntry? right) =>
        left is null
            ? right is null
            : right is not null
                && left.Version == right.Version
                && left.RemainingTimeToLive == right.RemainingTimeToLive
                && left.GetValue().AsSpan().SequenceEqual(right.GetValue());

    private void FinishUnavailable(Request request)
    {
        Remove(request.Deadline);
        request.Outcome = new(
            request.Id,
            request.Node,
            request.Map is null
                ? DataGridRaftOutcomeKind.OutcomeUnknown
                : DataGridRaftOutcomeKind.Unavailable,
            request.Started,
            Tick,
            0,
            null,
            null
        );
        Record($"client-unavailable request={request.Id:N}");
    }

    private (long Due, long Order) Add(long due, Scheduled item)
    {
        var bytes = item.Message?.ByteCount ?? 0;
        if (_events.Count >= 4096 || _packetBytes + bytes > 32L * 1024 * 1024)
            throw new InvalidOperationException("Simulation event/packet capacity exceeded.");
        var key = (due, checked(++_order));
        _events.Add(key, item);
        _packetBytes += bytes;
        return key;
    }

    private void Remove((long Due, long Order) key)
    {
        if (_events.Remove(key, out var item))
            _packetBytes -= item.Message?.ByteCount ?? 0;
    }

    private KeyValuePair<(long Due, long Order), Scheduled> NextMessage() =>
        _events.First(pair => pair.Value.Message is not null);

    private uint NextRandom() => _random = unchecked(_random * 1664525u + 1013904223u);

    private Guid NewId()
    {
        Span<byte> bytes = stackalloc byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, ((ulong)Seed << 32) | 0x52414654u);
        BinaryPrimitives.WriteUInt64BigEndian(bytes[8..], checked(++_requestNumber));
        return new(bytes, bigEndian: true);
    }

    private RaftNode Actor(int node)
    {
        CheckNode(node);
        return _nodes[node] ?? throw new InvalidOperationException("Node is stopped.");
    }

    private static void CheckNode(int node)
    {
        if (node is < 0 or > 2)
            throw new ArgumentOutOfRangeException(nameof(node));
    }

    private void Record(string line)
    {
        line = $"seed={Seed} tick={Tick} {line}";
        while (
            _trace.Count > 0
            && (_trace.Count >= 8192 || _traceBytes + line.Length > 4 * 1024 * 1024)
        )
        {
            _traceBytes -= _trace[0].Length;
            _trace.RemoveAt(0);
            TraceTruncated = true;
        }
        _trace.Add(line);
        _traceBytes += line.Length;
    }

    private void Fail(string reason) =>
        throw new InvalidDataException(
            $"seed={Seed} tick={Tick} {reason}\n{string.Join('\n', _trace)}"
        );

    private sealed record Scheduled(
        int Node,
        long Epoch,
        RaftToken Token,
        RaftMessage? Message = null,
        RaftWrite? Write = null,
        Guid? Client = null,
        bool Election = false
    );

    private sealed class Request(
        Guid id,
        int node,
        long started,
        long startedCommit,
        byte[]? command,
        Guid? map,
        DataGridKey? key
    )
    {
        internal Guid Id { get; } = id;
        internal int Node { get; } = node;
        internal long Started { get; } = started;
        internal long StartedCommit { get; } = startedCommit;
        internal (long Due, long Order) Deadline { get; set; }
        internal byte[]? Command { get; } = command;
        internal Guid? Map { get; } = map;
        internal DataGridKey? Key { get; } = key;
        internal bool ServerCompleted { get; set; }
        internal bool DropReply { get; set; }
        internal DataGridRaftOutcome? Outcome { get; set; }
    }
}
