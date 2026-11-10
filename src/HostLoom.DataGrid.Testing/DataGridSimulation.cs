using System.Buffers.Binary;

namespace HostLoom.DataGrid.Testing;

public sealed class DataGridSimulationRecord
{
    private readonly byte[] _bytes;

    public DataGridSimulationRecord(long index, ReadOnlySpan<byte> bytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(index);
        if (
            bytes.Length < DataGridCommittedCommandCodec.HeaderByteCount
            || bytes.Length > DataGridCommittedCommandCodec.MaxEncodedByteCount
        )
            throw new ArgumentOutOfRangeException(nameof(bytes));
        Index = index;
        _bytes = bytes.ToArray();
    }

    public long Index { get; }

    public byte[] GetBytes() => (byte[])_bytes.Clone();
}

/// <summary>Successful Append means persisted only within this simulation's crash model.</summary>
public interface IDataGridSimulationJournal
{
    long LastIndex { get; }
    DataGridLocalSnapshot? Checkpoint { get; }
    void Append(DataGridSimulationRecord record);
    IReadOnlyList<DataGridSimulationRecord> ReadAfter(long index);
    void SaveCheckpoint(DataGridLocalSnapshot snapshot);
}

public enum DataGridJournalFault
{
    None = 0,
    BeforeAppend = 1,
    AfterAppend = 2,
}

public sealed class InMemoryDataGridJournal : IDataGridSimulationJournal
{
    private readonly List<DataGridSimulationRecord> _records = [];
    private long _bytes;
    public long LastIndex { get; private set; }
    public DataGridLocalSnapshot? Checkpoint { get; private set; }
    public DataGridJournalFault NextFault { get; set; }

    public void Append(DataGridSimulationRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        var fault = NextFault;
        NextFault = DataGridJournalFault.None;
        if (fault == DataGridJournalFault.BeforeAppend)
            throw new IOException("Injected append failure before persistence.");
        if (LastIndex == long.MaxValue || record.Index != LastIndex + 1)
            throw new InvalidOperationException("Noncontiguous simulated journal.");
        var bytes = record.GetBytes();
        if (
            _records.Count >= 4096
            || bytes.Length > DataGridCommittedCommandCodec.MaxEncodedByteCount
            || _bytes + bytes.Length > 16L * 1024 * 1024
        )
            throw new IOException("Simulated journal capacity exceeded; checkpoint required.");
        _records.Add(record);
        _bytes += bytes.Length;
        LastIndex = record.Index;
        if (fault == DataGridJournalFault.AfterAppend)
            throw new IOException("Injected unknown append completion.");
    }

    public IReadOnlyList<DataGridSimulationRecord> ReadAfter(long index) =>
        _records.Where(record => record.Index > index).ToArray();

    public void SaveCheckpoint(DataGridLocalSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.Descriptor.LastIncludedIndex != LastIndex)
            throw new InvalidOperationException("Checkpoint must cover the applied simulated log.");
        Checkpoint = snapshot;
        _records.Clear();
        _bytes = 0;
    }
}

public interface IDataGridSimulationTransport
{
    int PendingCount { get; }
    long Tick { get; }
    void Enqueue(ReadOnlySpan<byte> payload, long delay);
    byte[]? Receive();
    void DropFirst();
    void DuplicateFirst();
    void DelayFirst(long additionalDelay);
}

/// <summary>A bounded, single-owner fake network. Tick is scheduling time, not grid UTC time.</summary>
public sealed class DataGridSimulatedTransport : IDataGridSimulationTransport
{
    private readonly SortedDictionary<(long Due, long Order), byte[]> _pending = [];
    private long _order;
    private long _bytes;
    public int PendingCount => _pending.Count;
    public long Tick { get; private set; }

    public void Enqueue(ReadOnlySpan<byte> payload, long delay)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(delay);
        if (
            payload.Length > DataGridCommittedCommandCodec.MaxEncodedByteCount
            || _pending.Count >= 4096
            || _bytes + payload.Length > 16L * 1024 * 1024
        )
            throw new InvalidOperationException("Simulated network capacity exceeded.");
        var due = checked(Tick + delay);
        var order = checked(_order + 1);
        _pending.Add((due, order), payload.ToArray());
        _bytes += payload.Length;
        _order = order;
    }

    public byte[]? Receive()
    {
        if (_pending.Count == 0)
            return null;
        var first = _pending.First();
        _pending.Remove(first.Key);
        _bytes -= first.Value.Length;
        Tick = Math.Max(Tick, first.Key.Due);
        return first.Value;
    }

    public void DropFirst()
    {
        if (_pending.Count == 0)
            throw new InvalidOperationException("No pending packet.");
        var first = _pending.First();
        _pending.Remove(first.Key);
        _bytes -= first.Value.Length;
    }

    public void DuplicateFirst()
    {
        if (_pending.Count == 0)
            throw new InvalidOperationException("No pending packet.");
        var first = _pending.First();
        Enqueue(first.Value, Math.Max(0, first.Key.Due - Tick));
    }

    public void DelayFirst(long additionalDelay)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(additionalDelay);
        if (_pending.Count == 0)
            throw new InvalidOperationException("No pending packet.");
        var first = _pending.First();
        var due = checked(first.Key.Due + additionalDelay);
        var delay = due - Tick;
        if (_order == long.MaxValue)
            throw new InvalidOperationException("Schedule exhausted.");
        DropFirst();
        Enqueue(first.Value, delay);
    }
}

/// <summary>Local commit simulation only: no elections, replication, TLS, sockets or device durability.</summary>
public sealed class DataGridSimulation
{
    private readonly DataGridBootstrapConfiguration _configuration;
    private readonly IDataGridSimulationJournal _journal;
    private readonly IDataGridSimulationTransport _transport;
    private readonly List<string> _trace = [];
    private DataGridLocalStateMachine? _state;
    private uint _random;
    private int _traceBytes;
    private ulong _checkpoints;

    public DataGridSimulation(
        DataGridBootstrapConfiguration configuration,
        uint seed,
        IDataGridSimulationJournal? journal = null,
        IDataGridSimulationTransport? transport = null
    )
    {
        ArgumentNullException.ThrowIfNull(configuration);
        _configuration = configuration;
        Seed = seed;
        _random = seed;
        _journal = journal ?? new InMemoryDataGridJournal();
        _transport = transport ?? new DataGridSimulatedTransport();
        Restart();
    }

    public uint Seed { get; }
    public bool TraceTruncated { get; private set; }
    public bool IsRunning => _state is not null;
    public DataGridLocalStateMachine State =>
        _state ?? throw new InvalidOperationException("Simulated node is stopped.");
    public int PendingCount => _transport.PendingCount;
    public IReadOnlyList<string> Trace => _trace.ToArray();

    public void Schedule(DataGridCommittedCommand command, long delay = 0)
    {
        var bytes = DataGridCommittedCommandCodec.Encode(command);
        _transport.Enqueue(bytes, delay);
        Record($"send delay={delay} bytes={Convert.ToHexString(bytes)}");
    }

    public void ScheduleRandom(DataGridCommittedCommand command, uint maximumDelay)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maximumDelay, 1000000u);
        _random = unchecked(_random * 1664525u + 1013904223u);
        Schedule(command, _random % (maximumDelay + 1));
    }

    public void DropNext()
    {
        _transport.DropFirst();
        Record("network-drop");
    }

    public void DuplicateNext()
    {
        _transport.DuplicateFirst();
        Record("network-duplicate");
    }

    public void DelayNext(long delay)
    {
        _transport.DelayFirst(delay);
        Record($"network-delay ticks={delay}");
    }

    public DataGridApplyResult? Step(bool crashAfterPersistence = false, bool loseReply = false)
    {
        var packet = _transport.Receive();
        if (packet is null)
            return null;
        if (_state is null)
        {
            Record("offline drop");
            return null;
        }
        var command = DataGridCommittedCommandCodec.ReadFrom(packet);
        var index = checked(_journal.LastIndex + 1);
        try
        {
            _journal.Append(new(index, packet));
        }
        catch
        {
            _state = null;
            Record($"persist-uncertain index={index}");
            throw;
        }
        if (crashAfterPersistence)
        {
            Stop();
            Record($"persisted-unapplied index={index}");
            return null;
        }
        DataGridApplyResult result;
        try
        {
            result = State.Apply(index, command);
        }
        catch
        {
            _state = null;
            Record($"apply-fault index={index}");
            throw;
        }
        Record(
            $"applied index={index} kind={command.Kind} status={result.Status} replyLost={loseReply}"
        );
        return loseReply ? null : result;
    }

    public void Stop()
    {
        _state = null;
        Record("stop");
    }

    public void Restart()
    {
        if (_state is not null)
            throw new InvalidOperationException("Stop before restarting.");
        var snapshot = _journal.Checkpoint;
        var candidate = snapshot is null
            ? new DataGridLocalStateMachine(_configuration)
            : DataGridLocalStateMachine.Restore(
                _configuration,
                snapshot.Descriptor,
                snapshot.GetContent()
            );
        foreach (var record in _journal.ReadAfter(candidate.LastAppliedIndex))
            candidate.Apply(
                record.Index,
                DataGridCommittedCommandCodec.ReadFrom(record.GetBytes())
            );
        if (candidate.LastAppliedIndex != _journal.LastIndex)
            throw new InvalidDataException("Incomplete simulated journal recovery.");
        _state = candidate;
        Record($"restart index={candidate.LastAppliedIndex}");
    }

    public DataGridLocalSnapshot Checkpoint()
    {
        Span<byte> id = stackalloc byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(id, ((ulong)Seed << 32) | 0x484c4753u);
        BinaryPrimitives.WriteUInt64BigEndian(id[8..], checked(++_checkpoints));
        var snapshot = State.SaveSnapshot(new Guid(id, bigEndian: true), 1);
        _journal.SaveCheckpoint(snapshot);
        Record($"checkpoint index={State.LastAppliedIndex}");
        return snapshot;
    }

    private void Record(string message)
    {
        var line = $"seed={Seed} tick={_transport.Tick} {message}";
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
}
