using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("HostLoom.DataGrid.Testing")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.Storage")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.Transport")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.Runtime")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.StorageProbe")]
[assembly: InternalsVisibleTo("HostLoom.Tests")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.ProcessProbe")]

namespace HostLoom.DataGrid.Consensus;

internal enum RaftRole
{
    Follower,
    Candidate,
    Leader,
    Faulted,
}

internal enum RaftMessageKind
{
    VoteRequest,
    VoteResponse,
    AppendRequest,
    AppendResponse,
    SnapshotRequest,
    SnapshotResponse,
}

internal enum RaftCompletionKind
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

internal readonly record struct RaftToken(long Epoch, long Sequence);

internal enum RaftCompactionStatus
{
    Unavailable,
    Busy,
    Unchanged,
    TooLarge,
    Prepared,
    Published,
}

internal sealed record RaftCompactionResult(
    RaftCompactionStatus Status,
    IReadOnlyList<RaftEffect> Effects
);

internal sealed class RaftEntry
{
    private readonly byte[] _command;

    internal RaftEntry(long index, long term, ReadOnlySpan<byte> command)
    {
        if (index <= 0 || term <= 0)
            throw new InvalidDataException("Invalid Raft entry position.");
        DataGridCommittedCommandCodec.ReadFrom(command);
        Index = index;
        Term = term;
        _command = command.ToArray();
    }

    internal long Index { get; }
    internal long Term { get; }
    internal int ByteCount => _command.Length;

    internal byte[] GetCommand() => (byte[])_command.Clone();

    internal bool SameAs(RaftEntry other) =>
        Index == other.Index
        && Term == other.Term
        && _command.AsSpan().SequenceEqual(other._command);
}

/// <summary>An immutable modeled durable transaction, not a filesystem image or WAL batch codec.</summary>
internal sealed class RaftDurableImage
{
    internal const int MaxEntries = 4096;
    internal const int MaxLogBytes = 16 * 1024 * 1024;
    internal const int MaxImageBytes = (int)DataGridSnapshotDescriptor.MaxSnapshotByteCount;
    internal const int ClientEntryLimit = MaxEntries - 128;
    internal const int ClientLogByteLimit =
        MaxLogBytes - DataGridCommittedCommandCodec.MaxEncodedByteCount;
    internal const int CompactEntryThreshold = 1024;
    internal const int CompactByteThreshold = 4 * 1024 * 1024;

    internal RaftDurableImage(
        DataGridBootstrapConfiguration configuration,
        long term = 0,
        int? vote = null,
        IEnumerable<RaftEntry>? entries = null,
        long commitIndex = 0,
        DataGridLocalSnapshot? snapshot = null
    )
    {
        if (term < 0 || vote is < 0 or > 2 || (term == 0 && vote is not null))
            throw new InvalidDataException("Invalid persistent term/vote.");
        Term = term;
        Vote = vote;
        Snapshot = snapshot;
        var log = entries?.ToArray() ?? [];
        if (log.Length > MaxEntries || log.Sum(entry => (long)entry.ByteCount) > MaxLogBytes)
            throw new InvalidDataException("Modeled log capacity exceeded.");
        var previous = snapshot?.Descriptor.LastIncludedIndex ?? 0;
        var previousTerm = snapshot?.Descriptor.LastIncludedTerm ?? 0;
        if (snapshot is not null)
        {
            if (snapshot.Descriptor.ByteCount > MaxImageBytes)
                throw new InvalidDataException("Modeled snapshot capacity exceeded.");
            DataGridLocalStateMachine.Restore(
                configuration,
                snapshot.Descriptor,
                snapshot.GetContent()
            );
        }
        foreach (var entry in log)
        {
            if (
                previous == long.MaxValue
                || entry.Index != previous + 1
                || entry.Term < previousTerm
                || entry.Term > term
            )
                throw new InvalidDataException("Invalid persistent log prefix.");
            previous = entry.Index;
            previousTerm = entry.Term;
        }
        if (previousTerm > term || commitIndex < SnapshotIndex || commitIndex > previous)
            throw new InvalidDataException("Invalid persistent commit position.");
        Entries = Array.AsReadOnly(log);
        CommitIndex = commitIndex;
    }

    internal long Term { get; }
    internal int? Vote { get; }
    internal IReadOnlyList<RaftEntry> Entries { get; }
    internal long CommitIndex { get; }
    internal DataGridLocalSnapshot? Snapshot { get; }
    internal long SnapshotIndex => Snapshot?.Descriptor.LastIncludedIndex ?? 0;
    internal long LastIndex => Entries.Count == 0 ? SnapshotIndex : Entries[^1].Index;

    internal long TermAt(long index) =>
        index == 0 ? 0
        : index == SnapshotIndex ? Snapshot!.Descriptor.LastIncludedTerm
        : EntryAt(index).Term;

    internal RaftEntry EntryAt(long index)
    {
        if (index <= SnapshotIndex || index > LastIndex)
            throw new InvalidDataException("Unavailable Raft log position.");
        return Entries[checked((int)(index - SnapshotIndex - 1))];
    }
}

internal sealed class RaftMessage
{
    private readonly byte[] _configuration;

    internal RaftMessage(
        RaftMessageKind kind,
        int from,
        int to,
        long term,
        RaftToken token,
        Guid incarnation,
        ReadOnlySpan<byte> configuration,
        long index = 0,
        long previousTerm = 0,
        long commit = 0,
        bool accepted = false,
        long conflict = 0,
        IEnumerable<RaftEntry>? entries = null,
        DataGridLocalSnapshot? snapshot = null
    )
    {
        Kind = kind;
        From = from;
        To = to;
        Term = term;
        Token = token;
        Incarnation = incarnation;
        _configuration = configuration.ToArray();
        Index = index;
        PreviousTerm = previousTerm;
        Commit = commit;
        Accepted = accepted;
        Conflict = conflict;
        Entries = Array.AsReadOnly(entries?.ToArray() ?? []);
        Snapshot = snapshot;
    }

    internal RaftMessageKind Kind { get; }
    internal int From { get; }
    internal int To { get; }
    internal long Term { get; }
    internal RaftToken Token { get; }
    internal Guid Incarnation { get; }
    internal long Index { get; }
    internal long PreviousTerm { get; }
    internal long Commit { get; }
    internal bool Accepted { get; }
    internal long Conflict { get; }
    internal IReadOnlyList<RaftEntry> Entries { get; }
    internal DataGridLocalSnapshot? Snapshot { get; }

    internal bool Matches(DataGridBootstrapConfiguration config) =>
        Incarnation == config.IncarnationId
        && _configuration.AsSpan().SequenceEqual(config.ComputeDigest());

    internal long ByteCount =>
        128 + Entries.Sum(entry => 20L + entry.ByteCount) + (Snapshot?.Descriptor.ByteCount ?? 0);
}

internal abstract record RaftEffect;

internal sealed record RaftWrite(RaftToken Token, RaftDurableImage Image) : RaftEffect;

internal sealed record RaftSend(RaftMessage Message) : RaftEffect;

// Delay zero asks the driver to sample its seeded 500–1000 ms election interval.
internal sealed record RaftTimer(RaftToken Token, int DelayMilliseconds) : RaftEffect;

internal sealed record RaftCancelTimer(RaftToken Token) : RaftEffect;

internal sealed record RaftCompletion(
    Guid RequestId,
    RaftCompletionKind Kind,
    long Index = 0,
    DataGridApplyResult? Result = null,
    DataGridLocalEntry? Entry = null,
    DataGridReadResult? ReadResult = null
) : RaftEffect;

internal sealed record RaftFault(string Reason) : RaftEffect;
