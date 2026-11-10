using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using HostLoom.DataGrid.Consensus;

[assembly: InternalsVisibleTo("HostLoom.Tests")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.ProcessProbe")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.Runtime")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.StorageProbe")]

namespace HostLoom.DataGrid.Storage;

/// <summary>Bounded semantic replay. No application becomes visible until the whole WAL validates.</summary>
internal static class DataGridWalRecovery
{
    internal const int MaxRecordCount = 1_000_000;

    internal static (RaftDurableImage Image, long LastNumber) Read(
        Stream stream,
        DataGridBootstrapConfiguration configuration,
        Guid member,
        Guid generation,
        Func<DataGridSnapshotDescriptor, DataGridLocalSnapshot> snapshotReader
    )
    {
        long number = 0,
            term = 0,
            commit = 0,
            logBytes = 0;
        int? vote = null;
        var entries = new List<RaftEntry>();
        DataGridLocalSnapshot? snapshot = null;
        var hasHardState = false;
        var checkpointed = false;
        while (ReadRecord(stream, checked(number + 1)) is { } record)
        {
            if (++number > MaxRecordCount)
                throw new InvalidDataException("WAL record inventory exceeded.");
            var payload = record.GetPayload();
            if (number == 1)
            {
                if (
                    record.Kind != DataGridWalRecordKind.Identity
                    || !payload.AsSpan().SequenceEqual(Identity(configuration, member, generation))
                )
                    throw new InvalidDataException("WAL identity mismatch.");
                continue;
            }
            if (number == 2 && record.Kind != DataGridWalRecordKind.HardState)
                throw new InvalidDataException("Missing seed hard state.");
            var reader = new GridReader(payload);
            switch (record.Kind)
            {
                case DataGridWalRecordKind.HardState:
                    var nextTerm = reader.Signed();
                    var nextVote = Vote(configuration, reader.Id(allowEmpty: true));
                    reader.End();
                    if (
                        nextTerm < term
                        || nextTerm < 0
                        || (nextTerm == 0 && nextVote is not null)
                        || (nextTerm == term && vote is not null && nextVote != vote)
                    )
                        throw new InvalidDataException("Invalid hard-state transition.");
                    if (!checkpointed && hasHardState)
                        throw new InvalidDataException("Repeated seed hard state.");
                    term = nextTerm;
                    vote = nextVote;
                    hasHardState = true;
                    break;
                case DataGridWalRecordKind.LogAppend:
                    var index = reader.Signed();
                    var entryTerm = reader.Signed();
                    var length = reader.Count(4, DataGridCommittedCommandCodec.MaxEncodedByteCount);
                    var command = reader.Take(length);
                    reader.End();
                    var lastIndex =
                        entries.Count == 0
                            ? snapshot?.Descriptor.LastIncludedIndex ?? 0
                            : entries[^1].Index;
                    var lastTerm =
                        entries.Count == 0
                            ? snapshot?.Descriptor.LastIncludedTerm ?? 0
                            : entries[^1].Term;
                    if (
                        index <= 0
                        || index != checked(lastIndex + 1)
                        || entryTerm < lastTerm
                        || entryTerm > term
                    )
                        throw new InvalidDataException("Invalid append position/term.");
                    if (
                        entries.Count >= RaftDurableImage.MaxEntries
                        || logBytes + length > RaftDurableImage.MaxLogBytes
                    )
                        throw new InvalidDataException("Retained WAL log capacity exceeded.");
                    entries.Add(new(index, entryTerm, command));
                    logBytes += length;
                    break;
                case DataGridWalRecordKind.LogTruncate:
                    var firstDiscarded = reader.Signed();
                    reader.End();
                    var tail =
                        entries.Count == 0
                            ? snapshot?.Descriptor.LastIncludedIndex ?? 0
                            : entries[^1].Index;
                    if (
                        !checkpointed
                        || firstDiscarded <= commit
                        || firstDiscarded <= (snapshot?.Descriptor.LastIncludedIndex ?? 0)
                        || firstDiscarded > tail
                            && (tail == long.MaxValue || firstDiscarded != tail + 1)
                    )
                        throw new InvalidDataException("Invalid truncation position.");
                    entries.RemoveAll(entry => entry.Index >= firstDiscarded);
                    logBytes = entries.Sum(entry => (long)entry.ByteCount);
                    break;
                case DataGridWalRecordKind.CommitCheckpoint:
                    var nextCommit = reader.Signed();
                    var commitTerm = reader.Signed();
                    reader.End();
                    var candidate = new RaftDurableImage(
                        configuration,
                        term,
                        vote,
                        entries,
                        nextCommit,
                        snapshot
                    );
                    if (nextCommit < commit || candidate.TermAt(nextCommit) != commitTerm)
                        throw new InvalidDataException("Invalid commit checkpoint.");
                    commit = nextCommit;
                    checkpointed = true;
                    break;
                case DataGridWalRecordKind.SnapshotManifest:
                    if (number != 3 || checkpointed || snapshot is not null || entries.Count != 0)
                        throw new InvalidDataException("Snapshot requires a new generation seed.");
                    var descriptor = DataGridSnapshotDescriptor.ReadFrom(payload);
                    if (
                        descriptor.LastIncludedTerm > term
                        || descriptor.ByteCount > RaftDurableImage.MaxImageBytes
                    )
                        throw new InvalidDataException("Invalid snapshot position/capacity.");
                    snapshot = snapshotReader(descriptor);
                    if (!snapshot.Descriptor.Encode().AsSpan().SequenceEqual(descriptor.Encode()))
                        throw new InvalidDataException(
                            "Snapshot reader returned a different image."
                        );
                    DataGridLocalStateMachine.Restore(
                        configuration,
                        descriptor,
                        snapshot.GetContent()
                    );
                    break;
                default:
                    throw new InvalidDataException("Unexpected WAL identity/kind.");
            }
        }
        if (!hasHardState || !checkpointed)
            throw new InvalidDataException("Incomplete generation seed.");
        return (new(configuration, term, vote, entries, commit, snapshot), number);
    }

    internal static DataGridWalRecord? ReadRecord(Stream stream, long expectedNumber)
    {
        Span<byte> header = stackalloc byte[DataGridWalRecord.HeaderByteCount];
        var first = stream.Read(header[..1]);
        if (first == 0)
            return null;
        ReadExactly(stream, header[1..]);
        var length = BinaryPrimitives.ReadUInt32BigEndian(header[8..]);
        var kind = BinaryPrimitives.ReadUInt16BigEndian(header[6..]);
        if (
            !header[..4].SequenceEqual("HLGW"u8)
            || BinaryPrimitives.ReadUInt16BigEndian(header[4..]) != DataGridWalRecord.FormatVersion
            || kind is < 1 or > 6
            || length > DataGridWalRecord.MaxPayloadByteCount
            || BinaryPrimitives.ReadInt64BigEndian(header[12..]) != expectedNumber
        )
            throw new InvalidDataException("Invalid WAL stream header.");
        var frame = new byte[
            checked(header.Length + (int)length + DataGridWalRecord.DigestByteCount)
        ];
        header.CopyTo(frame);
        ReadExactly(stream, frame.AsSpan(header.Length));
        return DataGridWalRecord.ReadFrom(frame, expectedNumber);
    }

    internal static byte[] Identity(
        DataGridBootstrapConfiguration config,
        Guid member,
        Guid generation
    )
    {
        using var writer = new GridWriter(80);
        writer.Id(config.IncarnationId);
        writer.Id(member);
        writer.Id(generation);
        writer.Raw(config.ComputeDigest());
        return writer.ToArray();
    }

    internal static byte[] HardState(DataGridBootstrapConfiguration config, RaftDurableImage image)
    {
        using var writer = new GridWriter(24);
        writer.Number((ulong)image.Term, 8);
        writer.Id(image.Vote is { } vote ? config.GetMember(vote).MemberId : Guid.Empty);
        return writer.ToArray();
    }

    internal static byte[] Append(RaftEntry entry)
    {
        using var writer = new GridWriter(DataGridWalRecord.MaxPayloadByteCount);
        writer.Number((ulong)entry.Index, 8);
        writer.Number((ulong)entry.Term, 8);
        writer.Number((ulong)entry.ByteCount, 4);
        writer.Raw(entry.GetCommand());
        return writer.ToArray();
    }

    internal static byte[] Position(long index, long? term = null)
    {
        using var writer = new GridWriter(16);
        writer.Number((ulong)index, 8);
        if (term is { } value)
            writer.Number((ulong)value, 8);
        return writer.ToArray();
    }

    private static int? Vote(DataGridBootstrapConfiguration config, Guid member)
    {
        if (member == Guid.Empty)
            return null;
        for (var i = 0; i < 3; i++)
            if (config.GetMember(i).MemberId == member)
                return i;
        throw new InvalidDataException("Foreign persistent voter.");
    }

    private static void ReadExactly(Stream stream, Span<byte> buffer)
    {
        try
        {
            stream.ReadExactly(buffer);
        }
        catch (EndOfStreamException exception)
        {
            throw new InvalidDataException("Torn WAL tail; repair is not automatic.", exception);
        }
    }
}
