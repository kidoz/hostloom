using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using HostLoom.DataGrid.Consensus;

[assembly: InternalsVisibleTo("HostLoom.Tests")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.ProcessProbe")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.Runtime")]
[assembly: InternalsVisibleTo("HostLoom.DataGrid.Client")]

namespace HostLoom.DataGrid.Transport;

internal static class DataGridPeerCodec
{
    internal static Guid Attempt(RaftToken token)
    {
        if (token.Epoch <= 0 || token.Sequence <= 0)
            throw new InvalidDataException("Invalid peer attempt.");
        Span<byte> bytes = stackalloc byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes, token.Epoch);
        BinaryPrimitives.WriteInt64BigEndian(bytes[8..], token.Sequence);
        return new(bytes, bigEndian: true);
    }

    internal static RaftToken Token(Guid attempt)
    {
        Span<byte> bytes = stackalloc byte[16];
        attempt.TryWriteBytes(bytes, bigEndian: true, out _);
        var token = new RaftToken(
            BinaryPrimitives.ReadInt64BigEndian(bytes),
            BinaryPrimitives.ReadInt64BigEndian(bytes[8..])
        );
        _ = Attempt(token);
        return token;
    }

    internal static DataGridProtocolFrame Frame(
        DataGridBootstrapConfiguration config,
        int sender,
        DataGridMessageKind kind,
        Guid attempt,
        ReadOnlySpan<byte> payload
    ) => new(kind, config.IncarnationId, config.GetMember(sender).MemberId, 0, 0, attempt, payload);

    internal static DataGridProtocolFrame Hello(
        DataGridBootstrapConfiguration config,
        int sender,
        Guid attempt,
        byte[] build
    )
    {
        if (build.Length != 32)
            throw new ArgumentException("Build identity requires SHA-256.", nameof(build));
        using var writer = new GridWriter(72);
        for (var i = 0; i < 4; i++)
            writer.Number(1, 2);
        writer.Raw(build);
        writer.Raw(config.ComputeDigest());
        return Frame(config, sender, DataGridMessageKind.MemberHello, attempt, writer.ToArray());
    }

    internal static void ValidateHello(
        DataGridProtocolFrame frame,
        DataGridBootstrapConfiguration config,
        byte[] build
    )
    {
        if (
            frame.Kind != DataGridMessageKind.MemberHello
            || frame.SessionNumber != 0
            || frame.RequestNumber != 0
        )
            throw new InvalidDataException("Member hello required.");
        if (
            !frame
                .GetPayload()
                .AsSpan()
                .SequenceEqual(Hello(config, 0, frame.AttemptId, build).GetPayload())
        )
            throw new InvalidDataException("Incompatible member build/configuration.");
    }

    internal static DataGridProtocolFrame Encode(
        RaftMessage message,
        DataGridBootstrapConfiguration config
    )
    {
        if (!message.Matches(config))
            throw new InvalidDataException("Foreign peer message.");
        using var writer = new GridWriter(DataGridProtocolFrame.MaxPayloadByteCount);
        writer.Number((ulong)message.Term, 8);
        DataGridMessageKind kind;
        switch (message.Kind)
        {
            case RaftMessageKind.VoteRequest:
                kind = DataGridMessageKind.VoteRequest;
                writer.Number((ulong)message.Index, 8);
                writer.Number((ulong)message.PreviousTerm, 8);
                break;
            case RaftMessageKind.VoteResponse:
                kind = DataGridMessageKind.VoteResponse;
                writer.Number(message.Accepted ? 1u : 0u, 1);
                break;
            case RaftMessageKind.AppendRequest:
                kind = DataGridMessageKind.AppendRequest;
                writer.Number((ulong)message.Index, 8);
                writer.Number((ulong)message.PreviousTerm, 8);
                writer.Number((ulong)message.Commit, 8);
                writer.Number((ulong)message.Entries.Count, 2);
                foreach (var entry in message.Entries)
                {
                    writer.Number((ulong)entry.Index, 8);
                    writer.Number((ulong)entry.Term, 8);
                    writer.Number((ulong)entry.ByteCount, 4);
                    writer.Raw(entry.GetCommand());
                }
                break;
            case RaftMessageKind.AppendResponse:
                kind = DataGridMessageKind.AppendResponse;
                writer.Number(message.Accepted ? 1u : 0u, 1);
                writer.Number((ulong)message.Index, 8);
                writer.Number((ulong)(message.Accepted ? 0 : Math.Max(1, message.Conflict)), 8);
                writer.Number(0, 8);
                break;
            default:
                throw new InvalidDataException("Snapshot messages require the transfer protocol.");
        }
        var frame = Frame(config, message.From, kind, Attempt(message.Token), writer.ToArray());
        _ = Decode(
            frame,
            config,
            message.From,
            message.To,
            response: kind is DataGridMessageKind.VoteResponse or DataGridMessageKind.AppendResponse
        );
        return frame;
    }

    internal static RaftMessage Decode(
        DataGridProtocolFrame frame,
        DataGridBootstrapConfiguration config,
        int from,
        int to,
        bool response
    )
    {
        if (
            frame.IncarnationId != config.IncarnationId
            || frame.SenderId != config.GetMember(from).MemberId
            || from == to
            || frame.SessionNumber != 0
            || frame.RequestNumber != 0
        )
            throw new InvalidDataException("Invalid member envelope.");
        var token = Token(frame.AttemptId);
        var reader = new GridReader(frame.GetPayload());
        var term = reader.Signed();
        if (term < 0 || (!response && term == 0))
            throw new InvalidDataException("Invalid peer term.");
        RaftMessage message;
        if (!response && frame.Kind == DataGridMessageKind.VoteRequest)
        {
            var index = reader.Signed();
            var previous = reader.Signed();
            ValidatePosition(index, previous, term);
            message = new(
                RaftMessageKind.VoteRequest,
                from,
                to,
                term,
                token,
                config.IncarnationId,
                config.ComputeDigest(),
                index,
                previous
            );
        }
        else if (response && frame.Kind == DataGridMessageKind.VoteResponse)
        {
            var accepted = reader.Flag();
            if (accepted && term == 0)
                throw new InvalidDataException("Zero-term grant.");
            message = new(
                RaftMessageKind.VoteResponse,
                from,
                to,
                term,
                token,
                config.IncarnationId,
                config.ComputeDigest(),
                accepted: accepted
            );
        }
        else if (!response && frame.Kind == DataGridMessageKind.AppendRequest)
        {
            var previousIndex = reader.Signed();
            var previousTerm = reader.Signed();
            ValidatePosition(previousIndex, previousTerm, term);
            var commit = reader.Signed();
            if (commit < 0)
                throw new InvalidDataException("Negative leader commit.");
            var count = reader.Count(2, 128);
            var entries = new List<RaftEntry>(count);
            var index = previousIndex;
            var lastTerm = previousTerm;
            for (var i = 0; i < count; i++)
            {
                var nextIndex = reader.Signed();
                var nextTerm = reader.Signed();
                if (
                    index == long.MaxValue
                    || nextIndex != index + 1
                    || nextTerm < lastTerm
                    || nextTerm <= 0
                    || nextTerm > term
                )
                    throw new InvalidDataException("Invalid peer log order/term.");
                entries.Add(
                    new(
                        nextIndex,
                        nextTerm,
                        reader.Take(
                            reader.Count(4, DataGridCommittedCommandCodec.MaxEncodedByteCount)
                        )
                    )
                );
                index = nextIndex;
                lastTerm = nextTerm;
            }
            message = new(
                RaftMessageKind.AppendRequest,
                from,
                to,
                term,
                token,
                config.IncarnationId,
                config.ComputeDigest(),
                previousIndex,
                previousTerm,
                commit,
                entries: entries
            );
        }
        else if (response && frame.Kind == DataGridMessageKind.AppendResponse)
        {
            var accepted = reader.Flag();
            var index = reader.Signed();
            var conflict = reader.Signed();
            var conflictTerm = reader.Signed();
            if (
                index < 0
                || (
                    accepted
                        ? conflict != 0 || conflictTerm != 0 || term == 0
                        : index != 0 || conflict < 1 || conflictTerm < 0 || conflictTerm > term
                )
            )
                throw new InvalidDataException("Invalid append response.");
            message = new(
                RaftMessageKind.AppendResponse,
                from,
                to,
                term,
                token,
                config.IncarnationId,
                config.ComputeDigest(),
                index: index,
                accepted: accepted,
                conflict: conflict
            );
        }
        else
            throw new InvalidDataException("Unexpected peer opcode/direction.");
        reader.End();
        return message;
    }

    internal static async ValueTask<DataGridProtocolFrame> ReadAsync(
        Stream stream,
        DataGridBootstrapConfiguration config,
        int sender,
        CancellationToken cancellationToken
    )
    {
        var prefix = new byte[4];
        await stream.ReadExactlyAsync(prefix, cancellationToken).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadUInt32BigEndian(prefix);
        if (
            length < DataGridProtocolFrame.HeaderByteCount - 4
            || length > DataGridProtocolFrame.MaxEncodedByteCount - 4
        )
            throw new InvalidDataException("Peer frame length rejected before allocation.");
        var bytes = new byte[checked((int)length + 4)];
        prefix.CopyTo(bytes, 0);
        await stream.ReadExactlyAsync(bytes.AsMemory(4), cancellationToken).ConfigureAwait(false);
        return DataGridProtocolFrame.ReadFrom(
            bytes,
            config.IncarnationId,
            config.GetMember(sender).MemberId
        );
    }

    internal static ValueTask WriteAsync(
        Stream stream,
        DataGridProtocolFrame frame,
        CancellationToken cancellationToken
    ) => stream.WriteAsync(frame.Encode(), cancellationToken);

    private static void ValidatePosition(long index, long previousTerm, long term)
    {
        if (
            index < 0
            || previousTerm < 0
            || previousTerm > term
            || (index == 0) != (previousTerm == 0)
        )
            throw new InvalidDataException("Invalid peer log position.");
    }
}
