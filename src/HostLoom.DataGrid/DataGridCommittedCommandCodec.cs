namespace HostLoom.DataGrid;

/// <summary>The v1 HLGR wrapper codec. A decoded command has no authority until a driver commits it.</summary>
public static class DataGridCommittedCommandCodec
{
    public const int HeaderByteCount = 36;
    public const int MaxEncodedByteCount =
        HeaderByteCount + DataGridMutationCommand.MaxEncodedByteCount;

    public static byte[] Encode(DataGridCommittedCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        using var body = new GridWriter(DataGridMutationCommand.MaxEncodedByteCount);
        switch (command.Kind)
        {
            case DataGridCommittedCommandKind.TimeAdvance:
                break;
            case DataGridCommittedCommandKind.OpenSession:
                body.Id(command.OpenNonce);
                body.Number((ulong)command.OpenNotAfter.UtcTicks, 8);
                break;
            case DataGridCommittedCommandKind.CloseSession:
                body.Number((ulong)command.SessionNumber, 8);
                body.Number(command.RequestNumber, 8);
                break;
            case DataGridCommittedCommandKind.Reclaim:
                body.Id(command.MapId);
                break;
            case DataGridCommittedCommandKind.Mutation:
                body.Raw(command.Mutation!.Encode());
                break;
            default:
                throw new ArgumentException("Unknown committed command.", nameof(command));
        }
        using var writer = new GridWriter(MaxEncodedByteCount);
        writer.Raw("HLGR"u8);
        writer.Number(1, 2);
        writer.Number((ulong)command.Kind, 2);
        writer.Number((ulong)command.ProposedTime.UtcTicks, 8);
        writer.Id(command.PrincipalId);
        writer.Number((ulong)body.Length, 4);
        writer.Raw(body.ToArray());
        return writer.ToArray();
    }

    public static DataGridCommittedCommand ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < HeaderByteCount || source.Length > MaxEncodedByteCount)
            throw new InvalidDataException("Invalid wrapper size.");
        try
        {
            var reader = new GridReader(source);
            if (!reader.Take(4).SequenceEqual("HLGR"u8) || reader.Number(2) != 1)
                throw new InvalidDataException("Invalid wrapper header.");
            var kind = (DataGridCommittedCommandKind)reader.Number(2);
            var time = reader.Time();
            var principal = reader.Id(allowEmpty: true);
            var body = new GridReader(
                reader.Take(reader.Count(4, DataGridMutationCommand.MaxEncodedByteCount))
            );
            reader.End();
            if (
                (
                    kind
                    is DataGridCommittedCommandKind.TimeAdvance
                        or DataGridCommittedCommandKind.Reclaim
                ) != (principal == Guid.Empty)
            )
                throw new InvalidDataException("Invalid wrapper principal.");
            DataGridCommittedCommand command = kind switch
            {
                DataGridCommittedCommandKind.TimeAdvance => DataGridCommittedCommand.AdvanceTime(
                    time
                ),
                DataGridCommittedCommandKind.OpenSession => DataGridCommittedCommand.OpenSession(
                    principal,
                    body.Id(),
                    body.Time(),
                    time
                ),
                DataGridCommittedCommandKind.CloseSession => DataGridCommittedCommand.CloseSession(
                    principal,
                    body.Signed(),
                    body.Number(8),
                    time
                ),
                DataGridCommittedCommandKind.Reclaim => DataGridCommittedCommand.Reclaim(
                    body.Id(),
                    time
                ),
                DataGridCommittedCommandKind.Mutation => DataGridCommittedCommand.Mutate(
                    principal,
                    DataGridMutationCommand.ReadFrom(body.Take(source.Length - HeaderByteCount)),
                    time
                ),
                _ => throw new InvalidDataException("Unknown wrapper kind."),
            };
            body.End();
            return command;
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid wrapper fields.", exception);
        }
    }
}
