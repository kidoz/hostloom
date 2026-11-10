namespace HostLoom.DataGrid;

public enum DataGridClientFailureCode
{
    None = 0,
    InvalidRequest = 1,
    Unauthorized = 2,
    ProtocolMismatch = 3,
    CapacityExceeded = 4,
    QuorumUnavailable = 5,
    SessionExpired = 6,
    RequestRetired = 7,
    RequestIdentityMismatch = 8,
    OutcomeUnknown = 9,
    MissingMap = 10,
    Redirect = 11,
}

public sealed record DataGridClientFailure
{
    public DataGridClientFailure(
        DataGridClientFailureCode code,
        bool mayHaveBeenAdmitted,
        Guid redirectMember = default,
        ushort retryAfterMilliseconds = 0
    )
    {
        if (
            code
                is < DataGridClientFailureCode.InvalidRequest
                    or > DataGridClientFailureCode.Redirect
            || (code == DataGridClientFailureCode.Redirect) != (redirectMember != Guid.Empty)
            || code == DataGridClientFailureCode.OutcomeUnknown && !mayHaveBeenAdmitted
            || retryAfterMilliseconds > 500
        )
            throw new ArgumentException("Invalid client failure.");
        Code = code;
        MayHaveBeenAdmitted = mayHaveBeenAdmitted;
        RedirectMember = redirectMember;
        RetryAfterMilliseconds = retryAfterMilliseconds;
    }

    public DataGridClientFailureCode Code { get; }
    public bool MayHaveBeenAdmitted { get; }
    public Guid RedirectMember { get; }
    public ushort RetryAfterMilliseconds { get; }
}

/// <summary>One captured read vector. Values, versions and remaining lifetimes share grid time.</summary>
public sealed class DataGridReadResult
{
    private readonly DataGridLocalEntry?[] _entries;

    internal DataGridReadResult(
        DataGridTimestamp capturedAt,
        IEnumerable<DataGridLocalEntry?> entries
    )
    {
        CapturedAt = capturedAt;
        _entries = entries.ToArray();
        if (_entries.Length is < 1 or > 128)
            throw new ArgumentOutOfRangeException(nameof(entries));
    }

    public DataGridTimestamp CapturedAt { get; }
    public int Count => _entries.Length;

    public DataGridLocalEntry? GetItem(int index) => _entries[index];
}

/// <summary>An owned logical call. Retry reuses this request and changes only its physical attempt.</summary>
public sealed class DataGridClientRequest
{
    private DataGridClientRequest(
        DataGridMessageKind kind,
        Guid map = default,
        DataGridKey[]? keys = null,
        DataGridMutationCommand? mutation = null,
        Guid nonce = default,
        DataGridTimestamp notAfter = default,
        long session = 0,
        ulong number = 0
    )
    {
        Kind = kind;
        MapId = map;
        Keys = Array.AsReadOnly(keys ?? []);
        Mutation = mutation;
        OpenNonce = nonce;
        OpenNotAfter = notAfter;
        SessionNumber = session;
        RequestNumber = number;
    }

    public DataGridMessageKind Kind { get; }
    public Guid MapId { get; }
    public IReadOnlyList<DataGridKey> Keys { get; }
    public DataGridMutationCommand? Mutation { get; }
    public Guid OpenNonce { get; }
    public DataGridTimestamp OpenNotAfter { get; }
    public long SessionNumber { get; }
    public ulong RequestNumber { get; }

    public static DataGridClientRequest Mutate(DataGridMutationCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        return new(
            DataGridMessageKind.Mutation,
            command.MapId,
            mutation: command,
            session: command.SessionNumber,
            number: command.RequestNumber
        );
    }

    public static DataGridClientRequest Read(Guid map, IReadOnlyList<DataGridKey> keys)
    {
        if (map == Guid.Empty)
            throw new ArgumentException("Map identity required.", nameof(map));
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count is < 1 or > 128)
            throw new ArgumentOutOfRangeException(nameof(keys));
        var owned = keys.ToArray();
        for (var i = 0; i < owned.Length; i++)
        {
            ArgumentNullException.ThrowIfNull(owned[i]);
            for (var j = 0; j < i; j++)
                if (owned[i].Equals(owned[j]))
                    throw new ArgumentException("Duplicate read key.", nameof(keys));
        }
        return new(DataGridMessageKind.Read, map, owned);
    }

    public static DataGridClientRequest OpenSession(Guid nonce, DataGridTimestamp notAfter)
    {
        if (nonce == Guid.Empty || notAfter.UtcTicks <= 0)
            throw new ArgumentException("A nonce and positive open deadline are required.");
        return new(DataGridMessageKind.OpenSession, nonce: nonce, notAfter: notAfter);
    }

    public static DataGridClientRequest CloseSession(long session, ulong number)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(session);
        ArgumentOutOfRangeException.ThrowIfZero(number);
        return new(DataGridMessageKind.CloseSession, session: session, number: number);
    }
}

/// <summary>A validated response; a conditional rejection remains a completed mutation result.</summary>
public sealed class DataGridClientReply
{
    private DataGridClientReply(
        DataGridMessageKind kind,
        DataGridApplyResult? result = null,
        DataGridReadResult? read = null,
        DataGridClientFailure? failure = null
    )
    {
        Kind = kind;
        Result = result;
        Read = read;
        Failure = failure;
    }

    public DataGridMessageKind Kind { get; }
    public DataGridApplyResult? Result { get; }
    public DataGridReadResult? Read { get; }
    public DataGridClientFailure? Failure { get; }

    internal static DataGridClientReply Completed(
        DataGridMessageKind kind,
        DataGridApplyResult? result = null,
        DataGridReadResult? read = null
    ) => new(kind, result, read);

    public static DataGridClientReply Failed(DataGridClientFailure failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        return new(DataGridMessageKind.Failure, failure: failure);
    }
}

public static class DataGridClientCodec
{
    public static DataGridProtocolFrame Hello(
        Guid incarnation,
        Guid sender,
        Guid attempt,
        ReadOnlySpan<byte> build,
        ReadOnlySpan<byte> configuration
    )
    {
        if (build.Length != 32 || configuration.Length != 32)
            throw new ArgumentException("SHA-256 identities required.");
        using var writer = new GridWriter(72);
        writer.Number(1, 2);
        writer.Number(1, 2);
        writer.Number(0, 2);
        writer.Number(0, 2);
        writer.Raw(build);
        writer.Raw(configuration);
        return new(
            DataGridMessageKind.ClientHello,
            incarnation,
            sender,
            0,
            0,
            attempt,
            writer.ToArray()
        );
    }

    public static void ValidateHello(
        DataGridProtocolFrame frame,
        DataGridBootstrapConfiguration configuration
    )
    {
        var reader = new GridReader(frame.GetPayload());
        if (
            frame.Kind != DataGridMessageKind.ClientHello
            || frame.IncarnationId != configuration.IncarnationId
            || reader.Number(2) != 1
            || reader.Number(2) != 1
            || reader.Number(2) != 0
            || reader.Number(2) != 0
        )
            throw new InvalidDataException("Incompatible client hello.");
        reader.Take(32); // Client/member builds may differ; versions and configuration must agree.
        if (!reader.Take(32).SequenceEqual(configuration.ComputeDigest()))
            throw new InvalidDataException("Foreign client configuration.");
        reader.End();
    }

    public static DataGridProtocolFrame EncodeRequest(
        DataGridClientRequest request,
        Guid incarnation,
        Guid principal,
        Guid attempt
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        using var writer = new GridWriter(DataGridProtocolFrame.MaxPayloadByteCount);
        switch (request.Kind)
        {
            case DataGridMessageKind.Mutation:
                writer.Raw(request.Mutation!.Encode());
                break;
            case DataGridMessageKind.Read:
                writer.Id(request.MapId);
                writer.Number((ulong)request.Keys.Count, 2);
                foreach (var key in request.Keys)
                    writer.Key(key);
                break;
            case DataGridMessageKind.OpenSession:
                writer.Id(request.OpenNonce);
                writer.Number((ulong)request.OpenNotAfter.UtcTicks, 8);
                break;
            case DataGridMessageKind.CloseSession:
                break;
            default:
                throw new InvalidDataException("Unsupported client request.");
        }
        var frame = new DataGridProtocolFrame(
            request.Kind,
            incarnation,
            principal,
            request.SessionNumber,
            request.RequestNumber,
            attempt,
            writer.ToArray()
        );
        _ = DecodeRequest(frame);
        return frame;
    }

    public static DataGridClientRequest DecodeRequest(DataGridProtocolFrame frame)
    {
        try
        {
            var reader = new GridReader(frame.GetPayload());
            DataGridClientRequest result;
            switch (frame.Kind)
            {
                case DataGridMessageKind.Mutation:
                    return DataGridClientRequest.Mutate(DataGridMutationCommand.ReadFrom(frame));
                case DataGridMessageKind.Read:
                    var map = reader.Id();
                    var count = reader.Count(2, 128);
                    var keys = new DataGridKey[count];
                    for (var i = 0; i < count; i++)
                        keys[i] = reader.Key(allowEmpty: false)!;
                    result = DataGridClientRequest.Read(map, keys);
                    break;
                case DataGridMessageKind.OpenSession:
                    result = DataGridClientRequest.OpenSession(reader.Id(), reader.Time());
                    break;
                case DataGridMessageKind.CloseSession:
                    result = DataGridClientRequest.CloseSession(
                        frame.SessionNumber,
                        frame.RequestNumber
                    );
                    break;
                default:
                    throw new InvalidDataException("Unexpected client opcode/direction.");
            }
            reader.End();
            return result;
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid client request.", exception);
        }
    }

    public static DataGridProtocolFrame EncodeReply(
        DataGridClientRequest request,
        DataGridClientReply reply,
        Guid incarnation,
        Guid member,
        Guid attempt
    )
    {
        using var writer = new GridWriter(DataGridProtocolFrame.MaxPayloadByteCount);
        Span<byte> version = stackalloc byte[28];
        if (reply.Failure is { } failure)
        {
            writer.Number((ulong)failure.Code, 2);
            writer.Number(failure.MayHaveBeenAdmitted ? 1u : 0u, 1);
            writer.Number(0, 1);
            writer.Id(failure.RedirectMember);
            writer.Number(failure.RetryAfterMilliseconds, 2);
        }
        else
            switch (reply.Kind)
            {
                case DataGridMessageKind.MutationResult:
                    var result = reply.Result!;
                    writer.Number((ulong)result.ItemCount, 2);
                    for (var i = 0; i < result.ItemCount; i++)
                    {
                        var item = result.GetItem(i);
                        writer.Number((ulong)item.Status, 2);
                        writer.Number(item.Status == DataGridMutationStatus.Stored ? 1u : 0u, 1);
                        version.Clear();
                        if (!item.Version.IsEmpty)
                            item.Version.WriteTo(version);
                        writer.Raw(version);
                    }
                    break;
                case DataGridMessageKind.ReadResult:
                    writer.Number((ulong)reply.Read!.CapturedAt.UtcTicks, 8);
                    writer.Number((ulong)reply.Read.Count, 2);
                    for (var i = 0; i < reply.Read.Count; i++)
                    {
                        var item = reply.Read.GetItem(i);
                        writer.Number(item is null ? 2u : 1u, 1);
                        if (item is null)
                            continue;
                        item.Version.WriteTo(version);
                        writer.Raw(version);
                        writer.Number(item.RemainingTimeToLive is null ? 0u : 1u, 1);
                        writer.Number((ulong)(item.RemainingTimeToLive?.Ticks ?? 0), 8);
                        var value = item.GetValue();
                        writer.Number((ulong)value.Length, 4);
                        writer.Raw(value);
                    }
                    break;
                case DataGridMessageKind.OpenSessionResult:
                    writer.Id(request.OpenNonce);
                    writer.Number((ulong)reply.Result!.SessionNumber, 8);
                    writer.Number((ulong)reply.Result.SessionExpiresAt!.Value.UtcTicks, 8);
                    break;
                case DataGridMessageKind.CloseSessionResult:
                    break;
                default:
                    throw new InvalidDataException("Unsupported client response.");
            }
        return new(
            reply.Kind,
            incarnation,
            member,
            request.SessionNumber,
            request.RequestNumber,
            attempt,
            writer.ToArray()
        );
    }

    public static DataGridClientReply DecodeReply(
        DataGridProtocolFrame frame,
        DataGridClientRequest request,
        Guid expectedAttempt,
        DataGridBootstrapConfiguration configuration
    )
    {
        if (
            frame.AttemptId != expectedAttempt
            || !Enumerable
                .Range(0, 3)
                .Any(i => configuration.GetMember(i).MemberId == frame.SenderId)
            || frame.IncarnationId != configuration.IncarnationId
            || frame.SessionNumber != request.SessionNumber
            || frame.RequestNumber != request.RequestNumber
        )
            throw new InvalidDataException("Mismatched client response identity.");
        try
        {
            var reader = new GridReader(frame.GetPayload());
            DataGridClientReply reply;
            if (frame.Kind == DataGridMessageKind.Failure)
            {
                var code = (DataGridClientFailureCode)reader.Number(2);
                var admission = reader.Flag();
                if (reader.Number(1) != 0)
                    throw new InvalidDataException("Reserved failure field.");
                var redirect = reader.Id(allowEmpty: true);
                var retry = reader.Number(2);
                if (
                    redirect != Guid.Empty
                    && !Enumerable
                        .Range(0, 3)
                        .Any(i => configuration.GetMember(i).MemberId == redirect)
                )
                    throw new InvalidDataException("Foreign redirect.");
                reply = DataGridClientReply.Failed(new(code, admission, redirect, (ushort)retry));
            }
            else if (
                request.Kind == DataGridMessageKind.Mutation
                && frame.Kind == DataGridMessageKind.MutationResult
            )
            {
                var count = reader.Count(2, 128);
                if (count != request.Mutation!.ItemCount)
                    throw new InvalidDataException("Mutation result count mismatch.");
                var items = new DataGridMutationResult[count];
                long storedIndex = 0;
                for (var i = 0; i < count; i++)
                {
                    var status = (DataGridMutationStatus)reader.Number(2);
                    var hasVersion = reader.Flag();
                    var bytes = reader.Take(28);
                    if (
                        status
                            is < DataGridMutationStatus.Stored
                                or > DataGridMutationStatus.InvalidRequest
                        || hasVersion != (status == DataGridMutationStatus.Stored)
                    )
                        throw new InvalidDataException("Invalid mutation result shape.");
                    var version = hasVersion
                        ? Version(bytes, configuration.IncarnationId)
                        : default;
                    if (
                        !hasVersion && bytes.ContainsAnyExcept((byte)0)
                        || hasVersion
                            && (
                                version.ItemIndex != i
                                || storedIndex != 0 && storedIndex != version.LogIndex
                            )
                    )
                        throw new InvalidDataException("Invalid retained result versions.");
                    if (hasVersion)
                        storedIndex = version.LogIndex;
                    items[i] = new(status, version);
                }
                reply = DataGridClientReply.Completed(
                    frame.Kind,
                    new(DataGridApplyStatus.Completed, items)
                );
            }
            else if (
                request.Kind == DataGridMessageKind.Read
                && frame.Kind == DataGridMessageKind.ReadResult
            )
            {
                var time = reader.Time();
                var count = reader.Count(2, 128);
                if (count != request.Keys.Count)
                    throw new InvalidDataException("Read count mismatch.");
                var items = new DataGridLocalEntry?[count];
                for (var i = 0; i < count; i++)
                {
                    var status = reader.Number(1);
                    if (status == 2)
                        continue;
                    if (status != 1)
                        throw new InvalidDataException("Invalid read status.");
                    var version = Version(reader.Take(28), configuration.IncarnationId);
                    var hasTtl = reader.Flag();
                    var remaining = reader.Signed();
                    if (hasTtl ? remaining <= 0 : remaining != 0)
                        throw new InvalidDataException("Invalid remaining lifetime.");
                    var value = reader.Take(reader.Count(4, 65536)).ToArray();
                    items[i] = new(value, version, hasTtl ? TimeSpan.FromTicks(remaining) : null);
                }
                reply = DataGridClientReply.Completed(frame.Kind, read: new(time, items));
            }
            else if (
                request.Kind == DataGridMessageKind.OpenSession
                && frame.Kind == DataGridMessageKind.OpenSessionResult
            )
            {
                if (reader.Id() != request.OpenNonce)
                    throw new InvalidDataException("Wrong open nonce.");
                var session = reader.Signed();
                var expires = reader.Time();
                if (session <= 0 || expires.UtcTicks <= 0)
                    throw new InvalidDataException("Invalid allocated session.");
                reply = DataGridClientReply.Completed(
                    frame.Kind,
                    new(DataGridApplyStatus.Completed, sessionNumber: session, expiresAt: expires)
                );
            }
            else if (
                request.Kind == DataGridMessageKind.CloseSession
                && frame.Kind == DataGridMessageKind.CloseSessionResult
            )
                reply = DataGridClientReply.Completed(frame.Kind);
            else
                throw new InvalidDataException("Unexpected client response opcode/direction.");
            reader.End();
            return reply;
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid client response.", exception);
        }
    }

    private static DataGridEntryVersion Version(ReadOnlySpan<byte> bytes, Guid incarnation)
    {
        var version = DataGridEntryVersion.ReadFrom(bytes);
        if (version.IncarnationId != incarnation || version.ItemIndex >= 128)
            throw new InvalidDataException("Foreign/invalid entry version.");
        return version;
    }
}
