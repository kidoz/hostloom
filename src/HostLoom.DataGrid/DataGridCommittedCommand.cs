namespace HostLoom.DataGrid;

public enum DataGridCommittedCommandKind
{
    None = 0,
    TimeAdvance = 1,
    OpenSession = 2,
    CloseSession = 3,
    Reclaim = 4,
    Mutation = 5,
}

/// <summary>A typed input to local committed application. Construction establishes no consensus authority.</summary>
public sealed class DataGridCommittedCommand
{
    private DataGridCommittedCommand(
        DataGridCommittedCommandKind kind,
        DataGridTimestamp time,
        Guid principal = default,
        Guid nonce = default,
        DataGridTimestamp notAfter = default,
        long session = 0,
        ulong sequence = 0,
        Guid map = default,
        DataGridMutationCommand? mutation = null
    )
    {
        Kind = kind;
        ProposedTime = time;
        PrincipalId = principal;
        OpenNonce = nonce;
        OpenNotAfter = notAfter;
        SessionNumber = session;
        RequestNumber = sequence;
        MapId = map;
        Mutation = mutation;
    }

    public DataGridCommittedCommandKind Kind { get; }
    public DataGridTimestamp ProposedTime { get; }
    public Guid PrincipalId { get; }
    public Guid OpenNonce { get; }
    public DataGridTimestamp OpenNotAfter { get; }
    public long SessionNumber { get; }
    public ulong RequestNumber { get; }
    public Guid MapId { get; }
    public DataGridMutationCommand? Mutation { get; }

    public static DataGridCommittedCommand AdvanceTime(DataGridTimestamp time) =>
        new(DataGridCommittedCommandKind.TimeAdvance, time);

    public static DataGridCommittedCommand OpenSession(
        Guid principal,
        Guid nonce,
        DataGridTimestamp notAfter,
        DataGridTimestamp time
    )
    {
        RequireId(principal);
        RequireId(nonce);
        return new(DataGridCommittedCommandKind.OpenSession, time, principal, nonce, notAfter);
    }

    public static DataGridCommittedCommand CloseSession(
        Guid principal,
        long session,
        ulong sequence,
        DataGridTimestamp time
    )
    {
        RequireId(principal);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(session);
        ArgumentOutOfRangeException.ThrowIfZero(sequence);
        return new(
            DataGridCommittedCommandKind.CloseSession,
            time,
            principal,
            session: session,
            sequence: sequence
        );
    }

    public static DataGridCommittedCommand Reclaim(Guid map, DataGridTimestamp time)
    {
        RequireId(map);
        return new(DataGridCommittedCommandKind.Reclaim, time, map: map);
    }

    public static DataGridCommittedCommand Mutate(
        Guid principal,
        DataGridMutationCommand mutation,
        DataGridTimestamp time
    )
    {
        RequireId(principal);
        ArgumentNullException.ThrowIfNull(mutation);
        return new(DataGridCommittedCommandKind.Mutation, time, principal, mutation: mutation);
    }

    private static void RequireId(Guid id)
    {
        if (id == Guid.Empty)
            throw new ArgumentException("A nonempty identity is required.", nameof(id));
    }
}

public enum DataGridApplyStatus
{
    Completed = 0,
    InvalidRequest = 1,
    CapacityExceeded = 2,
    SessionExpired = 3,
    RequestRetired = 4,
    RequestIdentityMismatch = 5,
    MissingMap = 6,
}

public enum DataGridMutationStatus
{
    None = 0,
    Stored = 1,
    Removed = 2,
    Missing = 3,
    VersionMismatch = 4,
    AlreadyExists = 5,
    CapacityExceeded = 6,
    InvalidRequest = 7,
}

public readonly record struct DataGridMutationResult(
    DataGridMutationStatus Status,
    DataGridEntryVersion Version = default
);

/// <summary>An immutable logical result. Retained mutation vectors do not change on retry.</summary>
public sealed class DataGridApplyResult
{
    private readonly DataGridMutationResult[] _items;

    internal DataGridApplyResult(
        DataGridApplyStatus status,
        DataGridMutationResult[]? items = null,
        long sessionNumber = 0,
        DataGridTimestamp? expiresAt = null
    )
    {
        Status = status;
        _items = items is null ? [] : (DataGridMutationResult[])items.Clone();
        SessionNumber = sessionNumber;
        SessionExpiresAt = expiresAt;
    }

    public DataGridApplyStatus Status { get; }
    public long SessionNumber { get; }
    public DataGridTimestamp? SessionExpiresAt { get; }
    public int ItemCount => _items.Length;

    public DataGridMutationResult GetItem(int index) => _items[index];
}

public sealed class DataGridLocalEntry
{
    private readonly byte[] _value;

    internal DataGridLocalEntry(byte[] value, DataGridEntryVersion version, TimeSpan? remaining)
    {
        _value = (byte[])value.Clone();
        Version = version;
        RemainingTimeToLive = remaining;
    }

    public DataGridEntryVersion Version { get; }
    public TimeSpan? RemainingTimeToLive { get; }

    public byte[] GetValue() => (byte[])_value.Clone();
}
