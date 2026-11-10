namespace HostLoom.DataGrid;

/// <summary>Single-owner deterministic local application. Not thread safe; contains no quorum or I/O.</summary>
/// <remarks>The driver serializes commands and reads and supplies only verified committed positions.</remarks>
public sealed partial class DataGridLocalStateMachine
{
    public const int MaxSessionCount = 4096;
    public const int MaxSessionsPerPrincipal = 64;
    private const long MaxResultBytes = 64L * 1024 * 1024;
    private static readonly DataGridKey MaximumKey = new(
        Enumerable.Repeat(byte.MaxValue, DataGridKey.MaxByteCount).ToArray()
    );
    private readonly DataGridBootstrapConfiguration _configuration;
    private readonly MapState[] _maps;
    private readonly SortedDictionary<long, SessionState> _sessions = [];
    private long _sessionHighWater;
    private long _resultBytes;
    private int _maintenanceMapOrdinal;

    public DataGridLocalStateMachine(DataGridBootstrapConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        _configuration = configuration;
        _maps = new MapState[configuration.MapCount];
        for (var i = 0; i < _maps.Length; i++)
            _maps[i] = new(configuration.GetMap(i));
    }

    public long LastAppliedIndex { get; private set; }
    public DataGridTimestamp AppliedTime { get; private set; }
    public int SessionCount => _sessions.Count;
    public long SessionHighWater => _sessionHighWater;
    public int StoredKeyCount => _maps.Sum(map => map.Entries.Count);
    public long StoredDataByteCount => _maps.Sum(map => map.Bytes);
    public Guid NextMaintenanceMapId => _maps[_maintenanceMapOrdinal].Configuration.MapId;

    public DataGridApplyResult Apply(long committedIndex, DataGridCommittedCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (LastAppliedIndex == long.MaxValue || committedIndex != LastAppliedIndex + 1)
            throw new InvalidOperationException(
                "Committed application requires the next contiguous index."
            );
        LastAppliedIndex = committedIndex;
        AppliedTime = AppliedTime.AdvanceTo(command.ProposedTime);
        return command.Kind switch
        {
            DataGridCommittedCommandKind.TimeAdvance => RetireSessions(),
            DataGridCommittedCommandKind.OpenSession => Open(command),
            DataGridCommittedCommandKind.CloseSession => Close(command),
            DataGridCommittedCommandKind.Reclaim => ReclaimMap(command.MapId),
            DataGridCommittedCommandKind.Mutation => Mutate(command.PrincipalId, command.Mutation!),
            _ => throw new InvalidOperationException("Unknown committed command."),
        };
    }

    /// <summary>Reads at the current applied grid time; the caller must establish authority/time first.</summary>
    public DataGridLocalEntry? Read(Guid mapId, DataGridKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var map = FindMap(mapId) ?? throw new ArgumentException("Unknown map.", nameof(mapId));
        if (
            !map.Entries.TryGetValue(Probe(key), out var entry)
            || entry.Expiration.IsExpired(AppliedTime)
        )
            return null;
        return new(
            entry.Value,
            entry.Version,
            entry.Expiration.GetRemainingTimeToLive(AppliedTime)
        );
    }

    /// <summary>One ordered local capture; no partial vector when the response budget cannot hold it.</summary>
    public IReadOnlyList<DataGridLocalEntry?> ReadMany(Guid mapId, IReadOnlyList<DataGridKey> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count is < 1 or > 128)
            throw new ArgumentOutOfRangeException(nameof(keys));
        var result = new DataGridLocalEntry?[keys.Count];
        var size = 10;
        for (var i = 0; i < keys.Count; i++)
        {
            ArgumentNullException.ThrowIfNull(keys[i]);
            for (var j = 0; j < i; j++)
                if (keys[j].Equals(keys[i]))
                    throw new ArgumentException("Duplicate read key.", nameof(keys));
            result[i] = Read(mapId, keys[i]);
            size += result[i] is { } entry ? 42 + entry.GetValue().Length : 1;
            if (size > DataGridProtocolFrame.MaxPayloadByteCount)
                throw new InvalidOperationException("Read response capacity exceeded.");
        }
        return Array.AsReadOnly(result);
    }

    private DataGridApplyResult Open(DataGridCommittedCommand command)
    {
        var existing = _sessions.Values.FirstOrDefault(session =>
            session.Principal == command.PrincipalId && session.Nonce == command.OpenNonce
        );
        if (existing is not null && existing.NotAfter != command.OpenNotAfter)
            return Failure(DataGridApplyStatus.RequestIdentityMismatch);
        if (command.OpenNotAfter.UtcTicks <= AppliedTime.UtcTicks)
            return Failure(DataGridApplyStatus.SessionExpired);
        if (command.OpenNotAfter.UtcTicks - AppliedTime.UtcTicks > TimeSpan.TicksPerSecond * 5)
            return Failure(DataGridApplyStatus.InvalidRequest);
        if (existing is not null)
            return existing.Closed || existing.Expires.UtcTicks <= AppliedTime.UtcTicks
                ? Failure(DataGridApplyStatus.SessionExpired)
                : new(
                    DataGridApplyStatus.Completed,
                    sessionNumber: existing.Number,
                    expiresAt: existing.Expires
                );
        if (
            _sessionHighWater == long.MaxValue
            || _sessions.Count >= MaxSessionCount
            || _sessions.Values.Count(session => session.Principal == command.PrincipalId)
                >= MaxSessionsPerPrincipal
        )
            return Failure(DataGridApplyStatus.CapacityExceeded);
        DataGridTimestamp expiry;
        try
        {
            expiry = DataGridExpiration
                .After(AppliedTime, TimeSpan.FromMinutes(10))
                .Deadline!.Value;
        }
        catch (ArgumentOutOfRangeException)
        {
            return Failure(DataGridApplyStatus.InvalidRequest);
        }
        var state = new SessionState(
            ++_sessionHighWater,
            command.PrincipalId,
            command.OpenNonce,
            command.OpenNotAfter,
            expiry
        );
        _sessions.Add(state.Number, state);
        return new(
            DataGridApplyStatus.Completed,
            sessionNumber: state.Number,
            expiresAt: state.Expires
        );
    }

    private DataGridApplyResult Close(DataGridCommittedCommand command)
    {
        if (!TrySession(command.SessionNumber, command.PrincipalId, out var session))
            return Failure(DataGridApplyStatus.SessionExpired);
        if (
            session.LastSequence == ulong.MaxValue
            || command.RequestNumber != session.LastSequence + 1
        )
            return Failure(DataGridApplyStatus.InvalidRequest);
        _resultBytes -= ResultBytes(session.Result);
        session.Result = null;
        session.Digest = [];
        session.LastSequence = command.RequestNumber;
        session.Closed = true;
        return new(DataGridApplyStatus.Completed);
    }

    private DataGridApplyResult Mutate(Guid principal, DataGridMutationCommand command)
    {
        if (!TrySession(command.SessionNumber, principal, out var session))
            return Failure(DataGridApplyStatus.SessionExpired);
        var digest = command.ComputeDigest();
        if (command.RequestNumber == session.LastSequence)
            return session.Digest.AsSpan().SequenceEqual(digest)
                ? session.Result!
                : Failure(DataGridApplyStatus.RequestIdentityMismatch);
        if (command.RequestNumber < session.LastSequence)
            return Failure(DataGridApplyStatus.RequestRetired);
        if (
            session.LastSequence == ulong.MaxValue
            || command.RequestNumber != session.LastSequence + 1
        )
            return Failure(DataGridApplyStatus.InvalidRequest);
        var map = FindMap(command.MapId);
        if (map is null)
            return Failure(DataGridApplyStatus.MissingMap);
        var reservedBytes = 2 + 31 * command.ItemCount;
        if (_resultBytes - ResultBytes(session.Result) + reservedBytes > MaxResultBytes)
            return Failure(DataGridApplyStatus.CapacityExceeded);
        var invalid =
            map.Configuration.Policy.Expiration == 1
            && command.ExpiryKind != DataGridExpiryKind.Never;
        for (var i = 0; i < command.ItemCount; i++)
            invalid |=
                !command.GetItem(i).ExpectedVersion.IsEmpty
                && command.GetItem(i).ExpectedVersion.IncarnationId != _configuration.IncarnationId;
        var expiration = DataGridExpiration.Never;
        try
        {
            expiration = command.ExpiryKind switch
            {
                DataGridExpiryKind.Never => DataGridExpiration.Never,
                DataGridExpiryKind.Relative => DataGridExpiration.After(
                    AppliedTime,
                    TimeSpan.FromTicks(command.ExpiryTicks)
                ),
                DataGridExpiryKind.Absolute => DataGridExpiration.At(new(command.ExpiryTicks)),
                _ => throw new InvalidOperationException("Unknown expiration."),
            };
        }
        catch (ArgumentOutOfRangeException)
        {
            invalid = true;
        }
        var items = new DataGridMutationResult[command.ItemCount];
        for (var i = 0; i < items.Length; i++)
            items[i] = invalid
                ? new(DataGridMutationStatus.InvalidRequest)
                : ApplyItem(map, command.Kind, command.GetItem(i), i, expiration);
        var result = new DataGridApplyResult(DataGridApplyStatus.Completed, items);
        _resultBytes += reservedBytes - ResultBytes(session.Result);
        session.LastSequence = command.RequestNumber;
        session.Digest = digest;
        session.Result = result;
        return result;
    }

    private DataGridMutationResult ApplyItem(
        MapState map,
        DataGridMutationKind kind,
        DataGridMutationItem item,
        int ordinal,
        DataGridExpiration expiration
    )
    {
        var stored = map.Entries.TryGetValue(Probe(item.Key), out var old);
        var live = stored && !old.Expiration.IsExpired(AppliedTime);
        var conditional =
            kind is DataGridMutationKind.CompareExchange or DataGridMutationKind.CompareRemove;
        if (conditional && !live)
            return new(DataGridMutationStatus.Missing);
        if (conditional && old.Version != item.ExpectedVersion)
            return new(DataGridMutationStatus.VersionMismatch);
        if (kind == DataGridMutationKind.PutIfAbsent && live)
            return new(DataGridMutationStatus.AlreadyExists);
        if (kind is DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove)
        {
            if (!live)
                return new(DataGridMutationStatus.Missing);
            map.Entries.Remove(old);
            map.Bytes -= old.Key.ByteCount + old.Value.Length;
            return new(DataGridMutationStatus.Removed);
        }
        var value = item.GetValue();
        var delta =
            item.Key.ByteCount + value.Length - (stored ? old.Key.ByteCount + old.Value.Length : 0);
        if (
            map.Entries.Count + (stored ? 0 : 1) > map.Configuration.MaxKeyCount
            || map.Bytes + delta > map.Configuration.MaxDataByteCount
            || StoredKeyCount + (stored ? 0 : 1) > DataGridMapConfiguration.GroupMaxKeyCount
            || StoredDataByteCount + delta > DataGridMapConfiguration.GroupMaxDataByteCount
        )
            return new(DataGridMutationStatus.CapacityExceeded);
        var version = new DataGridEntryVersion(
            _configuration.IncarnationId,
            LastAppliedIndex,
            ordinal
        );
        if (stored)
            map.Entries.Remove(old);
        map.Entries.Add(new(item.Key, value, version, expiration));
        map.Bytes += delta;
        return new(DataGridMutationStatus.Stored, version);
    }

    private DataGridApplyResult RetireSessions()
    {
        var retired = _sessions
            .Values.Where(session =>
                (session.Closed ? session.NotAfter : session.Expires).UtcTicks
                <= AppliedTime.UtcTicks
            )
            .Take(128)
            .Select(session => session.Number)
            .ToArray();
        foreach (var number in retired)
        {
            _resultBytes -= ResultBytes(_sessions[number].Result);
            _sessions.Remove(number);
        }
        return new(DataGridApplyStatus.Completed);
    }

    private DataGridApplyResult ReclaimMap(Guid id)
    {
        var map = FindMap(id);
        if (map is null)
            return Failure(DataGridApplyStatus.MissingMap);
        var start = map.Cursor is null ? null : Successor(map.Cursor);
        var candidates = (
            start is null
                ? map.Entries
                : map.Entries.GetViewBetween(Probe(start), Probe(MaximumKey))
        )
            .Take(128)
            .ToArray();
        if (start is not null && candidates.Length < 128)
            candidates =
            [
                .. candidates,
                .. map
                    .Entries.GetViewBetween(Probe(new DataGridKey([0])), Probe(map.Cursor!))
                    .Take(128 - candidates.Length),
            ];
        foreach (var entry in candidates)
        {
            map.Cursor = entry.Key;
            if (entry.Expiration.IsExpired(AppliedTime))
            {
                map.Entries.Remove(entry);
                map.Bytes -= entry.Key.ByteCount + entry.Value.Length;
            }
        }
        _maintenanceMapOrdinal = (Array.IndexOf(_maps, map) + 1) % _maps.Length;
        return new(DataGridApplyStatus.Completed);
    }

    private bool TrySession(long number, Guid principal, out SessionState session) =>
        _sessions.TryGetValue(number, out session!)
        && session.Principal == principal
        && !session.Closed
        && session.Expires.UtcTicks > AppliedTime.UtcTicks;

    private MapState? FindMap(Guid id) => Array.Find(_maps, map => map.Configuration.MapId == id);

    private static DataGridApplyResult Failure(DataGridApplyStatus status) => new(status);

    private static int ResultBytes(DataGridApplyResult? result) =>
        result is null ? 0 : 2 + 31 * result.ItemCount;

    private static Entry Probe(DataGridKey key) => new(key, [], default, default);

    private static DataGridKey? Successor(DataGridKey key)
    {
        var bytes = key.ToArray();
        if (bytes.Length < DataGridKey.MaxByteCount)
            return new([.. bytes, 0]);
        for (var i = bytes.Length - 1; i >= 0; i--)
            if (bytes[i] != byte.MaxValue)
            {
                bytes[i]++;
                return new(bytes.AsSpan(0, i + 1));
            }
        return null;
    }

    private readonly record struct Entry(
        DataGridKey Key,
        byte[] Value,
        DataGridEntryVersion Version,
        DataGridExpiration Expiration
    );

    private sealed class MapState(DataGridMapConfiguration configuration)
    {
        internal DataGridMapConfiguration Configuration { get; } = configuration;
        internal SortedSet<Entry> Entries { get; } =
            new(
                Comparer<Entry>.Create(
                    static (x, y) => DataGridKeyComparer.Instance.Compare(x.Key, y.Key)
                )
            );
        internal long Bytes { get; set; }
        internal DataGridKey? Cursor { get; set; }
    }

    private sealed class SessionState(
        long number,
        Guid principal,
        Guid nonce,
        DataGridTimestamp notAfter,
        DataGridTimestamp expires
    )
    {
        internal long Number { get; } = number;
        internal Guid Principal { get; } = principal;
        internal Guid Nonce { get; } = nonce;
        internal DataGridTimestamp NotAfter { get; } = notAfter;
        internal DataGridTimestamp Expires { get; } = expires;
        internal bool Closed { get; set; }
        internal ulong LastSequence { get; set; }
        internal byte[] Digest { get; set; } = [];
        internal DataGridApplyResult? Result { get; set; }
    }
}
