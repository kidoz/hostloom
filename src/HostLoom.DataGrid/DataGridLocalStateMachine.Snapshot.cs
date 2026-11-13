using System.Security.Cryptography;

namespace HostLoom.DataGrid;

public sealed class DataGridLocalSnapshot
{
    private readonly byte[] _content;

    internal DataGridLocalSnapshot(DataGridSnapshotDescriptor descriptor, byte[] content)
    {
        Descriptor = descriptor;
        _content = content;
    }

    public DataGridSnapshotDescriptor Descriptor { get; }

    public byte[] GetContent() => (byte[])_content.Clone();
}

public sealed partial class DataGridLocalStateMachine
{
    /// <summary>Captures local application state only. The supplied term is not an election vote.</summary>
    public DataGridLocalSnapshot SaveSnapshot(Guid snapshotId, long includedTerm)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(LastAppliedIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(includedTerm);
        if (snapshotId == Guid.Empty)
            throw new ArgumentException("A snapshot identity is required.", nameof(snapshotId));
        using var body = new GridWriter((int)DataGridSnapshotDescriptor.MaxSnapshotByteCount - 88);
        body.Raw("HLGB"u8);
        body.Number(1, 2);
        body.Number(4, 2);
        var section = body.BeginSection(1, 1);
        body.Number((ulong)_sessionHighWater, 8);
        body.Number((ulong)_maintenanceMapOrdinal, 2);
        body.EndSection(section);
        section = body.BeginSection(2, _maps.Length);
        foreach (var map in _maps)
        {
            body.Id(map.Configuration.MapId);
            body.Text(map.Configuration.Name);
            body.Key(map.Cursor);
        }
        body.EndSection(section);
        section = body.BeginSection(3, StoredKeyCount);
        foreach (var map in _maps)
        foreach (var entry in map.Entries)
        {
            body.Id(map.Configuration.MapId);
            body.Key(entry.Key);
            WriteVersion(body, entry.Version);
            body.Number(entry.Expiration.Deadline is null ? 0UL : 1UL, 1);
            body.Number((ulong)(entry.Expiration.Deadline?.UtcTicks ?? 0), 8);
            body.Number((ulong)entry.Value.Length, 4);
            body.Raw(entry.Value);
        }
        body.EndSection(section);
        section = body.BeginSection(4, _sessions.Count);
        foreach (var session in _sessions.Values)
        {
            body.Number((ulong)session.Number, 8);
            body.Id(session.Principal);
            body.Id(session.Nonce);
            body.Number((ulong)session.NotAfter.UtcTicks, 8);
            body.Number((ulong)session.Expires.UtcTicks, 8);
            body.Number(session.Closed ? 1UL : 0UL, 1);
            body.Number(session.LastSequence, 8);
            body.Number(session.Result is null ? 0UL : 1UL, 1);
            body.Raw(session.Digest.Length == 0 ? new byte[32] : session.Digest);
            body.Number((ulong)(session.Result?.ItemCount ?? 0), 2);
            if (session.Result is { } result)
                for (var i = 0; i < result.ItemCount; i++)
                {
                    var item = result.GetItem(i);
                    body.Number((ulong)item.Status, 2);
                    body.Number(item.Version.IsEmpty ? 0UL : 1UL, 1);
                    if (item.Version.IsEmpty)
                        body.Raw(new byte[28]);
                    else
                        WriteVersion(body, item.Version);
                }
        }
        body.EndSection(section);
        using var image = new GridWriter((int)DataGridSnapshotDescriptor.MaxSnapshotByteCount);
        image.Raw("HLGS"u8);
        image.Number(1, 2);
        image.Number(0, 2);
        image.Id(_configuration.IncarnationId);
        image.Raw(_configuration.ComputeDigest());
        image.Number((ulong)LastAppliedIndex, 8);
        image.Number((ulong)includedTerm, 8);
        image.Number((ulong)AppliedTime.UtcTicks, 8);
        image.Number((ulong)body.Length, 8);
        image.Raw(body.ToArray());
        var bytes = image.ToArray();
        return new(
            new(snapshotId, LastAppliedIndex, includedTerm, bytes.Length, SHA256.HashData(bytes)),
            bytes
        );
    }

    /// <summary>Validates a complete candidate image before returning a new local owner. Performs no I/O.</summary>
    public static DataGridLocalStateMachine Restore(
        DataGridBootstrapConfiguration configuration,
        DataGridSnapshotDescriptor descriptor,
        ReadOnlySpan<byte> content
    )
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(descriptor);
        if (
            content.Length != descriptor.ByteCount
            || content.Length < 88
            || content.Length > DataGridSnapshotDescriptor.MaxSnapshotByteCount
        )
            throw new InvalidDataException("Invalid snapshot image size.");
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(content, digest);
        if (!CryptographicOperations.FixedTimeEquals(digest, descriptor.GetDigest()))
            throw new InvalidDataException("Snapshot digest mismatch.");
        try
        {
            var reader = new GridReader(content);
            if (
                !reader.Take(4).SequenceEqual("HLGS"u8)
                || reader.Number(2) != 1
                || reader.Number(2) != 0
                || reader.Id() != configuration.IncarnationId
                || !reader.Take(32).SequenceEqual(configuration.ComputeDigest())
            )
                throw new InvalidDataException("Invalid snapshot identity/version/configuration.");
            var index = reader.Signed();
            var term = reader.Signed();
            var time = reader.Time();
            if (index != descriptor.LastIncludedIndex || term != descriptor.LastIncludedTerm)
                throw new InvalidDataException("Snapshot position mismatch.");
            var body = new GridReader(
                reader.Take(reader.Count(8, (int)DataGridSnapshotDescriptor.MaxSnapshotByteCount))
            );
            reader.End();
            if (!body.Take(4).SequenceEqual("HLGB"u8) || body.Number(2) != 1 || body.Number(2) != 4)
                throw new InvalidDataException("Invalid snapshot body.");
            var state = new DataGridLocalStateMachine(configuration)
            {
                LastAppliedIndex = index,
                AppliedTime = time,
            };
            var metadata = body.Section(1, 1, out var count);
            if (count != 1)
                throw new InvalidDataException("Missing snapshot metadata.");
            state._sessionHighWater = metadata.Signed();
            state._maintenanceMapOrdinal = metadata.Count(2, configuration.MapCount - 1);
            metadata.End();
            if (state._sessionHighWater < 0 || state._sessionHighWater > index)
                throw new InvalidDataException("Invalid session high-water mark.");
            var maps = body.Section(2, 16, out count);
            if (count != configuration.MapCount)
                throw new InvalidDataException("Map inventory mismatch.");
            for (var i = 0; i < count; i++)
            {
                if (
                    maps.Id() != state._maps[i].Configuration.MapId
                    || maps.Text(128) != state._maps[i].Configuration.Name
                )
                    throw new InvalidDataException("Map declaration mismatch.");
                state._maps[i].Cursor = maps.Key(allowEmpty: true);
            }
            maps.End();
            var entries = body.Section(3, DataGridMapConfiguration.GroupMaxKeyCount, out count);
            var previousMap = -1;
            DataGridKey? previousKey = null;
            var versions = new SortedSet<(long, int)>();
            for (var i = 0; i < count; i++)
            {
                var id = entries.Id();
                var ordinal = Array.FindIndex(state._maps, map => map.Configuration.MapId == id);
                var key = entries.Key(allowEmpty: false)!;
                var version = ReadVersion(ref entries, configuration.IncarnationId, index);
                if (
                    ordinal < 0
                    || ordinal < previousMap
                    || (
                        ordinal == previousMap
                        && DataGridKeyComparer.Instance.Compare(previousKey, key) >= 0
                    )
                    || !versions.Add((version.LogIndex, version.ItemIndex))
                )
                    throw new InvalidDataException("Invalid snapshot entry order/version.");
                previousMap = ordinal;
                previousKey = key;
                var hasExpiry = entries.Flag();
                var deadline = entries.Time();
                if (!hasExpiry && deadline.UtcTicks != 0)
                    throw new InvalidDataException("Invalid absent deadline.");
                var map = state._maps[ordinal];
                if (hasExpiry && map.Configuration.Policy.Expiration == 1)
                    throw new InvalidDataException("Deadline violates map policy.");
                var length = entries.Count(4, DataGridMutationItem.MaxValueByteCount);
                var value = entries.Take(length);
                var size = key.ByteCount + length;
                if (
                    map.Entries.Count >= map.Configuration.MaxKeyCount
                    || map.Bytes + size > map.Configuration.MaxDataByteCount
                    || state.StoredDataByteCount + size
                        > DataGridMapConfiguration.GroupMaxDataByteCount
                )
                    throw new InvalidDataException("Snapshot exceeds capacity.");
                map.Entries.Add(
                    new(
                        key,
                        value.ToArray(),
                        version,
                        hasExpiry ? DataGridExpiration.At(deadline) : default
                    )
                );
                map.Bytes += size;
            }
            entries.End();
            var sessions = body.Section(4, MaxSessionCount, out count);
            var sessionNonces = new SortedDictionary<Guid, SortedSet<Guid>>();
            long previous = 0;
            for (var i = 0; i < count; i++)
            {
                var number = sessions.Signed();
                var principal = sessions.Id();
                var nonce = sessions.Id();
                var notAfter = sessions.Time();
                var expires = sessions.Time();
                var closed = sessions.Flag();
                var sequence = sessions.Number(8);
                var hasResult = sessions.Flag();
                var fingerprint = sessions.Take(32).ToArray();
                var results = sessions.Count(2, 128);
                var opened = expires.UtcTicks - TimeSpan.FromMinutes(10).Ticks;
                if (!sessionNonces.TryGetValue(principal, out var principalNonces))
                {
                    principalNonces = [];
                    sessionNonces.Add(principal, principalNonces);
                }
                if (
                    number <= previous
                    || number > state._sessionHighWater
                    || sequence > (ulong)(index - number)
                    || opened < 0
                    || opened > time.UtcTicks
                    || notAfter.UtcTicks <= opened
                    || notAfter.UtcTicks - opened > TimeSpan.FromSeconds(5).Ticks
                    || principalNonces.Count >= MaxSessionsPerPrincipal
                    || !principalNonces.Add(nonce)
                )
                    throw new InvalidDataException("Invalid snapshot session.");
                previous = number;
                if (
                    hasResult != (results > 0)
                    || (hasResult && (closed || sequence == 0))
                    || (
                        !hasResult
                        && (
                            fingerprint.Any(part => part != 0)
                            || (closed ? sequence == 0 : sequence != 0)
                        )
                    )
                )
                    throw new InvalidDataException("Invalid retained session result.");
                var vector = new DataGridMutationResult[results];
                long resultIndex = 0;
                for (var j = 0; j < results; j++)
                {
                    var status = (DataGridMutationStatus)sessions.Number(2);
                    var hasVersion = sessions.Flag();
                    if (
                        status
                            is < DataGridMutationStatus.Stored
                                or > DataGridMutationStatus.InvalidRequest
                        || hasVersion != (status == DataGridMutationStatus.Stored)
                    )
                        throw new InvalidDataException("Invalid mutation result status.");
                    DataGridEntryVersion version = default;
                    if (hasVersion)
                    {
                        version = ReadVersion(ref sessions, configuration.IncarnationId, index);
                        if (version.ItemIndex != j)
                            throw new InvalidDataException("Invalid retained item position.");
                        if (resultIndex != 0 && resultIndex != version.LogIndex)
                            throw new InvalidDataException("Mixed retained command positions.");
                        resultIndex = version.LogIndex;
                    }
                    else
                        foreach (var part in sessions.Take(28))
                            if (part != 0)
                                throw new InvalidDataException("Unexpected result version.");
                    vector[j] = new(status, version);
                }
                var sessionState = new SessionState(number, principal, nonce, notAfter, expires)
                {
                    Closed = closed,
                    LastSequence = sequence,
                    Digest = hasResult ? fingerprint : [],
                    Result = hasResult ? new(DataGridApplyStatus.Completed, vector) : null,
                };
                state._resultBytes += ResultBytes(sessionState.Result);
                if (state._resultBytes > MaxResultBytes)
                    throw new InvalidDataException("Retained result capacity exceeded.");
                state._sessions.Add(number, sessionState);
            }
            sessions.End();
            body.End();
            return state;
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid snapshot field.", exception);
        }
    }

    private static void WriteVersion(GridWriter writer, DataGridEntryVersion version)
    {
        Span<byte> bytes = stackalloc byte[28];
        version.WriteTo(bytes);
        writer.Raw(bytes);
    }

    private static DataGridEntryVersion ReadVersion(
        ref GridReader reader,
        Guid incarnation,
        long maximumIndex
    )
    {
        var version = DataGridEntryVersion.ReadFrom(reader.Take(28));
        if (
            version.IncarnationId != incarnation
            || version.LogIndex > maximumIndex
            || version.ItemIndex >= 128
        )
            throw new InvalidDataException("Invalid entry version.");
        return version;
    }
}
