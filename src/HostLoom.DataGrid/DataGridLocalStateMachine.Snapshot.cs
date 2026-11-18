using System.Security.Cryptography;
using System.Text;

namespace HostLoom.DataGrid;

public sealed class DataGridLocalSnapshot
{
    private readonly DataGridSnapshotContent _content;
    private readonly object _validationGate = new();
    private DataGridBootstrapConfiguration? _validatedConfiguration;

    // Internal producers transfer exclusive ownership of these bytes. Public readers receive
    // copies; the immutable content and configuration make successful validation reusable.
    internal DataGridLocalSnapshot(DataGridSnapshotDescriptor descriptor, byte[] content)
        : this(descriptor, DataGridSnapshotContent.CopyFrom(content)) { }

    internal DataGridLocalSnapshot(
        DataGridSnapshotDescriptor descriptor,
        DataGridSnapshotContent content
    )
    {
        Descriptor = descriptor;
        _content = content;
    }

    public DataGridSnapshotDescriptor Descriptor { get; }

    public byte[] GetContent() => _content.ToArray();

    // Immutable blocks are retained by each actual reader/transfer owner.
    internal DataGridSnapshotContent Content => _content;

    internal void Validate(DataGridBootstrapConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        lock (_validationGate)
        {
            if (ReferenceEquals(_validatedConfiguration, configuration))
                return;
            DataGridLocalStateMachine.ValidateSnapshot(configuration, this);
            // Cache only success, on this snapshot and one exact immutable configuration.
            // A new snapshot/configuration still receives complete semantic/digest validation.
            _validatedConfiguration = configuration;
        }
    }
}

public sealed partial class DataGridLocalStateMachine
{
    // V1 field widths: fixed envelopes/sections, map names/cursors, entries and retained vectors.
    internal const long MaximumEncodedSnapshotBytes =
        154
        + 16L * (16 + 2 + 128 + 2 + DataGridKey.MaxByteCount)
        + DataGridMapConfiguration.GroupMaxDataByteCount
        + 59L * DataGridMapConfiguration.GroupMaxKeyCount
        + MaxSessionCount * (100L + 31L * DataGridMutationCommand.MaxItemCount);

    /// <summary>Captures local application state only. The supplied term is not an election vote.</summary>
    public DataGridLocalSnapshot SaveSnapshot(Guid snapshotId, long includedTerm)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(LastAppliedIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(includedTerm);
        if (snapshotId == Guid.Empty)
            throw new ArgumentException("A snapshot identity is required.", nameof(snapshotId));
        var layout = GetSnapshotLayout();
        using var image = GridWriter.Segmented(layout.TotalBytes);
        WriteSnapshot(image, includedTerm, layout);
        var bytes = image.DetachSnapshotContent();
        return new(
            new(snapshotId, LastAppliedIndex, includedTerm, bytes.Length, bytes.ComputeHash()),
            bytes
        );
    }

    // The caller still reconstructs the durable prefix independently. Only its additional
    // encoded image is omitted; canonical bytes are compared as the shared encoder emits them.
    internal void VerifyCanonicalSnapshot(DataGridLocalSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        snapshot.Validate(_configuration);
        var layout = GetSnapshotLayout();
        if (
            snapshot.Descriptor.LastIncludedIndex != LastAppliedIndex
            || snapshot.Descriptor.ByteCount != layout.TotalBytes
        )
            throw new InvalidDataException("Snapshot disagrees with the known log prefix.");
        using var comparison = GridWriter.Comparing(snapshot.Content);
        WriteSnapshot(comparison, snapshot.Descriptor.LastIncludedTerm, layout);
        comparison.CompleteComparison();
    }

    private readonly record struct SnapshotLayout(
        int TotalBytes,
        long MapBytes,
        long EntryBytes,
        long SessionBytes
    );

    private SnapshotLayout GetSnapshotLayout()
    {
        // V1 fixed envelope/sections, followed by exact section payload sizes. Both output
        // and comparison emit lengths up front; no placeholder bytes escape comparison.
        long maps = 0,
            sessions = 0;
        foreach (var map in _maps)
            maps +=
                20
                + Encoding.UTF8.GetByteCount(map.Configuration.Name)
                + (map.Cursor?.ByteCount ?? 0);
        foreach (var session in _sessions.Values)
            sessions += 100 + 31L * (session.Result?.ItemCount ?? 0);
        var entries = StoredDataByteCount + 59L * StoredKeyCount;
        return new(checked((int)(154 + maps + entries + sessions)), maps, entries, sessions);
    }

    private static void WriteSnapshotSection(GridWriter writer, int id, int count, long bytes)
    {
        writer.Number((ulong)id, 2);
        writer.Number(0, 2);
        writer.Number((ulong)count, 4);
        writer.Number((ulong)bytes, 4);
    }

    private void WriteSnapshot(GridWriter image, long includedTerm, SnapshotLayout layout)
    {
        image.Raw("HLGS"u8);
        image.Number(1, 2);
        image.Number(0, 2);
        image.Id(_configuration.IncarnationId);
        image.Raw(_configuration.ComputeDigest());
        image.Number((ulong)LastAppliedIndex, 8);
        image.Number((ulong)includedTerm, 8);
        image.Number((ulong)AppliedTime.UtcTicks, 8);
        image.Number((ulong)(layout.TotalBytes - 88), 8);
        var body = image;
        body.Raw("HLGB"u8);
        body.Number(1, 2);
        body.Number(4, 2);
        WriteSnapshotSection(body, 1, 1, 10);
        body.Number((ulong)_sessionHighWater, 8);
        body.Number((ulong)_maintenanceMapOrdinal, 2);
        WriteSnapshotSection(body, 2, _maps.Length, layout.MapBytes);
        foreach (var map in _maps)
        {
            body.Id(map.Configuration.MapId);
            body.Text(map.Configuration.Name);
            body.Key(map.Cursor);
        }
        WriteSnapshotSection(body, 3, StoredKeyCount, layout.EntryBytes);
        foreach (var map in _maps)
        foreach (var entry in map.Entries)
        {
            body.Id(map.Configuration.MapId);
            body.Key(entry.Key);
            WriteVersion(body, entry.Version);
            body.Number(entry.Expiration.Deadline is null ? 0UL : 1UL, 1);
            body.Number((ulong)(entry.Expiration.Deadline?.UtcTicks ?? 0), 8);
            body.Number((ulong)entry.Value.Length, 4);
            body.Raw(entry.Value.Span);
        }
        WriteSnapshotSection(body, 4, _sessions.Count, layout.SessionBytes);
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
    }

    /// <summary>Validates a complete candidate image before returning a new local owner. Performs no I/O.</summary>
    public static DataGridLocalStateMachine Restore(
        DataGridBootstrapConfiguration configuration,
        DataGridSnapshotDescriptor descriptor,
        ReadOnlySpan<byte> content
    ) => ReadSnapshot(configuration, descriptor, content, null, SnapshotReadMode.Restore)!;

    // Ephemeral store verification rebuilds its own keys, indexes, versions and replay state.
    // Only value bytes borrow slices of an already immutable durable snapshot. Public spans
    // still copy, and replacements never mutate the borrowed input or the live application.
    internal static DataGridLocalStateMachine RestoreForVerification(
        DataGridBootstrapConfiguration configuration,
        DataGridLocalSnapshot snapshot
    ) =>
        ReadSnapshot(
            configuration,
            snapshot.Descriptor,
            default,
            snapshot.Content,
            SnapshotReadMode.VerifyPrefix
        )!;

    internal static DataGridLocalStateMachine RestoreSnapshot(
        DataGridBootstrapConfiguration configuration,
        DataGridLocalSnapshot snapshot
    ) =>
        ReadSnapshot(
            configuration,
            snapshot.Descriptor,
            default,
            snapshot.Content,
            SnapshotReadMode.Restore
        )!;

    internal static void ValidateSnapshot(
        DataGridBootstrapConfiguration configuration,
        DataGridLocalSnapshot snapshot
    ) =>
        ReadSnapshot(
            configuration,
            snapshot.Descriptor,
            default,
            snapshot.Content,
            SnapshotReadMode.Validate
        );

    private enum SnapshotReadMode
    {
        Validate,
        Restore,
        VerifyPrefix,
    }

    private static DataGridLocalStateMachine? ReadSnapshot(
        DataGridBootstrapConfiguration configuration,
        DataGridSnapshotDescriptor descriptor,
        ReadOnlySpan<byte> content,
        DataGridSnapshotContent? blocks,
        SnapshotReadMode mode
    )
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(descriptor);
        var contentLength = blocks?.Length ?? content.Length;
        if (
            contentLength != descriptor.ByteCount
            || contentLength < 88
            || contentLength > DataGridSnapshotDescriptor.MaxSnapshotByteCount
        )
            throw new InvalidDataException("Invalid snapshot image size.");
        Span<byte> digest = stackalloc byte[32];
        if (blocks is null)
            SHA256.HashData(content, digest);
        else
            blocks.ComputeHash().CopyTo(digest);
        if (!CryptographicOperations.FixedTimeEquals(digest, descriptor.GetDigest()))
            throw new InvalidDataException("Snapshot digest mismatch.");
        try
        {
            var reader = blocks is null ? new GridReader(content) : new GridReader(blocks);
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
            var body = reader.Subreader(
                reader.Count(8, (int)DataGridSnapshotDescriptor.MaxSnapshotByteCount)
            );
            reader.End();
            if (!body.Take(4).SequenceEqual("HLGB"u8) || body.Number(2) != 1 || body.Number(2) != 4)
                throw new InvalidDataException("Invalid snapshot body.");
            var state =
                mode == SnapshotReadMode.Validate
                    ? null
                    : new DataGridLocalStateMachine(configuration)
                    {
                        LastAppliedIndex = index,
                        AppliedTime = time,
                    };
            var metadata = body.Section(1, 1, out var count);
            if (count != 1)
                throw new InvalidDataException("Missing snapshot metadata.");
            var sessionHighWater = metadata.Signed();
            var maintenanceMapOrdinal = metadata.Count(2, configuration.MapCount - 1);
            metadata.End();
            if (sessionHighWater < 0 || sessionHighWater > index)
                throw new InvalidDataException("Invalid session high-water mark.");
            if (state is not null)
            {
                state._sessionHighWater = sessionHighWater;
                state._maintenanceMapOrdinal = maintenanceMapOrdinal;
            }
            var maps = body.Section(2, 16, out count);
            if (count != configuration.MapCount)
                throw new InvalidDataException("Map inventory mismatch.");
            for (var i = 0; i < count; i++)
            {
                var map = configuration.GetMap(i);
                if (maps.Id() != map.MapId || maps.Text(128) != map.Name)
                    throw new InvalidDataException("Map declaration mismatch.");
                var cursorLength = maps.Count(2, DataGridKey.MaxByteCount);
                if (state is null)
                    maps.Skip(cursorLength);
                else
                    state._maps[i].Cursor = cursorLength == 0 ? null : new(maps.Take(cursorLength));
            }
            maps.End();
            var entries = body.Section(3, DataGridMapConfiguration.GroupMaxKeyCount, out count);
            ReadSnapshotEntries(
                ref entries,
                count,
                configuration,
                index,
                state,
                mode == SnapshotReadMode.VerifyPrefix
            );
            var sessions = body.Section(4, MaxSessionCount, out count);
            var sessionNonces = new SortedDictionary<Guid, SortedSet<Guid>>();
            long previous = 0;
            long resultBytes = 0;
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
                var fingerprint = sessions.Take(32);
                var results = sessions.Count(2, 128);
                var opened = expires.UtcTicks - TimeSpan.FromMinutes(10).Ticks;
                if (!sessionNonces.TryGetValue(principal, out var principalNonces))
                {
                    principalNonces = [];
                    sessionNonces.Add(principal, principalNonces);
                }
                if (
                    number <= previous
                    || number > sessionHighWater
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
                            fingerprint.ContainsAnyExcept((byte)0)
                            || (closed ? sequence == 0 : sequence != 0)
                        )
                    )
                )
                    throw new InvalidDataException("Invalid retained session result.");
                var vector =
                    state is null || !hasResult ? null : new DataGridMutationResult[results];
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
                    if (vector is not null)
                        vector[j] = new(status, version);
                }
                if (hasResult)
                    resultBytes += ResultBytes(results);
                if (resultBytes > MaxResultBytes)
                    throw new InvalidDataException("Retained result capacity exceeded.");
                state?._sessions.Add(
                    number,
                    new SessionState(number, principal, nonce, notAfter, expires)
                    {
                        Closed = closed,
                        LastSequence = sequence,
                        Digest = hasResult ? fingerprint.ToArray() : [],
                        Result = vector is null ? null : new(DataGridApplyStatus.Completed, vector),
                    }
                );
            }
            sessions.End();
            body.End();
            if (state is not null)
                state._resultBytes = resultBytes;
            return state;
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid snapshot field.", exception);
        }
    }

    private static void ReadSnapshotEntries(
        ref GridReader entries,
        int count,
        DataGridBootstrapConfiguration configuration,
        long index,
        DataGridLocalStateMachine? state,
        bool borrowValues
    )
    {
        // Version uniqueness spans all maps. Sorting exact pairs keeps it independent of
        // key order and bounds the index to one compact array under the group entry limit.
        var versions = new (long LogIndex, int ItemIndex)[count];
        var ordinal = 0;
        var map = configuration.GetMap(ordinal);
        ReadOnlySpan<byte> previousKey = default;
        var mapKeys = 0;
        long mapBytes = 0;
        long groupBytes = 0;
        for (var i = 0; i < count; i++)
        {
            var id = entries.Id();
            while (id != map.MapId)
            {
                if (++ordinal == configuration.MapCount)
                    throw new InvalidDataException("Invalid snapshot entry map order.");
                map = configuration.GetMap(ordinal);
                previousKey = default;
                mapKeys = 0;
                mapBytes = 0;
            }

            var keyLength = entries.Count(2, DataGridKey.MaxByteCount);
            if (keyLength == 0)
                throw new InvalidDataException("Empty key.");
            var key = entries.Take(keyLength);
            if (previousKey.SequenceCompareTo(key) >= 0)
                throw new InvalidDataException("Invalid snapshot entry order.");
            previousKey = key;
            var version = ReadVersion(ref entries, configuration.IncarnationId, index);
            versions[i] = (version.LogIndex, version.ItemIndex);

            var hasExpiry = entries.Flag();
            var deadline = entries.Time();
            if (!hasExpiry && deadline.UtcTicks != 0)
                throw new InvalidDataException("Invalid absent deadline.");
            if (hasExpiry && map.Policy.Expiration == 1)
                throw new InvalidDataException("Deadline violates map policy.");
            var length = entries.Count(4, DataGridMutationItem.MaxValueByteCount);
            var size = keyLength + length;
            mapBytes += size;
            groupBytes += size;
            if (
                ++mapKeys > map.MaxKeyCount
                || mapBytes > map.MaxDataByteCount
                || groupBytes > DataGridMapConfiguration.GroupMaxDataByteCount
            )
                throw new InvalidDataException("Snapshot exceeds capacity.");

            if (state is null)
                entries.Skip(length);
            else
            {
                var storedValue = entries.Value(length, borrowValues);
                state
                    ._maps[ordinal]
                    .Entries.Add(
                        new(
                            new DataGridKey(key),
                            storedValue,
                            version,
                            hasExpiry ? DataGridExpiration.At(deadline) : default
                        )
                    );
                state._maps[ordinal].Bytes = mapBytes;
            }
        }
        entries.End();
        Array.Sort(versions);
        for (var i = 1; i < versions.Length; i++)
            if (versions[i] == versions[i - 1])
                throw new InvalidDataException("Duplicate snapshot entry version.");
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
