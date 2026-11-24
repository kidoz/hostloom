using System.Diagnostics.CodeAnalysis;
using HostLoom.Caching;
using HostLoom.DataGrid.Client;

namespace HostLoom.DataGrid.Caching;

/// <summary>An experimental L2 cache store bound to one configured DataGrid map.</summary>
/// <remarks>The caller owns the client. Compose with the local cache tier disabled.</remarks>
[SuppressMessage(
    "Design",
    "CA1001:Types that own disposable fields should be disposable",
    Justification = "The semaphore is used only asynchronously and never creates a wait handle."
)]
public sealed class DataGridCacheStore : IDistributedCacheStore
{
    // At most 15 maximum-size values plus their wire metadata fit in one read reply.
    /// <summary>The maximum number of keys in one bulk read.</summary>
    public const int MaxReadKeys = 15;
    private const int MaxWaitingAndActiveMutations = 8;
    private static readonly TimeSpan SessionOpenWindow = TimeSpan.FromSeconds(4);

    private readonly DataGridClient _client;
    private readonly Guid _mapId;
    private readonly TimeProvider _time;
    private readonly SemaphoreSlim _mutation = new(1, 1);
    private int _mutations;
    private DataGridClientRequest? _opening;
    private long _session;
    private DataGridTimestamp _expiresAt;
    private ulong _sequence;

    public DataGridCacheStore(DataGridClient client, Guid mapId, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        if (mapId == Guid.Empty)
            throw new ArgumentException("A configured map identity is required.", nameof(mapId));
        _client = client;
        _mapId = mapId;
        _time = timeProvider ?? TimeProvider.System;
    }

    public CacheStoreCapabilities Capabilities => CacheStoreCapabilities.None;

    public async ValueTask<CacheStoreEntry?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        var result = await ReadAsync([DataGridKey.FromUtf8(key)], cancellationToken)
            .ConfigureAwait(false);
        return ToCacheEntry(result.GetItem(0));
    }

    public async ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        var unique = DistinctKeys(keys, MaxReadKeys, cancellationToken);
        var found = new Dictionary<string, CacheStoreEntry>(unique.Count, StringComparer.Ordinal);
        if (unique.Count == 0)
            return found;
        var result = await ReadAsync(
                unique.Select(DataGridKey.FromUtf8).ToArray(),
                cancellationToken
            )
            .ConfigureAwait(false);
        for (var index = 0; index < unique.Count; index++)
            if (ToCacheEntry(result.GetItem(index)) is { } entry)
                found.Add(unique[index], entry);
        return found;
    }

    public async ValueTask SetAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateTags(tagKeys, cancellationToken);
        if (timeToLive > TimeSpan.Zero)
            await SetManyAsync([new(key, payload)], timeToLive, cancellationToken)
                .ConfigureAwait(false);
    }

    public async ValueTask<bool> SetIfAbsentAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    )
    {
        ValidateTags(tagKeys, cancellationToken);
        if (timeToLive <= TimeSpan.Zero)
            return false;
        var result = await MutateAsync(
                DataGridMutationKind.PutIfAbsent,
                [new(DataGridKey.FromUtf8(key), payload.Span)],
                timeToLive,
                cancellationToken
            )
            .ConfigureAwait(false);
        return result.GetItem(0).Status switch
        {
            DataGridMutationStatus.Stored => true,
            DataGridMutationStatus.AlreadyExists => false,
            var status => throw RejectedItem(status, 0),
        };
    }

    public async ValueTask SetManyAsync(
        IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0 || timeToLive <= TimeSpan.Zero)
            return;
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            entries.Count,
            DataGridMutationCommand.MaxItemCount
        );
        var unique = new Dictionary<string, ReadOnlyMemory<byte>>(
            entries.Count,
            StringComparer.Ordinal
        );
        foreach (var (key, payload) in entries)
            unique[key] = payload;
        var items = new List<DataGridMutationItem>(unique.Count);
        var bytes = DataGridMutationCommand.HeaderByteCount;
        foreach (var (key, payload) in unique)
        {
            var gridKey = DataGridKey.FromUtf8(key);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(
                payload.Length,
                DataGridMutationItem.MaxValueByteCount
            );
            bytes += sizeof(ushort) + gridKey.ByteCount + sizeof(uint) + payload.Length;
            if (bytes > DataGridMutationCommand.MaxEncodedByteCount)
                throw new ArgumentException(
                    "The batch exceeds one DataGrid request.",
                    nameof(entries)
                );
            items.Add(new(gridKey, payload.Span));
        }
        var result = await MutateAsync(
                DataGridMutationKind.Put,
                items,
                timeToLive,
                cancellationToken
            )
            .ConfigureAwait(false);
        for (var index = 0; index < result.ItemCount; index++)
            if (result.GetItem(index).Status != DataGridMutationStatus.Stored)
                throw RejectedItem(result.GetItem(index).Status, index);
    }

    public async ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        var unique = DistinctKeys(keys, DataGridMutationCommand.MaxItemCount, cancellationToken);
        if (unique.Count == 0)
            return;
        var items = unique
            .Select(key => new DataGridMutationItem(DataGridKey.FromUtf8(key), []))
            .ToArray();
        var result = await MutateAsync(
                DataGridMutationKind.Remove,
                items,
                TimeSpan.Zero,
                cancellationToken
            )
            .ConfigureAwait(false);
        for (var index = 0; index < result.ItemCount; index++)
            if (
                result.GetItem(index).Status
                is not (DataGridMutationStatus.Removed or DataGridMutationStatus.Missing)
            )
                throw RejectedItem(result.GetItem(index).Status, index);
    }

    public ValueTask RemoveByTagAsync(string tagKey, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        throw new NotSupportedException("The DataGrid cache store does not support tags.");
    }

    private async ValueTask<DataGridReadResult> ReadAsync(
        IReadOnlyList<DataGridKey> keys,
        CancellationToken token
    )
    {
        var outcome = await _client
            .ExecuteAsync(DataGridClientRequest.Read(_mapId, keys), token)
            .ConfigureAwait(false);
        CheckReply(outcome, token);
        return outcome.Reply.Read!;
    }

    private async ValueTask<DataGridApplyResult> MutateAsync(
        DataGridMutationKind kind,
        IReadOnlyList<DataGridMutationItem> items,
        TimeSpan ttl,
        CancellationToken token
    )
    {
        token.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfGreaterThan(ttl.Ticks, DateTimeOffset.MaxValue.Ticks);
        if (Interlocked.Increment(ref _mutations) > MaxWaitingAndActiveMutations)
        {
            Interlocked.Decrement(ref _mutations);
            throw new CacheStoreException(
                CacheFailureKind.Unavailable,
                "The DataGrid cache write queue is full."
            );
        }
        try
        {
            await _mutation.WaitAsync(token).ConfigureAwait(false);
            try
            {
                await EnsureSessionAsync(token).ConfigureAwait(false);
                var request = DataGridClientRequest.Mutate(
                    new(
                        kind,
                        _mapId,
                        _session,
                        _sequence + 1,
                        kind == DataGridMutationKind.Remove
                            ? DataGridExpiryKind.Never
                            : DataGridExpiryKind.Relative,
                        ttl.Ticks,
                        items
                    )
                );
                var outcome = await _client.ExecuteAsync(request, token).ConfigureAwait(false);
                if (outcome.Reply.Failure is { } failure)
                {
                    // Advancing a sequence after an uncertain result could retire the only retained reply.
                    if (
                        outcome.MayHaveBeenAdmitted
                        || failure.Code == DataGridClientFailureCode.SessionExpired
                    )
                        _session = 0;
                    CheckReply(outcome, token);
                }
                else
                {
                    _sequence++;
                    if (_sequence == ulong.MaxValue)
                        _session = 0;
                }
                return outcome.Reply.Result!;
            }
            finally
            {
                _mutation.Release();
            }
        }
        finally
        {
            Interlocked.Decrement(ref _mutations);
        }
    }

    private async ValueTask EnsureSessionAsync(CancellationToken token)
    {
        var now = _time.GetUtcNow();
        if (_session != 0 && now.UtcTicks < _expiresAt.UtcTicks)
            return;
        _session = 0;
        if (_opening is null || _opening.OpenNotAfter.UtcTicks <= now.UtcTicks)
            _opening = DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                DataGridTimestamp.FromUtc(now.Add(SessionOpenWindow))
            );
        var outcome = await _client.ExecuteAsync(_opening, token).ConfigureAwait(false);
        if (!outcome.MayHaveBeenAdmitted || outcome.Reply.Failure is null)
            _opening = null;
        CheckReply(outcome, token);
        var result = outcome.Reply.Result!;
        _session = result.SessionNumber;
        _expiresAt = result.SessionExpiresAt!.Value;
        _sequence = 0;
    }

    private static CacheStoreEntry? ToCacheEntry(DataGridLocalEntry? entry) =>
        entry is null ? null : new(entry.GetValue(), entry.RemainingTimeToLive);

    private static List<string> DistinctKeys(
        IReadOnlyCollection<string> keys,
        int limit,
        CancellationToken token
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        token.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfGreaterThan(keys.Count, limit);
        return keys.Distinct(StringComparer.Ordinal).ToList();
    }

    private static void ValidateTags(IReadOnlyCollection<string>? tagKeys, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if (tagKeys is { Count: > 0 })
            throw new NotSupportedException("The DataGrid cache store does not support tags.");
    }

    private static void CheckReply(DataGridClientOutcome outcome, CancellationToken token)
    {
        if (outcome.Reply.Failure is not { } failure)
            return;
        if (outcome.MayHaveBeenAdmitted && token.IsCancellationRequested)
            throw new OperationCanceledException(
                "The DataGrid request may have been admitted.",
                token
            );
        token.ThrowIfCancellationRequested();
        var kind = failure.Code
            is DataGridClientFailureCode.QuorumUnavailable
                or DataGridClientFailureCode.Redirect
                or DataGridClientFailureCode.OutcomeUnknown
            ? CacheFailureKind.Unavailable
            : CacheFailureKind.Other;
        var admission = outcome.MayHaveBeenAdmitted ? " The operation may have been admitted." : "";
        throw new CacheStoreException(
            kind,
            $"DataGrid cache request failed ({failure.Code}).{admission}"
        );
    }

    private static CacheStoreException RejectedItem(DataGridMutationStatus status, int index) =>
        new(
            CacheFailureKind.Other,
            $"DataGrid cache mutation item {index} was rejected ({status})."
        );
}
