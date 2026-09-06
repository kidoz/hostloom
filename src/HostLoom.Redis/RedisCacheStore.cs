using HostLoom.Caching;
using HostLoom.Redis.Internal;
using StackExchange.Redis;

namespace HostLoom.Redis;

/// <summary>
/// The distributed tier on Redis strings: <c>SET … PX</c>, atomic scripted value/TTL reads,
/// bulk reads, deletion, and <c>SET … NX PX</c> for set-if-absent. Tags are sets
/// under the tag-index keys the cache hands in, expiring no sooner than their longest member.
/// Every failure surfaces as <see cref="CacheStoreException"/> with a backend-neutral kind.
/// </summary>
/// <remarks>
/// A tag set gains members and loses them only when the whole index is removed, so an entry
/// rewritten under different tags stays in its earlier sets and removing one of those tags removes
/// it too. Over-removal costs a refill, which is why it is not worth a read before every write.
/// </remarks>
public sealed class RedisCacheStore
    : IDistributedCacheStore,
        ICacheStoreHealthProbe,
        IAsyncDisposable
{
    private const int RemoveBatchSize = 500;
    private const string ReadScript = """
        local value = redis.call('GET', KEYS[1])
        if not value then return false end
        return {value, redis.call('PTTL', KEYS[1])}
        """;

    private const string RemoveTagMembersScript = """
        for i = 2, #KEYS do
            redis.call('UNLINK', KEYS[i])
            redis.call('SREM', KEYS[1], KEYS[i])
        end
        return #KEYS - 1
        """;
    private readonly RedisConnection _connection;
    private readonly bool _hashTags;
    private readonly bool _ownsConnection;

    /// <summary>Creates the store over the process's shared connection.</summary>
    public RedisCacheStore(RedisConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
        _hashTags = connection.Options.UseHashTags;
    }

    /// <summary>Creates the store over an externally owned multiplexer.</summary>
    /// <remarks>
    /// CA2000 is suppressed because the wrapper connection is stored in a field and released by
    /// <see cref="DisposeAsync"/>; it owns no unmanaged resource of its own.
    /// </remarks>
#pragma warning disable CA2000
    public RedisCacheStore(IConnectionMultiplexer multiplexer, RedisOptions? options = null)
        : this(new RedisConnection(multiplexer, options)) => _ownsConnection = true;
#pragma warning restore CA2000

    /// <summary>Releases the wrapper created by the multiplexer constructor; a shared connection is left alone.</summary>
    public ValueTask DisposeAsync() =>
        _ownsConnection ? _connection.DisposeAsync() : ValueTask.CompletedTask;

    /// <inheritdoc />
    public CacheStoreCapabilities Capabilities =>
        CacheStoreCapabilities.Tags | CacheStoreCapabilities.InvalidationChannel;

    /// <inheritdoc />
    public async ValueTask<CacheStoreEntry?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var result = await db.ScriptEvaluateAsync(ReadScript, [Key(key)])
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return Decode(result);
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToCacheStoreException(exception, "GET");
        }
    }

    /// <inheritdoc />
    public async ValueTask SetAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var redisKey = Key(key);
            // WaitAsync cancels only our wait. The SDK may still have this command queued,
            // so its payload must survive the caller returning a borrowed buffer to its pool.
            var ownedPayload = payload.ToArray();
            if (tagKeys is not { Count: > 0 })
            {
                await db.StringSetAsync(redisKey, ownedPayload, timeToLive)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            // One round trip: the value, then for each tag the membership and an expiry that is
            // set when the index is new (NX) and only ever extended afterwards (GT).
            var batch = db.CreateBatch();
            var pending = new List<Task>(1 + (tagKeys.Count * 3))
            {
                batch.StringSetAsync(redisKey, ownedPayload, timeToLive),
            };
            AppendTagIndexes(batch, pending, redisKey, tagKeys, timeToLive);
            batch.Execute();
            await Task.WhenAll(pending).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToCacheStoreException(exception, "SET");
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> SetIfAbsentAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var redisKey = Key(key);
            var written = await db.StringSetAsync(
                    redisKey,
                    payload.ToArray(),
                    timeToLive,
                    When.NotExists
                )
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (!written || tagKeys is not { Count: > 0 })
            {
                return written;
            }

            // A second round trip on purpose: the memberships must not appear when the atomic
            // write lost the race and the value belongs to another writer.
            var batch = db.CreateBatch();
            var pending = new List<Task>(tagKeys.Count * 3);
            AppendTagIndexes(batch, pending, redisKey, tagKeys, timeToLive);
            batch.Execute();
            await Task.WhenAll(pending).WaitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToCacheStoreException(exception, "SET NX");
        }
    }

    /// <inheritdoc />
    public async ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count == 0)
        {
            return;
        }

        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            await db.KeyDeleteAsync(Keys(keys)).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToCacheStoreException(exception, "UNLINK");
        }
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        var found = new Dictionary<string, CacheStoreEntry>(StringComparer.Ordinal);
        if (keys.Count == 0)
        {
            return found;
        }

        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var ordered = keys.ToArray();
            // Each script sees the value and its TTL atomically. Separate MGET/PTTL commands
            // could pair an old value with a replacement's TTL, or turn expiry into unknown TTL.
            var batch = db.CreateBatch();
            var pending = new Task<RedisResult>[ordered.Length];
            for (var i = 0; i < ordered.Length; i++)
            {
                pending[i] = batch.ScriptEvaluateAsync(ReadScript, [Key(ordered[i])]);
            }

            batch.Execute();
            var results = await Task.WhenAll(pending)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            for (var i = 0; i < ordered.Length; i++)
            {
                if (Decode(results[i]) is { } entry)
                {
                    found[ordered[i]] = entry;
                }
            }

            return found;
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToCacheStoreException(exception, "MGET");
        }
    }

    /// <inheritdoc />
    public async ValueTask SetManyAsync(
        IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return;
        }

        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var batch = db.CreateBatch();
            var pending = new List<Task>(entries.Count);
            foreach (var (key, payload) in entries)
            {
                pending.Add(batch.StringSetAsync(Key(key), payload.ToArray(), timeToLive));
            }

            batch.Execute();
            await Task.WhenAll(pending).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToCacheStoreException(exception, "SET (batch)");
        }
    }

    /// <inheritdoc />
    public async ValueTask RemoveByTagAsync(
        string tagKey,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(tagKey);
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var tag = Key(tagKey);
            var members = await db.SetMembersAsync(tag)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            for (var offset = 0; offset < members.Length; offset += RemoveBatchSize)
            {
                var count = Math.Min(RemoveBatchSize, members.Length - offset);
                var chunk = new RedisKey[count + 1];
                chunk[0] = tag;
                for (var i = 0; i < count; i++)
                {
                    chunk[i + 1] = (byte[])members[offset + i]!;
                }

                // Remove only the memberships in this snapshot, atomically with their values.
                // SREM removes an empty index; a concurrent writer's new members survive.
                await db.ScriptEvaluateAsync(RemoveTagMembersScript, chunk)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToCacheStoreException(exception, "SMEMBERS/UNLINK");
        }
    }

    /// <inheritdoc />
    public async ValueTask<CacheStoreHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        var timeout = _connection.Options.HealthTimeout;
        try
        {
            using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            bounded.CancelAfter(timeout);
            var db = await _connection.GetDatabaseAsync(bounded.Token).ConfigureAwait(false);
            var latency = await db.PingAsync().WaitAsync(bounded.Token).ConfigureAwait(false);
            return CacheStoreHealth.Healthy(
                $"Redis answered PING in {latency.TotalMilliseconds:F1} ms ({_connection.Describe()})."
            );
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            return CacheStoreHealth.Unhealthy(
                $"Redis did not answer PING within {timeout} ({_connection.Describe()}): {exception.GetType().Name}."
            );
        }
    }

    private void AppendTagIndexes(
        IBatch batch,
        List<Task> pending,
        RedisKey redisKey,
        IReadOnlyCollection<string> tagKeys,
        TimeSpan timeToLive
    )
    {
        foreach (var tagKey in tagKeys)
        {
            var tag = Key(tagKey);
            pending.Add(batch.SetAddAsync(tag, (byte[])redisKey!));
            pending.Add(batch.KeyExpireAsync(tag, timeToLive, ExpireWhen.HasNoExpiry));
            pending.Add(batch.KeyExpireAsync(tag, timeToLive, ExpireWhen.GreaterThanCurrentExpiry));
        }
    }

    private RedisKey Key(string key) => RedisKeys.ToRedisKey(key, _hashTags);

    private static CacheStoreEntry? Decode(RedisResult result)
    {
        if (result.IsNull)
        {
            return null;
        }

        var parts = (RedisResult[])result!;
        var milliseconds = (long)parts[1];
        if (milliseconds == -2 || milliseconds == 0)
        {
            return null;
        }

        return new CacheStoreEntry(
            (byte[])parts[0]!,
            milliseconds < 0 ? null : TimeSpan.FromMilliseconds(milliseconds)
        );
    }

    private RedisKey[] Keys(IReadOnlyCollection<string> keys)
    {
        var result = new RedisKey[keys.Count];
        var i = 0;
        foreach (var key in keys)
        {
            result[i++] = Key(key);
        }

        return result;
    }
}
