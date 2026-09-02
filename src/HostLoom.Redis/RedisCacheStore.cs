using HostLoom.Caching;
using HostLoom.Redis.Internal;
using StackExchange.Redis;

namespace HostLoom.Redis;

/// <summary>
/// The distributed tier on Redis strings: <c>SET … PX</c>, <c>GET</c> with <c>PTTL</c>
/// pipelined, <c>MGET</c>, <c>UNLINK</c>, and <c>SET … NX PX</c> for set-if-absent. Tags are sets
/// under the tag-index keys the cache hands in, expiring no sooner than their longest member.
/// Every failure surfaces as <see cref="CacheStoreException"/> with a backend-neutral kind.
/// </summary>
public sealed class RedisCacheStore
    : IDistributedCacheStore,
        ICacheStoreHealthProbe,
        IAsyncDisposable
{
    private const int RemoveBatchSize = 500;
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
            var result = await db.StringGetWithExpiryAsync(Key(key))
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (result.Value.IsNull)
            {
                return null;
            }

            return new CacheStoreEntry((byte[])result.Value!, result.Expiry);
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
            if (tagKeys is not { Count: > 0 })
            {
                await db.StringSetAsync(redisKey, payload, timeToLive)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            // One round trip: the value, then for each tag the membership and an expiry that is
            // set when the index is new (NX) and only ever extended afterwards (GT).
            var batch = db.CreateBatch();
            var pending = new List<Task>(1 + (tagKeys.Count * 3))
            {
                batch.StringSetAsync(redisKey, payload, timeToLive),
            };
            foreach (var tagKey in tagKeys)
            {
                var tag = Key(tagKey);
                pending.Add(batch.SetAddAsync(tag, (byte[])redisKey!));
                pending.Add(batch.KeyExpireAsync(tag, timeToLive, ExpireWhen.HasNoExpiry));
                pending.Add(
                    batch.KeyExpireAsync(tag, timeToLive, ExpireWhen.GreaterThanCurrentExpiry)
                );
            }

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
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            return await db.StringSetAsync(Key(key), payload, timeToLive, When.NotExists)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
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
            var values = await db.StringGetAsync(Keys(ordered))
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            // Second round trip: the remaining time to live of every key that was found.
            var batch = db.CreateBatch();
            var ttls = new Task<TimeSpan?>?[ordered.Length];
            for (var i = 0; i < ordered.Length; i++)
            {
                if (!values[i].IsNull)
                {
                    ttls[i] = batch.KeyTimeToLiveAsync(Key(ordered[i]));
                }
            }

            batch.Execute();
            for (var i = 0; i < ordered.Length; i++)
            {
                if (ttls[i] is { } ttl)
                {
                    var remaining = await ttl.WaitAsync(cancellationToken).ConfigureAwait(false);
                    found[ordered[i]] = new CacheStoreEntry((byte[])values[i]!, remaining);
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
                pending.Add(batch.StringSetAsync(Key(key), payload, timeToLive));
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
                var chunk = new RedisKey[count];
                for (var i = 0; i < count; i++)
                {
                    chunk[i] = (byte[])members[offset + i]!;
                }

                await db.KeyDeleteAsync(chunk).WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            await db.KeyDeleteAsync(tag).WaitAsync(cancellationToken).ConfigureAwait(false);
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

    private RedisKey Key(string key) => RedisKeys.ToRedisKey(key, _hashTags);

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
