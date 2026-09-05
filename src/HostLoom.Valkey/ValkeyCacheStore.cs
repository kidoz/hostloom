using HostLoom.Caching;
using HostLoom.Valkey.Internal;
using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>
/// Binary standalone Valkey cache store with atomic value/TTL reads, conditional writes and tag indexes.
/// Keys are already prefixed by the kernel and are passed unchanged. The connection is borrowed.
/// </summary>
public sealed class ValkeyCacheStore : IDistributedCacheStore, ICacheStoreHealthProbe
{
    private static readonly ValkeyScript Read = new(
        """
        local value = redis.call('GET', KEYS[1])
        if not value then return false end
        return {value, redis.call('PTTL', KEYS[1])}
        """
    );
    private static readonly ValkeyScript Write = new(
        """
        if ARGV[3] == 'NX' and redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
        redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
        for i = 2, #KEYS do
            redis.call('SADD', KEYS[i], KEYS[1])
            local ttl = redis.call('PTTL', KEYS[i])
            if ttl < tonumber(ARGV[2]) then
                redis.call('PEXPIRE', KEYS[i], ARGV[2])
            end
        end
        return 1
        """
    );
    private readonly ValkeyConnection _connection;

    /// <summary>Borrows a connection shared with locks and invalidation publishing.</summary>
    public ValkeyCacheStore(ValkeyConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

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
            var reply = await _connection
                .ScriptAsync(Read, [key], [], cancellationToken)
                .ConfigureAwait(false);
            return Decode(reply);
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.Cache(exception, "read");
        }
    }

    /// <inheritdoc />
    public async ValueTask SetAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    ) =>
        _ = await WriteAsync(key, payload, timeToLive, tagKeys, false, cancellationToken)
            .ConfigureAwait(false);

    /// <inheritdoc />
    public ValueTask<bool> SetIfAbsentAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    ) => WriteAsync(key, payload, timeToLive, tagKeys, true, cancellationToken);

    private async ValueTask<bool> WriteAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys,
        bool ifAbsent,
        CancellationToken token
    )
    {
        var milliseconds = ValkeyFailures.Milliseconds(timeToLive);
        try
        {
            if (tagKeys is not { Count: > 0 })
            {
                var command = ifAbsent
                    ? new ValkeyCommand("SET", key, payload, "PX", milliseconds, "NX")
                    : new ValkeyCommand("SET", key, payload, "PX", milliseconds);
                var reply = await _connection.ExecuteAsync(command, token).ConfigureAwait(false);
                return !reply.IsNull
                    && string.Equals(reply.AsString(), "OK", StringComparison.Ordinal);
            }
            ValkeyArgument[] keys = [key, .. tagKeys.Select(static tag => (ValkeyArgument)tag)];
            var written = await _connection
                .ScriptAsync(Write, keys, [payload, milliseconds, ifAbsent ? "NX" : ""], token)
                .ConfigureAwait(false);
            return written.AsInt64() == 1;
        }
        catch (Exception exception) when (!ValkeyFailures.IsCallerCancellation(exception, token))
        {
            throw ValkeyFailures.Cache(exception, "write");
        }
    }

    /// <inheritdoc />
    public async ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        cancellationToken.ThrowIfCancellationRequested();
        if (keys.Count == 0)
            return;
        try
        {
            await _connection
                .ExecuteAsync(
                    new ValkeyCommand(
                        "UNLINK",
                        [.. keys.Select(static key => (ValkeyArgument)key)]
                    ),
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.Cache(exception, "remove");
        }
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        cancellationToken.ThrowIfCancellationRequested();
        var found = new Dictionary<string, CacheStoreEntry>(StringComparer.Ordinal);
        if (keys.Count == 0)
            return found;
        try
        {
            var ordered = keys.ToArray();
            var commands = ordered.Select(static key => Read.CreateCommand([key], [])).ToArray();
            var replies = await _connection
                .PipelineAsync(commands, cancellationToken)
                .ConfigureAwait(false);
            for (var i = 0; i < ordered.Length; i++)
            {
                if (Decode(replies[i]) is { } entry)
                    found[ordered[i]] = entry;
            }
            return found;
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.Cache(exception, "bulk read");
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
        cancellationToken.ThrowIfCancellationRequested();
        var milliseconds = ValkeyFailures.Milliseconds(timeToLive);
        if (entries.Count == 0)
            return;
        try
        {
            var commands = entries
                .Select(entry => new ValkeyCommand(
                    "SET",
                    entry.Key,
                    entry.Value,
                    "PX",
                    milliseconds
                ))
                .ToArray();
            await _connection.PipelineAsync(commands, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.Cache(exception, "bulk write");
        }
    }

    /// <inheritdoc />
    public async ValueTask RemoveByTagAsync(
        string tagKey,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var reply = await _connection
                .ExecuteAsync(new ValkeyCommand("SMEMBERS", tagKey), cancellationToken)
                .ConfigureAwait(false);
            var members = reply.AsArray();
            foreach (var chunk in members.Chunk(500))
            {
                await _connection
                    .ExecuteAsync(
                        new ValkeyCommand(
                            "UNLINK",
                            [.. chunk.Select(static member => (ValkeyArgument)member.AsBytes())]
                        ),
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            await _connection
                .ExecuteAsync(new ValkeyCommand("UNLINK", tagKey), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.Cache(exception, "remove tag");
        }
    }

    /// <inheritdoc />
    public async ValueTask<CacheStoreHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _connection.PingAsync(cancellationToken).ConfigureAwait(false);
            return CacheStoreHealth.Healthy("Valkey answered PING.");
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            return CacheStoreHealth.Unhealthy($"Valkey PING failed ({exception.GetType().Name}).");
        }
    }

    private static CacheStoreEntry? Decode(RespValue reply)
    {
        if (reply.IsNull)
            return null;
        var parts = reply.AsArray();
        if (parts.Count != 2)
            throw new ValkeyProtocolException("Unexpected cache read response.");
        var ttl = parts[1].AsInt64();
        if (ttl == -2 || ttl == 0)
            return null;
        return new CacheStoreEntry(
            parts[0].AsBytes(),
            ttl < 0 ? null : TimeSpan.FromMilliseconds(ttl)
        );
    }
}
