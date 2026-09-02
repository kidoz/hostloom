using HostLoom.Locking;
using HostLoom.Redis.Internal;
using StackExchange.Redis;

namespace HostLoom.Redis;

/// <summary>
/// The lock provider on Redis: acquire is <c>SET key owner NX PX lease</c>; release and extend
/// are Lua compare-and-set scripts, so only the owner can release or extend. StackExchange.Redis
/// sends <c>EVALSHA</c> once a script is known to the server and falls back to <c>EVAL</c>.
/// </summary>
public sealed class RedisLockProvider : ILockProvider, ILockProviderHealthProbe, IAsyncDisposable
{
    private const string ReleaseScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """;

    private const string ExtendScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        else
            return 0
        end
        """;

    private readonly RedisConnection _connection;
    private readonly bool _hashTags;
    private readonly bool _ownsConnection;

    /// <summary>Creates the provider over the process's shared connection.</summary>
    public RedisLockProvider(RedisConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
        _hashTags = connection.Options.UseHashTags;
    }

    /// <summary>Creates the provider over an externally owned multiplexer.</summary>
    /// <remarks>
    /// CA2000 is suppressed because the wrapper connection is stored in a field and released by
    /// <see cref="DisposeAsync"/>; it owns no unmanaged resource of its own.
    /// </remarks>
#pragma warning disable CA2000
    public RedisLockProvider(IConnectionMultiplexer multiplexer, RedisOptions? options = null)
        : this(new RedisConnection(multiplexer, options)) => _ownsConnection = true;
#pragma warning restore CA2000

    /// <summary>Releases the wrapper created by the multiplexer constructor; a shared connection is left alone.</summary>
    public ValueTask DisposeAsync() =>
        _ownsConnection ? _connection.DisposeAsync() : ValueTask.CompletedTask;

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            return await db.StringSetAsync(Key(key), owner, lease, When.NotExists)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToLockProviderException(exception, "SET NX PX");
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var result = await db.ScriptEvaluateAsync(ReleaseScript, [Key(key)], [owner])
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return (long)result == 1;
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToLockProviderException(exception, "release script");
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var result = await db.ScriptEvaluateAsync(
                    ExtendScript,
                    [Key(key)],
                    [owner, (long)lease.TotalMilliseconds]
                )
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return (long)result == 1;
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToLockProviderException(exception, "extend script");
        }
    }

    /// <inheritdoc />
    public async ValueTask<LockProviderHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        var timeout = _connection.Options.HealthTimeout;
        try
        {
            using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            bounded.CancelAfter(timeout);
            var db = await _connection.GetDatabaseAsync(bounded.Token).ConfigureAwait(false);
            var latency = await db.PingAsync().WaitAsync(bounded.Token).ConfigureAwait(false);
            return LockProviderHealth.Healthy(
                $"Redis answered PING in {latency.TotalMilliseconds:F1} ms ({_connection.Describe()})."
            );
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            return LockProviderHealth.Unhealthy(
                $"Redis did not answer PING within {timeout} ({_connection.Describe()}): {exception.GetType().Name}."
            );
        }
    }

    private RedisKey Key(string key) => RedisKeys.ToRedisKey(key, _hashTags);
}
