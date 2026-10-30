using HostLoom.RateLimiting;
using HostLoom.Redis.Internal;
using StackExchange.Redis;

namespace HostLoom.Redis;

/// <summary>Atomic Redis token buckets using server time and one expiring key per limiter.</summary>
public sealed class RedisRateLimitStore
    : IRateLimitStore,
        IRateLimitStoreHealthProbe,
        IAsyncDisposable
{
    private const string AcquireScript = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill = tonumber(ARGV[2])
        local period = tonumber(ARGV[3])
        local permits = tonumber(ARGV[4])
        local time = redis.call('TIME')
        local now = time[1] * 1000 + math.floor(time[2] / 1000)
        local tokens = capacity
        local last = now
        local raw = redis.call('GET', key)
        if raw then
            local separator = string.find(raw, ':', 1, true)
            tokens = tonumber(string.sub(raw, 1, separator - 1))
            last = tonumber(string.sub(raw, separator + 1))
            if last > now then last = now end
        end
        local elapsedPeriods = math.floor((now - last) / period)
        if elapsedPeriods > 0 then
            local periodsToFull = math.ceil((capacity - tokens) / refill)
            if elapsedPeriods >= periodsToFull then
                tokens = capacity
            else
                tokens = tokens + elapsedPeriods * refill
            end
            last = last + elapsedPeriods * period
        end
        local acquired = 0
        local retry = 0
        if tokens >= permits then
            tokens = tokens - permits
            acquired = 1
        else
            local periodsNeeded = math.ceil((permits - tokens) / refill)
            local nextPeriod = period - (now - last)
            if nextPeriod < 1 then nextPeriod = 1 end
            retry = (periodsNeeded - 1) * period + nextPeriod
        end
        local periodsUntilFull = math.ceil((capacity - tokens) / refill)
        local ttl = period + periodsUntilFull * period
        redis.call('SET', key, tostring(tokens) .. ':' .. tostring(last), 'PX', ttl)
        return { acquired, retry, tokens }
        """;

    private readonly RedisConnection _connection;
    private readonly bool _hashTags;
    private readonly bool _ownsConnection;

    /// <summary>Creates the store over the profile's shared connection.</summary>
    public RedisRateLimitStore(RedisConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
        _hashTags = connection.Options.UseHashTags;
    }

    /// <summary>Creates the store over an externally owned multiplexer.</summary>
#pragma warning disable CA2000
    public RedisRateLimitStore(IConnectionMultiplexer multiplexer, RedisOptions? options = null)
        : this(new RedisConnection(multiplexer, options)) => _ownsConnection = true;
#pragma warning restore CA2000

    /// <summary>Releases only the wrapper created by the multiplexer constructor.</summary>
    public ValueTask DisposeAsync() =>
        _ownsConnection ? _connection.DisposeAsync() : ValueTask.CompletedTask;

    /// <inheritdoc />
    public async ValueTask<RateLimitDecision> AcquireAsync(
        string key,
        TokenBucketRateLimitPolicy policy,
        int permitCount,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(policy);
        policy.ThrowIfInvalid();
        ArgumentOutOfRangeException.ThrowIfNegative(permitCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(permitCount, policy.TokenLimit);
        try
        {
            var db = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
            var result = await db.ScriptEvaluateAsync(
                    AcquireScript,
                    [Key(key)],
                    [
                        policy.TokenLimit,
                        policy.TokensPerPeriod,
                        RedisFailures.Milliseconds(policy.ReplenishmentPeriod),
                        permitCount,
                    ]
                )
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            var parts = (RedisResult[])result!;
            return new RateLimitDecision(
                (long)parts[0] == 1,
                (int)(long)parts[2],
                TimeSpan.FromMilliseconds((long)parts[1])
            );
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw RedisFailures.ToRateLimitStoreException(exception, "rate-limit script");
        }
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitStoreHealth> CheckHealthAsync(
        CancellationToken cancellationToken
    )
    {
        var health = await RedisHealthProbe
            .CheckAsync(_connection, cancellationToken)
            .ConfigureAwait(false);
        return health.Reachable
            ? RateLimitStoreHealth.Healthy(health.Description)
            : RateLimitStoreHealth.Unhealthy(health.Description);
    }

    private RedisKey Key(string key) => RedisKeys.ToRedisKey(key, _hashTags);
}
