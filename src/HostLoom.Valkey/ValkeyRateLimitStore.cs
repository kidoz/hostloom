using HostLoom.RateLimiting;
using HostLoom.Valkey.Internal;
using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>Standalone Valkey token buckets with atomic server-time decisions and no ambiguous replay.</summary>
public sealed class ValkeyRateLimitStore : IRateLimitStore, IRateLimitStoreHealthProbe
{
    private static readonly ValkeyScript Acquire = new(
        """
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
        """
    );

    private readonly ValkeyConnection _connection;

    /// <summary>Borrows the shared command connection; the caller retains ownership.</summary>
    public ValkeyRateLimitStore(ValkeyConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

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
            var reply = await _connection
                .ScriptAsync(
                    Acquire,
                    [key],
                    [
                        policy.TokenLimit,
                        policy.TokensPerPeriod,
                        ValkeyFailures.Milliseconds(policy.ReplenishmentPeriod),
                        permitCount,
                    ],
                    cancellationToken
                )
                .ConfigureAwait(false);
            var parts = reply.AsArray();
            return new RateLimitDecision(
                parts[0].AsInt64() == 1,
                (int)parts[2].AsInt64(),
                TimeSpan.FromMilliseconds(parts[1].AsInt64())
            );
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.RateLimit(exception, "rate-limit script");
        }
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitStoreHealth> CheckHealthAsync(
        CancellationToken cancellationToken
    )
    {
        try
        {
            await _connection.PingAsync(cancellationToken).ConfigureAwait(false);
            return RateLimitStoreHealth.Healthy("Valkey answered PING.");
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            return RateLimitStoreHealth.Unhealthy(
                $"Valkey PING failed ({exception.GetType().Name})."
            );
        }
    }
}
