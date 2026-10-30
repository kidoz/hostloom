namespace HostLoom.RateLimiting.Internal;

internal readonly record struct TokenBucketState(
    int Tokens,
    long LastRefillMilliseconds,
    long ExpiresAtMilliseconds
);

internal static class TokenBucketAlgorithm
{
    public static (TokenBucketState State, RateLimitDecision Decision) Evaluate(
        TokenBucketState? current,
        long nowMilliseconds,
        TokenBucketRateLimitPolicy policy,
        int permitCount
    )
    {
        var period = policy.ReplenishmentMilliseconds;
        var tokens = policy.TokenLimit;
        var lastRefill = nowMilliseconds;
        if (current is { } existing && nowMilliseconds < existing.ExpiresAtMilliseconds)
        {
            tokens = existing.Tokens;
            lastRefill = Math.Min(existing.LastRefillMilliseconds, nowMilliseconds);
        }

        var elapsedPeriods = (nowMilliseconds - lastRefill) / period;
        if (elapsedPeriods > 0)
        {
            var periodsToFull = CeilingDivide(policy.TokenLimit - tokens, policy.TokensPerPeriod);
            tokens =
                elapsedPeriods >= periodsToFull
                    ? policy.TokenLimit
                    : tokens + (int)(elapsedPeriods * policy.TokensPerPeriod);
            lastRefill += elapsedPeriods * period;
        }

        var acquired = tokens >= permitCount;
        if (acquired)
        {
            tokens -= permitCount;
        }

        var retryAfter = TimeSpan.Zero;
        if (!acquired)
        {
            var periodsNeeded = CeilingDivide(permitCount - tokens, policy.TokensPerPeriod);
            var nextPeriod = period - (nowMilliseconds - lastRefill);
            var retryMilliseconds = (periodsNeeded - 1) * period + Math.Max(1, nextPeriod);
            retryAfter = TimeSpan.FromMilliseconds(retryMilliseconds);
        }

        var periodsUntilFull = CeilingDivide(policy.TokenLimit - tokens, policy.TokensPerPeriod);
        var timeToLive = period + periodsUntilFull * period;
        var state = new TokenBucketState(tokens, lastRefill, nowMilliseconds + timeToLive);
        var decision = acquired
            ? RateLimitDecision.Acquired(tokens)
            : RateLimitDecision.Rejected(tokens, retryAfter);
        return (state, decision);
    }

    private static long CeilingDivide(int value, int divisor) =>
        value <= 0 ? 0 : ((long)value + divisor - 1) / divisor;
}
