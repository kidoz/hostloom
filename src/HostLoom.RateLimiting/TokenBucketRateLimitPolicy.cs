namespace HostLoom.RateLimiting;

/// <summary>Capacity and whole-period replenishment for one token bucket.</summary>
public sealed class TokenBucketRateLimitPolicy
{
    /// <summary>Maximum permits the bucket can hold, from 1 through 1,000,000.</summary>
    public int TokenLimit { get; set; } = 1;

    /// <summary>Permits added each replenishment period, from 1 through <see cref="TokenLimit"/>.</summary>
    public int TokensPerPeriod { get; set; } = 1;

    /// <summary>Time between replenishments, greater than zero and at most one day.</summary>
    public TimeSpan ReplenishmentPeriod { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Returns every violation, or an empty list when the policy is usable.</summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (TokenLimit is < 1 or > 1_000_000)
        {
            problems.Add("TokenLimit must be between 1 and 1,000,000.");
        }

        if (TokensPerPeriod < 1)
        {
            problems.Add("TokensPerPeriod must be positive.");
        }
        else if (TokenLimit >= 1 && TokensPerPeriod > TokenLimit)
        {
            problems.Add("TokensPerPeriod must not exceed TokenLimit.");
        }

        if (ReplenishmentPeriod <= TimeSpan.Zero || ReplenishmentPeriod > TimeSpan.FromDays(1))
        {
            problems.Add("ReplenishmentPeriod must be positive and no longer than one day.");
        }

        return problems;
    }

    /// <summary>Throws <see cref="ArgumentException"/> when the policy is invalid.</summary>
    public void ThrowIfInvalid()
    {
        var problems = Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                string.Join(" ", problems),
                nameof(TokenBucketRateLimitPolicy)
            );
        }
    }

    internal long ReplenishmentMilliseconds =>
        (long)Math.Ceiling(ReplenishmentPeriod.TotalMilliseconds);

    internal TokenBucketRateLimitPolicy Snapshot() =>
        new()
        {
            TokenLimit = TokenLimit,
            TokensPerPeriod = TokensPerPeriod,
            ReplenishmentPeriod = ReplenishmentPeriod,
        };
}
