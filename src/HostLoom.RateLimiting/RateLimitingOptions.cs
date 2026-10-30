namespace HostLoom.RateLimiting;

/// <summary>How acquisition behaves when the store cannot return a decision.</summary>
public enum RateLimitUnavailableBehavior
{
    /// <summary>Reject the operation with unavailable metadata.</summary>
    Deny,

    /// <summary>Allow the operation without a confirmed permit and mark it degraded.</summary>
    Allow,
}

/// <summary>Configuration for one named rate limiter.</summary>
public sealed class RateLimitingOptions
{
    /// <summary>Required. Prefixes every store key as <c>{namespace}:rate:{key}</c>.</summary>
    public string Namespace { get; set; } = "";

    /// <summary>Longest consumer key accepted before the namespace prefix.</summary>
    public int MaxKeyLength { get; set; } = 512;

    /// <summary>Token-bucket behavior shared by every instance using the same key.</summary>
    public TokenBucketRateLimitPolicy TokenBucket { get; set; } = new();

    /// <summary>Whether an unreachable or failed store denies or explicitly allows work.</summary>
    public RateLimitUnavailableBehavior OnUnavailable { get; set; } =
        RateLimitUnavailableBehavior.Deny;

    /// <summary>Returns every violation, or an empty list when the options are usable.</summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (!RateLimitKey.IsValidNamespace(Namespace))
        {
            problems.Add("RateLimiting:Namespace is required and must match [a-z0-9-]+.");
        }

        if (MaxKeyLength <= 0)
        {
            problems.Add("RateLimiting:MaxKeyLength must be positive.");
        }

        if (TokenBucket is null)
        {
            problems.Add("RateLimiting:TokenBucket is required.");
        }
        else
        {
            foreach (var problem in TokenBucket.Validate())
            {
                problems.Add($"RateLimiting:TokenBucket:{problem}");
            }
        }

        if (!Enum.IsDefined(OnUnavailable))
        {
            problems.Add("RateLimiting:OnUnavailable is not a defined behavior.");
        }

        return problems;
    }

    internal void ThrowIfInvalid()
    {
        var problems = Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(string.Join(" ", problems), nameof(RateLimitingOptions));
        }
    }

    internal RateLimitingOptions Snapshot() =>
        new()
        {
            Namespace = Namespace,
            MaxKeyLength = MaxKeyLength,
            TokenBucket = TokenBucket.Snapshot(),
            OnUnavailable = OnUnavailable,
        };
}
