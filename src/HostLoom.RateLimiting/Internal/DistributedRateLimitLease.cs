using System.Threading.RateLimiting;

namespace HostLoom.RateLimiting.Internal;

internal sealed class DistributedRateLimitLease : RateLimitLease
{
    private readonly Dictionary<string, object?> _metadata = new(StringComparer.Ordinal);

    private DistributedRateLimitLease(bool isAcquired) => IsAcquired = isAcquired;

    public static DistributedRateLimitLease SynchronousFailure { get; } = new(false);

    public override bool IsAcquired { get; }

    public override IEnumerable<string> MetadataNames => _metadata.Keys;

    public static DistributedRateLimitLease Acquired(RateLimitDecision decision, bool degraded)
    {
        var lease = new DistributedRateLimitLease(true);
        lease._metadata[RateLimitMetadata.RemainingPermitsName] = decision.RemainingPermits;
        if (degraded)
        {
            lease._metadata[RateLimitMetadata.DegradedName] = true;
        }

        return lease;
    }

    public static DistributedRateLimitLease Rejected(RateLimitDecision decision)
    {
        var lease = new DistributedRateLimitLease(false);
        lease._metadata[RateLimitMetadata.RemainingPermitsName] = decision.RemainingPermits;
        if (decision.RetryAfter > TimeSpan.Zero)
        {
            lease._metadata[MetadataName.RetryAfter.Name] = decision.RetryAfter;
        }

        return lease;
    }

    public static DistributedRateLimitLease Unavailable()
    {
        var lease = new DistributedRateLimitLease(false);
        lease._metadata[RateLimitMetadata.UnavailableName] = true;
        return lease;
    }

    public override bool TryGetMetadata(string metadataName, out object? metadata) =>
        _metadata.TryGetValue(metadataName, out metadata);
}
