using System.Threading.RateLimiting;
using HostLoom.RateLimiting;
using Xunit;

namespace HostLoom.Conformance;

/// <summary>Creates independent limiter instances over one shared backend and clock.</summary>
public sealed class RateLimitConformanceFixture
{
    /// <summary>Creates a limiter with a consumer key and policy.</summary>
    public required Func<
        string,
        TokenBucketRateLimitPolicy,
        DistributedRateLimiter
    > CreateLimiter { get; init; }

    /// <summary>The clock every in-process component uses, or wall time for remote backends.</summary>
    public required ConformanceClock Clock { get; init; }
}

/// <summary>Backend-neutral token-bucket scenarios.</summary>
public static class RateLimitConformance
{
    /// <summary>Every scenario by name for unit and integration theory data.</summary>
    public static IReadOnlyDictionary<
        string,
        Func<RateLimitConformanceFixture, Task>
    > Scenarios { get; } =
        new Dictionary<string, Func<RateLimitConformanceFixture, Task>>(StringComparer.Ordinal)
        {
            [nameof(Capacity_WeightedPermitsAndCancellation)] =
                Capacity_WeightedPermitsAndCancellation,
            [nameof(Refill_ClampsAtCapacity)] = Refill_ClampsAtCapacity,
            [nameof(Instances_ShareQuotaAndKeysStayIsolated)] =
                Instances_ShareQuotaAndKeysStayIsolated,
            [nameof(IdleBucket_StartsFullAgain)] = IdleBucket_StartsFullAgain,
            [nameof(ConcurrentWeightedAcquisitions_DoNotExceedCapacity)] =
                ConcurrentWeightedAcquisitions_DoNotExceedCapacity,
            [nameof(LeaseDisposal_DoesNotRefundPermits)] = LeaseDisposal_DoesNotRefundPermits,
        };

    public static async Task Capacity_WeightedPermitsAndCancellation(
        RateLimitConformanceFixture fixture
    )
    {
        var limiter = fixture.CreateLimiter(
            "capacity",
            new TokenBucketRateLimitPolicy
            {
                TokenLimit = 3,
                TokensPerPeriod = 1,
                ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            }
        );
        using var acquired = await limiter.AcquireAsync(2);
        Assert.True(acquired.IsAcquired);
        Assert.True(acquired.TryGetMetadata(RateLimitMetadata.RemainingPermits, out int remaining));
        Assert.Equal(1, remaining);

        using var rejected = await limiter.AcquireAsync(2);
        Assert.False(rejected.IsAcquired);
        Assert.True(rejected.TryGetMetadata(MetadataName.RetryAfter, out TimeSpan retryAfter));
        Assert.True(retryAfter > TimeSpan.Zero);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await limiter.AcquireAsync(4)
        );

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await limiter.AcquireAsync(1, cancellation.Token)
        );
        using var probe = await limiter.AcquireAsync(0);
        Assert.True(probe.IsAcquired);
        Assert.True(probe.TryGetMetadata(RateLimitMetadata.RemainingPermits, out int afterCancel));
        Assert.Equal(1, afterCancel);
    }

    public static async Task Refill_ClampsAtCapacity(RateLimitConformanceFixture fixture)
    {
        var limiter = fixture.CreateLimiter(
            "refill",
            new TokenBucketRateLimitPolicy
            {
                TokenLimit = 2,
                TokensPerPeriod = 1,
                ReplenishmentPeriod = TimeSpan.FromMilliseconds(500),
            }
        );
        using var initial = await limiter.AcquireAsync(2);
        Assert.True(initial.IsAcquired);
        await fixture.Clock.AdvanceAsync(TimeSpan.FromMilliseconds(600));
        using var one = await limiter.AcquireAsync();
        Assert.True(one.IsAcquired);
        using var empty = await limiter.AcquireAsync();
        Assert.False(empty.IsAcquired);

        await fixture.Clock.AdvanceAsync(TimeSpan.FromMilliseconds(1_100));
        using var full = await limiter.AcquireAsync(2);
        Assert.True(full.IsAcquired);
        Assert.True(full.TryGetMetadata(RateLimitMetadata.RemainingPermits, out int remaining));
        Assert.Equal(0, remaining);
    }

    public static async Task Instances_ShareQuotaAndKeysStayIsolated(
        RateLimitConformanceFixture fixture
    )
    {
        var policy = new TokenBucketRateLimitPolicy
        {
            TokenLimit = 1,
            TokensPerPeriod = 1,
            ReplenishmentPeriod = TimeSpan.FromMinutes(1),
        };
        var first = fixture.CreateLimiter("shared", policy);
        var second = fixture.CreateLimiter("shared", policy);
        var isolated = fixture.CreateLimiter("isolated", policy);

        using var acquired = await first.AcquireAsync();
        Assert.True(acquired.IsAcquired);
        using var rejected = await second.AcquireAsync();
        Assert.False(rejected.IsAcquired);
        using var other = await isolated.AcquireAsync();
        Assert.True(other.IsAcquired);
    }

    public static async Task IdleBucket_StartsFullAgain(RateLimitConformanceFixture fixture)
    {
        var limiter = fixture.CreateLimiter(
            "idle",
            new TokenBucketRateLimitPolicy
            {
                TokenLimit = 2,
                TokensPerPeriod = 1,
                ReplenishmentPeriod = TimeSpan.FromMilliseconds(50),
            }
        );
        using var initial = await limiter.AcquireAsync(2);
        Assert.True(initial.IsAcquired);
        await fixture.Clock.AdvanceAsync(TimeSpan.FromMilliseconds(200));
        using var replenished = await limiter.AcquireAsync(2);
        Assert.True(replenished.IsAcquired);
    }

    public static async Task ConcurrentWeightedAcquisitions_DoNotExceedCapacity(
        RateLimitConformanceFixture fixture
    )
    {
        var policy = new TokenBucketRateLimitPolicy
        {
            TokenLimit = 100,
            TokensPerPeriod = 100,
            ReplenishmentPeriod = TimeSpan.FromMinutes(1),
        };
        using var first = fixture.CreateLimiter("concurrent", policy);
        using var second = fixture.CreateLimiter("concurrent", policy);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var cancellationToken = timeout.Token;
        var attempts = Enumerable
            .Range(0, 128)
            .Select(index =>
                Task.Run(
                    async () =>
                    {
                        await start.Task.WaitAsync(cancellationToken);
                        var limiter = index % 2 == 0 ? first : second;
                        using var lease = await limiter.AcquireAsync(2, cancellationToken);
                        Assert.True(
                            lease.TryGetMetadata(
                                RateLimitMetadata.RemainingPermits,
                                out int remaining
                            )
                        );
                        Assert.InRange(remaining, 0, 100);
                        return lease.IsAcquired;
                    },
                    cancellationToken
                )
            )
            .ToArray();

        start.SetResult();
        var outcomes = await Task.WhenAll(attempts)
            .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);

        Assert.Equal(50, outcomes.Count(acquired => acquired));
        using var exhausted = await first.AcquireAsync(1, cancellationToken);
        Assert.False(exhausted.IsAcquired);
    }

    public static async Task LeaseDisposal_DoesNotRefundPermits(RateLimitConformanceFixture fixture)
    {
        using var limiter = fixture.CreateLimiter(
            "no-refund",
            new TokenBucketRateLimitPolicy
            {
                TokenLimit = 2,
                ReplenishmentPeriod = TimeSpan.FromMinutes(1),
            }
        );
        using (var acquired = await limiter.AcquireAsync(2))
        {
            Assert.True(acquired.IsAcquired);
        }

        using var rejected = await limiter.AcquireAsync(1);
        Assert.False(rejected.IsAcquired);
        Assert.True(rejected.TryGetMetadata(RateLimitMetadata.RemainingPermits, out int remaining));
        Assert.Equal(0, remaining);
    }
}
