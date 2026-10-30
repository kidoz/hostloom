using System.Threading.RateLimiting;
using HostLoom.Conformance;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class RateLimitingTests
{
    [Fact]
    public async Task SynchronousAttempt_IsConservativeBeforeAsyncAcquisition()
    {
        using var limiter = new DistributedRateLimiter(
            Options(),
            "catalog",
            new InMemoryRateLimitStore()
        );

        Assert.False(limiter.AttemptAcquire().IsAcquired);
        using var lease = await limiter.AcquireAsync(
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(lease.IsAcquired);
        using var rejected = await limiter.AcquireAsync(
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.False(rejected.IsAcquired);
    }

    [Theory]
    [InlineData(RateLimitUnavailableBehavior.Deny)]
    [InlineData(RateLimitUnavailableBehavior.Allow)]
    public async Task StoreFailure_UsesExplicitUnavailableBehavior(
        RateLimitUnavailableBehavior behavior
    )
    {
        var store = new FaultingRateLimitStore();
        var options = Options();
        options.OnUnavailable = behavior;
        using var limiter = new DistributedRateLimiter(options, "catalog", store);

        using var lease = await limiter.AcquireAsync(
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(behavior == RateLimitUnavailableBehavior.Allow, lease.IsAcquired);
        Assert.Equal(
            behavior == RateLimitUnavailableBehavior.Deny,
            lease.TryGetMetadata(RateLimitMetadata.Unavailable, out bool unavailable) && unavailable
        );
        Assert.Equal(
            behavior == RateLimitUnavailableBehavior.Allow,
            lease.TryGetMetadata(RateLimitMetadata.Degraded, out bool degraded) && degraded
        );
        Assert.Equal(1, store.Calls);
    }

    [Fact]
    public void OptionsPoliciesAndKeys_AreValidated()
    {
        Assert.Throws<ArgumentException>(() =>
            new DistributedRateLimiter(
                new RateLimitingOptions { Namespace = "Invalid" },
                "catalog",
                new InMemoryRateLimitStore()
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DistributedRateLimiter(Options(), "not valid", new InMemoryRateLimitStore())
        );
        Assert.Throws<ArgumentException>(() =>
            new TokenBucketRateLimitPolicy { TokenLimit = 0 }.ThrowIfInvalid()
        );
        Assert.Throws<ArgumentException>(() =>
            new TokenBucketRateLimitPolicy { TokenLimit = 1, TokensPerPeriod = 2 }.ThrowIfInvalid()
        );
        Assert.Throws<ArgumentException>(() =>
            new TokenBucketRateLimitPolicy
            {
                ReplenishmentPeriod = TimeSpan.FromDays(2),
            }.ThrowIfInvalid()
        );
    }

    [Fact]
    public async Task InMemoryStore_ReclaimsExpiredBuckets()
    {
        var clock = new ManualTimeProvider();
        var store = new InMemoryRateLimitStore(clock);
        var policy = new TokenBucketRateLimitPolicy
        {
            TokenLimit = 1,
            ReplenishmentPeriod = TimeSpan.FromMilliseconds(50),
        };

        _ = await store.AcquireAsync(
            "ns:rate:catalog",
            policy,
            1,
            TestContext.Current.CancellationToken
        );
        Assert.Equal(1, store.Count);
        clock.Advance(TimeSpan.FromSeconds(2));

        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task NamedProfiles_ResolveIndependentLimiters()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomRateLimiting(
                "first",
                options =>
                {
                    options.Namespace = "catalog";
                    options.TokenBucket = new TokenBucketRateLimitPolicy { TokenLimit = 1 };
                }
            )
            .UseInMemory();
        services
            .AddHostLoomRateLimiting(
                "second",
                options =>
                {
                    options.Namespace = "catalog";
                    options.TokenBucket = new TokenBucketRateLimitPolicy { TokenLimit = 2 };
                }
            )
            .UseInMemory();
        await using var container = services.BuildServiceProvider();
        var first = container.GetRequiredKeyedService<RateLimiter>("first");
        var second = container.GetRequiredKeyedService<RateLimiter>("second");

        Assert.True(
            (
                await first.AcquireAsync(cancellationToken: TestContext.Current.CancellationToken)
            ).IsAcquired
        );
        Assert.False(
            (
                await first.AcquireAsync(cancellationToken: TestContext.Current.CancellationToken)
            ).IsAcquired
        );
        Assert.True(
            (await second.AcquireAsync(2, TestContext.Current.CancellationToken)).IsAcquired
        );
        Assert.Same(first, container.GetRequiredKeyedService<RateLimiter>("first"));
    }

    [Fact]
    public async Task StoreSelectionAndHealthNames_AreDeterministic()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddHostLoomRateLimiting(options => options.Namespace = "catalog")
            .UseInMemory()
            .AddHealthChecks();
        Assert.Throws<InvalidOperationException>(() => builder.UseInMemory());
        services
            .AddHostLoomRateLimiting("named", options => options.Namespace = "catalog")
            .UseInMemory()
            .AddHealthChecks();
        await using var container = services.BuildServiceProvider();
        var health = container.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;

        Assert.Contains(
            health.Registrations,
            registration => registration.Name == "hostloom-ratelimit-ready"
        );
        Assert.Contains(
            health.Registrations,
            registration => registration.Name == "hostloom-ratelimit-ready-named"
        );
    }

    [Fact]
    public void SensitiveKeys_DoNotContainTheOriginalValue()
    {
        var hashed = RateLimitKey.FromSensitive("secret-token");
        var keyed = RateLimitKey.FromSensitive("pin", "shared-key"u8);

        Assert.NotEqual("secret-token", hashed);
        Assert.Equal(32, hashed.Length);
        Assert.Equal(32, keyed.Length);
        Assert.Equal(keyed, RateLimitKey.FromSensitive("pin", "shared-key"u8));
        Assert.NotEqual(keyed, RateLimitKey.FromSensitive("pin", "other-key"u8));
    }

    private static RateLimitingOptions Options() =>
        new()
        {
            Namespace = "catalog",
            TokenBucket = new TokenBucketRateLimitPolicy
            {
                TokenLimit = 1,
                TokensPerPeriod = 1,
                ReplenishmentPeriod = TimeSpan.FromMinutes(1),
            },
        };

    private sealed class FaultingRateLimitStore : IRateLimitStore
    {
        public int Calls { get; private set; }

        public ValueTask<RateLimitDecision> AcquireAsync(
            string key,
            TokenBucketRateLimitPolicy policy,
            int permitCount,
            CancellationToken cancellationToken = default
        )
        {
            Calls++;
            return ValueTask.FromException<RateLimitDecision>(
                new RateLimitStoreException(RateLimitFailureKind.Unavailable, "Store unavailable.")
            );
        }
    }
}
