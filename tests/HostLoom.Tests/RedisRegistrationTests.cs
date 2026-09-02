using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Everything about the Redis package that needs no Redis: options, registration, redaction,
/// key layout, message encoding, failure mapping, and the fail-open and fail-fast startup paths
/// against a port nothing listens on.
/// </summary>
public sealed class RedisRegistrationTests
{
    // Nothing listens here; connection attempts fail fast instead of timing out.
    private const string Unreachable = "localhost:1";

    private static JsonSerializerOptions Json() =>
        new() { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    private static void Quick(RedisOptions options)
    {
        options.Configuration = Unreachable;
        options.ConnectTimeout = TimeSpan.FromMilliseconds(300);
        options.CommandTimeout = TimeSpan.FromMilliseconds(300);
        options.HealthTimeout = TimeSpan.FromSeconds(2);
    }

    [Fact]
    public void Validate_ReportsEachProblemNamingTheOptionKey()
    {
        var options = new RedisOptions
        {
            DatabaseIndex = -1,
            ConnectTimeout = TimeSpan.Zero,
            ClientName = "",
        };

        var problems = options.Validate();

        Assert.Contains(
            problems,
            p => p.StartsWith("Redis:Configuration", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.StartsWith("Redis:DatabaseIndex", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.StartsWith("Redis:ConnectTimeout", StringComparison.Ordinal)
        );
        Assert.Contains(problems, p => p.StartsWith("Redis:ClientName", StringComparison.Ordinal));
    }

    [Fact]
    public void BuildConfiguration_ClonesAndAppliesClientNameTimeoutsAndFailOpen()
    {
        var supplied = ConfigurationOptions.Parse("localhost:6379");
        var options = new RedisOptions
        {
            ConfigurationOptions = supplied,
            ClientName = "svc-1",
            ConnectTimeout = TimeSpan.FromSeconds(7),
            CommandTimeout = TimeSpan.FromSeconds(3),
        };

        var built = options.BuildConfiguration();
        options.FailFast = true;
        var failFast = options.BuildConfiguration();

        Assert.NotSame(supplied, built);
        Assert.Equal("svc-1", built.ClientName);
        Assert.Equal(7_000, built.ConnectTimeout);
        Assert.Equal(3_000, built.AsyncTimeout);
        Assert.False(built.AbortOnConnectFail);
        Assert.True(failFast.AbortOnConnectFail);
        Assert.True(built.AllowAdmin);
        Assert.Equal(RedisProtocol.Resp2, built.Protocol);
        Assert.Null(supplied.ClientName);
    }

    [Fact]
    public async Task Describe_RedactsThePassword()
    {
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                Configuration = "localhost:6379,password=hunter2",
                DatabaseIndex = 3,
            }
        );

        var description = connection.Describe();

        Assert.DoesNotContain("hunter2", description, StringComparison.Ordinal);
        Assert.Contains("localhost:6379", description, StringComparison.Ordinal);
        Assert.Contains("database 3", description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UseRedis_OnCaching_RegistersStoreChannelAndProbeWithoutConnecting()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseRedis(Quick)
            .UseSystemTextJson(Json());
        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IDistributedCacheStore>();
        var probe = provider.GetRequiredService<ICacheStoreHealthProbe>();
        var channel = provider.GetRequiredService<ICacheInvalidationChannel>();
        var connection = provider.GetRequiredService<RedisConnection>();

        Assert.IsType<RedisCacheStore>(store);
        Assert.Same(store, probe);
        var redisChannel = Assert.IsType<RedisCacheInvalidationChannel>(channel);
        Assert.Equal("svc:cache:invalidate", redisChannel.ChannelName);
        Assert.False(connection.IsConnected);
    }

    [Fact]
    public async Task UseRedis_OnBothBuilders_SharesOneConnection()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseRedis(Quick)
            .UseSystemTextJson(Json());
        services.AddHostLoomLocking(locking => locking.Namespace = "svc").UseRedis();
        await using var provider = services.BuildServiceProvider();

        var provider1 = provider.GetRequiredService<ILockProvider>();
        Assert.IsType<RedisLockProvider>(provider1);
        Assert.Same(provider1, provider.GetRequiredService<ILockProviderHealthProbe>());
        Assert.Single(services, d => d.ServiceType == typeof(RedisConnection));
        Assert.Single(
            services,
            d =>
                d.ServiceType == typeof(IHostedService)
                && d.ImplementationType?.Name == "RedisConnectionStarter"
        );
    }

    [Fact]
    public void UseRedis_AfterUseInMemory_ThrowsNamingInMemory()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseInMemory();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.UseRedis(Quick));

        Assert.Contains("InMemory", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HealthProbe_WhenUnreachable_ReportsUnhealthyWithoutThrowing()
    {
        var options = new RedisOptions();
        Quick(options);
        await using var connection = new RedisConnection(options);
        await using var store = new RedisCacheStore(connection);
        await using var locks = new RedisLockProvider(connection);

        var cacheHealth = await store.CheckHealthAsync(TestContext.Current.CancellationToken);
        var lockHealth = await locks.CheckHealthAsync(TestContext.Current.CancellationToken);

        Assert.False(cacheHealth.IsHealthy);
        Assert.False(lockHealth.IsHealthy);
        Assert.Contains(Unreachable, cacheHealth.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FailFast_WithUnreachableRedis_FailsHostStartup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseRedis(options =>
            {
                Quick(options);
                options.FailFast = true;
            })
            .UseSystemTextJson(Json());
        using var host = builder.Build();

        await Assert.ThrowsAnyAsync<RedisException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task FailOpen_WithUnreachableRedis_HostStartsCacheServesAndReadinessIsUnhealthy()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseRedis(Quick)
            .UseSystemTextJson(Json())
            .AddHealthChecks();
        builder.Services.AddHostLoomLocking(locking => locking.Namespace = "svc").UseRedis();
        using var host = builder.Build();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var cache = host.Services.GetRequiredService<ICache>();
        var value = await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(42),
            TimeSpan.FromMinutes(1),
            TestContext.Current.CancellationToken
        );
        var lookup = await cache.TryGetAsync<int>("k", TestContext.Current.CancellationToken);
        var readiness = await host
            .Services.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Tags.Contains("ready"), TestContext.Current.CancellationToken);
        var locking = host.Services.GetRequiredService<IDistributedLock>();
        var lockFailure = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
            await locking.ExecuteWithLockAsync(
                "k",
                static _ => ValueTask.FromResult(1),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(42, value);
        Assert.True(lookup.Found);
        Assert.Equal(CacheTier.L1, lookup.Tier);
        Assert.Equal(HealthStatus.Unhealthy, readiness.Status);
        Assert.Equal(LockFailureKind.Unavailable, lockFailure.Kind);
        Assert.Equal("RedisCacheStore", CachingProbe.Describe(cache).Store);
    }

    [Theory]
    [InlineData("svc:cache:data:k", false, "svc:cache:data:k")]
    [InlineData("svc:cache:data:k", true, "{svc}:cache:data:k")]
    [InlineData("svc:lock:k", true, "{svc}:lock:k")]
    [InlineData("nocolon", true, "{nocolon}")]
    public void RedisKeys_WrapTheNamespaceInAHashTagWhenAsked(
        string key,
        bool hashTags,
        string expected
    ) => Assert.Equal(expected, (string?)RedisKeys.ToRedisKey(key, hashTags));

    [Fact]
    public void InvalidationMessage_RoundTripsKeysAndTags()
    {
        var message = new CacheInvalidation(["a", "b:c"], ["catalog"]);

        var decoded = RedisCacheInvalidationChannel.Decode(
            RedisCacheInvalidationChannel.Encode(message)
        );

        Assert.NotNull(decoded);
        Assert.Equal(["a", "b:c"], decoded.Keys);
        Assert.Equal(["catalog"], decoded.Tags);
        Assert.Null(RedisCacheInvalidationChannel.Decode("v9\nkx"));
        Assert.Null(RedisCacheInvalidationChannel.Decode(null));
    }

    [Fact]
    public void Failures_MapToBackendNeutralKinds()
    {
        Assert.Equal(
            CacheFailureKind.Timeout,
            RedisFailures.ToCacheStoreException(new TimeoutException("slow"), "GET").Kind
        );
        Assert.Equal(
            CacheFailureKind.Unavailable,
            RedisFailures.ToCacheStoreException(new ObjectDisposedException("mux"), "GET").Kind
        );
        Assert.Equal(
            CacheFailureKind.Other,
            RedisFailures
                .ToCacheStoreException(new InvalidOperationException("WRONGTYPE"), "GET")
                .Kind
        );
        Assert.Equal(
            LockFailureKind.Timeout,
            RedisFailures.ToLockProviderException(new TimeoutException(), "SET").Kind
        );
    }
}
