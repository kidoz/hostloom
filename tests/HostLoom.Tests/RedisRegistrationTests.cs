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
using NSubstitute;
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
        Assert.Equal(5, built.ConfigCheckSeconds);
        Assert.Equal(60, supplied.ConfigCheckSeconds);
        Assert.Null(supplied.ClientName);
    }

    [Theory]
    [InlineData(0, 5)]
    [InlineData(1, 1)]
    [InlineData(60, 5)]
    public void BuildConfiguration_BoundsTopologyChecks(int configured, int expected)
    {
        var options = new RedisOptions
        {
            Configuration = $"localhost:6379,configCheckSeconds={configured}",
        };
        Assert.Equal(expected, options.BuildConfiguration().ConfigCheckSeconds);
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

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task UseRedis_WithRegisteredMultiplexer_UsesSharedConnectionAndConfiguredOptions(
        bool multiplexerFirst,
        bool cachingFirst
    )
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var unrelated = Substitute.For<IConnectionMultiplexer>();
        var shared = Substitute.For<IConnectionMultiplexer>();
        var database = Substitute.For<IDatabase>();
        shared.GetDatabase(3, Arg.Any<object>()).Returns(database);
        database
            .ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Any<RedisKey[]>(),
                Arg.Any<RedisValue[]>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(RedisResult.Create(1L));
        database.KeyDeleteAsync(Arg.Any<RedisKey[]>(), Arg.Any<CommandFlags>()).Returns(1L);
        var connections = 0;
        void Configure(RedisOptions options)
        {
            options.DatabaseIndex = 3;
            options.UseHashTags = true;
            options.ConnectionFactory = _ =>
            {
                connections++;
                // The completed task returns a borrowed test double, not a scoped resource.
#pragma warning disable CA2025
                return Task.FromResult(shared);
#pragma warning restore CA2025
            };
        }

        if (multiplexerFirst)
            services.AddSingleton(unrelated);
        if (cachingFirst)
        {
            services.AddHostLoomCaching().UseRedis(Configure);
            services.AddHostLoomLocking().UseRedis();
        }
        else
        {
            services.AddHostLoomLocking().UseRedis(Configure);
            services.AddHostLoomCaching().UseRedis();
        }
        if (!multiplexerFirst)
            services.AddSingleton(unrelated);
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        var locks = provider.GetRequiredService<RedisLockProvider>();
        var store = provider.GetRequiredService<RedisCacheStore>();
        Assert.Same(locks, provider.GetRequiredService<ILockProvider>());
        Assert.Same(locks, provider.GetRequiredService<ILockProviderHealthProbe>());
        Assert.Same(store, provider.GetRequiredService<IDistributedCacheStore>());
        Assert.Same(store, provider.GetRequiredService<ICacheStoreHealthProbe>());
        Assert.False(provider.GetRequiredService<RedisConnection>().IsConnected);
        Assert.Equal(0, connections);

        Assert.True(
            await locks.ReleaseAsync(
                "catalog:lock:refresh",
                "owner",
                TestContext.Current.CancellationToken
            )
        );
        await store.RemoveAsync(["catalog:cache:data:eu"], TestContext.Current.CancellationToken);

        Assert.Equal(1, connections);
        shared.Received(2).GetDatabase(3, Arg.Any<object>());
        unrelated.DidNotReceive().GetDatabase(Arg.Any<int>(), Arg.Any<object>());
        await database
            .Received(1)
            .ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Is<RedisKey[]>(keys =>
                    keys.Length == 1 && keys[0] == (RedisKey)"{catalog}:lock:refresh"
                ),
                Arg.Any<RedisValue[]>(),
                Arg.Any<CommandFlags>()
            );
        await database
            .Received(1)
            .KeyDeleteAsync(
                Arg.Is<RedisKey[]>(keys =>
                    keys.Length == 1 && keys[0] == (RedisKey)"{catalog}:cache:data:eu"
                ),
                Arg.Any<CommandFlags>()
            );

        await locks.DisposeAsync();
        await store.DisposeAsync();
        Assert.Same(
            shared,
            await provider
                .GetRequiredService<RedisConnection>()
                .GetMultiplexerAsync(TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UseRedis_PreservesExplicitStoreAndProviderRegistrations(bool useFactory)
    {
        await using var connection = new RedisConnection(Substitute.For<IConnectionMultiplexer>());
        await using var store = new RedisCacheStore(connection);
        await using var locks = new RedisLockProvider(connection);
        var services = new ServiceCollection();
        if (useFactory)
        {
            services.AddSingleton(_ => store);
            services.AddSingleton(_ => locks);
        }
        else
        {
            services.AddSingleton(store);
            services.AddSingleton(locks);
        }
        services.AddHostLoomCaching().UseRedis(Quick);
        services.AddHostLoomLocking().UseRedis();
        await using var provider = services.BuildServiceProvider();

        Assert.Same(store, provider.GetRequiredService<IDistributedCacheStore>());
        Assert.Same(locks, provider.GetRequiredService<ILockProvider>());
    }

    [Fact]
    public void UseRedis_AfterUseInMemory_ThrowsNamingInMemory()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseInMemory();
        var registrations = services.ToArray();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.UseRedis(Quick));

        Assert.Contains("InMemory", exception.Message, StringComparison.Ordinal);
        Assert.Equal(registrations, services);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Repeated_locking_UseRedis_explains_duplicate_selection_without_changing_options(
        bool conflictingSettings
    )
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomLocking()
            .UseRedis(options =>
            {
                Quick(options);
                options.DatabaseIndex = 1;
            });
        // Repeating the feature registration is valid; choosing the provider again is not.
        var again = services.AddHostLoomLocking();
        var before = services.ToArray();

        var failure = Assert.Throws<InvalidOperationException>(() =>
            again.UseRedis(options =>
            {
                Quick(options);
                options.DatabaseIndex = conflictingSettings ? 2 : 1;
            })
        );

        Assert.Contains(
            "Redis locking was configured more than once",
            failure.Message,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "Repeated AddHostLoomLocking() calls are allowed",
            failure.Message,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "repeated provider selection is not",
            failure.Message,
            StringComparison.Ordinal
        );
        Assert.Equal(before, services);
        await using var provider = services.BuildServiceProvider();
        var options = provider
            .GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisOptions>>()
            .Value;
        Assert.Equal(1, options.DatabaseIndex);
        Assert.Equal(Unreachable, options.Configuration);
        Assert.Single(services, d => d.ServiceType == typeof(ILockProvider));
    }

    [Fact]
    public void UseRedis_OnLockingAfterUseInMemory_DoesNotChangeRegistrations()
    {
        var services = new ServiceCollection();
        var builder = services.AddHostLoomLocking().UseInMemory();
        var registrations = services.ToArray();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.UseRedis(Quick));

        Assert.Contains("InMemory", exception.Message, StringComparison.Ordinal);
        Assert.Equal(registrations, services);
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
        // Readiness output names no endpoint; the endpoint stays in logs via Describe().
        foreach (var description in new[] { cacheHealth.Description, lockHealth.Description })
        {
            Assert.StartsWith("Redis unreachable", description, StringComparison.Ordinal);
            Assert.DoesNotContain(Unreachable, description, StringComparison.Ordinal);
        }

        Assert.Contains(Unreachable, connection.Describe(), StringComparison.Ordinal);
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
        Assert.False(decoded.FlushAll);
        Assert.Equal("v1\n*", RedisCacheInvalidationChannel.Encode(CacheInvalidation.Flush));
        var flush = RedisCacheInvalidationChannel.Decode("v1\n*\nka");
        Assert.NotNull(flush);
        Assert.True(flush.FlushAll);
        Assert.Equal(["a"], flush.Keys);
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
