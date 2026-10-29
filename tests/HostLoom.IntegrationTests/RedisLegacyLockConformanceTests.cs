using HostLoom.Conformance;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Locking.Testing;
using HostLoom.Redis;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(RedisLegacyLockConformanceTests))]
[CollectionDefinition(nameof(RedisLegacyLockConformanceTests), DisableParallelization = true)]
public sealed class RedisLegacyLockConformanceTests
{
    public static bool Available => RedisAvailability.Redis;

    public static TheoryData<string, string> Scenarios
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (var scenario in LockConformance.Scenarios.Keys)
            {
                data.Add(scenario, "new");
                data.Add(scenario, "container");
            }

            return data;
        }
    }

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesWithTheLegacyIdentityOnRedis(
        string scenario,
        string composition
    )
    {
        await using var connection = Connection();
        await using var provider = new RedisLockProvider(connection);
        var legacy = new LegacyLockIdentityProvider(provider);
        var faults = new FaultingLockProvider(legacy);
        var containers = new List<ServiceProvider>();
        var fixture = new LockConformanceFixture
        {
            Clock = new RealConformanceClock(),
            Faults = faults,
            CreateLock =
                composition == "new"
                    ? () =>
                        new DistributedLock(new LockingOptions { Namespace = "migrated" }, faults)
                    : () => FromContainer(faults, containers),
        };

        try
        {
            await LockConformance
                .Scenarios[scenario](fixture)
                .WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
        }
        finally
        {
            foreach (var container in containers)
                await container.DisposeAsync();
        }
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task ActualLegacyHolder_BlocksMigrationAndRollbackRetainsIdentity()
    {
        var consumerKey = "inventory-" + Guid.NewGuid().ToString("N")[..8];
        var legacyKey = "lock:" + consumerKey;
        var canonicalKey = "migrated:lock:" + consumerKey;
        await using var connection = Connection();
        await using var provider = new RedisLockProvider(connection);
        await using var migrated = new DistributedLock(
            new LockingOptions { Namespace = "migrated" },
            new LegacyLockIdentityProvider(provider)
        );
        var db = await connection.GetDatabaseAsync(TestContext.Current.CancellationToken);

        try
        {
            Assert.True(
                await db.StringSetAsync(
                    legacyKey,
                    "legacy-owner",
                    TimeSpan.FromMinutes(1),
                    When.NotExists
                )
            );
            Assert.Null(
                await migrated.TryAcquireAsync(
                    consumerKey,
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            Assert.True((await db.StringGetAsync(canonicalKey)).IsNull);
            Assert.True(
                await provider.ReleaseAsync(
                    legacyKey,
                    "legacy-owner",
                    TestContext.Current.CancellationToken
                )
            );

            var held = await migrated.TryAcquireAsync(
                consumerKey,
                cancellationToken: TestContext.Current.CancellationToken
            );
            Assert.NotNull(held);
            Assert.False(
                await db.StringSetAsync(
                    legacyKey,
                    "rollback-owner",
                    TimeSpan.FromMinutes(1),
                    When.NotExists
                )
            );

            await held.DisposeAsync();
            Assert.True(
                await db.StringSetAsync(
                    legacyKey,
                    "rollback-owner",
                    TimeSpan.FromMinutes(1),
                    When.NotExists
                )
            );
        }
        finally
        {
            await db.KeyDeleteAsync([legacyKey, canonicalKey]);
        }
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task CanonicalAndLegacyIdentities_WithoutAdapterRequireDrainAndSwitch()
    {
        var consumerKey = "inventory-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = Connection();
        await using var provider = new RedisLockProvider(connection);
        await using var canonical = new DistributedLock(
            new LockingOptions { Namespace = "migrated" },
            provider
        );
        await using var legacy = new DistributedLock(
            new LockingOptions { Namespace = "migrated" },
            new LegacyLockIdentityProvider(provider)
        );

        var canonicalHeld = await canonical.TryAcquireAsync(
            consumerKey,
            cancellationToken: TestContext.Current.CancellationToken
        );
        var legacyHeld = await legacy.TryAcquireAsync(
            consumerKey,
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.NotNull(canonicalHeld);
        Assert.NotNull(legacyHeld);
        await canonicalHeld.DisposeAsync();
        await legacyHeld.DisposeAsync();
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task StaleLegacyOwner_CannotReleaseOrExtendSuccessor()
    {
        var consumerKey = "inventory-" + Guid.NewGuid().ToString("N")[..8];
        var canonicalKey = "migrated:lock:" + consumerKey;
        var legacyKey = "lock:" + consumerKey;
        var token = TestContext.Current.CancellationToken;
        await using var connection = Connection();
        await using var provider = new RedisLockProvider(connection);
        var legacy = new LegacyLockIdentityProvider(provider);
        var db = await connection.GetDatabaseAsync(token);

        try
        {
            Assert.True(
                await legacy.TryAcquireAsync(
                    canonicalKey,
                    "stale-owner",
                    TimeSpan.FromMilliseconds(50),
                    token
                )
            );
            await CacheConformance.WaitUntilAsync(async () => !await db.KeyExistsAsync(legacyKey));
            Assert.True(
                await legacy.TryAcquireAsync(
                    canonicalKey,
                    "successor-owner",
                    TimeSpan.FromMinutes(1),
                    token
                )
            );

            Assert.False(
                await legacy.ExtendAsync(
                    canonicalKey,
                    "stale-owner",
                    TimeSpan.FromMinutes(1),
                    token
                )
            );
            Assert.False(await legacy.ReleaseAsync(canonicalKey, "stale-owner", token));
            Assert.Equal("successor-owner", await db.StringGetAsync(legacyKey));
        }
        finally
        {
            await db.KeyDeleteAsync(legacyKey);
        }
    }

    private static RedisConnection Connection() =>
        new(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration,
                DatabaseIndex = 2,
                ClientName = "hostloom-legacy-conformance",
            }
        );

    private static IDistributedLock FromContainer(
        FaultingLockProvider faults,
        List<ServiceProvider> containers
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services
            .AddHostLoomLocking(locking => locking.Namespace = "migrated")
            .UseProvider<FaultingLockProvider>("Faulting");
        var container = services.BuildServiceProvider();
        containers.Add(container);
        return container.GetRequiredService<IDistributedLock>();
    }
}
