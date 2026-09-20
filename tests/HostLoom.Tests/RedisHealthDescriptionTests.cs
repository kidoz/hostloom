using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Readiness output is often reachable without credentials, so the health descriptions say
/// whether Redis answers and how fast, and nothing about where it is or who is asking.
/// </summary>
public sealed class RedisHealthDescriptionTests
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task HealthDescriptions_NameNoEndpointClientMachineOrProcess()
    {
        var db = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        mux.ClientName.Returns("hostloom-buildhost-4242");
        db.PingAsync(Arg.Any<CommandFlags>())
            .Returns(Task.FromResult(TimeSpan.FromMilliseconds(1.5)));
        var options = new RedisOptions
        {
            Configuration = "cache-host.internal:6379,password=hunter2",
            DatabaseIndex = 3,
        };
        await using var connection = new RedisConnection(mux, options);
        await using var store = new RedisCacheStore(connection);
        await using var locks = new RedisLockProvider(connection);

        var cache = await store.CheckHealthAsync(Token);
        var lockHealth = await locks.CheckHealthAsync(Token);

        Assert.True(cache.IsHealthy);
        Assert.True(lockHealth.IsHealthy);
        foreach (var description in new[] { cache.Description, lockHealth.Description })
        {
            Assert.StartsWith("Redis reachable", description, StringComparison.Ordinal);
            Assert.Contains("1.5 ms", description, StringComparison.Ordinal);
            Assert.Contains("database 3", description, StringComparison.Ordinal);
            AssertDisclosesNothing(description);
        }

        db.PingAsync(Arg.Any<CommandFlags>())
            .Returns(Task.FromException<TimeSpan>(new TimeoutException("no answer")));

        var down = await store.CheckHealthAsync(Token);
        var lockDown = await locks.CheckHealthAsync(Token);

        Assert.False(down.IsHealthy);
        Assert.False(lockDown.IsHealthy);
        foreach (var description in new[] { down.Description, lockDown.Description })
        {
            Assert.StartsWith("Redis unreachable", description, StringComparison.Ordinal);
            Assert.Contains(nameof(TimeoutException), description, StringComparison.Ordinal);
            AssertDisclosesNothing(description);
        }

        // The full description stays available for logs.
        Assert.Contains(
            "cache-host.internal:6379",
            connection.Describe(),
            StringComparison.Ordinal
        );
        Assert.DoesNotContain("hunter2", connection.Describe(), StringComparison.Ordinal);
    }

    private static void AssertDisclosesNothing(string description)
    {
        Assert.DoesNotContain("cache-host", description, StringComparison.Ordinal);
        Assert.DoesNotContain("6379", description, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", description, StringComparison.Ordinal);
        Assert.DoesNotContain("buildhost", description, StringComparison.Ordinal);
        Assert.DoesNotContain("4242", description, StringComparison.Ordinal);
        Assert.DoesNotContain("hostloom-", description, StringComparison.Ordinal);
    }
}
