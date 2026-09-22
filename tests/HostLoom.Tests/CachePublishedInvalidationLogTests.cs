using HostLoom.Caching;
using HostLoom.Caching.Internal;
using Xunit;

namespace HostLoom.Tests;

/// <summary>How an instance recognises the echo of its own publishes.</summary>
public sealed class CachePublishedInvalidationLogTests
{
    private readonly TestClock _clock = new();
    private readonly CacheInvalidationOptions _options = new()
    {
        Timeout = TimeSpan.FromSeconds(5),
    };

    [Fact]
    public void The_echo_of_a_remembered_publish_is_recognised_once()
    {
        var log = Log();
        log.Remember(new CacheInvalidation(["orders:1"], ["catalog"]));

        // The echo is a decoded copy, equal in content but not the same object.
        Assert.True(log.IsOwnEcho(new CacheInvalidation(["orders:1"], ["catalog"])));
        Assert.False(log.IsOwnEcho(new CacheInvalidation(["orders:1"], ["catalog"])));
    }

    [Fact]
    public void A_different_message_is_not_an_echo_and_does_not_consume_one()
    {
        var log = Log();
        log.Remember(new CacheInvalidation(["orders:1"], []));

        Assert.False(log.IsOwnEcho(new CacheInvalidation(["orders:2"], [])));
        Assert.False(log.IsOwnEcho(new CacheInvalidation([], ["orders:1"])));
        Assert.False(log.IsOwnEcho(new CacheInvalidation(["orders:1", "orders:2"], [])));
        Assert.False(log.IsOwnEcho(CacheInvalidation.Flush));
        Assert.True(log.IsOwnEcho(new CacheInvalidation(["orders:1"], [])));
    }

    [Fact]
    public void Two_identical_publishes_expect_two_echoes()
    {
        var log = Log();
        log.Remember(new CacheInvalidation([], ["catalog"]));
        log.Remember(new CacheInvalidation([], ["catalog"]));

        Assert.True(log.IsOwnEcho(new CacheInvalidation([], ["catalog"])));
        Assert.True(log.IsOwnEcho(new CacheInvalidation([], ["catalog"])));
        Assert.False(log.IsOwnEcho(new CacheInvalidation([], ["catalog"])));
    }

    [Fact]
    public void A_publish_that_failed_is_forgotten_by_reference()
    {
        var log = Log();
        var failed = new CacheInvalidation(["orders:1"], []);
        log.Remember(failed);

        // An equal message that was never remembered forgets nothing.
        log.Forget(new CacheInvalidation(["orders:1"], []));
        log.Forget(failed);

        Assert.False(log.IsOwnEcho(new CacheInvalidation(["orders:1"], [])));
    }

    [Fact]
    public void An_echo_later_than_the_publish_timeout_is_treated_as_someone_elses()
    {
        var log = Log();
        log.Remember(new CacheInvalidation(["orders:1"], []));
        log.Remember(new CacheInvalidation(["orders:2"], []));

        _clock.Advance(_options.Timeout);
        Assert.True(log.IsOwnEcho(new CacheInvalidation(["orders:1"], [])));
        _clock.Advance(TimeSpan.FromTicks(1));
        Assert.False(log.IsOwnEcho(new CacheInvalidation(["orders:2"], [])));
    }

    [Fact]
    public void Only_the_latest_sixty_four_publishes_are_remembered()
    {
        var log = Log();
        for (var i = 0; i <= 64; i++)
        {
            log.Remember(new CacheInvalidation([$"orders:{i}"], []));
        }

        Assert.False(log.IsOwnEcho(new CacheInvalidation(["orders:0"], [])));
        Assert.True(log.IsOwnEcho(new CacheInvalidation(["orders:1"], [])));
        Assert.True(log.IsOwnEcho(new CacheInvalidation(["orders:64"], [])));
    }

    private PublishedInvalidationLog Log() => new(_clock, _options);
}
