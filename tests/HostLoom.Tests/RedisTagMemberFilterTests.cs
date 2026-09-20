using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A tag index is a plain set. Its members are only trusted as keys to unlink when they sit
/// under the namespace's cache-data prefix; anything else is forgotten from the index instead.
/// </summary>
public sealed class RedisTagMemberFilterTests
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("svc:catalog", "svc:cache:tag:catalog")]
    [InlineData("svc:cache:lease:catalog", "svc:cache:tag:catalog")]
    [InlineData("other:cache:data:catalog", "svc:cache:tag:catalog")]
    [InlineData("svc:cache:data:", "svc:cache:tag:catalog")]
    [InlineData("svc:cache:data:catalog", "svc:tag")]
    [InlineData("svc:cache:data:catalog", "svc:cache:tag:")]
    public async Task Tagged_writes_reject_unremovable_keys_before_backend_io(
        string key,
        string tag
    )
    {
        var mux = Substitute.For<IConnectionMultiplexer>();
        await using var store = new RedisCacheStore(mux);
        // Construction registers connection observers; only operation-time calls are under test.
        mux.ClearReceivedCalls();
        var token = TestContext.Current.CancellationToken;
        // Include a valid first tag to ensure the whole input is checked before dispatch.
        string[] tags = ["svc:cache:tag:valid", tag];
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.SetAsync(key, "value"u8.ToArray(), TimeSpan.FromMinutes(1), tags, token).AsTask()
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store
                .SetIfAbsentAsync(key, "value"u8.ToArray(), TimeSpan.FromMinutes(1), tags, token)
                .AsTask()
        );
        Assert.Empty(mux.ReceivedCalls());
    }

    [Theory]
    [InlineData("svc:custom-index")]
    [InlineData("nocolon")]
    [InlineData(":cache:tag:catalog")]
    [InlineData("svc:cache:tag:")]
    public void Invalid_tag_domains_are_rejected(string tag) =>
        Assert.Throws<ArgumentException>(() => RedisCacheStore.DataPrefixFor(tag));

    [Theory]
    [InlineData("svc:cache:tag:catalog", "svc:cache:data:")]
    [InlineData("svc:cache:tag:catalog:eu", "svc:cache:data:")]
    public void DataPrefixFor_DerivesTheNamespaceDataPrefixFromTheTagKey(
        string tagKey,
        string expected
    ) => Assert.Equal(expected, RedisCacheStore.DataPrefixFor(tagKey));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RemoveByTag_UnlinksOnlyMembersUnderTheDataPrefixAndForgetsTheRest(
        bool hashTags
    )
    {
        var db = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        var prefix = hashTags ? "{svc}:cache:data:" : "svc:cache:data:";
        var tag = hashTags ? "{svc}:cache:tag:catalog" : "svc:cache:tag:catalog";
        RedisValue[] members =
        [
            prefix + "catalog:eu",
            "svc:lock:catalog:refresh",
            prefix + "catalog:us",
            "other:cache:data:catalog:eu",
            "{svc}:cache:lease:catalog:eu",
        ];
        db.SetMembersAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
            .Returns(Task.FromResult(members));
        RedisKey[]? unlinked = null;
        db.ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Any<RedisKey[]?>(),
                Arg.Any<RedisValue[]?>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(call =>
            {
                unlinked = call.ArgAt<RedisKey[]?>(1);
                return Task.FromResult(RedisResult.Create(2));
            });
        RedisValue[]? forgotten = null;
        db.SetRemoveAsync(Arg.Any<RedisKey>(), Arg.Any<RedisValue[]>(), Arg.Any<CommandFlags>())
            .Returns(call =>
            {
                forgotten = call.ArgAt<RedisValue[]>(1);
                return Task.FromResult(3L);
            });

        await using var store = new RedisCacheStore(
            mux,
            new RedisOptions { Configuration = "external", UseHashTags = hashTags }
        );
        await store.RemoveByTagAsync("svc:cache:tag:catalog", Token);

        Assert.NotNull(unlinked);
        Assert.Equal(
            [tag, prefix + "catalog:eu", prefix + "catalog:us"],
            unlinked.Select(static key => (string?)key)
        );
        Assert.NotNull(forgotten);
        Assert.Equal(
            [
                "svc:lock:catalog:refresh",
                "other:cache:data:catalog:eu",
                "{svc}:cache:lease:catalog:eu",
            ],
            forgotten.Select(static value => (string?)value)
        );
    }

    [Fact]
    public async Task RemoveByTag_DoesNotTouchTheIndexTwiceWhenEveryMemberIsAcceptable()
    {
        var db = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        db.SetMembersAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
            .Returns(Task.FromResult<RedisValue[]>(["svc:cache:data:catalog:eu"]));
        db.ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Any<RedisKey[]?>(),
                Arg.Any<RedisValue[]?>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(Task.FromResult(RedisResult.Create(1)));

        await using var store = new RedisCacheStore(mux);
        await store.RemoveByTagAsync("svc:cache:tag:catalog", Token);

        await db.DidNotReceive()
            .SetRemoveAsync(Arg.Any<RedisKey>(), Arg.Any<RedisValue[]>(), Arg.Any<CommandFlags>());
    }
}
