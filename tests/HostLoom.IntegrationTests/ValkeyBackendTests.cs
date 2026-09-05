using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Valkey;
using Microsoft.Extensions.DependencyInjection;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.IntegrationTests;

public sealed class ValkeyBackendTests
{
    public static bool Available => ValkeyAvailability.Available;
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task Store_PreservesBinaryTtlTagsConditionalAndBulkSemantics(
        ValkeyProtocol protocol
    )
    {
        await using var connection = new ValkeyConnection(ValkeyAvailability.Options(protocol));
        var store = new ValkeyCacheStore(connection);
        var prefix = "valkey-store-" + Guid.NewGuid().ToString("N");
        var key = prefix + ":cache:data:catalog";
        var tag = prefix + ":cache:tag:books";
        var other = prefix + ":cache:tag:music";
        byte[] value = [0, 255, 13, 10, 128];
        try
        {
            Assert.True(
                await store.SetIfAbsentAsync(key, value, TimeSpan.FromSeconds(30), [tag], Token)
            );
            Assert.False(
                await store.SetIfAbsentAsync(
                    key,
                    new byte[] { 1 },
                    TimeSpan.FromSeconds(60),
                    [other],
                    Token
                )
            );
            await store.RemoveByTagAsync(other, Token);
            var read = await store.GetAsync(key, Token);
            Assert.NotNull(read);
            Assert.Equal(value, read.Value.Payload.ToArray());
            Assert.InRange(
                read.Value.RemainingTimeToLive!.Value,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(30)
            );
            await store.SetManyAsync(
                [new(key + "2", value), new(key + "3", ReadOnlyMemory<byte>.Empty)],
                TimeSpan.FromSeconds(30),
                Token
            );
            var many = await store.GetManyAsync(
                [key, key + "2", key + "3", key + "missing"],
                Token
            );
            Assert.Equal(3, many.Count);
            Assert.Empty(many[key + "3"].Payload.ToArray());
            await store.RemoveByTagAsync(tag, Token);
            Assert.Null(await store.GetAsync(key, Token));
            Assert.NotNull(await store.GetAsync(key + "2", Token));
            Assert.True((await store.CheckHealthAsync(Token)).IsHealthy);
        }
        finally
        {
            await store.RemoveAsync([key, key + "2", key + "3", tag, other], Token);
        }
    }

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task Lock_HasOneWinnerAndOwnerCheckedReleaseAndExtension(ValkeyProtocol protocol)
    {
        await using var first = new ValkeyConnection(ValkeyAvailability.Options(protocol));
        await using var second = new ValkeyConnection(ValkeyAvailability.Options(protocol));
        var providers = new[] { new ValkeyLockProvider(first), new ValkeyLockProvider(second) };
        var key = "valkey-lock-" + Guid.NewGuid().ToString("N");
        var lease = TimeSpan.FromSeconds(30);
        var winners = await Task.WhenAll(
            Enumerable
                .Range(0, 32)
                .Select(async i =>
                    (
                        Index: i,
                        Won: await providers[i % 2]
                            .TryAcquireAsync(
                                key,
                                i.ToString(CultureInfo.InvariantCulture),
                                lease,
                                Token
                            )
                    )
                )
        );
        var winner = Assert.Single(winners, item => item.Won);
        var owner = winner.Index.ToString(CultureInfo.InvariantCulture);
        try
        {
            Assert.False(await providers[0].ReleaseAsync(key, "wrong-owner", Token));
            Assert.False(await providers[1].ExtendAsync(key, "wrong-owner", lease, Token));
            Assert.True(await providers[0].ExtendAsync(key, owner, lease, Token));
            Assert.True(await providers[1].ReleaseAsync(key, owner, Token));
            Assert.True(await providers[1].TryAcquireAsync(key, "next-owner", lease, Token));
            Assert.False(await providers[0].ReleaseAsync(key, owner, Token));
            Assert.False(await providers[0].ExtendAsync(key, owner, lease, Token));
        }
        finally
        {
            await providers[0].ReleaseAsync(key, owner, Token);
            await providers[1].ReleaseAsync(key, "next-owner", Token);
        }
    }

    [Fact(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task UseValkey_ComposesSerializedL2AndLocksThroughContainer()
    {
        var ns = "valkey-di-" + Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(options => options.Namespace = ns)
            .UseValkey(options => options.Connection = ValkeyAvailability.Options().Connection)
            .UseSystemTextJson(
                new JsonSerializerOptions { TypeInfoResolver = ValkeyBackendJson.Default }
            );
        services.AddHostLoomLocking(options => options.Namespace = ns).UseValkey();
        await using var container = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        var cache = container.GetRequiredService<ICache>();
        await cache.SetAsync(
            "catalog:eu",
            "books",
            new CacheEntryOptions(TimeSpan.FromSeconds(30)),
            Token
        );
        await using var other = new TieredCache(
            new CachingOptions { Namespace = ns },
            container.GetRequiredService<IDistributedCacheStore>(),
            new SystemTextJsonCacheValueSerializer(
                new JsonSerializerOptions { TypeInfoResolver = ValkeyBackendJson.Default }
            )
        );
        var found = await other.TryGetAsync<string>("catalog:eu", Token);
        Assert.Equal(CacheTier.L2, found.Tier);
        Assert.Equal("books", found.Value);
        var locking = container.GetRequiredService<IDistributedLock>();
        await using var handle = await locking.TryAcquireAsync(
            "catalog:eu",
            cancellationToken: Token
        );
        Assert.NotNull(handle);
        Assert.True(handle.IsHeld);
        await cache.RemoveAsync("catalog:eu", Token);
    }

    [Fact(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Invalidation_RecoversAfterItsSocketIsClosedAndIsolatesHandlerFailures()
    {
        var options = ValkeyAvailability.Options();
        await using var connection = new ValkeyConnection(options);
        await using var admin = await ValkeyClient.ConnectAsync(
            ValkeyAvailability.Options().Connection,
            Token
        );
        await using var channel = new ValkeyCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "valkey-channel-" + Guid.NewGuid().ToString("N") }
        );
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(15));
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var faulty = channel.Subscribe(_ =>
            throw new InvalidOperationException("handler failure")
        );
        using var subscription = channel.Subscribe(message =>
        {
            if (message.Keys.Contains("after-reconnect"))
                received.TrySetResult();
        });
        await channel.StartAsync(timeout.Token);
        var clients = await admin.ExecuteAsync(
            new ValkeyCommand("CLIENT", "LIST", "TYPE", "pubsub"),
            timeout.Token
        );
        var line = Assert.Single(
            clients.AsString()!.Split('\n', StringSplitOptions.RemoveEmptyEntries),
            item => item.Split(' ').Contains("name=" + options.Connection.ClientName)
        );
        var id = line.Split(' ').Single(item => item.StartsWith("id=", StringComparison.Ordinal))[
            3..
        ];
        await admin.ExecuteAsync(new ValkeyCommand("CLIENT", "KILL", "ID", id), timeout.Token);
        // Wait for an acknowledged replacement connection, not a fixed reconnect delay.
        while (true)
        {
            var current = await admin.ExecuteAsync(
                new ValkeyCommand("CLIENT", "LIST", "TYPE", "pubsub"),
                timeout.Token
            );
            if (
                current
                    .AsString()!
                    .Split('\n')
                    .Any(item =>
                        item.Split(' ').Contains("name=" + options.Connection.ClientName)
                        && !item.Split(' ').Contains("id=" + id)
                    )
            )
                break;
            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
        while (!received.Task.IsCompleted)
        {
            await channel.PublishAsync(
                new CacheInvalidation(["after-reconnect"], []),
                timeout.Token
            );
            await Task.WhenAny(
                received.Task,
                Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token)
            );
            timeout.Token.ThrowIfCancellationRequested();
        }
        await received.Task;
        Assert.True(channel.IsSubscribed);
    }

    [Fact(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Invalidation_OverflowIsCountedAndDeliveryContinues()
    {
        var options = ValkeyAvailability.Options();
        options.InvalidationQueueCapacity = 1;
        await using var connection = new ValkeyConnection(options);
        await using var channel = new ValkeyCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "valkey-overflow-" + Guid.NewGuid().ToString("N") }
        );
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        using var release = new ManualResetEventSlim();
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var subscription = channel.Subscribe(message =>
        {
            if (message.Keys.Contains("block"))
            {
                blocked.TrySetResult();
                release.Wait(timeout.Token);
            }
            if (message.Keys.Contains("sentinel"))
                completed.TrySetResult();
        });
        await channel.StartAsync(timeout.Token);
        try
        {
            await channel.PublishAsync(new CacheInvalidation(["block"], []), timeout.Token);
            await blocked.Task.WaitAsync(timeout.Token);
            // The callback is deliberately blocked: the SDK reader must keep draining and bound its queue.
            for (var i = 0; i < 100; i++)
                await channel.PublishAsync(new CacheInvalidation(["catalog"], []), timeout.Token);
        }
        finally
        {
            release.Set();
        }
        while (!completed.Task.IsCompleted || channel.DroppedMessages == 0)
        {
            await channel.PublishAsync(new CacheInvalidation(["sentinel"], []), timeout.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
        }
        Assert.True(channel.DroppedMessages > 0);
        Assert.True(channel.IsSubscribed);
    }

    [Fact(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task PipelineServerError_IsReportedAndConnectionRemainsUsable()
    {
        var options = ValkeyAvailability.Options();
        await using var connection = new ValkeyConnection(options);
        await using var admin = await ValkeyClient.ConnectAsync(options.Connection, Token);
        var store = new ValkeyCacheStore(connection);
        var key = "valkey-wrong-type-" + Guid.NewGuid().ToString("N");
        try
        {
            await admin.ExecuteAsync(new ValkeyCommand("SADD", key, "books"), Token);
            await admin.ExecuteAsync(new ValkeyCommand("PEXPIRE", key, 30_000), Token);
            var failure = await Assert.ThrowsAsync<CacheStoreException>(() =>
                store.GetManyAsync([key, key + ":missing"], Token).AsTask()
            );
            Assert.Equal(CacheFailureKind.Other, failure.Kind);
            Assert.IsType<ValkeyServerException>(failure.InnerException);
            Assert.True((await store.CheckHealthAsync(Token)).IsHealthy);
        }
        finally
        {
            await store.RemoveAsync([key], Token);
        }
    }
}

[JsonSerializable(typeof(string))]
internal sealed partial class ValkeyBackendJson : JsonSerializerContext;
