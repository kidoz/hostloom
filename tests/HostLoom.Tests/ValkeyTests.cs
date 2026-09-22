using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Valkey;
using HostLoom.Valkey.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.Tests;

public sealed class ValkeyTests
{
    [Fact]
    public async Task Registration_IsLazyAndSharesConnectionAndHealthProviders()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(options => options.Namespace = "valkey-unit")
            .UseValkey()
            .UseSystemTextJson(
                new JsonSerializerOptions { TypeInfoResolver = ValkeyTestJson.Default }
            );
        services.AddHostLoomLocking().UseValkey();
        await using var container = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        var connection = container.GetRequiredService<ValkeyConnection>();
        Assert.Equal(ValkeyConnectionState.NeverConnected, connection.State);
        Assert.Single(container.GetServices<ValkeyConnection>());
        Assert.Same(
            container.GetRequiredService<IDistributedCacheStore>(),
            container.GetRequiredService<ICacheStoreHealthProbe>()
        );
        Assert.Same(
            container.GetRequiredService<ILockProvider>(),
            container.GetRequiredService<ILockProviderHealthProbe>()
        );
        Assert.IsType<ValkeyCacheInvalidationChannel>(
            container.GetRequiredService<ICacheInvalidationChannel>()
        );
    }

    [Fact]
    public void Registration_RejectsSecondBackend()
    {
        var services = new ServiceCollection();
        var cache = services
            .AddHostLoomCaching(options => options.Namespace = "valkey-unit")
            .UseValkey();
        Assert.Throws<InvalidOperationException>(() => cache.UseInMemory());
        var locking = services.AddHostLoomLocking().UseValkey();
        Assert.Throws<InvalidOperationException>(() => locking.UseInMemory());
    }

    [Theory]
    [InlineData(CacheInvalidationMode.Tracking)]
    [InlineData(CacheInvalidationMode.Broadcast)]
    public async Task ExplicitUnsupportedInvalidationMode_IsRejected(CacheInvalidationMode mode)
    {
        await using var connection = new ValkeyConnection(new ValkeyOptions());
        var options = new CachingOptions { Namespace = "valkey-unit" };
        options.Invalidation.Mode = mode;
        Assert.Throws<NotSupportedException>(() =>
            new ValkeyCacheInvalidationChannel(connection, options)
        );
    }

    [Fact]
    public async Task Options_AreValidatedAndSnapshotted()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnection(new ValkeyOptions { CommandTimeout = TimeSpan.Zero })
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnection(new ValkeyOptions { InvalidationQueueCapacity = 0 })
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnection(
                new ValkeyOptions { InvalidationProbeInterval = TimeSpan.FromSeconds(-1) }
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnection(
                new ValkeyOptions { Connection = new ValkeyClientOptions { Port = 0 } }
            )
        );
        var options = new ValkeyOptions();
        await using var connection = new ValkeyConnection(options);
        options.CommandTimeout = TimeSpan.Zero;
        Assert.Equal(TimeSpan.FromSeconds(5), connection.Settings.CommandTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), connection.Settings.InvalidationProbeInterval);
    }

    [Fact]
    public void Codec_RoundTripsUnicodeAndQuotedKeysAndRejectsMalformedMessages()
    {
        // Every item must be a valid cache key (the kernel validates before publishing), so the
        // interesting inputs are the ones JSON has to escape, not whitespace or control characters.
        var input = new CacheInvalidation(
            ["catalog:eu", "каталог", "catalog:\\eu\"/"],
            ["books\"", "music"]
        );
        var decoded = ValkeyInvalidationCodec.Decode(ValkeyInvalidationCodec.Encode(input));
        Assert.NotNull(decoded);
        Assert.Equal(input.Keys, decoded.Keys);
        Assert.Equal(input.Tags, decoded.Tags);
        Assert.False(decoded.FlushAll);
        var flush = ValkeyInvalidationCodec.Decode(
            ValkeyInvalidationCodec.Encode(CacheInvalidation.Flush)
        );
        Assert.NotNull(flush);
        Assert.True(flush.FlushAll);
        Assert.Null(ValkeyInvalidationCodec.Decode(Encoding.UTF8.GetBytes("[1,[],[],false]")));
        foreach (
            var malformed in new[]
            {
                "{}",
                "[2,[],[]]",
                "[1,[null],[]]",
                "[1,[],[3]]",
                "[",
                "[1.2,[],[]]",
            }
        )
            Assert.Null(ValkeyInvalidationCodec.Decode(Encoding.UTF8.GetBytes(malformed)));
        Assert.Null(
            ValkeyInvalidationCodec.Decode(new byte[ValkeyInvalidationCodec.MaxPayloadBytes + 1])
        );
        Assert.Throws<ArgumentException>(() =>
            ValkeyInvalidationCodec.Encode(new CacheInvalidation(new string[10_001], []))
        );
    }

    [Fact]
    public void FailureMapping_DistinguishesTimeoutUnavailableAndServerErrors()
    {
        Assert.Equal(CacheFailureKind.Timeout, ValkeyFailures.Classify(new TimeoutException()));
        Assert.Equal(
            CacheFailureKind.Unavailable,
            ValkeyFailures.Classify(new ValkeyConnectionException("lost", new IOException()))
        );
        Assert.Equal(
            CacheFailureKind.Other,
            ValkeyFailures.Classify(new ValkeyServerException("NOPERM"))
        );
        Assert.Equal(
            LockFailureKind.Other,
            ValkeyFailures.Lock(new ValkeyProtocolException("invalid"), "test").Kind
        );
        Assert.DoesNotContain(
            "secret",
            ValkeyFailures.Cache(new ValkeyServerException("secret"), "test").Message,
            StringComparison.Ordinal
        );
        Assert.Equal(1, ValkeyFailures.Milliseconds(TimeSpan.FromTicks(1)));
    }

    [Fact]
    public async Task DisposedConnection_ReportsTypedFailuresAndCallerCancellation()
    {
        var connection = new ValkeyConnection(new ValkeyOptions());
        var store = new ValkeyCacheStore(connection);
        var locking = new ValkeyLockProvider(connection);
        await connection.DisposeAsync();
        await connection.DisposeAsync();
        Assert.Equal(
            CacheFailureKind.Unavailable,
            (
                await Assert.ThrowsAsync<CacheStoreException>(() =>
                    store.GetAsync("catalog", TestContext.Current.CancellationToken).AsTask()
                )
            ).Kind
        );
        Assert.Equal(
            LockFailureKind.Unavailable,
            (
                await Assert.ThrowsAsync<LockProviderException>(() =>
                    locking
                        .TryAcquireAsync(
                            "catalog",
                            "owner",
                            TimeSpan.FromSeconds(1),
                            TestContext.Current.CancellationToken
                        )
                        .AsTask()
                )
            ).Kind
        );
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            store.RemoveAsync([], canceled.Token).AsTask()
        );
    }

    [Fact]
    public async Task Invalidation_DisposalBeforeStartDoesNotOpenConnection()
    {
        await using var connection = new ValkeyConnection(new ValkeyOptions());
        var channel = new ValkeyCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "valkey-unit" }
        );
        await channel.DisposeAsync();
        await channel.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => channel.Subscribe(_ => { }));
        Assert.Equal(ValkeyConnectionState.NeverConnected, connection.State);
    }

    [Fact]
    public async Task Invalidation_WarningsAreThrottledOnTheInjectedClockAcrossThreads()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var logger = new RecordingLogger<ValkeyCacheInvalidationChannel>();
        await using var connection = new ValkeyConnection(new ValkeyOptions());
        await using var channel = new ValkeyCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "valkey-unit" },
            logger,
            clock
        );

        // The worker, the probe loop and failing handlers warn concurrently; one interval
        // admits exactly one line, even at the clock's first timestamp.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var warners = Enumerable
            .Range(0, 16)
            .Select(_ =>
                Task.Run(
                    async () =>
                    {
                        await gate.Task;
                        channel.Warn("concurrent");
                    },
                    token
                )
            )
            .ToArray();
        gate.SetResult();
        await Task.WhenAll(warners).WaitAsync(TimeSpan.FromSeconds(10), token);
        Assert.Single(logger.Entries);

        // The interval is measured on the injected clock, not the wall clock.
        clock.Advance(ValkeyCacheInvalidationChannel.WarningInterval - TimeSpan.FromTicks(1));
        channel.Warn("inside the interval");
        Assert.Single(logger.Entries);
        clock.Advance(TimeSpan.FromTicks(1));
        channel.Warn("next interval");
        Assert.Equal(2, logger.Entries.Count);
        Assert.All(logger.Entries, entry => Assert.Equal(LogLevel.Warning, entry.Level));
        Assert.Equal(ValkeyConnectionState.NeverConnected, connection.State);
    }

    [Fact]
    public async Task StartupFailFast_ControlsFailureAndReadinessStaysUnhealthy()
    {
        using var endpoint = new System.Net.Sockets.Socket(
            System.Net.Sockets.AddressFamily.InterNetwork,
            System.Net.Sockets.SocketType.Stream,
            System.Net.Sockets.ProtocolType.Tcp
        );
        endpoint.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 0));
        var port = ((System.Net.IPEndPoint)endpoint.LocalEndPoint!).Port;
        var options = new ValkeyOptions
        {
            Connection = new ValkeyClientOptions { Host = "127.0.0.1", Port = port },
            HealthTimeout = TimeSpan.FromMilliseconds(100),
        };
        await using var strict = new ValkeyConnection(options);
        var starter = new ValkeyConnectionStarter(strict);
        await Assert.ThrowsAnyAsync<Exception>(() =>
            starter.StartAsync(TestContext.Current.CancellationToken)
        );
        options.FailFast = false;
        await using var tolerant = new ValkeyConnection(options);
        await new ValkeyConnectionStarter(tolerant).StartAsync(
            TestContext.Current.CancellationToken
        );
        Assert.False(
            (
                await new ValkeyLockProvider(tolerant).CheckHealthAsync(
                    TestContext.Current.CancellationToken
                )
            ).IsHealthy
        );
    }
}

[JsonSerializable(typeof(string))]
internal sealed partial class ValkeyTestJson : JsonSerializerContext;
