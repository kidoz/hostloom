using System.Security.Authentication;
using System.Text.Json;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Locking;
using HostLoom.Valkey;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(ValkeyDeploymentTests))]
[CollectionDefinition(nameof(ValkeyDeploymentTests), DisableParallelization = true)]
public sealed class ValkeyDeploymentTests
{
    public static bool Enabled =>
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(ValkeyDeploymentFixture.Variable));
    private const string Skip =
        "Run scripts/test-valkey-deployment.py for an owned TLS/ACL restart fixture.";
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Theory(Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task TlsAcl_AllowCacheScriptsLocksAndInvalidationButDenyOtherKeysAndCommands(
        ValkeyProtocol protocol
    )
    {
        using var fixture = new ValkeyDeploymentFixture();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        var token = timeout.Token;
        await using var connection = new ValkeyConnection(fixture.Options(protocol));
        var store = new ValkeyCacheStore(connection);
        var locking = new ValkeyLockProvider(connection);
        await WaitUntilAsync(async () => (await store.CheckHealthAsync(token)).IsHealthy, token);
        var ns = "deployment-" + Guid.NewGuid().ToString("N");
        var key = ns + ":cache:data:catalog";
        var tag = ns + ":cache:tag:books";
        await using var channel = new ValkeyCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = ns }
        );
        var delivered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var subscription = channel.Subscribe(message =>
        {
            if (message.Keys.Contains("catalog"))
                delivered.TrySetResult();
        });
        await channel.StartAsync(token);
        try
        {
            Assert.True(
                await store.SetIfAbsentAsync(
                    key,
                    new byte[] { 0, 255, 13 },
                    TimeSpan.FromSeconds(30),
                    [tag],
                    token
                )
            );
            Assert.False(
                await store.SetIfAbsentAsync(
                    key,
                    new byte[] { 1 },
                    TimeSpan.FromSeconds(30),
                    [tag],
                    token
                )
            );
            Assert.Equal(
                new byte[] { 0, 255, 13 },
                (await store.GetAsync(key, token))!.Value.Payload.ToArray()
            );
            Assert.Single(await store.GetManyAsync([key, key + ":missing"], token));
            await store.RemoveByTagAsync(tag, token);
            Assert.Null(await store.GetAsync(key, token));
            Assert.True(
                await locking.TryAcquireAsync(
                    ns + ":lock:catalog",
                    "owner",
                    TimeSpan.FromSeconds(30),
                    token
                )
            );
            Assert.True(
                await locking.ExtendAsync(
                    ns + ":lock:catalog",
                    "owner",
                    TimeSpan.FromSeconds(30),
                    token
                )
            );
            Assert.True(await locking.ReleaseAsync(ns + ":lock:catalog", "owner", token));
            await channel.PublishAsync(new CacheInvalidation(["catalog"], []), token);
            await delivered.Task.WaitAsync(token);
            var denied = await Assert.ThrowsAsync<CacheStoreException>(() =>
                store.GetAsync("outside:catalog", token).AsTask()
            );
            Assert.Equal(CacheFailureKind.Other, denied.Kind);
            Assert.Equal(
                "NOPERM",
                Assert.IsType<ValkeyServerException>(denied.InnerException).ErrorCode
            );
            await using var raw = await ValkeyClient.ConnectAsync(
                fixture.Options(protocol).Connection,
                token
            );
            var command = await Assert.ThrowsAsync<ValkeyServerException>(() =>
                raw.ExecuteAsync(new ValkeyCommand("CONFIG", "GET", "port"), token)
            );
            Assert.Equal("NOPERM", command.ErrorCode);
            var publish = await Assert.ThrowsAsync<ValkeyServerException>(() =>
                raw.ExecuteAsync(new ValkeyCommand("PUBLISH", "outside:invalidate", "value"), token)
            );
            Assert.Equal("NOPERM", publish.ErrorCode);
            Assert.True((await store.CheckHealthAsync(token)).IsHealthy);
        }
        finally
        {
            await store.RemoveAsync([key, tag, ns + ":lock:catalog"], token);
        }
    }

    [Theory(Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(false, false)]
    [InlineData(true, true)]
    public async Task Tls_RejectsUntrustedCertificateAndMismatchedHostname(
        bool trustRoot,
        bool wrongHost
    )
    {
        using var fixture = new ValkeyDeploymentFixture();
        await using var connection = new ValkeyConnection(
            fixture.Options(trustRoot: trustRoot, wrongHost: wrongHost)
        );
        var failure = await Assert.ThrowsAsync<CacheStoreException>(() =>
            new ValkeyCacheStore(connection).GetAsync("deployment-catalog", Token).AsTask()
        );
        Assert.True(HasCause<AuthenticationException>(failure));
        Assert.Equal(ValkeyConnectionState.Faulted, connection.State);
    }

    [Fact(Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task Acl_RejectsWrongCredentialsAndDoesNotReportLockContention()
    {
        using var fixture = new ValkeyDeploymentFixture();
        await using var connection = new ValkeyConnection(fixture.Options(wrongPassword: true));
        var failure = await Assert.ThrowsAsync<LockProviderException>(() =>
            new ValkeyLockProvider(connection)
                .TryAcquireAsync(
                    "deployment-lock:catalog",
                    "owner",
                    TimeSpan.FromSeconds(10),
                    Token
                )
                .AsTask()
        );
        Assert.Equal(LockFailureKind.Other, failure.Kind);
        Assert.True(HasCause<ValkeyServerException>(failure));
        Assert.Equal(ValkeyConnectionState.Faulted, connection.State);
    }

    [Theory(Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task Restart_RecoversSameTlsAclClientsReloadsScriptsAndObservesLostLease(
        ValkeyProtocol protocol
    )
    {
        using var fixture = new ValkeyDeploymentFixture();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        var token = timeout.Token;
        await using var connection = new ValkeyConnection(fixture.Options(protocol));
        var store = new ValkeyCacheStore(connection);
        var provider = new ValkeyLockProvider(connection);
        await WaitUntilAsync(async () => (await store.CheckHealthAsync(token)).IsHealthy, token);
        var ns = "deployment-" + Guid.NewGuid().ToString("N");
        var clock = new ManualTimeProvider();
        var options = new CachingOptions { Namespace = ns };
        var serializer = new SystemTextJsonCacheValueSerializer(
            new JsonSerializerOptions { TypeInfoResolver = ValkeyBackendJson.Default }
        );
        await using var channel = new ValkeyCacheInvalidationChannel(connection, options);
        await using var reader = new TieredCache(options, store, serializer, channel, clock);
        var writerOptions = new CachingOptions { Namespace = ns };
        writerOptions.L1.Enabled = false;
        await using var writer = new TieredCache(writerOptions, store, serializer, channel, clock);
        var resumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = channel.Subscribe(message =>
        {
            if (message.Keys.Contains("resumed"))
                resumed.TrySetResult();
        });
        await channel.StartAsync(token);
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(5))
        {
            LocalExpiration = TimeSpan.FromSeconds(2),
        };
        await reader.SetAsync("catalog", "before", entry, token);
        Assert.Equal("before", (await writer.TryGetAsync<string>("catalog", token)).Value);
        await using var locking = new DistributedLock(
            new LockingOptions { Namespace = ns },
            provider,
            clock
        );
        await using var oldLease = await locking.TryAcquireAsync(
            "catalog",
            new LockOptions { Lease = TimeSpan.FromSeconds(30), AutoExtend = false },
            token
        );
        Assert.NotNull(oldLease);
        Assert.True(await oldLease.ExtendAsync(TimeSpan.FromSeconds(30), token));
        var validatedBefore = fixture.CertificateValidations;
        try
        {
            await fixture.SetRunningAsync(false, token);
            Assert.False((await store.CheckHealthAsync(token)).IsHealthy);
            Assert.False((await provider.CheckHealthAsync(token)).IsHealthy);
            var failedLock = await Assert.ThrowsAsync<LockProviderException>(() =>
                provider
                    .TryAcquireAsync(
                        ns + ":lock:unavailable",
                        "owner",
                        TimeSpan.FromSeconds(10),
                        token
                    )
                    .AsTask()
            );
            Assert.True(failedLock.Kind is LockFailureKind.Unavailable or LockFailureKind.Timeout);
            Assert.Equal(
                "factory",
                await reader.GetOrCreateAsync(
                    "outage",
                    _ => ValueTask.FromResult<string?>("factory"),
                    TimeSpan.FromSeconds(30),
                    token
                )
            );
            Assert.Equal(CacheTier.L1, (await reader.TryGetAsync<string>("catalog", token)).Tier);
            Assert.False(await oldLease.ExtendAsync(TimeSpan.FromSeconds(30), token));
            await fixture.SetRunningAsync(true, token);
            await WaitUntilAsync(
                async () => (await store.CheckHealthAsync(token)).IsHealthy,
                token
            );
            Assert.True((await provider.CheckHealthAsync(token)).IsHealthy);
            // Persistence was disabled: the old value and script cache are gone. A read reloads its script.
            Assert.False((await writer.TryGetAsync<string>("catalog", token)).Found);
            Assert.False((await writer.TryGetAsync<string>("outage", token)).Found);
            await writer.SetAsync("catalog", "after", entry, token);
            await using (
                var databaseProbe = await ValkeyClient.ConnectAsync(
                    fixture.Options(protocol).Connection,
                    token
                )
            )
            {
                Assert.False(
                    (
                        await databaseProbe.ExecuteAsync(
                            new ValkeyCommand("GET", ns + ":cache:data:catalog"),
                            token
                        )
                    ).IsNull
                );
                await databaseProbe.ExecuteAsync(new ValkeyCommand("SELECT", 0), token);
                Assert.True(
                    (
                        await databaseProbe.ExecuteAsync(
                            new ValkeyCommand("GET", ns + ":cache:data:catalog"),
                            token
                        )
                    ).IsNull
                );
            }
            Assert.Equal("before", (await reader.TryGetAsync<string>("catalog", token)).Value);
            clock.Advance(TimeSpan.FromSeconds(3));
            var refreshed = await reader.TryGetAsync<string>("catalog", token);
            Assert.Equal("after", refreshed.Value);
            Assert.Equal(CacheTier.L2, refreshed.Tier);
            // An old handle must detect ownership loss, and cannot remove the replacement owner's lease.
            Assert.True(
                await provider.TryAcquireAsync(
                    ns + ":lock:catalog",
                    "replacement",
                    TimeSpan.FromSeconds(30),
                    token
                )
            );
            Assert.False(await oldLease.ExtendAsync(TimeSpan.FromSeconds(30), token));
            Assert.False(oldLease.IsHeld);
            Assert.True(oldLease.LostToken.IsCancellationRequested);
            await oldLease.DisposeAsync();
            Assert.False(
                await provider.TryAcquireAsync(
                    ns + ":lock:catalog",
                    "third-owner",
                    TimeSpan.FromSeconds(30),
                    token
                )
            );
            Assert.True(await provider.ReleaseAsync(ns + ":lock:catalog", "replacement", token));
            await WaitUntilAsync(
                async () =>
                {
                    await channel.PublishAsync(new CacheInvalidation(["resumed"], []), token);
                    return resumed.Task.IsCompleted;
                },
                token
            );
            Assert.True(channel.IsSubscribed);
            Assert.True(fixture.CertificateValidations >= validatedBefore + 2);
            await writer.RemoveAsync("catalog", token);
            await WaitUntilAsync(
                async () => !(await reader.TryGetAsync<string>("catalog", token)).Found,
                token
            );
        }
        finally
        {
            // Even an assertion failure leaves the owned fixture running for the remaining cases.
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(25));
            await fixture.SetRunningAsync(true, cleanup.Token);
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> predicate, CancellationToken token)
    {
        while (!await predicate())
            await Task.Delay(TimeSpan.FromMilliseconds(25), token);
    }

    private static bool HasCause<T>(Exception exception)
        where T : Exception
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
            if (current is T)
                return true;
        return false;
    }
}
