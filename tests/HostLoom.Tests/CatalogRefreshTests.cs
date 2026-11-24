using System.Collections.Concurrent;
using System.Collections.Immutable;
using HostLoom.Actors;
using HostLoom.Benchmarks;
using HostLoom.Conformance;
using HostLoom.Examples.Actors;
using Xunit;

namespace HostLoom.Tests;

public sealed class CatalogRefreshTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan CallerBudget = TimeSpan.FromMilliseconds(25);
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    // Only the first EU load of the selected version waits. Later attempts can retry normally.
    private sealed class Source(int pausedVersion)
    {
        private readonly ConcurrentDictionary<(string, int), int> _loads = new();
        private readonly TaskCompletionSource _entered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource<bool> _resume = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        internal Task WaitForLoadAsync() => _entered.Task.WaitAsync(Bound, Token);

        internal void Finish(bool success) => _resume.SetResult(success);

        internal int Reads(string region, int version) =>
            _loads.GetValueOrDefault((region, version));

        internal int TotalReads => _loads.Values.Sum();

        internal async ValueTask<ImmutableArray<string>> LoadAsync(
            string region,
            int version,
            CancellationToken stopping
        )
        {
            var count = _loads.AddOrUpdate((region, version), 1, static (_, n) => n + 1);
            if (region == "eu" && version == pausedVersion && count == 1)
            {
                _entered.SetResult();
                if (!await _resume.Task.WaitAsync(Bound, stopping))
                    throw new IOException("Catalog source unavailable.");
            }
            return ["notebook", FormattableString.Invariant($"{region}-{version}")];
        }
    }

    private static Task<CatalogReply> SendAsync(
        RegionalCatalogClient client,
        CatalogRequest request
    ) => client.SendAsync(request, Bound).AsTask().WaitAsync(Bound, Token);

    private static void AssertPublished(CatalogReply reply, string region, int version)
    {
        Assert.Equal(CatalogStatus.Published, reply.Status);
        Assert.NotNull(reply.Snapshot);
        Assert.Equal(region, reply.Snapshot.Region);
        Assert.Equal(version, reply.Snapshot.Version);
        Assert.Equal(
            ["notebook", FormattableString.Invariant($"{region}-{version}")],
            reply.Snapshot.Items.ToArray()
        );
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OverlappingRefreshesShareOneLoadWhileAnotherRegionProgresses(bool actors)
    {
        var source = new Source(1);
        await using var workers = new CatalogRefreshWorkers(
            ["eu", "us"],
            actors,
            source.LoadAsync,
            Bound,
            new ManualTimeProvider()
        );
        var request = CatalogRequest.Refresh(1);
        var first = SendAsync(workers["eu"], request);
        await source.WaitForLoadAsync();
        var second = SendAsync(workers["eu"], request);

        AssertPublished(await SendAsync(workers["us"], request), "us", 1);
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);
        source.Finish(true);
        var replies = await Task.WhenAll(first, second);

        AssertPublished(replies[0], "eu", 1);
        Assert.Equal(CatalogStatus.Published, replies[1].Status);
        Assert.Same(replies[0].Snapshot, replies[1].Snapshot);
        Assert.Equal(1, source.Reads("eu", 1));
        Assert.Equal(1, source.Reads("us", 1));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OverlappingFailuresPreservePublicationAndAllowRetry(bool actors)
    {
        var source = new Source(2);
        await using var workers = new CatalogRefreshWorkers(
            ["eu"],
            actors,
            source.LoadAsync,
            Bound,
            new ManualTimeProvider()
        );
        var client = workers["eu"];
        var original = await SendAsync(client, CatalogRequest.Refresh(1));
        AssertPublished(original, "eu", 1);
        var target = CatalogRequest.Refresh(2);
        var first = SendAsync(client, target);
        await source.WaitForLoadAsync();
        var second = SendAsync(client, target);
        source.Finish(false);
        foreach (var reply in await Task.WhenAll(first, second))
        {
            Assert.Equal(CatalogStatus.SourceUnavailable, reply.Status);
            Assert.Same(original.Snapshot, reply.Snapshot);
        }
        var current = await SendAsync(client, CatalogRequest.Read);
        AssertPublished(current, "eu", 1);
        Assert.Same(original.Snapshot, current.Snapshot);
        Assert.Equal(1, source.Reads("eu", 2));

        var retry = await SendAsync(client, target);
        AssertPublished(retry, "eu", 2);
        var stale = await SendAsync(client, CatalogRequest.Refresh(1));
        Assert.Equal(CatalogStatus.Published, stale.Status);
        Assert.Same(retry.Snapshot, stale.Snapshot);
        Assert.Equal(2, source.Reads("eu", 2));
        Assert.Equal(3, source.TotalReads);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task TimedOutCallerCanReconcileAfterTheSourceFinishes(bool actors, bool succeeds)
    {
        var source = new Source(2);
        var clock = new ManualTimeProvider();
        await using var workers = new CatalogRefreshWorkers(
            ["eu"],
            actors,
            source.LoadAsync,
            Bound,
            clock
        );
        var client = workers["eu"];
        var original = await SendAsync(client, CatalogRequest.Refresh(1));
        var caller = client.SendAsync(CatalogRequest.Refresh(2), CallerBudget).AsTask();
        await source.WaitForLoadAsync();
        clock.Advance(CallerBudget);
        await Assert.ThrowsAsync<TimeoutException>(() => caller).WaitAsync(Bound, Token);

        var earlyCheck = client.CheckVersionAsync(2, CallerBudget).AsTask();
        clock.Advance(CallerBudget);
        await Assert.ThrowsAsync<TimeoutException>(() => earlyCheck).WaitAsync(Bound, Token);

        var check = client.CheckVersionAsync(2, Bound).AsTask();
        Assert.False(check.IsCompleted);
        source.Finish(succeeds);
        var current = await check.WaitAsync(Bound, Token);

        if (succeeds)
            AssertPublished(current, "eu", 2);
        else
        {
            Assert.Equal(CatalogStatus.BelowMinimum, current.Status);
            Assert.Same(original.Snapshot, current.Snapshot);
        }
        var olderTarget = await client.CheckVersionAsync(1, Bound).AsTask().WaitAsync(Bound, Token);
        AssertPublished(olderTarget, "eu", succeeds ? 2 : 1);
        Assert.Same(current.Snapshot, olderTarget.Snapshot);
        Assert.Equal(1, source.Reads("eu", 2));
        Assert.Equal(2, source.TotalReads);
    }

    [Fact]
    public async Task RefusedRequestsSkipTheHandlerAndCanRetryAfterTheMailboxDrains()
    {
        var clock = new ManualTimeProvider();
        await using var system = new ActorSystem(2, 1, Bound, clock);
        var first = CatalogRequest.Refresh(1);
        var second = CatalogRequest.Refresh(2);
        var third = CatalogRequest.Refresh(3);
        var probe = new ActorTestProbe<CatalogRequest, CatalogReply>(
            3,
            Bound,
            request =>
                new(
                    CatalogStatus.Published,
                    new CatalogSnapshot("eu", request.MinimumVersion.GetValueOrDefault(), [])
                )
        );
        var handle = system.Spawn<CatalogRequest, CatalogReply>(() => probe);
        var client = new RegionalCatalogClient(
            (request, timeout) => handle.Ref.AskAsync(request, timeout),
            Bound,
            clock
        );
        var peer = RegionalCatalogClient.Spawn(system, "us", new Source(1).LoadAsync, Bound, clock);

        var active = SendAsync(client, first);
        Task<CatalogReply> queued;
        Task<CatalogReply> duplicate;
        try
        {
            await probe.WaitForFirstEntryAsync(Token);
            queued = SendAsync(client, second);
            duplicate = SendAsync(client, second);
            foreach (var request in new[] { third, CatalogRequest.Read })
            {
                var refused = await SendAsync(client, request);
                Assert.Equal(CatalogStatus.Overloaded, refused.Status);
                Assert.Null(refused.Snapshot);
            }
            var check = await client.CheckVersionAsync(3, Bound).AsTask().WaitAsync(Bound, Token);
            Assert.Equal(CatalogStatus.Overloaded, check.Status);
            Assert.Null(check.Snapshot);
            AssertPublished(await SendAsync(peer, first), "us", 1);
            Assert.Equal([first], probe.Entries);
        }
        finally
        {
            probe.Release();
        }

        Assert.Equal(CatalogStatus.Published, (await active).Status);
        var admitted = await queued;
        Assert.Equal(CatalogStatus.Published, admitted.Status);
        Assert.Equal(admitted, await duplicate);
        Assert.Equal([first, second], probe.Entries);
        var retry = await SendAsync(client, third);
        Assert.Equal(CatalogStatus.Published, retry.Status);
        Assert.Equal(3, retry.Snapshot?.Version);
        await handle.StopAsync(Token).WaitAsync(Bound, Token);
        foreach (var request in new[] { third, CatalogRequest.Read })
        {
            var refused = await SendAsync(client, request);
            Assert.Equal(CatalogStatus.Unavailable, refused.Status);
            Assert.Null(refused.Snapshot);
        }
        var stoppedCheck = await client
            .CheckVersionAsync(3, Bound)
            .AsTask()
            .WaitAsync(Bound, Token);
        Assert.Equal(CatalogStatus.Unavailable, stoppedCheck.Status);
        Assert.Null(stoppedCheck.Snapshot);
        Assert.Equal([first, second, third], probe.Entries);
    }

    [Fact]
    public async Task ActorShutdownDrainsAbandonedRefreshAndQueuedReadAndClosesAdmission()
    {
        var clock = new ManualTimeProvider();
        var source = new Source(1);
        await using var system = new ActorSystem(1, 4, Bound, clock);
        var client = RegionalCatalogClient.Spawn(system, "eu", source.LoadAsync, Bound, clock);
        var caller = client.SendAsync(CatalogRequest.Refresh(1), CallerBudget).AsTask();
        await source.WaitForLoadAsync();
        clock.Advance(CallerBudget);
        await Assert.ThrowsAsync<TimeoutException>(() => caller).WaitAsync(Bound, Token);

        var read = SendAsync(client, CatalogRequest.Read);
        var stop = system.StopAsync(Token);
        Assert.False(read.IsCompleted);
        Assert.False(stop.IsCompleted);
        var rejected = await SendAsync(client, CatalogRequest.Read);
        Assert.Equal(CatalogStatus.Unavailable, rejected.Status);
        Assert.Null(rejected.Snapshot);
        source.Finish(true);

        AssertPublished(await read, "eu", 1);
        await stop.WaitAsync(Bound, Token);
        Assert.Equal(1, source.TotalReads);
    }
}
