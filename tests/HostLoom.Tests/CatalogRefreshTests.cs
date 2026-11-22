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
        Assert.False(reply.SourceUnavailable);
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
        Assert.False(replies[1].SourceUnavailable);
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
            Assert.True(reply.SourceUnavailable);
            Assert.Same(original.Snapshot, reply.Snapshot);
        }
        var current = await SendAsync(client, CatalogRequest.Read);
        AssertPublished(current, "eu", 1);
        Assert.Same(original.Snapshot, current.Snapshot);
        Assert.Equal(1, source.Reads("eu", 2));

        var retry = await SendAsync(client, target);
        AssertPublished(retry, "eu", 2);
        var stale = await SendAsync(client, CatalogRequest.Refresh(1));
        Assert.False(stale.SourceUnavailable);
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

        var read = SendAsync(client, CatalogRequest.Read);
        Assert.False(read.IsCompleted);
        source.Finish(succeeds);
        var current = await read;

        AssertPublished(current, "eu", succeeds ? 2 : 1);
        if (!succeeds)
            Assert.Same(original.Snapshot, current.Snapshot);
        Assert.Equal(1, source.Reads("eu", 2));
        Assert.Equal(2, source.TotalReads);
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
        var rejected = await Assert.ThrowsAsync<ActorAdmissionException>(() =>
            SendAsync(client, CatalogRequest.Read)
        );
        Assert.Equal(ActorAdmission.Stopped, rejected.Reason);
        source.Finish(true);

        AssertPublished(await read, "eu", 1);
        await stop.WaitAsync(Bound, Token);
        Assert.Equal(1, source.TotalReads);
    }
}
