using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridMaintenanceTlsTests
{
    [Fact(Timeout = 30000)]
    public async Task CompactionDefersWhileCollectingABoundedBacklogThenPublishesTheSameState()
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        using var fixture = new DataGridPeerFixture();
        var path = Directory.CreateTempSubdirectory("hostloom-maintenance-backlog-").FullName;
        try
        {
            using (var initial = DataGridFileStore.Bootstrap(path, fixture.Configuration, 0))
            {
                var command = DataGridCommittedCommandCodec.Encode(
                    DataGridCommittedCommand.AdvanceTime(default)
                );
                initial.Persist(
                    new(fixture.Configuration, 1, 0, [new RaftEntry(1, 1, command)], 1),
                    token
                );
                for (var i = 0; i < 64; i++)
                    initial.RollGeneration(token);
            }
            using (var store = DataGridFileStore.Open(path, fixture.Configuration, 0))
            {
                await using var runtime = new DataGridPeerRuntime(
                    store,
                    fixture.Credentials(0),
                    fixture.Listeners[0],
                    DataGridPeerFixture.Routes,
                    1,
                    new SilentClock()
                );
                Assert.Equal(RaftCompactionStatus.Busy, await runtime.CompactAsync(token));
                Assert.Equal(RaftCompactionStatus.Published, await runtime.CompactAsync(token));
                Assert.Null(runtime.Failure);
            }
            using var recovered = DataGridFileStore.Open(path, fixture.Configuration, 0);
            Assert.Equal(1, recovered.Image.CommitIndex);
            Assert.Equal(1, recovered.Image.SnapshotIndex);
            Assert.Equal(1, recovered.CollectObsoleteFiles(token));
            Assert.Equal(4, Directory.GetFiles(path).Length);
        }
        finally
        {
            Directory.Delete(path, recursive: true);
        }
    }

    [Fact(Timeout = 90000)]
    public async Task AutomaticExpiryReleasesQuotaAndPreservesRetainedResultsAcrossFailoverAndRestart()
    {
        RequireLinux();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        timeout.CancelAfter(TimeSpan.FromSeconds(80));
        var token = timeout.Token;
        await using var lab = new DataGridSessionPressureTlsTests.Lab(1, mapKeys: 34);
        var leader = await lab.Cluster.Leader(token);
        var client = lab.Client(0, leader);
        var retained = await lab.Open(client, token);
        var rejected = await lab.Open(client, token);
        var ttl = await lab.Open(client, token);
        var writer = await lab.Open(client, token);
        var storedRequest = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(retained)
        );
        var stored = Item(await lab.Send(client, storedRequest, token));
        var rejectedRequest = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(rejected, kind: DataGridMutationKind.PutIfAbsent)
        );
        Assert.Equal(
            DataGridMutationStatus.AlreadyExists,
            Item(await lab.Send(client, rejectedRequest, token)).Status
        );
        var expiring = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            DataGridPeerFixture.Map,
            ttl,
            1,
            DataGridExpiryKind.Relative,
            TimeSpan.FromSeconds(5).Ticks,
            Enumerable
                .Range(0, 33)
                .Select(i => new DataGridMutationItem(
                    DataGridKey.FromUtf8(FormattableString.Invariant($"catalog:expiry:{i}")),
                    [1],
                    default
                ))
                .ToArray()
        );
        var filled = await lab.Send(client, DataGridClientRequest.Mutate(expiring), token);
        Assert.Null(filled.Reply.Failure);
        Assert.All(
            Enumerable.Range(0, 33),
            i => Assert.Equal(DataGridMutationStatus.Stored, filled.Reply.Result!.GetItem(i).Status)
        );
        var key = DataGridKey.FromUtf8("catalog:us");
        var refused = Request(writer, 1, DataGridMutationKind.Put, key, [9]);
        Assert.Equal(
            DataGridMutationStatus.CapacityExceeded,
            Item(await lab.Send(client, refused, token)).Status
        );
        lab.Clock.Advance(TimeSpan.FromSeconds(6));
        // No Reclaim, Compact or read barrier is issued: only automatic replicated cleanup can free the charge.
        await lab.WaitFor(
            () => lab.Cluster.Nodes.All(node => node!.Diagnostics.Maps[0].ChargedKeys == 1),
            token
        );
        Assert.Equal(
            DataGridMutationStatus.CapacityExceeded,
            Item(await lab.Send(client, refused, token)).Status
        );
        var live = Item(
            await lab.Send(client, Request(writer, 2, DataGridMutationKind.Put, key, [9]), token)
        );
        Assert.Equal(DataGridMutationStatus.Stored, live.Status);
        Assert.Equal(
            DataGridMutationStatus.Removed,
            Item(
                await lab.Send(
                    client,
                    Request(writer, 3, DataGridMutationKind.Remove, DataGridPeerFixture.Key, []),
                    token
                )
            ).Status
        );
        await Retained(client);
        await lab.AppliedEverywhere(lab.Cluster.Nodes[leader]!.Status.AppliedIndex, token);
        await lab.Cluster.Stop(leader);
        var next = await lab.Cluster.Leader(token);
        client = lab.Client(0, next);
        await Retained(client);
        lab.Cluster.Start(leader);
        await lab.AppliedEverywhere(lab.Cluster.Nodes[next]!.Status.AppliedIndex, token);
        await lab.StopAll();
        lab.Cluster.StartAll();
        client = lab.Client(0, await lab.Cluster.Leader(token));
        await Retained(client);
        lab.Healthy();

        async Task Retained(GridClient caller)
        {
            Assert.Equal(stored, Item(await lab.Send(caller, storedRequest, token)));
            Assert.Equal(
                DataGridMutationStatus.AlreadyExists,
                Item(await lab.Send(caller, rejectedRequest, token)).Status
            );
            var read = await lab.Send(
                caller,
                DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key, key]),
                token
            );
            Assert.Null(read.Reply.Failure);
            Assert.Null(read.Reply.Read!.GetItem(0));
            Assert.Equal(live.Version, read.Reply.Read.GetItem(1)!.Version);
            Assert.Equal(new byte[] { 9 }, read.Reply.Read.GetItem(1)!.GetValue());
        }
    }

    [Fact(Timeout = 120000)]
    public async Task AutomaticSnapshotAboveEightMiBCatchesUpAStoppedFollowerAndRecovers()
    {
        RequireLinux();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        timeout.CancelAfter(TimeSpan.FromSeconds(110));
        var token = timeout.Token;
        await using var lab = new DataGridSessionPressureTlsTests.Lab(
            1,
            mapBytes: 16 * 1024 * 1024
        );
        var leader = await lab.Cluster.Leader(token);
        var follower = (leader + 1) % 3;
        await lab.Cluster.Stop(follower);
        var client = lab.Client(0, leader);
        var session = await lab.Open(client, token);
        DataGridClientRequest? last = null;
        DataGridMutationResult final = default;
        DataGridKey? key = null;
        for (var batch = 0; batch < 16; batch++)
        {
            var items = Enumerable
                .Range(0, 12)
                .Select(i => new DataGridMutationItem(
                    DataGridKey.FromUtf8(FormattableString.Invariant($"catalog:large:{batch}:{i}")),
                    Enumerable.Repeat((byte)batch, 64 * 1024).ToArray(),
                    default
                ))
                .ToArray();
            last = DataGridClientRequest.Mutate(
                new(
                    DataGridMutationKind.Put,
                    DataGridPeerFixture.Map,
                    session,
                    (ulong)batch + 1,
                    DataGridExpiryKind.Never,
                    0,
                    items
                )
            );
            var result = await lab.Send(client, last, token);
            Assert.Null(result.Reply.Failure);
            Assert.All(
                Enumerable.Range(0, 12),
                i =>
                    Assert.Equal(
                        DataGridMutationStatus.Stored,
                        result.Reply.Result!.GetItem(i).Status
                    )
            );
            final = result.Reply.Result!.GetItem(11);
            key = items[11].Key;
        }
        await lab.WaitFor(
            () =>
                lab.Cluster.Nodes[leader]!.Diagnostics.Storage.PublishedSnapshotBytes
                > 8 * 1024 * 1024,
            token
        );
        lab.Cluster.Start(follower);
        await lab.AppliedEverywhere(final.Version.LogIndex, token);
        Assert.True(lab.Cluster.Nodes[follower]!.Status.SnapshotIndex > 0);
        Assert.NotNull(last);
        Assert.NotNull(key);
        await Check();
        await lab.StopAll();
        lab.Cluster.StartAll();
        client = lab.Client(0, await lab.Cluster.Leader(token));
        await Check();
        await lab.WaitFor(
            () => lab.Cluster.Paths.All(path => Directory.GetFiles(path).Length <= 4),
            token
        );
        lab.Healthy();

        async Task Check()
        {
            var replay = await lab.Send(client, last!, token);
            Assert.Null(replay.Reply.Failure);
            Assert.Equal(final, replay.Reply.Result!.GetItem(11));
            var read = await lab.Send(
                client,
                DataGridClientRequest.Read(DataGridPeerFixture.Map, [key!]),
                token
            );
            Assert.Null(read.Reply.Failure);
            Assert.Equal(final.Version, read.Reply.Read!.GetItem(0)!.Version);
            Assert.Equal(
                Enumerable.Repeat((byte)15, 64 * 1024).ToArray(),
                read.Reply.Read.GetItem(0)!.GetValue()
            );
        }
    }

    private sealed class SilentClock : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch;

        public override long GetTimestamp() => 0;

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        ) => new SilentTimer();

        private sealed class SilentTimer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            public void Dispose() { }

            public ValueTask DisposeAsync() => default;
        }
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS maintenance qualification requires Linux."
        );

    private static DataGridMutationResult Item(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        return outcome.Reply.Result!.GetItem(0);
    }

    private static DataGridClientRequest Request(
        long session,
        ulong number,
        DataGridMutationKind kind,
        DataGridKey key,
        byte[] value
    ) =>
        DataGridClientRequest.Mutate(
            new(
                kind,
                DataGridPeerFixture.Map,
                session,
                number,
                DataGridExpiryKind.Never,
                0,
                [new(key, value, default)]
            )
        );
}
