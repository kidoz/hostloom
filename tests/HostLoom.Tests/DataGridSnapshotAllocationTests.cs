using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridSnapshotAllocationTests
{
    [Fact]
    public void SnapshotEncodingStaysV1WithoutMaterializingSeparateBodyCopies()
    {
        var config = new DataGridBootstrapConfiguration(
            DataGridPeerFixture.Cluster,
            Enumerable
                .Range(0, 3)
                .Select(i => new DataGridMemberConfiguration(
                    new Guid(i + 1, 0, 0, new byte[8]),
                    $"node-{(char)('a' + i)}.example",
                    (ushort)(14001 + i)
                ))
                .ToArray(),
            [
                new(
                    DataGridPeerFixture.Map,
                    "catalog",
                    DataGridMapOwnership.Authoritative,
                    "catalog",
                    "",
                    10000,
                    16 * 1024 * 1024,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
        var machine = Populate(
            config,
            i => DataGridKey.FromUtf8(FormattableString.Invariant($"catalog:{i:D8}"))
        );
        var id = Guid.Parse("50112233-4455-6677-8899-aabbccddeeff");
        _ = machine.SaveSnapshot(id, 1);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var snapshot = machine.SaveSnapshot(id, 1);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Snapshot capture: {allocated} allocated bytes; snapshot {snapshot.Descriptor.ByteCount} bytes."
        );
        // Captured from the prior v1 encoder with this fixed input; protects byte compatibility.
        Assert.Equal(
            "DF475C01C5AB6F79BE2E26D1A9DB6760071F76BD4D9B25BCB5ED283B1FEC9BBA",
            Convert.ToHexString(SHA256.HashData(snapshot.GetContent()))
        );
        // One exact owned buffer plus small per-key metadata, rather than full-image copies.
        Assert.True(allocated < 2 * snapshot.Descriptor.ByteCount);
    }

    [Fact]
    public void ValidationReuseRemainsBoundToSnapshotAndConfigurationIdentity()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var machine = new DataGridLocalStateMachine(config);
        machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        Assert.Throws<ArgumentNullException>(() =>
            new RaftDurableImage(null!, 1, commitIndex: 1, snapshot: snapshot)
        );
        void Validate(
            DataGridBootstrapConfiguration configuration,
            DataGridLocalSnapshot candidate
        ) => _ = new RaftDurableImage(configuration, 1, commitIndex: 1, snapshot: candidate);
        Validate(config, snapshot);
        var members = Enumerable.Range(0, 3).Select(config.GetMember).ToArray();
        var maps = Enumerable.Range(0, config.MapCount).Select(config.GetMap).ToArray();
        var other = new DataGridBootstrapConfiguration(Guid.NewGuid(), members, maps);
        Assert.Throws<InvalidDataException>(() => Validate(other, snapshot));
        Validate(new(config.IncarnationId, members, maps), snapshot);
        Validate(config, snapshot);
        var damaged = snapshot.GetContent();
        damaged[0] ^= 1;
        Assert.Throws<InvalidDataException>(() =>
            Validate(config, new(snapshot.Descriptor, damaged))
        );
        var consistentDigest = new DataGridSnapshotDescriptor(
            snapshot.Descriptor.SnapshotId,
            1,
            1,
            damaged.Length,
            SHA256.HashData(damaged)
        );
        Assert.Throws<InvalidDataException>(() => Validate(config, new(consistentDigest, damaged)));
        Validate(config, snapshot);
    }

    [Fact]
    public void ConcurrentFirstValidationPreservesSnapshotPublicCopyIsolation()
    {
        using var fixture = new DataGridPeerFixture();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var original = snapshot.GetContent();
        Parallel.For(
            0,
            32,
            _ =>
            {
                snapshot.GetContent()[0] ^= 1;
                var image = new RaftDurableImage(
                    fixture.Configuration,
                    1,
                    commitIndex: 1,
                    snapshot: snapshot
                );
                Assert.Same(snapshot, image.Snapshot);
            }
        );
        Assert.Equal(original, snapshot.GetContent());
    }

    [Fact]
    public void ReusingValidatedSnapshotDoesNotRebuildItsApplicationForEveryDurableImage()
    {
        using var fixture = new DataGridPeerFixture(10000, 16 * 1024 * 1024);
        var machine = Populate(fixture.Configuration, DataGridFeasibilityCase.Key);
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        _ = new RaftDurableImage(
            fixture.Configuration,
            1,
            commitIndex: machine.LastAppliedIndex,
            snapshot: snapshot
        );
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 8; i++)
            _ = new RaftDurableImage(
                fixture.Configuration,
                1,
                commitIndex: machine.LastAppliedIndex,
                snapshot: snapshot
            );
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Eight same-snapshot durable images: {allocated} allocated bytes; snapshot {snapshot.Descriptor.ByteCount} bytes."
        );
        // An algorithmic allocation bound, not a service memory/latency target: unchanged
        // durable metadata must not deserialize or copy a complete application image.
        Assert.True(
            allocated < snapshot.Descriptor.ByteCount,
            $"Reusing a snapshot allocated {allocated} bytes for {snapshot.Descriptor.ByteCount} bytes of state."
        );
    }

    private static DataGridLocalStateMachine Populate(
        DataGridBootstrapConfiguration config,
        Func<int, DataGridKey> key
    )
    {
        var machine = new DataGridLocalStateMachine(config);
        var opened = machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                DataGridPeerFixture.Nonce,
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        ulong sequence = 1;
        for (var first = 0; first < 10000; first += 128)
        {
            var items = Enumerable
                .Range(first, Math.Min(128, 10000 - first))
                .Select(i => new DataGridMutationItem(key(i), new byte[1024]))
                .ToArray();
            var result = machine.Apply(
                machine.LastAppliedIndex + 1,
                DataGridCommittedCommand.Mutate(
                    DataGridPeerFixture.Principal,
                    new(
                        DataGridMutationKind.Put,
                        DataGridPeerFixture.Map,
                        opened.SessionNumber,
                        sequence++,
                        DataGridExpiryKind.Never,
                        0,
                        items
                    ),
                    default
                )
            );
            Assert.All(
                Enumerable.Range(0, result.ItemCount),
                i => Assert.Equal(DataGridMutationStatus.Stored, result.GetItem(i).Status)
            );
        }
        return machine;
    }
}
