using System.Text;
using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridP0CompletionTests
{
    private static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly DataGridMapPolicy Policy = new(1, 1, 2, 1, 1);

    [Fact]
    public void CanonicalConfigurationMatchesIndependentDigestAndSortsMembers()
    {
        var members = Members();
        var first = new DataGridBootstrapConfiguration(Cluster, members, [Catalog()]);
        Array.Reverse(members);
        var second = new DataGridBootstrapConfiguration(Cluster, members, [Catalog()]);
        Assert.Equal(206, first.Encode().Length);
        Assert.Equal(
            "484C47460001000010112233445566778899AABBCCDDEEFF00030001",
            Convert.ToHexString(first.Encode().AsSpan(0, 28))
        );
        // Golden SHA-256 generated independently using Python struct/uuid/hashlib.
        Assert.Equal(
            "5AA4960AD9D6B49396A4DE935FD69DE4DA08D0FC987E845B627C74A82696B928",
            Convert.ToHexString(first.ComputeDigest())
        );
        Assert.Equal(first.Encode(), second.Encode());
        members[0] = new(Guid.NewGuid(), "replacement.example", 7400);
        first.Encode()[0] = 0;
        Assert.Equal(first.ComputeDigest(), second.ComputeDigest());
    }

    [Fact]
    public void CanonicalMapOrderingAndQuotaChangesAffectOnlyTheIntendedIdentity()
    {
        var other = new DataGridMapConfiguration(
            new("10112233-4455-6677-8899-aabbccddeeff"),
            "inventory",
            DataGridMapOwnership.Authoritative,
            "inventory-service",
            "",
            10,
            10000,
            Policy
        );
        var first = new DataGridBootstrapConfiguration(Cluster, Members(), [other, Catalog()]);
        var second = new DataGridBootstrapConfiguration(Cluster, Members(), [Catalog(), other]);
        Assert.Equal(first.Encode(), second.Encode());
        Assert.Equal(Map, first.GetMap(0).MapId);
        var changed = new DataGridBootstrapConfiguration(
            Cluster,
            Members(),
            [Catalog(keys: 1001), other]
        );
        Assert.NotEqual(first.ComputeDigest(), changed.ComputeDigest());
    }

    [Fact]
    public void BootstrapRejectsMissingDuplicateAndOversubscribedDeclarations()
    {
        var members = Members();
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(Cluster, members[..2], [Catalog()])
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(Cluster, members, [])
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(
                Cluster,
                [members[0], members[0], members[2]],
                [Catalog()]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(
                Cluster,
                [
                    members[0],
                    new(members[1].MemberId, members[0].DnsName, members[0].Port),
                    members[2],
                ],
                [Catalog()]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(Cluster, members, [Catalog(), Catalog()])
        );
        var other = new DataGridMapConfiguration(
            Guid.NewGuid(),
            "inventory",
            DataGridMapOwnership.Authoritative,
            "inventory-service",
            "",
            1,
            1,
            Policy
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(Cluster, members, [Catalog(keys: 100000), other])
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(
                Cluster,
                members,
                [Catalog(bytes: 256L * 1024 * 1024), other]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridBootstrapConfiguration(Guid.Empty, members, [Catalog()])
        );
    }

    [Fact]
    public void MapPolicyOwnerAndTextValidationRejectUnsupportedDeclarations()
    {
        Assert.Throws<ArgumentException>(() => Catalog(policy: new DataGridMapPolicy()));
        Assert.Throws<ArgumentException>(() => Catalog(policy: Policy with { Eviction = 2 }));
        Assert.Throws<ArgumentException>(() => Catalog(policy: Policy with { Consistency = 2 }));
        Assert.Throws<ArgumentException>(() => Catalog(policy: Policy with { Durability = 2 }));
        Assert.Throws<ArgumentException>(() => Catalog(policy: Policy with { Expiration = 3 }));
        Assert.Throws<ArgumentException>(() => Catalog(policy: Policy with { Schema = 2 }));
        Assert.Throws<ArgumentException>(() =>
            new DataGridMapConfiguration(
                Map,
                "catalog",
                DataGridMapOwnership.Derived,
                "catalog-service",
                "",
                1,
                1,
                Policy
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridMapConfiguration(
                Map,
                "catalog",
                DataGridMapOwnership.Authoritative,
                "catalog-service",
                "loader",
                1,
                1,
                Policy
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridMapConfiguration(
                Map,
                "catalog\0",
                DataGridMapOwnership.Authoritative,
                "owner",
                "",
                1,
                1,
                Policy
            )
        );
        Assert.Throws<EncoderFallbackException>(() =>
            new DataGridMapConfiguration(
                Map,
                "\ud800",
                DataGridMapOwnership.Authoritative,
                "owner",
                "",
                1,
                1,
                Policy
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridMapConfiguration(
                Map,
                new string('é', 65),
                DataGridMapOwnership.Authoritative,
                "owner",
                "",
                1,
                1,
                Policy
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() => Catalog(keys: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Catalog(bytes: 0));
    }

    [Theory]
    [InlineData("Node-A.example")]
    [InlineData("127.0.0.1")]
    [InlineData("node.example.")]
    [InlineData("-node.example")]
    [InlineData("node..example")]
    [InlineData("node_name.example")]
    [InlineData("nöde.example")]
    public void NoncanonicalMemberEndpointIsRejected(string name) =>
        Assert.Throws<ArgumentException>(() => new DataGridMemberConfiguration(Map, name, 7400));

    [Fact]
    public void ByteTreeOrderingIsStableForOpaqueKeysPrefixesAndEquivalentCopies()
    {
        var tree = new SortedDictionary<DataGridKey, int>(DataGridKeyComparer.Instance)
        {
            [new([255])] = 4,
            [new([0, 1])] = 2,
            [new([1])] = 3,
            [new([0])] = 1,
        };
        Assert.Equal([1, 2, 3, 4], tree.Values);
        Assert.Equal(2, tree[new([0, 1])]);
        Assert.Equal(0, DataGridKeyComparer.Instance.Compare(new([0, 1]), new([0, 1])));
        Assert.True(DataGridKeyComparer.Instance.Compare(null, new([0])) < 0);
        Assert.True(DataGridKeyComparer.Instance.Compare(new([0]), null) > 0);
    }

    [Fact]
    public void CompareRemoveMatchesIndependentFixtureAndBindsExpectedVersion()
    {
        var command = Delete(new(Cluster, 10));
        var encoded = command.Encode();
        Assert.Equal(
            "484C474F0001000500112233445566778899AABBCCDDEEFF0000000000000001000000000000000200000001000000000000000000016B10112233445566778899AABBCCDDEEFF000000000000000A00000000",
            Convert.ToHexString(encoded)
        );
        Assert.Equal(
            "44F7422B080E969206F7671CA0057BC6434A9AC8C8214E19F7886883D9F648F9",
            Convert.ToHexString(command.ComputeDigest())
        );
        Assert.Equal(encoded, DataGridMutationCommand.ReadFrom(Frame(encoded)).Encode());
        Assert.NotEqual(command.ComputeDigest(), Delete(new(Cluster, 11)).ComputeDigest());
        Assert.Throws<InvalidDataException>(() =>
            DataGridMutationCommand.ReadFrom(Frame(Delete(new(Map, 10)).Encode()))
        );
        for (var i = 0; i < encoded.Length; i++)
        {
            var prefix = encoded[..i];
            Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(prefix));
        }
        Assert.Throws<InvalidDataException>(() =>
            DataGridMutationCommand.ReadFrom([.. encoded, 0])
        );
        Assert.Throws<ArgumentException>(() => Delete(default));
        Assert.Throws<ArgumentException>(() =>
            Delete(new(Cluster, 10), DataGridExpiryKind.Relative, 1)
        );
        Assert.Throws<ArgumentException>(() => Delete(new(Cluster, 10), value: new byte[] { 1 }));
        var malformed = (byte[])encoded.Clone();
        malformed.AsSpan(55, 28).Clear();
        Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(malformed));
    }

    [Fact]
    public void CompareRemoveBatchLimitsAndDuplicateKeysMatchOtherMutations()
    {
        var items = Enumerable
            .Range(0, 128)
            .Select(i => new DataGridMutationItem(
                DataGridKey.FromUtf8($"catalog:{i}"),
                [],
                new(Cluster, 10, i)
            ))
            .ToArray();
        var command = new DataGridMutationCommand(
            DataGridMutationKind.CompareRemove,
            Map,
            1,
            2,
            DataGridExpiryKind.Never,
            0,
            items
        );
        Assert.Equal(128, DataGridMutationCommand.ReadFrom(command.Encode()).ItemCount);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.CompareRemove,
                Map,
                1,
                2,
                DataGridExpiryKind.Never,
                0,
                [.. items, new(DataGridKey.FromUtf8("extra"), [], new(Cluster, 10))]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.CompareRemove,
                Map,
                1,
                2,
                DataGridExpiryKind.Never,
                0,
                [items[0], items[0]]
            )
        );
    }

    private static DataGridMutationCommand Delete(
        DataGridEntryVersion version,
        DataGridExpiryKind expiry = DataGridExpiryKind.Never,
        long ticks = 0,
        byte[]? value = null
    ) =>
        new(
            DataGridMutationKind.CompareRemove,
            Map,
            1,
            2,
            expiry,
            ticks,
            [new(DataGridKey.FromUtf8("k"), value ?? [], version)]
        );

    private static DataGridProtocolFrame Frame(byte[] body) =>
        new(DataGridMessageKind.Mutation, Cluster, Map, 1, 2, Cluster, body);

    private static DataGridMapConfiguration Catalog(
        int keys = 1000,
        long bytes = 1048576,
        DataGridMapPolicy? policy = null
    ) =>
        new(
            Map,
            "catalog",
            DataGridMapOwnership.Derived,
            "catalog-service",
            "catalog-loader",
            keys,
            bytes,
            policy ?? Policy
        );

    private static DataGridMemberConfiguration[] Members() =>
        [
            new(new("00000000-0000-0000-0000-000000000001"), "node-a.example", 7400),
            new(new("00000000-0000-0000-0000-000000000002"), "node-b.example", 7400),
            new(new("00000000-0000-0000-0000-000000000003"), "node-c.example", 7400),
        ];
}
