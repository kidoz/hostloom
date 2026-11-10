namespace HostLoom.DataGrid.ProcessProbe;

// Shared only by the child-process harness and its owning tests, not deployment configuration.
internal static class ProbeConfiguration
{
    internal static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    internal static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    internal static readonly Guid Principal = new("20112233-4455-6677-8899-aabbccddeeff");

    internal static DataGridBootstrapConfiguration Create(IReadOnlyList<int> peerPorts) =>
        new(
            Cluster,
            Enumerable
                .Range(0, 3)
                .Select(i => new DataGridMemberConfiguration(
                    new Guid(i + 1, 0, 0, new byte[8]),
                    $"node-{(char)('a' + i)}.example",
                    checked((ushort)peerPorts[i])
                ))
                .ToArray(),
            [
                new(
                    Map,
                    "catalog",
                    DataGridMapOwnership.Derived,
                    "catalog-service",
                    "catalog-loader",
                    1000,
                    1048576,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
}
