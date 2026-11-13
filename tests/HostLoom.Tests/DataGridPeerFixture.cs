using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Transport;

namespace HostLoom.Tests;

internal sealed class DataGridPeerFixture : IDisposable
{
    internal static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    internal static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    internal static readonly Guid Principal = new("20112233-4455-6677-8899-aabbccddeeff");
    internal static readonly Guid Nonce = new("40112233-4455-6677-8899-aabbccddeeff");
    internal static readonly DataGridKey Key = DataGridKey.FromUtf8("catalog:eu");

    internal DataGridPeerFixture(int mapKeys = 1000, long mapBytes = 1048576)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest(
            "CN=HostLoom test root",
            key,
            HashAlgorithmName.SHA256
        );
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign,
                true
            )
        );
        Root = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-10),
            DateTimeOffset.UtcNow.AddDays(30)
        );
        Certificates = Enumerable
            .Range(0, 3)
            .Select(i => Issue($"node-{(char)('a' + i)}.example"))
            .ToArray();
        Listeners = Enumerable
            .Range(0, 3)
            .Select(_ => new TcpListener(IPAddress.Loopback, 0))
            .ToArray();
        foreach (var listener in Listeners)
            listener.Start(16);
        Configuration = new(
            Cluster,
            Enumerable
                .Range(0, 3)
                .Select(i => new DataGridMemberConfiguration(
                    new Guid(i + 1, 0, 0, new byte[8]),
                    $"node-{(char)('a' + i)}.example",
                    (ushort)((IPEndPoint)Listeners[i].LocalEndpoint).Port
                ))
                .ToArray(),
            [
                new(
                    Map,
                    "catalog",
                    DataGridMapOwnership.Derived,
                    "catalog-service",
                    "catalog-loader",
                    mapKeys,
                    mapBytes,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
        Fingerprints = Certificates
            .Select(certificate => certificate.GetCertHash(HashAlgorithmName.SHA256))
            .ToArray();
    }

    internal DataGridBootstrapConfiguration Configuration { get; }
    internal byte[] Build { get; } = SHA256.HashData("hostloom-peer-test-build"u8);
    internal X509Certificate2 Root { get; }
    internal X509Certificate2[] Certificates { get; }
    internal byte[][] Fingerprints { get; }
    internal TcpListener[] Listeners { get; }
    internal static IPAddress[] Routes =>
        [IPAddress.Loopback, IPAddress.Loopback, IPAddress.Loopback];

    internal X509Certificate2 Issue(
        string dns,
        bool expired = false,
        bool clientOnly = false,
        DateTimeOffset? notAfter = null
    )
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=catalog-member", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true)
        );
        var usages = new OidCollection { new("1.3.6.1.5.5.7.3.2") };
        if (!clientOnly)
            usages.Add(new("1.3.6.1.5.5.7.3.1"));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(usages, true));
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(dns);
        request.CertificateExtensions.Add(san.Build());
        using var certificate = request.Create(
            Root,
            DateTimeOffset.UtcNow.AddDays(-2),
            notAfter ?? DateTimeOffset.UtcNow.AddDays(expired ? -1 : 7),
            RandomNumberGenerator.GetBytes(16)
        );
        return certificate.CopyWithPrivateKey(key);
    }

    internal DataGridPeerCredentials Credentials(
        int local,
        byte[]? build = null,
        IEnumerable<string>? denied = null,
        X509Certificate2? certificate = null,
        X509Certificate2? root = null,
        byte[][]? fingerprints = null
    ) =>
        new(
            Configuration,
            local,
            certificate ?? Certificates[local],
            root ?? Root,
            fingerprints ?? Fingerprints,
            build ?? Build,
            denied
        );

    internal RaftMessage Message(
        RaftMessageKind kind,
        long term = 1,
        long index = 0,
        long previous = 0,
        bool accepted = false,
        long conflict = 0,
        IEnumerable<RaftEntry>? entries = null
    ) =>
        new(
            kind,
            0,
            1,
            term,
            new(1, 1),
            Cluster,
            Configuration.ComputeDigest(),
            index,
            previous,
            accepted: accepted,
            conflict: conflict,
            entries: entries
        );

    public void Dispose()
    {
        foreach (var listener in Listeners)
            listener.Stop();
        foreach (var certificate in Certificates)
            certificate.Dispose();
        Root.Dispose();
    }
}
