using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace HostLoom.DataGrid.Transport;

/// <summary>Immutable experiment policy; certificate/key lifetimes remain with the caller.</summary>
[SuppressMessage(
    "Security",
    "CA5398:Avoid hardcoded SslProtocols values",
    Justification = "The v1 peer contract requires exactly TLS 1.3 and must refuse negotiation to earlier versions."
)]
internal sealed class DataGridPeerCredentials
{
    internal static readonly SslApplicationProtocol Protocol = new("hostloom-grid-v1");
    private readonly byte[][] _fingerprints;
    private readonly HashSet<string> _denied;
    private readonly X509Certificate2 _root;

    internal DataGridPeerCredentials(
        DataGridBootstrapConfiguration configuration,
        int localMember,
        X509Certificate2 certificate,
        X509Certificate2 root,
        IReadOnlyList<byte[]> memberFingerprints,
        byte[] buildDigest,
        IEnumerable<string>? deniedSha256 = null
    )
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentNullException.ThrowIfNull(root);
        if (
            localMember is < 0 or > 2
            || !certificate.HasPrivateKey
            || memberFingerprints.Count != 3
            || buildDigest.Length != 32
        )
            throw new ArgumentException("Invalid member credentials.");
        Configuration = configuration;
        LocalMember = localMember;
        Certificate = certificate;
        _root = root;
        _buildDigest = (byte[])buildDigest.Clone();
        _fingerprints = memberFingerprints
            .Select(bytes =>
                bytes.Length == 32
                    ? (byte[])bytes.Clone()
                    : throw new ArgumentException(
                        "SHA-256 member binding required.",
                        nameof(memberFingerprints)
                    )
            )
            .ToArray();
        if (_fingerprints.Select(Convert.ToHexString).Distinct(StringComparer.Ordinal).Count() != 3)
            throw new ArgumentException(
                "Each certificate binds exactly one member.",
                nameof(memberFingerprints)
            );
        _denied = DataGridTlsSupport.DeniedHashes(deniedSha256);
        if (Resolve(certificate) != localMember)
            throw new ArgumentException("Local certificate binding mismatch.", nameof(certificate));
    }

    internal DataGridBootstrapConfiguration Configuration { get; }
    internal int LocalMember { get; }
    internal X509Certificate2 Certificate { get; }
    internal X509Certificate2 Root => _root;

    internal DataGridPeerCredentials Copy(DataGridCertificateOwner owner) =>
        new(
            Configuration,
            LocalMember,
            owner.Copy(Certificate),
            owner.CopyPublic(_root),
            _fingerprints,
            BuildDigest,
            _denied
        );

    private readonly byte[] _buildDigest;
    internal byte[] BuildDigest => (byte[])_buildDigest.Clone();

    internal X509ChainPolicy ChainPolicy(bool serverCertificate) =>
        DataGridTlsSupport.ChainPolicy(_root, serverCertificate);

    internal int Resolve(X509Certificate2 certificate)
    {
        var digest = certificate.GetCertHash(HashAlgorithmName.SHA256);
        if (
            _denied.Contains(Convert.ToHexString(digest))
            || certificate.NotBefore.ToUniversalTime() > DateTime.UtcNow
            || certificate.NotAfter.ToUniversalTime() <= DateTime.UtcNow
        )
            return -1;
        for (var i = 0; i < 3; i++)
            if (
                CryptographicOperations.FixedTimeEquals(digest, _fingerprints[i])
                && certificate.MatchesHostname(
                    Configuration.GetMember(i).DnsName,
                    allowWildcards: false,
                    allowCommonName: false
                )
            )
                return i;
        return -1;
    }

    internal SslClientAuthenticationOptions ClientOptions(int remote) =>
        new()
        {
            EnabledSslProtocols = SslProtocols.Tls13,
            TargetHost = Configuration.GetMember(remote).DnsName,
            ClientCertificates = new X509CertificateCollection { Certificate },
            CertificateChainPolicy = ChainPolicy(serverCertificate: true),
            AllowRenegotiation = false,
            AllowTlsResume = false,
            ApplicationProtocols = [Protocol],
            RemoteCertificateValidationCallback = (_, certificate, _, errors) =>
                errors == SslPolicyErrors.None
                && certificate is X509Certificate2 leaf
                && Resolve(leaf) == remote
                && remote != LocalMember,
        };

    internal SslServerAuthenticationOptions ServerOptions() =>
        new()
        {
            EnabledSslProtocols = SslProtocols.Tls13,
            ServerCertificate = Certificate,
            ClientCertificateRequired = true,
            CertificateChainPolicy = ChainPolicy(serverCertificate: false),
            AllowRenegotiation = false,
            AllowTlsResume = false,
            ApplicationProtocols = [Protocol],
            RemoteCertificateValidationCallback = (_, certificate, _, errors) =>
                errors == SslPolicyErrors.None
                && certificate is X509Certificate2 leaf
                && Resolve(leaf) is var member
                && member >= 0
                && member != LocalMember,
        };
}
