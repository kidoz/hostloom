using System.Net;
using System.Security.Cryptography.X509Certificates;

namespace HostLoom.DataGrid.Client;

public sealed class DataGridClientEndpoint
{
    private readonly byte[] _fingerprint;
    private readonly IPAddress? _address;

    public DataGridClientEndpoint(
        Guid memberId,
        string dnsName,
        ushort port,
        ReadOnlySpan<byte> certificateSha256,
        IPAddress? address = null
    )
    {
        _ = new DataGridMemberConfiguration(memberId, dnsName, port);
        if (certificateSha256.Length != 32)
            throw new ArgumentException(
                "SHA-256 server binding required.",
                nameof(certificateSha256)
            );
        MemberId = memberId;
        DnsName = dnsName;
        Port = port;
        _fingerprint = certificateSha256.ToArray();
        _address = address is null ? null : IPAddress.Parse(address.ToString());
    }

    public Guid MemberId { get; }
    public string DnsName { get; }
    public ushort Port { get; }

    public byte[] GetCertificateSha256() => (byte[])_fingerprint.Clone();

    internal IPAddress? Address => _address;
}

/// <summary>Explicit trust and routes. Keep input certificates alive until client construction or reload returns; the client owns captured copies.</summary>
public sealed class DataGridClientOptions
{
    internal readonly X509Certificate2 Certificate,
        Root;
    private readonly byte[] _build;

    public DataGridClientOptions(
        DataGridBootstrapConfiguration configuration,
        Guid principalId,
        X509Certificate2 certificate,
        X509Certificate2 root,
        IReadOnlyList<DataGridClientEndpoint> endpoints,
        ReadOnlySpan<byte> buildDigest,
        int initialMember = 0
    )
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(certificate);
        ArgumentNullException.ThrowIfNull(root);
        if (
            principalId == Guid.Empty
            || !certificate.HasPrivateKey
            || endpoints.Count != 3
            || buildDigest.Length != 32
            || initialMember is < 0 or > 2
        )
            throw new ArgumentException("Invalid client configuration.");
        var ordered = new DataGridClientEndpoint[3];
        for (var i = 0; i < 3; i++)
        {
            var matches = endpoints
                .Where(endpoint => endpoint.MemberId == configuration.GetMember(i).MemberId)
                .ToArray();
            if (matches.Length != 1 || matches[0].DnsName != configuration.GetMember(i).DnsName)
                throw new ArgumentException(
                    "Routes must bind the configured members and DNS names.",
                    nameof(endpoints)
                );
            ordered[i] = matches[0];
        }
        Configuration = configuration;
        PrincipalId = principalId;
        Certificate = certificate;
        Root = root;
        Endpoints = Array.AsReadOnly(ordered);
        _build = buildDigest.ToArray();
        InitialMember = initialMember;
    }

    public DataGridBootstrapConfiguration Configuration { get; }
    public Guid PrincipalId { get; }
    public IReadOnlyList<DataGridClientEndpoint> Endpoints { get; }
    public int InitialMember { get; }
    internal byte[] Build => (byte[])_build.Clone();
}
