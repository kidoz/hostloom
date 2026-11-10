using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace HostLoom.DataGrid.Transport;

[Flags]
public enum DataGridMapActions
{
    None = 0,
    Read = 1,
    Mutate = 2,
}

public readonly record struct DataGridMapGrant(Guid MapId, DataGridMapActions Actions);

public sealed class DataGridClientPrincipal
{
    private readonly byte[] _fingerprint;

    public DataGridClientPrincipal(
        Guid principalId,
        ReadOnlySpan<byte> certificateSha256,
        bool allowSessions,
        IReadOnlyList<DataGridMapGrant> maps
    )
    {
        ArgumentNullException.ThrowIfNull(maps);
        if (principalId == Guid.Empty || certificateSha256.Length != 32 || maps.Count > 16)
            throw new ArgumentException("Invalid client principal.");
        PrincipalId = principalId;
        _fingerprint = certificateSha256.ToArray();
        AllowSessions = allowSessions;
        var owned = maps.ToArray();
        if (
            owned.Any(map =>
                map.MapId == Guid.Empty
                || map.Actions
                    is <= DataGridMapActions.None
                        or > (DataGridMapActions.Read | DataGridMapActions.Mutate)
            )
            || owned.Select(map => map.MapId).Distinct().Count() != owned.Length
        )
            throw new ArgumentException("Invalid map grants.", nameof(maps));
        Maps = Array.AsReadOnly(owned);
    }

    public Guid PrincipalId { get; }
    public bool AllowSessions { get; }
    public IReadOnlyList<DataGridMapGrant> Maps { get; }

    public byte[] GetCertificateSha256() => (byte[])_fingerprint.Clone();

    internal bool Matches(ReadOnlySpan<byte> fingerprint) =>
        CryptographicOperations.FixedTimeEquals(fingerprint, _fingerprint);

    internal bool Allows(DataGridClientRequest request) =>
        request.Kind is DataGridMessageKind.OpenSession or DataGridMessageKind.CloseSession
            ? AllowSessions
            : Maps.Any(map =>
                map.MapId == request.MapId
                && (
                    map.Actions
                    & (
                        request.Kind == DataGridMessageKind.Read
                            ? DataGridMapActions.Read
                            : DataGridMapActions.Mutate
                    )
                ) != 0
            );
}

/// <summary>Immutable explicit client identities and grants; the caller retains root/certificate lifetimes.</summary>
public sealed class DataGridClientAccessPolicy
{
    private readonly DataGridClientPrincipal[] _principals;
    private readonly HashSet<string> _denied;

    public DataGridClientAccessPolicy(
        DataGridBootstrapConfiguration configuration,
        X509Certificate2 root,
        IReadOnlyList<DataGridClientPrincipal> principals,
        IEnumerable<string>? deniedCertificateSha256 = null
    )
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(principals);
        ArgumentNullException.ThrowIfNull(root);
        if (principals.Count is < 1 or > 128)
            throw new ArgumentOutOfRangeException(nameof(principals));
        Configuration = configuration;
        Root = root;
        _principals = principals.ToArray();
        if (_principals.Any(principal => principal is null))
            throw new ArgumentException("Null principal.", nameof(principals));
        if (
            _principals.Select(p => p.PrincipalId).Distinct().Count() != _principals.Length
            || _principals
                .Select(p => Convert.ToHexString(p.GetCertificateSha256()))
                .Distinct(StringComparer.Ordinal)
                .Count() != _principals.Length
        )
            throw new ArgumentException(
                "Principal IDs and certificate bindings must be unique.",
                nameof(principals)
            );
        foreach (var principal in _principals)
        foreach (var grant in principal.Maps)
            if (
                !Enumerable
                    .Range(0, configuration.MapCount)
                    .Any(i => configuration.GetMap(i).MapId == grant.MapId)
            )
                throw new ArgumentException("Unknown granted map.", nameof(principals));
        _denied = DataGridTlsSupport.DeniedHashes(deniedCertificateSha256);
    }

    public DataGridBootstrapConfiguration Configuration { get; }
    internal X509Certificate2 Root { get; }
    internal int Count => _principals.Length;

    internal DataGridClientAccessPolicy Copy(DataGridCertificateOwner owner) =>
        new(Configuration, owner.CopyPublic(Root), _principals, _denied);

    internal DataGridClientPrincipal Principal(int index) => _principals[index];

    internal int Resolve(X509Certificate2 certificate)
    {
        if (!DataGridTlsSupport.Current(certificate))
            return -1;
        var hash = certificate.GetCertHash(HashAlgorithmName.SHA256);
        if (_denied.Contains(Convert.ToHexString(hash)))
            return -1;
        for (var i = 0; i < _principals.Length; i++)
            if (_principals[i].Matches(hash))
                return i;
        return -1;
    }
}
