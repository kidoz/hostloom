using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;

namespace HostLoom.DataGrid.Transport;

/// <summary>One atomic peer/client security owner for a fixed member/configuration/build.</summary>
internal sealed class DataGridSecurityContext : IAsyncDisposable
{
    private readonly DataGridCredentialStore<DataGridSecuritySnapshot> _store;
    private readonly byte[] _configuration,
        _build;
    internal DataGridBootstrapConfiguration Configuration { get; }
    internal int LocalMember { get; }
    internal bool HasClients { get; }
    internal long Generation => _store.Version;

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The credential store takes ownership of the validated snapshot and disposes it after reader drain."
    )]
    internal DataGridSecurityContext(
        DataGridPeerCredentials member,
        DataGridClientAccessPolicy? clients = null
    )
    {
        var material = DataGridSecuritySnapshot.Create(member, clients);
        Configuration = member.Configuration;
        LocalMember = member.LocalMember;
        HasClients = clients is not null;
        _configuration = Configuration.ComputeDigest();
        _build = member.BuildDigest;
        _store = new(material, 128);
    }

    internal DataGridCredentialStore<DataGridSecuritySnapshot>.Lease Acquire(
        TcpClient? socket = null
    ) => _store.Acquire(socket);

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Publish takes ownership of the replacement on both successful and rejected publication."
    )]
    internal DataGridCredentialReload Reload(
        DataGridPeerCredentials member,
        DataGridClientAccessPolicy? clients = null
    )
    {
        if ((clients is not null) != HasClients)
            throw new ArgumentException("Reload cannot change listener roles.");
        if (
            member.LocalMember != LocalMember
            || !member.Configuration.ComputeDigest().AsSpan().SequenceEqual(_configuration)
            || !member.BuildDigest.AsSpan().SequenceEqual(_build)
        )
            throw new ArgumentException(
                "Credential reload cannot change member, configuration or build identity."
            );
        return _store.Publish(DataGridSecuritySnapshot.Create(member, clients));
    }

    public ValueTask DisposeAsync() => _store.DisposeAsync();
}

internal sealed class DataGridCertificateOwner : IDisposable
{
    private readonly List<X509Certificate2> _certificates = [];
    internal long ValidUntilUtcTicks { get; private set; } = long.MaxValue;

    internal X509Certificate2 Copy(X509Certificate2 source)
    {
        var copy = new X509Certificate2(source);
        _certificates.Add(copy);
        ValidUntilUtcTicks = Math.Min(ValidUntilUtcTicks, copy.NotAfter.ToUniversalTime().Ticks);
        return copy;
    }

    internal X509Certificate2 CopyPublic(X509Certificate2 source)
    {
        var copy = X509CertificateLoader.LoadCertificate(source.RawData);
        _certificates.Add(copy);
        ValidUntilUtcTicks = Math.Min(ValidUntilUtcTicks, copy.NotAfter.ToUniversalTime().Ticks);
        return copy;
    }

    public void Dispose()
    {
        foreach (var certificate in _certificates)
            certificate.Dispose();
    }
}

internal sealed class DataGridSecuritySnapshot : IDataGridCredentialMaterial
{
    private readonly DataGridCertificateOwner _owner;
    internal DataGridPeerCredentials Member { get; }
    internal DataGridClientAccessPolicy? Clients { get; }
    public long ValidUntilUtcTicks => _owner.ValidUntilUtcTicks;

    private DataGridSecuritySnapshot(
        DataGridCertificateOwner owner,
        DataGridPeerCredentials member,
        DataGridClientAccessPolicy? clients
    )
    {
        _owner = owner;
        Member = member;
        Clients = clients;
    }

    internal static DataGridSecuritySnapshot Create(
        DataGridPeerCredentials member,
        DataGridClientAccessPolicy? clients
    )
    {
        if (
            clients is not null
            && !clients
                .Configuration.ComputeDigest()
                .AsSpan()
                .SequenceEqual(member.Configuration.ComputeDigest())
        )
            throw new ArgumentException("Client/peer security configuration mismatch.");
        var owner = new DataGridCertificateOwner();
        try
        {
            var copied = member.Copy(owner);
            DataGridTlsSupport.ValidateIdentity(copied.Certificate, copied.Root, server: true);
            DataGridTlsSupport.ValidateIdentity(copied.Certificate, copied.Root, server: false);
            var policy = clients?.Copy(owner);
            if (policy is not null)
                DataGridTlsSupport.ValidateRoot(policy.Root);
            return new(owner, copied, policy);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    public void Dispose() => _owner.Dispose();
}

internal sealed class DataGridPrincipalAdmission
{
    private readonly object _sync = new();
    private readonly Dictionary<Guid, int> _active = [];

    internal IDisposable Acquire(Guid principal, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        lock (_sync)
        {
            var count = _active.GetValueOrDefault(principal);
            if (count == 8 || count == 0 && _active.Count == 128)
                throw new IOException("Client principal capacity.");
            _active[principal] = count + 1;
            return new Lease(this, principal);
        }
    }

    private sealed class Lease(DataGridPrincipalAdmission owner, Guid principal) : IDisposable
    {
        private DataGridPrincipalAdmission? _owner = owner;

        public void Dispose()
        {
            var held = Interlocked.Exchange(ref _owner, null);
            if (held is null)
                return;
            lock (held._sync)
            {
                var count = held._active[principal] - 1;
                if (count == 0)
                    held._active.Remove(principal);
                else
                    held._active[principal] = count;
            }
        }
    }
}
