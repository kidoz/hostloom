using HostLoom.DataGrid.Transport;

namespace HostLoom.DataGrid.Client;

internal sealed class DataGridOwnedClientCredentials : IDataGridCredentialMaterial
{
    private readonly DataGridCertificateOwner _owner;
    internal DataGridClientOptions Options { get; }
    public long ValidUntilUtcTicks => _owner.ValidUntilUtcTicks;

    private DataGridOwnedClientCredentials(
        DataGridCertificateOwner owner,
        DataGridClientOptions options
    )
    {
        _owner = owner;
        Options = options;
    }

    internal static DataGridOwnedClientCredentials Create(DataGridClientOptions options)
    {
        var owner = new DataGridCertificateOwner();
        try
        {
            var copy = new DataGridClientOptions(
                options.Configuration,
                options.PrincipalId,
                owner.Copy(options.Certificate),
                owner.CopyPublic(options.Root),
                options.Endpoints,
                options.Build,
                options.InitialMember
            );
            DataGridTlsSupport.ValidateIdentity(copy.Certificate, copy.Root, server: false);
            return new(owner, copy);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    public void Dispose() => _owner.Dispose();
}
