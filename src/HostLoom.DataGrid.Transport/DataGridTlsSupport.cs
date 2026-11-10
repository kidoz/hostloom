using System.Buffers.Binary;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace HostLoom.DataGrid.Transport;

internal static class DataGridTlsSupport
{
    internal static X509ChainPolicy ChainPolicy(X509Certificate2 root, bool server)
    {
        var policy = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.NoCheck,
            DisableCertificateDownloads = true,
            VerificationFlags = X509VerificationFlags.NoFlag,
        };
        policy.CustomTrustStore.Add(root);
        policy.ApplicationPolicy.Add(new Oid(server ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2"));
        return policy;
    }

    internal static HashSet<string> DeniedHashes(IEnumerable<string>? values)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var count = 0;
        foreach (var hash in values ?? [])
        {
            if (
                ++count > 4096
                || hash is null
                || hash.Length != 64
                || hash.Any(character => !Uri.IsHexDigit(character))
            )
                throw new ArgumentException("Invalid certificate denylist.");
            result.Add(hash);
        }
        return result;
    }

    internal static void ValidateRoot(X509Certificate2 root)
    {
        if (
            !Current(root)
            || !root
                .Extensions.OfType<X509BasicConstraintsExtension>()
                .Any(extension => extension.CertificateAuthority)
        )
            throw new AuthenticationException("Invalid private trust root.");
    }

    internal static void ValidateIdentity(
        X509Certificate2 certificate,
        X509Certificate2 root,
        bool server
    )
    {
        ValidateRoot(root);
        if (!Current(certificate) || !certificate.HasPrivateKey)
            throw new AuthenticationException("Invalid local credential.");
        using var chain = new X509Chain();
        chain.ChainPolicy = ChainPolicy(root, server);
        if (!chain.Build(certificate))
            throw new AuthenticationException("Local credential chain or usage rejected.");
    }

    internal static bool Current(X509Certificate2 certificate) =>
        certificate.NotBefore.ToUniversalTime() <= DateTime.UtcNow
        && certificate.NotAfter.ToUniversalTime() > DateTime.UtcNow;

    internal static async ValueTask<DataGridProtocolFrame> ReadAsync(
        Stream stream,
        Guid incarnation,
        Guid sender,
        CancellationToken token
    )
    {
        var prefix = new byte[4];
        await stream.ReadExactlyAsync(prefix, token).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadUInt32BigEndian(prefix);
        if (length < 68 || length > DataGridProtocolFrame.MaxEncodedByteCount - 4)
            throw new InvalidDataException("Frame size rejected before allocation.");
        var bytes = new byte[checked((int)length + 4)];
        prefix.CopyTo(bytes, 0);
        await stream.ReadExactlyAsync(bytes.AsMemory(4), token).ConfigureAwait(false);
        return DataGridProtocolFrame.ReadFrom(bytes, incarnation, sender);
    }

    internal static ValueTask WriteAsync(
        Stream stream,
        DataGridProtocolFrame frame,
        CancellationToken token
    ) => stream.WriteAsync(frame.Encode(), token);
}

internal sealed class DataGridTransportLease : IDisposable
{
    private SemaphoreSlim? _semaphore;
    private readonly DataGridTransportRole _role;
    private readonly DataGridTransportDirection _direction;
    private readonly DataGridTransportResource _resource;
    private readonly long _bytes;

    private DataGridTransportLease(
        SemaphoreSlim semaphore,
        DataGridTransportRole role,
        DataGridTransportDirection direction,
        DataGridTransportResource resource,
        long bytes
    )
    {
        _semaphore = semaphore;
        _role = role;
        _direction = direction;
        _resource = resource;
        _bytes = bytes;
        DataGridTransportDiagnostics.Change(role, direction, resource, 1, bytes);
    }

    internal static DataGridTransportLease? TryAcquire(
        SemaphoreSlim semaphore,
        DataGridTransportRole role,
        DataGridTransportDirection direction,
        DataGridTransportResource resource,
        long bytes = 0,
        CancellationToken token = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfNegative(bytes);
        if (semaphore.Wait(0, token))
            return new(semaphore, role, direction, resource, bytes);
        DataGridTransportDiagnostics.Reject(
            role,
            direction,
            resource,
            DataGridTransportRejection.Capacity
        );
        return null;
    }

    internal static DataGridTransportLease Acquire(
        SemaphoreSlim semaphore,
        DataGridTransportRole role,
        DataGridTransportDirection direction,
        DataGridTransportResource resource,
        long bytes = 0,
        CancellationToken token = default
    ) =>
        TryAcquire(semaphore, role, direction, resource, bytes, token)
        ?? throw new DataGridTransportAdmissionException();

    public void Dispose()
    {
        var held = Interlocked.Exchange(ref _semaphore, null);
        if (held is null)
            return;
        try
        {
            DataGridTransportDiagnostics.Change(_role, _direction, _resource, -1, -_bytes);
        }
        finally
        {
            held.Release();
        }
    }
}
