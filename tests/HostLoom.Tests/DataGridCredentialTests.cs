using System.Security.Authentication;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridCredentialTests
{
    [Fact]
    public async Task SnapshotOwnsLeafKeyAndOnlyPublicTrustAfterInputsAreDisposed()
    {
        using var fixture = new DataGridPeerFixture();
        using var leaf = fixture.Issue("node-a.example");
        var pins = fixture.Fingerprints.Select(bytes => (byte[])bytes.Clone()).ToArray();
        pins[0] = leaf.GetCertHash(HashAlgorithmName.SHA256);
        var credentials = fixture.Credentials(0, certificate: leaf, fingerprints: pins);
        await using var security = new DataGridSecurityContext(credentials);
        leaf.Dispose();
        using var lease = security.Acquire();
        Assert.True(lease.Material.Member.Certificate.HasPrivateKey);
        Assert.False(lease.Material.Member.Root.HasPrivateKey);
        DataGridTlsSupport.ValidateIdentity(
            lease.Material.Member.Certificate,
            lease.Material.Member.Root,
            server: true
        );
    }

    [Fact]
    public async Task ReloadPublishesWholePairAndKeepsOldMaterialUntilReaderDrain()
    {
        using var fixture = new DataGridPeerFixture();
        var principal = new DataGridClientPrincipal(
            DataGridPeerFixture.Principal,
            fixture.Fingerprints[0],
            true,
            [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
        );
        var policy = new DataGridClientAccessPolicy(
            fixture.Configuration,
            fixture.Root,
            [principal]
        );
        await using var security = new DataGridSecurityContext(fixture.Credentials(0), policy);
        using var old = security.Acquire();
        var denied = new DataGridClientAccessPolicy(
            fixture.Configuration,
            fixture.Root,
            [principal],
            [Convert.ToHexString(fixture.Fingerprints[0])]
        );
        var reload = security.Reload(fixture.Credentials(0), denied);
        Assert.Equal(2, reload.Generation);
        Assert.False(reload.Retired.IsCompleted);
        Assert.Throws<DataGridCredentialRetiredException>(() => old.Check());
        Assert.True(old.Material.Member.Certificate.HasPrivateKey);
        using (var current = security.Acquire())
            Assert.Equal(-1, current.Material.Clients!.Resolve(fixture.Certificates[0]));
        Assert.Throws<IOException>(() => security.Reload(fixture.Credentials(0), policy));
        old.Dispose();
        await reload.Retired.WaitAsync(TestContext.Current.CancellationToken);
        var next = security.Reload(fixture.Credentials(0), policy);
        Assert.Equal(3, next.Generation);
        await next.Retired.WaitAsync(TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData("root")]
    [InlineData("usage")]
    [InlineData("expired")]
    [InlineData("identity")]
    public async Task InvalidReplacementLeavesGenerationAndOldAdmissionIntact(string reason)
    {
        using var fixture = new DataGridPeerFixture();
        using var foreign = new DataGridPeerFixture();
        await using var security = new DataGridSecurityContext(fixture.Credentials(0));
        using var old = security.Acquire();
        using var leaf = fixture.Issue(
            "node-a.example",
            expired: reason == "expired",
            clientOnly: reason == "usage"
        );
        var pins = fixture.Fingerprints.Select(bytes => (byte[])bytes.Clone()).ToArray();
        if (reason is "usage" or "expired")
            pins[0] = leaf.GetCertHash(HashAlgorithmName.SHA256);
        var failure = Record.Exception(() =>
            security.Reload(
                fixture.Credentials(
                    reason == "identity" ? 1 : 0,
                    root: reason == "root" ? foreign.Root : null,
                    certificate: reason is "usage" or "expired" ? leaf : null,
                    fingerprints: pins
                )
            )
        );
        Assert.True(failure is AuthenticationException or ArgumentException);
        Assert.Equal(1, security.Generation);
        old.Check();
    }

    [Fact(Timeout = 20000)]
    public async Task SlowCancellationCannotBlockPublicationOrFreeOldMaterialEarly()
    {
        using var fixture = new DataGridPeerFixture();
        await using var security = new DataGridSecurityContext(fixture.Credentials(0));
        using var old = security.Acquire();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = old.Revoked.Register(() =>
        {
            entered.TrySetResult();
            release.Task.GetAwaiter().GetResult();
        });
        var reload = security.Reload(fixture.Credentials(0));
        try
        {
            await entered.Task.WaitAsync(TestContext.Current.CancellationToken);
            old.Dispose();
            Assert.False(reload.Retired.IsCompleted);
            using var current = security.Acquire();
            current.Check();
            Assert.Equal(2, security.Generation);
            Assert.Throws<IOException>(() => security.Reload(fixture.Credentials(0)));
        }
        finally
        {
            release.TrySetResult();
        }
        await reload.Retired.WaitAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void DenylistInputIsBoundedEvenWithRepeatedRecords()
    {
        using var fixture = new DataGridPeerFixture();
        var repeated = Enumerable.Repeat(Convert.ToHexString(fixture.Fingerprints[0]), 4097);
        Assert.Throws<ArgumentException>(() => fixture.Credentials(1, denied: repeated));
        Assert.Throws<ArgumentException>(() => fixture.Credentials(1, denied: ["invalid"]));
    }

    [Fact]
    public async Task SdkMaterialIsOwnedAndReloadRejectsLogicalIdentityChanges()
    {
        using var fixture = new DataGridPeerFixture();
        using var certificate = fixture.Issue("catalog-client.example", clientOnly: true);
        var endpoints = Enumerable
            .Range(0, 3)
            .Select(i => new DataGridClientEndpoint(
                fixture.Configuration.GetMember(i).MemberId,
                fixture.Configuration.GetMember(i).DnsName,
                fixture.Configuration.GetMember(i).Port,
                fixture.Fingerprints[i]
            ))
            .ToArray();
        var options = new DataGridClientOptions(
            fixture.Configuration,
            DataGridPeerFixture.Principal,
            certificate,
            fixture.Root,
            endpoints,
            fixture.Build
        );
        await using var client = new HostLoom.DataGrid.Client.DataGridClient(options);
        using var replacement = fixture.Issue("catalog-client.example", clientOnly: true);
        var next = new DataGridClientOptions(
            fixture.Configuration,
            DataGridPeerFixture.Principal,
            replacement,
            fixture.Root,
            endpoints,
            fixture.Build
        );
        certificate.Dispose();
        var reload = client.ReloadCredentials(next);
        replacement.Dispose();
        await reload.Retired.WaitAsync(TestContext.Current.CancellationToken);
        using var valid = fixture.Issue("catalog-client.example", clientOnly: true);
        var changed = new DataGridClientOptions(
            fixture.Configuration,
            Guid.NewGuid(),
            valid,
            fixture.Root,
            endpoints,
            fixture.Build
        );
        Assert.Throws<ArgumentException>(() => client.ReloadCredentials(changed));
        client.Disable();
        var same = new DataGridClientOptions(
            fixture.Configuration,
            DataGridPeerFixture.Principal,
            valid,
            fixture.Root,
            endpoints,
            fixture.Build
        );
        Assert.Throws<ObjectDisposedException>(() => client.ReloadCredentials(same));
    }

    [Fact]
    public void PrincipalReservationSurvivesGenerationsAndReleasesExactlyOnce()
    {
        var pool = new DataGridPrincipalAdmission();
        var held = Enumerable
            .Range(0, 8)
            .Select(_ =>
                pool.Acquire(DataGridPeerFixture.Principal, TestContext.Current.CancellationToken)
            )
            .ToArray();
        try
        {
            Assert.Throws<IOException>(() =>
                pool.Acquire(DataGridPeerFixture.Principal, TestContext.Current.CancellationToken)
            );
        }
        finally
        {
            foreach (var lease in held)
            {
                lease.Dispose();
                lease.Dispose();
            }
        }
        using var next = pool.Acquire(
            DataGridPeerFixture.Principal,
            TestContext.Current.CancellationToken
        );
    }
}
