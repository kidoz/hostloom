using System.Diagnostics;
using System.Globalization;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using HostLoom.Valkey;
using ValkeyDotNet;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>Reads only the runner's ephemeral fixture; lifecycle commands require exact ownership.</summary>
internal sealed class ValkeyDeploymentFixture : IDisposable
{
    internal const string Variable = "HOSTLOOM_VALKEY_DEPLOYMENT_FIXTURE";
    private const string Label = "hostloom.valkey.deployment-owner";
    private readonly string _container;
    private readonly string _owner;
    private readonly int _port;
    private readonly string _password;
    private readonly X509Certificate2 _root;
    private int _validations;

    internal ValkeyDeploymentFixture()
    {
        var path =
            Environment.GetEnvironmentVariable(Variable)
            ?? throw new InvalidOperationException(
                "Use scripts/test-valkey-deployment.py to create an isolated fixture."
            );
        using var document = JsonDocument.Parse(File.ReadAllText(path));
        var data = document.RootElement;
        _container = data.GetProperty("container").GetString()!;
        _owner = data.GetProperty("owner").GetString()!;
        _port = data.GetProperty("port").GetInt32();
        _password = data.GetProperty("password").GetString()!;
        _root = X509CertificateLoader.LoadCertificateFromFile(data.GetProperty("ca").GetString()!);
        if (
            _container.Length != 64
            || !_container.All(char.IsAsciiHexDigit)
            || _owner.Length != 32
            || !_owner.All(char.IsAsciiHexDigit)
            || _port is < 1 or > 65535
        )
            throw new InvalidOperationException("Invalid deployment fixture identity.");
    }

    internal int CertificateValidations => Volatile.Read(ref _validations);

    internal ValkeyOptions Options(
        ValkeyProtocol protocol = ValkeyProtocol.Resp3,
        bool trustRoot = true,
        bool wrongHost = false,
        bool wrongPassword = false
    ) =>
        new()
        {
            CommandTimeout = TimeSpan.FromSeconds(2),
            HealthTimeout = TimeSpan.FromMilliseconds(500),
            Connection = new ValkeyClientOptions
            {
                Host = wrongHost ? "127.0.0.1" : "localhost",
                Port = _port,
                Protocol = protocol,
                Database = 2,
                Username = "catalog",
                Password = wrongPassword ? _password + "invalid" : _password,
                ClientName = "hostloom-deployment-" + Guid.NewGuid().ToString("N"),
                UseTls = true,
                ConnectTimeout = TimeSpan.FromSeconds(2),
                CertificateValidationCallback = trustRoot ? ValidateCertificate : null,
            },
        };

    private bool ValidateCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors errors
    )
    {
        Interlocked.Increment(ref _validations);
        // The runtime checks the requested host name. Replace only trust-chain construction,
        // keeping name matching, validity, server-auth EKU and signature verification enabled.
        if (
            certificate is null
            || (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != SslPolicyErrors.None
        )
            return false;
        using var leaf = new X509Certificate2(certificate);
        using var customChain = new X509Chain();
        customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        customChain.ChainPolicy.CustomTrustStore.Add(_root);
        customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        customChain.ChainPolicy.DisableCertificateDownloads = true;
        customChain.ChainPolicy.ApplicationPolicy.Add(
            new System.Security.Cryptography.Oid("1.3.6.1.5.5.7.3.1")
        );
        return customChain.Build(leaf);
    }

    internal async Task SetRunningAsync(bool running, CancellationToken cancellationToken)
    {
        using var inspection = JsonDocument.Parse(
            await DockerAsync(["inspect", _container], cancellationToken)
        );
        var data = inspection.RootElement[0];
        var binding = data.GetProperty("HostConfig")
            .GetProperty("PortBindings")
            .GetProperty("6379/tcp");
        if (
            data.GetProperty("Id").GetString() != _container
            || data.GetProperty("Name").GetString() != "/hostloom-valkey-deployment-" + _owner
            || data.GetProperty("Config").GetProperty("Labels").GetProperty(Label).GetString()
                != _owner
            || data.GetProperty("HostConfig").GetProperty("Memory").GetInt64() != 128 * 1024 * 1024
            || data.GetProperty("HostConfig").GetProperty("NanoCpus").GetInt64() != 1_000_000_000
            || binding.GetArrayLength() != 1
            || binding[0].GetProperty("HostIp").GetString() != "127.0.0.1"
            || binding[0].GetProperty("HostPort").GetString()
                != _port.ToString(CultureInfo.InvariantCulture)
        )
            throw new InvalidOperationException(
                "Fixture ownership or containment mismatch; refusing container control."
            );
        await DockerAsync(
            running ? ["start", _container] : ["stop", "--time", "1", _container],
            cancellationToken
        );
    }

    private static async Task<string> DockerAsync(string[] arguments, CancellationToken token)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(token);
        bounded.CancelAfter(TimeSpan.FromSeconds(20));
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);
        using var process =
            Process.Start(start) ?? throw new InvalidOperationException("Docker did not start.");
        var output = process.StandardOutput.ReadToEndAsync(bounded.Token);
        var error = process.StandardError.ReadToEndAsync(bounded.Token);
        try
        {
            await process.WaitForExitAsync(bounded.Token);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(CancellationToken.None);
            throw;
        }
        var result = await output;
        _ = await error;
        if (process.ExitCode != 0)
            throw new InvalidOperationException("Docker fixture lifecycle operation failed.");
        return result;
    }

    public void Dispose() => _root.Dispose();
}
