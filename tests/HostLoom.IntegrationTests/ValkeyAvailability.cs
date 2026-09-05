using System.Globalization;
using System.Net.Sockets;
using HostLoom.Valkey;
using ValkeyDotNet;

namespace HostLoom.IntegrationTests;

/// <summary>Dedicated standalone test endpoint. Release runs must not silently skip Valkey evidence.</summary>
public static class ValkeyAvailability
{
    public const string Skip =
        "Valkey is not listening on localhost:16379. Start the valkey compose service.";
    public static int Port =>
        int.Parse(
            Environment.GetEnvironmentVariable("HOSTLOOM_VALKEY_PORT") ?? "16379",
            CultureInfo.InvariantCulture
        );
    public static bool Available { get; } = Probe();

    public static ValkeyOptions Options(ValkeyProtocol protocol = ValkeyProtocol.Resp3) =>
        new()
        {
            Connection = new ValkeyClientOptions
            {
                Host = "localhost",
                Port = Port,
                Protocol = protocol,
                ClientName = "hostloom-valkey-test-" + Guid.NewGuid().ToString("N"),
            },
        };

    private static bool Probe()
    {
        bool listening;
        try
        {
            using var client = new TcpClient();
            listening =
                client.ConnectAsync("localhost", Port).Wait(TimeSpan.FromSeconds(2))
                && client.Connected;
        }
        catch (Exception)
        {
            listening = false;
        }
        if (
            !listening
            && (
                Environment.GetEnvironmentVariable("HOSTLOOM_REQUIRE_BROKERS") == "1"
                || Environment.GetEnvironmentVariable("HOSTLOOM_REQUIRE_VALKEY") == "1"
            )
        )
            throw new InvalidOperationException(
                "Valkey integration is required but the configured local listener is unavailable."
            );
        return listening;
    }
}
