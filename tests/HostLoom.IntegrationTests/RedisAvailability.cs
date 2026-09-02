using System.Net.Sockets;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Decides whether the Redis from <c>docker-compose.yml</c> is listening, with the same contract
/// as <see cref="BrokerAvailability"/>: skip honestly without a listener, fail under
/// <c>HOSTLOOM_REQUIRE_BROKERS=1</c> because a release must not pass on skipped evidence.
/// </summary>
public static class RedisAvailability
{
    public const string Host = "localhost";

    public const int Port = 6379;

    public const string Configuration = "localhost:6379";

    public static bool Redis { get; } = Probe();

    public const string Skip =
        "Redis is not listening on localhost:6379. Start it with `docker compose up -d`.";

    private static bool Probe()
    {
        var listening = Listening();
        if (
            !listening
            && string.Equals(
                Environment.GetEnvironmentVariable(BrokerAvailability.RequireVariable),
                "1",
                StringComparison.Ordinal
            )
        )
        {
            throw new InvalidOperationException(
                $"Redis is required on {Host}:{Port} because {BrokerAvailability.RequireVariable}=1, but nothing is "
                    + "listening. Start the brokers with `docker compose up -d --wait`."
            );
        }

        return listening;
    }

    private static bool Listening()
    {
        try
        {
            using var client = new TcpClient();
            return client.ConnectAsync(Host, Port).Wait(TimeSpan.FromSeconds(3))
                && client.Connected;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
