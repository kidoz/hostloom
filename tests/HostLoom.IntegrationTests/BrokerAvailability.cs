using System.Net.Sockets;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Decides whether the brokers from <c>docker-compose.yml</c> are listening. Probed once per test
/// run, so a suite against a closed port skips instead of spending every test's timeout budget
/// failing to connect. Skipped is the honest outcome here: without a broker these tests prove
/// nothing, and reporting them as passed would be worse than not running them.
/// </summary>
public static class BrokerAvailability
{
    /// <summary>
    /// Set to <c>1</c> where skipping would be dishonest — release CI publishes packages on the
    /// strength of this suite, so a closed port there means the brokers failed to start, not that
    /// the tests are inapplicable. Skipping would let a release proceed having validated adapter
    /// compilation and nothing else.
    /// </summary>
    public const string RequireVariable = "HOSTLOOM_REQUIRE_BROKERS";

    public static bool RabbitMq { get; } = Probe("RabbitMQ", "localhost", 5672);

    public static bool Kafka { get; } = Probe("Kafka", "localhost", 9092);

    public const string RabbitMqSkip =
        "RabbitMQ is not listening on localhost:5672. Start it with `docker compose up -d`.";

    public const string KafkaSkip =
        "Kafka is not listening on localhost:9092. Start it with `docker compose up -d`.";

    private static bool Probe(string broker, string host, int port)
    {
        var listening = Listening(host, port);
        if (
            !listening
            && string.Equals(
                Environment.GetEnvironmentVariable(RequireVariable),
                "1",
                StringComparison.Ordinal
            )
        )
        {
            throw new InvalidOperationException(
                $"{broker} is required on {host}:{port} because {RequireVariable}=1, but nothing is "
                    + "listening. Start the brokers with `docker compose up -d --wait`."
            );
        }

        return listening;
    }

    private static bool Listening(string host, int port)
    {
        try
        {
            using var client = new TcpClient();
            return client.ConnectAsync(host, port).Wait(TimeSpan.FromSeconds(3))
                && client.Connected;
        }
        catch (Exception)
        {
            // Any failure to reach the port means the same thing to the caller: no broker.
            return false;
        }
    }
}
