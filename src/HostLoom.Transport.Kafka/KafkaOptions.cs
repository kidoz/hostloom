namespace HostLoom.Transport.Kafka;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Stable group prefix shared by instances of the same logical service.</summary>
    public string ConsumerGroup { get; set; } = "hostloom";

    /// <summary>
    /// Topic on which this client service receives replies. Provision it with enough retention for
    /// the maximum request timeout. Each client instance uses a unique consumer group and filters by correlation id.
    /// </summary>
    public string ResponseTopic { get; set; } = "hostloom.responses";

    public string ClientId { get; set; } =
        $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    public bool EnableIdempotence { get; set; } = true;
}
