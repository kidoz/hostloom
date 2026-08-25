namespace HostLoom.Transport.RabbitMq;

public sealed class RabbitMqOptions
{
    public Uri Uri { get; set; } = new("amqp://guest:guest@localhost:5672/");

    public string ClientProvidedName { get; set; } =
        $"hostloom-{Environment.MachineName}-{Environment.ProcessId}";

    public ushort PrefetchCount { get; set; } = 16;

    public bool DurableRequestQueues { get; set; } = true;

    /// <summary>Whether topic exchanges and their subscription queues survive a broker restart.</summary>
    public bool DurableTopics { get; set; } = true;
}
