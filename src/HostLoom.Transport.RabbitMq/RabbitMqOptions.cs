namespace HostLoom.Transport.RabbitMq;

public sealed class RabbitMqOptions
{
    public Uri Uri { get; set; } = new("amqp://guest:guest@localhost:5672/");

    public string ClientProvidedName { get; set; } =
        $"hostloom-{Environment.MachineName}-{Environment.ProcessId}";

    /// <summary>Physical queue identity scheme. Version2 isolates request and event routes. Legacy requires coordinated migration and retains ambiguous dotted subscription names.</summary>
    public RabbitMqQueueNaming QueueNaming { get; set; } = RabbitMqQueueNaming.Version2;

    public ushort PrefetchCount { get; set; } = 16;

    public bool DurableRequestQueues { get; set; } = true;

    /// <summary>Whether topic exchanges and their subscription queues survive a broker restart.</summary>
    public bool DurableTopics { get; set; } = true;

    /// <summary>
    /// Whether a request may name any queue in its <c>ReplyTo</c> property. Off by default: a
    /// listener then answers only server-named reply queues (<c>amq.gen-…</c>) and the direct
    /// reply-to pseudo-queue (<c>amq.rabbitmq.reply-to</c>), which is what HostLoom's own client
    /// uses, and rejects any other request as malformed before its handler runs. Turn it on only
    /// for a foreign client that replies through a queue it declared itself.
    /// </summary>
    public bool AllowNamedReplyQueues { get; set; }

    /// <summary>
    /// When set, request and subscription queues are declared with this <c>x-dead-letter-exchange</c>,
    /// so a delivery rejected without requeue (a failed handler, a malformed frame) is routed there
    /// instead of dropped. Declare the exchange yourself. Changing this on queues that already
    /// exist fails the declaration; the queue must be deleted or migrated first.
    /// </summary>
    public string? DeadLetterExchange { get; set; }
}
