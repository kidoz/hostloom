namespace HostLoom.Transport.RabbitMq;

public sealed class RabbitMqOptions
{
    public Uri Uri { get; set; } = new("amqp://guest:guest@localhost:5672/");

    public string ClientProvidedName { get; set; } =
        $"hostloom-{Environment.MachineName}-{Environment.ProcessId}";

    /// <summary>Physical queue identity scheme. Version2 isolates request and event routes. Legacy requires coordinated migration and retains ambiguous dotted subscription names.</summary>
    public RabbitMqQueueNaming QueueNaming { get; set; } = RabbitMqQueueNaming.Version2;

    /// <summary>Maximum concurrent confirmed publications, each with exclusive channel ownership.</summary>
    public int MaxConcurrentPublishes { get; set; } = 16;

    /// <summary>Bounds event publication, including channel acquisition and confirmation.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public ushort PrefetchCount { get; set; } = 16;

    /// <summary>
    /// Deliveries a request listener handles concurrently on its channel, between 1 and
    /// <see cref="PrefetchCount"/>. Requests are independent, so they run in parallel by default;
    /// bound the handler itself with a receive-pipeline concurrency limit when it needs one.
    /// </summary>
    public ushort RequestDispatchConcurrency { get; set; } = 16;

    /// <summary>
    /// Deliveries an event subscription handles concurrently on its channel, between 1 and
    /// <see cref="PrefetchCount"/>. The default of 1 keeps a subscription's events in queue
    /// order; any higher value gives that up, and the handlers must then be safe to overlap.
    /// </summary>
    public ushort EventDispatchConcurrency { get; set; } = 1;

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
