using Microsoft.Extensions.Logging;

namespace HostLoom;

/// <summary>Stable event ids for every log line the outbox relay writes.</summary>
public static class OutboxEvents
{
    /// <summary>Information: the relay started with its poll interval, batch size, and lease.</summary>
    public static readonly EventId RelayStarted = new(3300, "OutboxRelayStarted");

    /// <summary>Warning: publishing one message failed; it stays pending for a later claim after its backoff.</summary>
    public static readonly EventId PublishFailed = new(3301, "OutboxPublishFailed");

    /// <summary>Warning: the store failed during a drain; the relay waits for the next interval.</summary>
    public static readonly EventId StoreFailed = new(3302, "OutboxStoreFailed");

    /// <summary>Information: the relay stopped.</summary>
    public static readonly EventId RelayStopped = new(3303, "OutboxRelayStopped");

    /// <summary>Error: a message exhausted <c>Outbox:MaxAttempts</c> and was dead-lettered; no claim returns it again.</summary>
    public static readonly EventId DeadLettered = new(3304, "OutboxDeadLettered");

    /// <summary>
    /// Warning: a message was published but the store failed to mark it. No attempt is counted;
    /// the claim lease expires and the next claim publishes the message again.
    /// </summary>
    public static readonly EventId MarkPublishedFailed = new(3305, "OutboxMarkPublishedFailed");
}
