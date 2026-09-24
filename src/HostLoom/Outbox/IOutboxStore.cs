namespace HostLoom;

/// <summary>
/// Durable storage behind the transactional outbox. An application implements it over the
/// database its handlers write to, so <see cref="AppendAsync"/> joins the caller's unit of work
/// and an event is stored if and only if the business change commits. The relay then claims,
/// publishes, and marks messages from a scope of its own.
/// </summary>
/// <remarks>
/// A store must make <see cref="ClaimAsync"/> atomic against concurrent relays: two instances
/// must not receive the same message inside one lease. Delivery is at-least-once: a relay that
/// dies between publishing and marking, or a <see cref="MarkPublishedAsync"/> that throws, lets
/// the next claim publish the message again after the lease, which is what the inbox on the
/// receiving side is for. A failed mark does not count as a failed attempt. A message the relay has given up
/// on is dead-lettered and never claimed again; an operator requeues it from the store.
/// </remarks>
public interface IOutboxStore
{
    /// <summary>Stores <paramref name="message"/> as pending, inside the caller's unit of work when the store has one.</summary>
    ValueTask AppendAsync(OutboxMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Takes up to <paramref name="batchSize"/> of the oldest pending messages that are not under
    /// an unexpired lease and whose <see cref="OutboxMessage.NextAttemptAt"/> is unset or has
    /// passed, leases them for <paramref name="lease"/>, and returns them in enqueue order.
    /// Dead-lettered messages are never returned. An empty list means nothing is due.
    /// </summary>
    ValueTask<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    );

    /// <summary>Removes or marks <paramref name="messageId"/> as published; it is never claimed again.</summary>
    ValueTask MarkPublishedAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that publishing <paramref name="messageId"/> failed with <paramref name="error"/>
    /// (the failure's type name, never its message), increments its attempts, releases its lease,
    /// and holds it back until <paramref name="nextAttemptAt"/>, when a later claim retries it.
    /// </summary>
    ValueTask MarkFailedAsync(
        Guid messageId,
        string error,
        DateTimeOffset nextAttemptAt,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Records that <paramref name="messageId"/> exhausted its attempts with <paramref name="error"/>
    /// and moves it to the dead-letter state, where no claim returns it. The store keeps the
    /// message for an operator to inspect and requeue.
    /// </summary>
    ValueTask MarkDeadLetteredAsync(
        Guid messageId,
        string error,
        CancellationToken cancellationToken = default
    );
}
