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
/// dies between publishing and marking lets the next claim publish the message again after the
/// lease, which is what the inbox on the receiving side is for.
/// </remarks>
public interface IOutboxStore
{
    /// <summary>Stores <paramref name="message"/> as pending, inside the caller's unit of work when the store has one.</summary>
    ValueTask AppendAsync(OutboxMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Takes up to <paramref name="batchSize"/> of the oldest pending messages that are not under
    /// an unexpired lease, leases them for <paramref name="lease"/>, and returns them in enqueue
    /// order. An empty list means nothing is pending.
    /// </summary>
    ValueTask<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    );

    /// <summary>Removes or marks <paramref name="messageId"/> as published; it is never claimed again.</summary>
    ValueTask MarkPublishedAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that publishing <paramref name="messageId"/> failed with <paramref name="error"/>,
    /// increments its attempts, and releases its lease so a later claim retries it.
    /// </summary>
    ValueTask MarkFailedAsync(
        Guid messageId,
        string error,
        CancellationToken cancellationToken = default
    );
}
