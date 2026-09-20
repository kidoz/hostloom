namespace HostLoom;

/// <summary>
/// A per-process <see cref="IOutboxStore"/> with real leases and backoff on a
/// <see cref="TimeProvider"/>, so tests and single-process deployments exercise the relay's state
/// machine without a database. It joins no transaction: an append is stored at once, whether or
/// not the caller's own write later commits, which is the guarantee a database-backed store adds.
/// </summary>
public sealed class InMemoryOutboxStore(TimeProvider? timeProvider = null) : IOutboxStore
{
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;
    private readonly Lock _gate = new();
    private readonly Dictionary<Guid, Entry> _entries = [];
    private readonly List<OutboxMessage> _published = [];
    private readonly List<OutboxMessage> _deadLettered = [];
    private long _sequence;

    /// <summary>Messages appended and neither published nor dead-lettered, oldest first, including those waiting out a backoff.</summary>
    public IReadOnlyList<OutboxMessage> Pending
    {
        get
        {
            lock (_gate)
            {
                return
                [
                    .. _entries
                        .Values.OrderBy(static entry => entry.Sequence)
                        .Select(static entry => entry.Message),
                ];
            }
        }
    }

    /// <summary>Messages marked published, in the order they were marked.</summary>
    public IReadOnlyList<OutboxMessage> Published
    {
        get
        {
            lock (_gate)
            {
                return [.. _published];
            }
        }
    }

    /// <summary>Messages that exhausted their attempts, in the order they were dead-lettered. No claim returns them.</summary>
    public IReadOnlyList<OutboxMessage> DeadLettered
    {
        get
        {
            lock (_gate)
            {
                return [.. _deadLettered];
            }
        }
    }

    /// <summary>The last error recorded for a message, or <see langword="null"/>.</summary>
    public string? LastError { get; private set; }

    /// <inheritdoc />
    public ValueTask AppendAsync(
        OutboxMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(message);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (!_entries.TryAdd(message.MessageId, new Entry(message, ++_sequence)))
            {
                throw new InvalidOperationException(
                    $"Outbox message '{message.MessageId}' is already pending."
                );
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchSize, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var now = _clock.GetUtcNow();
            List<OutboxMessage> claimed = [];
            foreach (var entry in _entries.Values.OrderBy(static entry => entry.Sequence))
            {
                if (entry.LeasedUntil > now || entry.Message.NextAttemptAt > now)
                {
                    continue;
                }

                entry.LeasedUntil = now + lease;
                claimed.Add(entry.Message);
                if (claimed.Count == batchSize)
                {
                    break;
                }
            }

            return ValueTask.FromResult<IReadOnlyList<OutboxMessage>>(claimed);
        }
    }

    /// <inheritdoc />
    public ValueTask MarkPublishedAsync(
        Guid messageId,
        CancellationToken cancellationToken = default
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_entries.Remove(messageId, out var entry))
            {
                _published.Add(entry.Message);
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask MarkFailedAsync(
        Guid messageId,
        string error,
        DateTimeOffset nextAttemptAt,
        CancellationToken cancellationToken = default
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_entries.TryGetValue(messageId, out var entry))
            {
                entry.Message = entry.Message with
                {
                    Attempts = entry.Message.Attempts + 1,
                    NextAttemptAt = nextAttemptAt,
                };
                entry.LeasedUntil = DateTimeOffset.MinValue;
                LastError = error;
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask MarkDeadLetteredAsync(
        Guid messageId,
        string error,
        CancellationToken cancellationToken = default
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_entries.Remove(messageId, out var entry))
            {
                _deadLettered.Add(
                    entry.Message with
                    {
                        Attempts = entry.Message.Attempts + 1,
                        NextAttemptAt = null,
                    }
                );
                LastError = error;
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Returns a dead-lettered message to the pending set with its attempts reset, the way an
    /// operator would after fixing what made it fail. Returns <see langword="false"/> when the
    /// message is not dead-lettered.
    /// </summary>
    public bool Requeue(Guid messageId)
    {
        lock (_gate)
        {
            var index = _deadLettered.FindIndex(message => message.MessageId == messageId);
            if (index < 0)
            {
                return false;
            }

            var message = _deadLettered[index] with { Attempts = 0, NextAttemptAt = null };
            _deadLettered.RemoveAt(index);
            _entries[messageId] = new Entry(message, ++_sequence);
            return true;
        }
    }

    private sealed class Entry(OutboxMessage message, long sequence)
    {
        public OutboxMessage Message { get; set; } = message;

        public long Sequence { get; } = sequence;

        public DateTimeOffset LeasedUntil { get; set; } = DateTimeOffset.MinValue;
    }
}
