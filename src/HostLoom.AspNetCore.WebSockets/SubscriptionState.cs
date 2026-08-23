namespace HostLoom.AspNetCore.WebSockets;

internal sealed class SubscriptionState(
    ulong streamId,
    string topic,
    string? key,
    int initialCredit
)
{
    private int _credit = initialCredit;
    private long _lastAcknowledged;

    public ulong StreamId { get; } = streamId;

    public string Topic { get; } = topic;

    public string? Key { get; } = key;

    public long LastAcknowledged => Interlocked.Read(ref _lastAcknowledged);

    public bool TryConsumeCredit()
    {
        while (true)
        {
            var current = Volatile.Read(ref _credit);
            if (current <= 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _credit, current - 1, current) == current)
            {
                return true;
            }
        }
    }

    public bool TryAddCredit(int amount, int maximum)
    {
        if (amount <= 0)
        {
            return false;
        }

        while (true)
        {
            var current = Volatile.Read(ref _credit);
            if (amount > maximum - current)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _credit, current + amount, current) == current)
            {
                return true;
            }
        }
    }

    public void Acknowledge(long sequence)
    {
        while (true)
        {
            var current = Interlocked.Read(ref _lastAcknowledged);
            if (
                sequence <= current
                || Interlocked.CompareExchange(ref _lastAcknowledged, sequence, current) == current
            )
            {
                return;
            }
        }
    }
}
