using System.Collections.Concurrent;

namespace HostLoom.AspNetCore.WebSockets;

internal interface IWebSocketEventSink
{
    string Id { get; }

    bool TryQueueEvent(
        string topic,
        string? subscriptionKey,
        string? eventKey,
        ReadOnlyMemory<byte> payload,
        string eventId,
        long sequence
    );

    void Abort();
}

internal sealed class WebSocketSessionRegistry
{
    private readonly ConcurrentDictionary<SubscriptionGroup, GroupState> _groups = [];

    public void Subscribe(IWebSocketEventSink session, string topic, string? key)
    {
        var group = _groups.GetOrAdd(
            new SubscriptionGroup(topic, key),
            static _ => new GroupState()
        );
        group.Sessions[session.Id] = session;
    }

    public void Unsubscribe(IWebSocketEventSink session, string topic, string? key)
    {
        var groupKey = new SubscriptionGroup(topic, key);
        if (_groups.TryGetValue(groupKey, out var group))
        {
            group.Sessions.TryRemove(session.Id, out _);
            if (group.Sessions.IsEmpty)
            {
                _groups.TryRemove(new KeyValuePair<SubscriptionGroup, GroupState>(groupKey, group));
            }
        }
    }

    public void Publish(string topic, string? key, ReadOnlyMemory<byte> payload)
    {
        var eventId = Guid.NewGuid().ToString("N");
        PublishGroup(new SubscriptionGroup(topic, null), topic, key, payload, eventId);
        if (key is not null)
        {
            PublishGroup(new SubscriptionGroup(topic, key), topic, key, payload, eventId);
        }
    }

    private void PublishGroup(
        SubscriptionGroup groupKey,
        string topic,
        string? eventKey,
        ReadOnlyMemory<byte> payload,
        string eventId
    )
    {
        if (!_groups.TryGetValue(groupKey, out var group))
        {
            return;
        }

        var sequence = Interlocked.Increment(ref group.Sequence);
        foreach (var session in group.Sessions.Values)
        {
            if (!session.TryQueueEvent(topic, groupKey.Key, eventKey, payload, eventId, sequence))
            {
                session.Abort();
            }
        }
    }

    private readonly record struct SubscriptionGroup(string Topic, string? Key);

    private sealed class GroupState
    {
        public ConcurrentDictionary<string, IWebSocketEventSink> Sessions { get; } =
            new(StringComparer.Ordinal);

        public long Sequence;
    }
}
