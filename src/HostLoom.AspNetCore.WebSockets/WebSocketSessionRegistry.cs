using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

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

internal interface IWebSocketSessionHandle : IWebSocketEventSink
{
    WebSocketSessionInfo GetInfo();

    Task Completion { get; }

    void RequestDisconnect(WebSocketCloseStatus status, string reason);
}

internal sealed class WebSocketSessionRegistry
    : IWebSocketSessionDirectory,
        IWebSocketSessionControl
{
    private readonly ConcurrentDictionary<SubscriptionGroup, GroupState> _groups = [];
    private readonly ConcurrentDictionary<string, IWebSocketSessionHandle> _sessions = new(
        StringComparer.Ordinal
    );

    public int Count => _sessions.Count;

    public IReadOnlyList<WebSocketSessionInfo> GetSessions() =>
        [.. _sessions.Values.Select(static session => session.GetInfo())];

    public IReadOnlyList<WebSocketSessionInfo> GetSessionsBySubject(string subject)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        return
        [
            .. _sessions
                .Values.Select(static session => session.GetInfo())
                .Where(info => string.Equals(info.Subject, subject, StringComparison.Ordinal)),
        ];
    }

    public async ValueTask<bool> DisconnectAsync(
        string sessionId,
        string reason,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        ValidateReason(reason);
        if (!_sessions.TryGetValue(sessionId, out var session))
        {
            return false;
        }

        session.RequestDisconnect(WebSocketCloseStatus.PolicyViolation, reason);
        await session.Completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async ValueTask<int> DisconnectSubjectAsync(
        string subject,
        string reason,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ValidateReason(reason);
        var sessions = _sessions
            .Values.Where(session =>
                string.Equals(session.GetInfo().Subject, subject, StringComparison.Ordinal)
            )
            .ToArray();
        foreach (var session in sessions)
        {
            session.RequestDisconnect(WebSocketCloseStatus.PolicyViolation, reason);
        }

        await Task.WhenAll(sessions.Select(static session => session.Completion))
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        return sessions.Length;
    }

    internal void Register(IWebSocketSessionHandle session)
    {
        if (!_sessions.TryAdd(session.Id, session))
        {
            throw new InvalidOperationException($"Session '{session.Id}' is already registered.");
        }
    }

    internal void Unregister(IWebSocketSessionHandle session) =>
        _sessions.TryRemove(new KeyValuePair<string, IWebSocketSessionHandle>(session.Id, session));

    internal async Task DisconnectAllAsync(
        WebSocketCloseStatus status,
        string reason,
        CancellationToken cancellationToken
    )
    {
        ValidateReason(reason);
        var sessions = _sessions.Values.ToArray();
        foreach (var session in sessions)
        {
            session.RequestDisconnect(status, reason);
        }

        await Task.WhenAll(sessions.Select(static session => session.Completion))
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

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

    private static void ValidateReason(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        if (Encoding.UTF8.GetByteCount(reason) > 123)
        {
            throw new ArgumentException(
                "A WebSocket close reason must be at most 123 UTF-8 bytes.",
                nameof(reason)
            );
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
