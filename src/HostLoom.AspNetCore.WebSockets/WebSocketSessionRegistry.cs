using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

namespace HostLoom.AspNetCore.WebSockets;

internal interface IWebSocketEventSink
{
    Guid Id { get; }

    bool TryQueueEvent(
        string topic,
        string? subscriptionKey,
        string? eventKey,
        ReadOnlyMemory<byte> payload,
        Guid eventId,
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
    private readonly Lock _groupsGate = new();
    private readonly ConcurrentDictionary<Guid, IWebSocketSessionHandle> _sessions = [];

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
        Guid sessionId,
        string reason,
        CancellationToken cancellationToken = default
    )
    {
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
        _sessions.TryRemove(new KeyValuePair<Guid, IWebSocketSessionHandle>(session.Id, session));

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
        lock (_groupsGate)
        {
            var group = _groups.GetOrAdd(
                new SubscriptionGroup(topic, key),
                static _ => new GroupState()
            );
            if (group.Sessions.TryGetValue(session.Id, out var membership))
            {
                membership.AddReference();
            }
            else
            {
                group.Sessions[session.Id] = new SessionMembership(session);
            }
        }
    }

    public void Unsubscribe(IWebSocketEventSink session, string topic, string? key)
    {
        var groupKey = new SubscriptionGroup(topic, key);
        lock (_groupsGate)
        {
            if (!_groups.TryGetValue(groupKey, out var group))
            {
                return;
            }

            if (
                group.Sessions.TryGetValue(session.Id, out var membership)
                && membership.ReleaseReference()
            )
            {
                group.Sessions.TryRemove(
                    new KeyValuePair<Guid, SessionMembership>(session.Id, membership)
                );
            }

            if (group.Sessions.IsEmpty)
            {
                _groups.TryRemove(new KeyValuePair<SubscriptionGroup, GroupState>(groupKey, group));
            }
        }
    }

    public void Publish(string topic, string? key, ReadOnlyMemory<byte> payload)
    {
        var eventId = Guid.NewGuid();
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
        Guid eventId
    )
    {
        if (!_groups.TryGetValue(groupKey, out var group))
        {
            return;
        }

        var sequence = Interlocked.Increment(ref group.Sequence);
        foreach (var membership in group.Sessions.Values)
        {
            var session = membership.Session;
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
        public ConcurrentDictionary<Guid, SessionMembership> Sessions { get; } = [];

        public long Sequence;
    }

    private sealed class SessionMembership(IWebSocketEventSink session)
    {
        private int _references = 1;

        public IWebSocketEventSink Session { get; } = session;

        public void AddReference() => _references++;

        public bool ReleaseReference() => --_references == 0;
    }
}
