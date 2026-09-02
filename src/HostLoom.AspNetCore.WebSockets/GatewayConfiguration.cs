using System.Diagnostics.CodeAnalysis;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class GatewayConfiguration(HostLoomWebSocketOptions options)
{
    private readonly Dictionary<string, RequestRoute> _requests = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TopicRoute> _topics = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, TopicRoute> _topicsByEventType = [];

    public HostLoomWebSocketOptions Options { get; } = options;

    public void AddRequest(RequestRoute route)
    {
        ValidateName(route.Name, nameof(route.Name));
        if (!_requests.TryAdd(route.Name, route))
        {
            throw new InvalidOperationException(
                $"WebSocket operation '{route.Name}' is already registered."
            );
        }
    }

    public void AddTopic(TopicRoute route)
    {
        ValidateName(route.Name, nameof(route.Name));
        if (_topicsByEventType.ContainsKey(route.EventType))
        {
            throw new InvalidOperationException(
                $"Event type '{route.EventType}' is already exposed by a WebSocket topic. "
                    + "A HostLoom event handler does not receive its source topic, so one event type can map to only one public topic."
            );
        }

        if (!_topics.TryAdd(route.Name, route))
        {
            throw new InvalidOperationException(
                $"WebSocket topic '{route.Name}' is already registered."
            );
        }

        _topicsByEventType.Add(route.EventType, route);
    }

    public bool TryGetRequest(string name, [NotNullWhen(true)] out RequestRoute? route) =>
        _requests.TryGetValue(name, out route);

    public bool TryGetTopic(string name, [NotNullWhen(true)] out TopicRoute? route) =>
        _topics.TryGetValue(name, out route);

    public TopicRoute GetTopic(Type eventType) =>
        _topicsByEventType.TryGetValue(eventType, out var route)
            ? route
            : throw new InvalidOperationException(
                $"Event type '{eventType}' is not exposed by the WebSocket gateway."
            );

    public void AddTopicSnapshot(string topic, Type eventType, Type invokerType)
    {
        ValidateName(topic, nameof(topic));
        if (!_topics.TryGetValue(topic, out var route))
        {
            throw new InvalidOperationException(
                $"WebSocket topic '{topic}' must be registered before its snapshot provider."
            );
        }

        if (route.EventType != eventType)
        {
            throw new InvalidOperationException(
                $"WebSocket topic '{topic}' exposes event type '{route.EventType}', not '{eventType}'."
            );
        }

        if (route.SnapshotInvokerType is not null)
        {
            throw new InvalidOperationException(
                $"WebSocket topic '{topic}' already has a snapshot provider."
            );
        }

        route.SnapshotInvokerType = invokerType;
    }

    private static void ValidateName(string value, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        if (
            value.Length > 128
            || value.Any(static character =>
                !(char.IsAsciiLetterOrDigit(character) || character is '.' or '_' or '-')
            )
        )
        {
            throw new ArgumentException(
                "Public names must be at most 128 ASCII letters, digits, dots, underscores, or hyphens.",
                parameterName
            );
        }
    }
}

internal sealed record RequestRoute(
    string Name,
    RequestAddress Destination,
    Type InvokerType,
    string? AuthorizationPolicy
);

internal sealed record TopicRoute(
    string Name,
    Type EventType,
    Func<object, string?> KeySelector,
    string? AuthorizationPolicy
)
{
    public Type? SnapshotInvokerType { get; set; }
}
