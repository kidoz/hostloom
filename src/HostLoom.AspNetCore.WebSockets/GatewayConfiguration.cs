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

    public void AddTopicSnapshot(
        string topic,
        Type eventType,
        Type invokerType,
        Type snapshotProviderType
    )
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
        route.SnapshotProviderType = snapshotProviderType;
    }

    public WebSocketGatewayDescription Describe()
    {
        var requests = _requests
            .Values.OrderBy(static route => route.Name, StringComparer.Ordinal)
            .Select(static route => new WebSocketRequestDescription(
                route.Name,
                route.Destination.Value,
                TypeName(route.RequestType),
                TypeName(route.ResponseType),
                route.AuthorizationPolicy
            ))
            .ToArray();
        var topics = _topics
            .Values.OrderBy(static route => route.Name, StringComparer.Ordinal)
            .Select(static route => new WebSocketTopicDescription(
                route.Name,
                route.Source.Value,
                route.Subscription,
                TypeName(route.EventType),
                route.Keyed,
                route.AllowTopicWideSubscription,
                route.AuthorizationPolicy,
                route.SnapshotProviderType is null ? null : TypeName(route.SnapshotProviderType)
            ))
            .ToArray();

        var decisions = new List<WebSocketCompositionDecision>(2 + topics.Length)
        {
            new(
                "WebSockets:Gateway",
                "Enabled",
                $"registered requests={requests.Length}, topics={topics.Length}; "
                    + $"WebSockets:RequireAuthenticatedUser={Options.RequireAuthenticatedUser}; "
                    + $"WebSockets:IncludeRemoteFaultMessages={Options.IncludeRemoteFaultMessages}"
            ),
            new(
                "WebSockets:Origins",
                Options.OriginMode.ToString(),
                $"WebSockets:OriginMode={Options.OriginMode}; "
                    + $"WebSockets:AllowMissingOrigin={Options.AllowMissingOrigin}; "
                    + $"WebSockets:AllowedOrigins.Count={Options.AllowedOrigins.Count}"
            ),
        };
        decisions.AddRange(
            topics.Select(static topic => new WebSocketCompositionDecision(
                $"WebSockets:Topic:{topic.Topic}",
                $"{topic.Source} via {topic.Subscription}",
                $"AddTopic registered event={topic.EventType}; keyed={topic.Keyed}; "
                    + $"topicWide={topic.AllowTopicWideSubscription}; "
                    + $"policy={topic.AuthorizationPolicy ?? "(none)"}; "
                    + $"snapshot={topic.SnapshotProvider ?? "(none)"}"
            ))
        );

        return new WebSocketGatewayDescription(
            Options.RequireAuthenticatedUser,
            Options.IncludeRemoteFaultMessages,
            Options.OriginMode,
            Options.AllowMissingOrigin,
            Options.AllowedOrigins.Count,
            Options.ProtocolPreference.ToArray(),
            requests,
            topics,
            decisions
        );
    }

    /// <summary>
    /// Every distinct policy name a route references, with the routes that use it, so startup can
    /// prove each one resolves before a client can trigger the lookup.
    /// </summary>
    public IReadOnlyList<AuthorizationPolicyUsage> GetAuthorizationPolicyUsages()
    {
        var usages = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var route in _requests.Values)
        {
            if (route.AuthorizationPolicy is { } policy)
            {
                Add(usages, policy, $"request '{route.Name}'");
            }
        }

        foreach (var route in _topics.Values)
        {
            if (route.AuthorizationPolicy is { } policy)
            {
                Add(usages, policy, $"topic '{route.Name}'");
            }
        }

        return
        [
            .. usages
                .OrderBy(static usage => usage.Key, StringComparer.Ordinal)
                .Select(static usage => new AuthorizationPolicyUsage(
                    usage.Key,
                    usage.Value.OrderBy(static route => route, StringComparer.Ordinal).ToArray()
                )),
        ];

        static void Add(Dictionary<string, List<string>> usages, string policy, string route)
        {
            if (!usages.TryGetValue(policy, out var routes))
            {
                routes = [];
                usages.Add(policy, routes);
            }

            routes.Add(route);
        }
    }

    private static string TypeName(Type type) => type.FullName ?? type.Name;

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
    Type RequestType,
    Type ResponseType,
    Type InvokerType,
    string? AuthorizationPolicy
);

/// <param name="AllowTopicWideSubscription">
/// Whether a subscribe without a key is accepted. A keyless registration is always topic-wide; a
/// keyed registration must opt in, because a keyless subscriber to a keyed topic receives every
/// key's events regardless of what the policy approved for one key.
/// </param>
internal sealed record TopicRoute(
    string Name,
    RequestAddress Source,
    string Subscription,
    Type EventType,
    bool Keyed,
    bool AllowTopicWideSubscription,
    Func<object, string?> KeySelector,
    string? AuthorizationPolicy
)
{
    public Type? SnapshotInvokerType { get; set; }

    public Type? SnapshotProviderType { get; set; }
}

/// <summary>One authorization policy name and the public routes that reference it.</summary>
internal sealed record AuthorizationPolicyUsage(string Policy, IReadOnlyList<string> Routes);
