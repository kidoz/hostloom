using System.Collections.ObjectModel;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>
/// Describes the registered WebSocket gateway without resolving application services, starting a
/// host, or contacting a transport.
/// </summary>
public sealed class WebSocketGatewayProbe
{
    private readonly GatewayConfiguration _configuration;

    internal WebSocketGatewayProbe(GatewayConfiguration configuration) =>
        _configuration = configuration;

    /// <summary>Returns an immutable snapshot of the current gateway composition.</summary>
    public WebSocketGatewayDescription Describe() => _configuration.Describe();
}

/// <summary>An immutable, execution-free snapshot of a WebSocket gateway composition.</summary>
/// <param name="RequireAuthenticatedUser">Whether the mapped endpoint requires authentication.</param>
/// <param name="IncludeRemoteFaultMessages">Whether downstream fault messages may reach clients.</param>
/// <param name="OriginMode">How browser-supplied Origin headers are validated.</param>
/// <param name="AllowMissingOrigin">Whether clients without an Origin header are accepted.</param>
/// <param name="AllowedOriginCount">The number of configured allowlist entries, without their values.</param>
/// <param name="Protocols">Preferred subprotocols in negotiation order.</param>
/// <param name="Requests">Registered public request operations.</param>
/// <param name="Topics">Registered public event topics.</param>
/// <param name="Decisions">
/// Composition-ledger-shaped values that an application may record when it references the optional
/// diagnostics package.
/// </param>
public sealed record WebSocketGatewayDescription(
    bool RequireAuthenticatedUser,
    bool IncludeRemoteFaultMessages,
    WebSocketOriginMode OriginMode,
    bool AllowMissingOrigin,
    int AllowedOriginCount,
    IReadOnlyList<string> Protocols,
    IReadOnlyList<WebSocketRequestDescription> Requests,
    IReadOnlyList<WebSocketTopicDescription> Topics,
    IReadOnlyList<WebSocketCompositionDecision> Decisions
)
{
    /// <summary>Preferred subprotocols in negotiation order.</summary>
    public IReadOnlyList<string> Protocols { get; } =
        new ReadOnlyCollection<string>([.. Protocols]);

    /// <summary>Registered public request operations, ordered by operation name.</summary>
    public IReadOnlyList<WebSocketRequestDescription> Requests { get; } =
        new ReadOnlyCollection<WebSocketRequestDescription>([.. Requests]);

    /// <summary>Registered public event topics, ordered by topic name.</summary>
    public IReadOnlyList<WebSocketTopicDescription> Topics { get; } =
        new ReadOnlyCollection<WebSocketTopicDescription>([.. Topics]);

    /// <summary>Values suitable for explicit recording in an optional composition ledger.</summary>
    public IReadOnlyList<WebSocketCompositionDecision> Decisions { get; } =
        new ReadOnlyCollection<WebSocketCompositionDecision>([.. Decisions]);
}

/// <summary>A registered WebSocket request route.</summary>
/// <param name="Operation">The public client operation.</param>
/// <param name="Destination">The HostLoom request destination.</param>
/// <param name="RequestType">The registered request contract type.</param>
/// <param name="ResponseType">The registered response contract type.</param>
/// <param name="AuthorizationPolicy">The per-request policy, or null when none was configured.</param>
public sealed record WebSocketRequestDescription(
    string Operation,
    string Destination,
    string RequestType,
    string ResponseType,
    string? AuthorizationPolicy
);

/// <summary>A registered WebSocket event topic.</summary>
/// <param name="Topic">The public client topic.</param>
/// <param name="Source">The HostLoom event source.</param>
/// <param name="Subscription">The HostLoom subscription name.</param>
/// <param name="EventType">The registered event contract type.</param>
/// <param name="Keyed">Whether the registration supplied a topic key selector.</param>
/// <param name="AuthorizationPolicy">The per-subscription policy, or null when none was configured.</param>
/// <param name="SnapshotProvider">The snapshot provider type, or null when none was configured.</param>
public sealed record WebSocketTopicDescription(
    string Topic,
    string Source,
    string Subscription,
    string EventType,
    bool Keyed,
    string? AuthorizationPolicy,
    string? SnapshotProvider
);

/// <summary>
/// One gateway choice in the shape accepted by an optional HostLoom composition ledger. This
/// package deliberately does not reference that diagnostics package.
/// </summary>
/// <param name="Component">The stable component identity.</param>
/// <param name="Choice">The configured choice.</param>
/// <param name="Reason">The registration or option values that explain the choice.</param>
public sealed record WebSocketCompositionDecision(string Component, string Choice, string Reason);
